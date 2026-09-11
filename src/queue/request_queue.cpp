#include "loom/queue/request_queue.h"

#include <stdexcept>
#include <mutex>
#include <ctime>
#include <chrono>
#include <atomic>
#include <algorithm>

#ifdef LOOM_HAS_SQLITE
#include <sqlite3.h>
#endif

namespace loom {
namespace queue {

namespace {

std::string now_iso() {
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
    return buf;
}

const char* status_to_str(RequestStatus s) {
    switch (s) {
        case RequestStatus::Pending:  return "pending";
        case RequestStatus::Inflight: return "inflight";
        case RequestStatus::Done:     return "done";
        case RequestStatus::Failed:   return "failed";
        case RequestStatus::Skipped:  return "skipped";
    }
    return "pending";
}

RequestStatus str_to_status(const char* s) {
    if (!s) return RequestStatus::Pending;
    std::string v(s);
    if (v == "inflight") return RequestStatus::Inflight;
    if (v == "done")     return RequestStatus::Done;
    if (v == "failed")   return RequestStatus::Failed;
    if (v == "skipped")  return RequestStatus::Skipped;
    return RequestStatus::Pending;
}

std::string make_request_id() {
    static std::atomic<int64_t> counter{0};
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();
    return std::to_string(ns) + "-" + std::to_string(counter.fetch_add(1));
}

#ifdef LOOM_HAS_SQLITE

// return code check 
void check_rc(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE) {
        throw std::runtime_error(std::string(what) + ": " +
                                 (db ? sqlite3_errmsg(db) : "sqlite error"));
    }
}

void exec(sqlite3* db, const char* sql) {
    char* errmsg = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errmsg);
    if (rc != SQLITE_OK) {
        std::string msg = errmsg ? errmsg : sqlite3_errmsg(db);
        sqlite3_free(errmsg);
        throw std::runtime_error(std::string("sqlite exec failed: ") + msg);
    }
}

const char* kSchema = R"SQL(
CREATE TABLE IF NOT EXISTS request_queue (
    request_id      TEXT PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    step_id         TEXT NOT NULL,
    method          TEXT DEFAULT 'GET',
    url             TEXT NOT NULL,
    headers         TEXT DEFAULT '{}',
    body            TEXT,
    status          TEXT DEFAULT 'pending',
    retry_count     INT DEFAULT 0,
    created_at      TEXT,
    started_at      TEXT,
    completed_at    TEXT,
    response_status INT,
    response_body   TEXT,
    result_path     TEXT
);
CREATE INDEX IF NOT EXISTS idx_request_queue_status
    ON request_queue(dag_run_id, status);
)SQL";

#endif  // LOOM_HAS_SQLITE

}  // namespace

struct RequestQueue::Impl {
    std::string db_path;
    std::string dag_run_id;
    FlushPolicy flush_policy;

#ifdef LOOM_HAS_SQLITE
    sqlite3* db = nullptr;
    sqlite3_stmt* stmt_insert = nullptr;
    sqlite3_stmt* stmt_select_pending = nullptr;
    sqlite3_stmt* stmt_transition = nullptr;
    sqlite3_stmt* stmt_count = nullptr;
#endif

    // Serializes all queue operations. The SQLite connection and prepared
    // statements are shared across threads (scheduler worker + HTTP loop).
    mutable std::mutex mutex;

    // In-memory fallback (used when compiled without SQLite).
    std::vector<RequestRecord> records;
};

RequestQueue::RequestQueue(const std::string& db_path, const std::string& dag_run_id)
    : impl_(std::make_unique<Impl>()) {
    impl_->db_path = db_path;
    impl_->dag_run_id = dag_run_id;

#ifdef LOOM_HAS_SQLITE
    check_rc(sqlite3_open(db_path.c_str(), &impl_->db), impl_->db, "sqlite3_open");
    exec(impl_->db, "PRAGMA journal_mode=WAL;");
    exec(impl_->db, kSchema);

    const char* insert_sql = R"SQL(
        INSERT INTO request_queue
            (request_id, dag_run_id, step_id, method, url, headers, body,
             status, retry_count, created_at, started_at, completed_at,
             response_status, response_body, result_path)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    )SQL";
    check_rc(sqlite3_prepare_v2(impl_->db, insert_sql, -1, &impl_->stmt_insert, nullptr),
             impl_->db, "prepare insert");

    const char* select_sql = R"SQL(
        SELECT request_id, dag_run_id, step_id, method, url, headers, body,
               status, retry_count, created_at, started_at, completed_at,
               response_status, response_body, result_path
        FROM request_queue
        WHERE dag_run_id = ? AND status = 'pending'
        ORDER BY rowid ASC LIMIT 1;
    )SQL";
    check_rc(sqlite3_prepare_v2(impl_->db, select_sql, -1, &impl_->stmt_select_pending, nullptr),
             impl_->db, "prepare select pending");

    const char* transition_sql = R"SQL(
        UPDATE request_queue
        SET status = ?,
            started_at = COALESCE(?, started_at),
            completed_at = COALESCE(?, completed_at),
            response_status = COALESCE(?, response_status),
            response_body = COALESCE(?, response_body),
            retry_count = retry_count + ?
        WHERE request_id = ? AND dag_run_id = ?;
    )SQL";
    check_rc(sqlite3_prepare_v2(impl_->db, transition_sql, -1, &impl_->stmt_transition, nullptr),
             impl_->db, "prepare transition");

    const char* count_sql = R"SQL(
        SELECT status, COUNT(*) FROM request_queue WHERE dag_run_id = ? GROUP BY status;
    )SQL";
    check_rc(sqlite3_prepare_v2(impl_->db, count_sql, -1, &impl_->stmt_count, nullptr),
             impl_->db, "prepare count");

    // Durability: any requests left inflight by a previous run are resumable.
    resume();
#endif
}

RequestQueue::~RequestQueue() {
#ifdef LOOM_HAS_SQLITE
    if (impl_->stmt_insert)         sqlite3_finalize(impl_->stmt_insert);
    if (impl_->stmt_select_pending) sqlite3_finalize(impl_->stmt_select_pending);
    if (impl_->stmt_transition)     sqlite3_finalize(impl_->stmt_transition);
    if (impl_->stmt_count)          sqlite3_finalize(impl_->stmt_count);
    if (impl_->db)                  sqlite3_close(impl_->db);
#endif
}

void RequestQueue::enqueue(const std::vector<RequestRecord>& records) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    exec(impl_->db, "BEGIN;");
    for (const auto& r : records) {
        std::string id = r.request_id.empty() ? make_request_id() : r.request_id;
        sqlite3_stmt* s = impl_->stmt_insert;
        sqlite3_reset(s);
        sqlite3_bind_text(s, 1, id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 2, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 3, r.step_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 4, r.method.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 5, r.url.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 6, r.headers.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 7, r.body.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(s, 8, "pending", -1, SQLITE_STATIC);
        sqlite3_bind_int(s, 9, r.retry_count);
        sqlite3_bind_text(s, 10, r.created_at.empty() ? now_iso().c_str() : r.created_at.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_null(s, 11);
        sqlite3_bind_null(s, 12);
        sqlite3_bind_null(s, 13);
        sqlite3_bind_null(s, 14);
        sqlite3_bind_text(s, 15, r.result_path.empty() ? nullptr : r.result_path.c_str(), -1, SQLITE_TRANSIENT);
        check_rc(sqlite3_step(s), impl_->db, "insert");
    }
    exec(impl_->db, "COMMIT;");
#else
    for (const auto& r : records) {
        RequestRecord rec = r;
        if (rec.request_id.empty()) rec.request_id = make_request_id();
        rec.dag_run_id = impl_->dag_run_id;
        rec.status = RequestStatus::Pending;
        if (rec.created_at.empty()) rec.created_at = now_iso();
        impl_->records.push_back(std::move(rec));
    }
#endif
}

std::optional<RequestRecord> RequestQueue::dequeue() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    exec(impl_->db, "BEGIN IMMEDIATE;");

    sqlite3_stmt* s = impl_->stmt_select_pending;
    sqlite3_reset(s);
    sqlite3_bind_text(s, 1, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

    std::optional<RequestRecord> result;
    if (sqlite3_step(s) == SQLITE_ROW) {
        RequestRecord r;
        r.request_id = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
        r.dag_run_id = reinterpret_cast<const char*>(sqlite3_column_text(s, 1));
        r.step_id    = reinterpret_cast<const char*>(sqlite3_column_text(s, 2));
        r.method     = reinterpret_cast<const char*>(sqlite3_column_text(s, 3));
        r.url        = reinterpret_cast<const char*>(sqlite3_column_text(s, 4));
        r.headers    = reinterpret_cast<const char*>(sqlite3_column_text(s, 5));
        if (sqlite3_column_text(s, 6)) r.body = reinterpret_cast<const char*>(sqlite3_column_text(s, 6));
        r.status     = str_to_status(reinterpret_cast<const char*>(sqlite3_column_text(s, 7)));
        r.retry_count = sqlite3_column_int(s, 8);
        if (sqlite3_column_text(s, 9))  r.created_at = reinterpret_cast<const char*>(sqlite3_column_text(s, 9));
        if (sqlite3_column_text(s, 10)) r.started_at = reinterpret_cast<const char*>(sqlite3_column_text(s, 10));
        if (sqlite3_column_text(s, 11)) r.completed_at = reinterpret_cast<const char*>(sqlite3_column_text(s, 11));
        r.response_status = sqlite3_column_int(s, 12);
        if (sqlite3_column_text(s, 13)) r.response_body = reinterpret_cast<const char*>(sqlite3_column_text(s, 13));
        if (sqlite3_column_text(s, 14)) r.result_path = reinterpret_cast<const char*>(sqlite3_column_text(s, 14));

        // Transition to inflight as part of the same transaction.
        sqlite3_stmt* t = impl_->stmt_transition;
        sqlite3_reset(t);
        sqlite3_bind_text(t, 1, "inflight", -1, SQLITE_STATIC);
        sqlite3_bind_text(t, 2, now_iso().c_str(), -1, SQLITE_TRANSIENT);  // started_at
        sqlite3_bind_null(t, 3);
        sqlite3_bind_null(t, 4);
        sqlite3_bind_null(t, 5);
        sqlite3_bind_int(t, 6, 0);
        sqlite3_bind_text(t, 7, r.request_id.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(t, 8, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
        check_rc(sqlite3_step(t), impl_->db, "dequeue transition");

        r.status = RequestStatus::Inflight;
        result = std::move(r);
    }
    sqlite3_reset(s);

    exec(impl_->db, "COMMIT;");
    return result;
#else
    for (auto& r : impl_->records) {
        if (r.status == RequestStatus::Pending) {
            r.status = RequestStatus::Inflight;
            r.started_at = now_iso();
            return r;
        }
    }
    return std::nullopt;
#endif
}

void RequestQueue::mark_inflight(const std::string& request_id) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* t = impl_->stmt_transition;
    sqlite3_reset(t);
    sqlite3_bind_text(t, 1, "inflight", -1, SQLITE_STATIC);
    sqlite3_bind_text(t, 2, now_iso().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_null(t, 3);
    sqlite3_bind_null(t, 4);
    sqlite3_bind_null(t, 5);
    sqlite3_bind_int(t, 6, 0);
    sqlite3_bind_text(t, 7, request_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(t, 8, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    check_rc(sqlite3_step(t), impl_->db, "mark_inflight");
#else
    for (auto& r : impl_->records) {
        if (r.request_id == request_id) { r.status = RequestStatus::Inflight; r.started_at = now_iso(); return; }
    }
#endif
}

void RequestQueue::mark_done(const std::string& request_id, int response_status,
                             const std::string& response_body) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* t = impl_->stmt_transition;
    sqlite3_reset(t);
    sqlite3_bind_text(t, 1, "done", -1, SQLITE_STATIC);
    sqlite3_bind_null(t, 2);
    sqlite3_bind_text(t, 3, now_iso().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(t, 4, response_status);
    sqlite3_bind_text(t, 5, response_body.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(t, 6, 0);
    sqlite3_bind_text(t, 7, request_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(t, 8, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    check_rc(sqlite3_step(t), impl_->db, "mark_done");
#else
    for (auto& r : impl_->records) {
        if (r.request_id == request_id) {
            r.status = RequestStatus::Done;
            r.completed_at = now_iso();
            r.response_status = response_status;
            r.response_body = response_body;
            return;
        }
    }
#endif
}

void RequestQueue::mark_failed(const std::string& request_id, const std::string& error) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* t = impl_->stmt_transition;
    sqlite3_reset(t);
    sqlite3_bind_text(t, 1, "failed", -1, SQLITE_STATIC);
    sqlite3_bind_null(t, 2);
    sqlite3_bind_text(t, 3, now_iso().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_null(t, 4);
    sqlite3_bind_text(t, 5, error.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(t, 6, 0);
    sqlite3_bind_text(t, 7, request_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(t, 8, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    check_rc(sqlite3_step(t), impl_->db, "mark_failed");
#else
    for (auto& r : impl_->records) {
        if (r.request_id == request_id) {
            r.status = RequestStatus::Failed;
            r.completed_at = now_iso();
            r.response_body = error;
            return;
        }
    }
#endif
}

void RequestQueue::mark_skipped(const std::string& request_id) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* t = impl_->stmt_transition;
    sqlite3_reset(t);
    sqlite3_bind_text(t, 1, "skipped", -1, SQLITE_STATIC);
    sqlite3_bind_null(t, 2);
    sqlite3_bind_text(t, 3, now_iso().c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_null(t, 4);
    sqlite3_bind_null(t, 5);
    sqlite3_bind_int(t, 6, 0);
    sqlite3_bind_text(t, 7, request_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(t, 8, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    check_rc(sqlite3_step(t), impl_->db, "mark_skipped");
#else
    for (auto& r : impl_->records) {
        if (r.request_id == request_id) { r.status = RequestStatus::Skipped; r.completed_at = now_iso(); return; }
    }
#endif
}

bool RequestQueue::is_exhausted() const {
    return pending_count() == 0 && inflight_count() == 0;
}

int RequestQueue::pending_count() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* s = impl_->stmt_count;
    sqlite3_reset(s);
    sqlite3_bind_text(s, 1, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    int pending = 0;
    while (sqlite3_step(s) == SQLITE_ROW) {
        const char* status = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
        if (std::string(status) == "pending") pending = sqlite3_column_int(s, 1);
    }
    sqlite3_reset(s);
    return pending;
#else
    return static_cast<int>(std::count_if(impl_->records.begin(), impl_->records.end(),
        [](const RequestRecord& r) { return r.status == RequestStatus::Pending; }));
#endif
}

int RequestQueue::inflight_count() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* s = impl_->stmt_count;
    sqlite3_reset(s);
    sqlite3_bind_text(s, 1, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    int inflight = 0;
    while (sqlite3_step(s) == SQLITE_ROW) {
        const char* status = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
        if (std::string(status) == "inflight") inflight = sqlite3_column_int(s, 1);
    }
    sqlite3_reset(s);
    return inflight;
#else
    return static_cast<int>(std::count_if(impl_->records.begin(), impl_->records.end(),
        [](const RequestRecord& r) { return r.status == RequestStatus::Inflight; }));
#endif
}

int RequestQueue::done_count() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* s = impl_->stmt_count;
    sqlite3_reset(s);
    sqlite3_bind_text(s, 1, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    int done = 0;
    while (sqlite3_step(s) == SQLITE_ROW) {
        const char* status = reinterpret_cast<const char*>(sqlite3_column_text(s, 0));
        if (std::string(status) == "done") done = sqlite3_column_int(s, 1);
    }
    sqlite3_reset(s);
    return done;
#else
    return static_cast<int>(std::count_if(impl_->records.begin(), impl_->records.end(),
        [](const RequestRecord& r) { return r.status == RequestStatus::Done; }));
#endif
}

void RequestQueue::set_flush_policy(const FlushPolicy& policy) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->flush_policy = policy;
}

void RequestQueue::flush_completed() {
    // Archive/retention handled in a later sprint (S3 connector not yet wired).
}

void RequestQueue::resume() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    // Reset any rows stuck in 'inflight' (e.g. after a crash) back to 'pending'.
#ifdef LOOM_HAS_SQLITE
    sqlite3_stmt* stmt = nullptr;
    const char* sql = "UPDATE request_queue SET status='pending', started_at=NULL "
                      "WHERE status='inflight' AND dag_run_id=?;";
    check_rc(sqlite3_prepare_v2(impl_->db, sql, -1, &stmt, nullptr), impl_->db, "prepare resume");
    sqlite3_bind_text(stmt, 1, impl_->dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    check_rc(sqlite3_step(stmt), impl_->db, "resume");
    sqlite3_finalize(stmt);
#else
    for (auto& r : impl_->records) {
        if (r.status == RequestStatus::Inflight) {
            r.status = RequestStatus::Pending;
            r.started_at.clear();
        }
    }
#endif
}

}  // namespace queue
}  // namespace loom
