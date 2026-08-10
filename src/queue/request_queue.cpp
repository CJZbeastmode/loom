#include "loom/queue/request_queue.h"

#ifdef LOOM_HAS_SQLITE
#include <sqlite3.h>
#endif

namespace loom {
namespace queue {

struct RequestQueue::Impl {
    std::string db_path;
    std::string dag_run_id;
    FlushPolicy flush_policy;
};

RequestQueue::RequestQueue(const std::string& db_path, const std::string& dag_run_id)
    : impl_(std::make_unique<Impl>()) {
    impl_->db_path = db_path;
    impl_->dag_run_id = dag_run_id;
}

RequestQueue::~RequestQueue() = default;

void RequestQueue::enqueue(const std::vector<RequestRecord>& /*records*/) {}

std::optional<RequestRecord> RequestQueue::dequeue() {
    return std::nullopt;
}

void RequestQueue::mark_inflight(const std::string& /*request_id*/) {}

void RequestQueue::mark_done(const std::string& /*request_id*/, int /*response_status*/, const std::string& /*response_body*/) {}

void RequestQueue::mark_failed(const std::string& /*request_id*/, const std::string& /*error*/) {}

void RequestQueue::mark_skipped(const std::string& /*request_id*/) {}

bool RequestQueue::is_exhausted() const {
    return true;
}

int RequestQueue::pending_count() const { return 0; }
int RequestQueue::inflight_count() const { return 0; }
int RequestQueue::done_count() const { return 0; }

void RequestQueue::set_flush_policy(const FlushPolicy& /*policy*/) {}

void RequestQueue::flush_completed() {}

void RequestQueue::resume() {}

}  // namespace queue
}  // namespace loom
