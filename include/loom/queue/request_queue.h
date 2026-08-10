#pragma once

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <optional>

namespace loom {
namespace queue {

enum class RequestStatus {
    Pending,
    Inflight,
    Done,
    Failed,
    Skipped,
};

struct RequestRecord {
    std::string request_id;
    std::string dag_run_id;
    std::string step_id;
    std::string method = "GET";
    std::string url;
    std::string headers;
    std::string body;
    RequestStatus status = RequestStatus::Pending;
    int retry_count = 0;
    std::string created_at;
    std::string started_at;
    std::string completed_at;
    int response_status = 0;
    std::string response_body;
    std::string result_path;
};

struct FlushPolicy {
    std::string trigger;           // "batch_complete", "buffer_size:1000", "interval:60s"
    std::string archive_connection;
    std::string archive_format;    // "json.gz", "parquet", "json"
    int retention_days = 14;
    bool keep_failed = true;
};

class RequestQueue {
public:
    explicit RequestQueue(const std::string& db_path, const std::string& dag_run_id);
    ~RequestQueue();

    void enqueue(const std::vector<RequestRecord>& records);
    std::optional<RequestRecord> dequeue();
    void mark_inflight(const std::string& request_id);
    void mark_done(const std::string& request_id, int response_status, const std::string& response_body);
    void mark_failed(const std::string& request_id, const std::string& error);
    void mark_skipped(const std::string& request_id);
    bool is_exhausted() const;
    int pending_count() const;
    int inflight_count() const;
    int done_count() const;

    void set_flush_policy(const FlushPolicy& policy);
    void flush_completed();
    void resume();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace queue
}  // namespace loom
