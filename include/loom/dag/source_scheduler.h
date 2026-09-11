#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

#include "loom/http/client.h"
#include "loom/queue/rate_limiter.h"
#include "loom/queue/request_queue.h"
#include "loom/queue/stream_queue.h"

namespace loom {
namespace dag {

// A completed HTTP response, ready to flow downstream into compute operators.
struct Response {
    std::string request_id;
    std::string step_id;
    int status_code = 0;
    std::string body;
    bool ok = false;
    std::string error;
};

// Drives the streaming ingestion loop for a single connection:
//
//   while (rate_limiter.try_acquire() &&
//          inflight < max_inflight &&
//          !backpressured) {
//       dequeue → submit HTTP request
//   }
//
// Completed responses are marked done/failed on the durable queue and pushed
// into the bounded StreamQueue downstream.  Terminates when the request queue
// is exhausted AND all inflight requests have resolved.
class SourceScheduler {
public:
    struct Config {
        std::chrono::milliseconds poll_interval;
        Config() : poll_interval(2) {}
    };

    SourceScheduler(queue::RequestQueue* queue,
                    queue::RateLimiter* limiter,
                    http::HttpClient* client,
                    queue::StreamQueue<Response>* output,
                    Config config = Config{});

    ~SourceScheduler();

    SourceScheduler(const SourceScheduler&) = delete;
    SourceScheduler& operator=(const SourceScheduler&) = delete;

    // Perform one dispatch tick (non-blocking). Returns false when the source
    // is done (queue exhausted and no inflight). Useful for tests.
    bool tick();

    // Run the dispatch loop to completion (blocking).
    void run();

    // Launch the dispatch loop on a background thread.
    void start();
    void stop();

    bool is_done() const;
    int dispatched() const;
    int completed() const;
    int failed() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace dag
}  // namespace loom
