#include "loom/dag/source_scheduler.h"

#include <thread>

namespace loom {
namespace dag {

struct SourceScheduler::Impl {
    queue::RequestQueue* queue;
    queue::RateLimiter* limiter;
    http::HttpClient* client;
    queue::StreamQueue<Response>* output;
    Config config;

    std::atomic<bool> running{false};
    std::thread worker;

    std::atomic<int> dispatched{0};
    std::atomic<int> completed{0};
    std::atomic<int> failed{0};
};

SourceScheduler::SourceScheduler(queue::RequestQueue* queue,
                                 queue::RateLimiter* limiter,
                                 http::HttpClient* client,
                                 queue::StreamQueue<Response>* output,
                                 Config config)
    : impl_(std::make_unique<Impl>()) {
    impl_->queue = queue;
    impl_->limiter = limiter;
    impl_->client = client;
    impl_->output = output;
    impl_->config = config;
}

SourceScheduler::~SourceScheduler() {
    stop();
}

bool SourceScheduler::tick() {
    // Dispatch as many requests as the three constraints allow.
    while (impl_->limiter->try_acquire() &&
           impl_->client->inflight_count() < impl_->client->max_inflight() &&
           !impl_->output->is_backpressured()) {
        auto record = impl_->queue->dequeue();
        if (!record) break;

        http::HttpRequest req;
        req.method = record->method;
        req.url = record->url;
        req.body = record->body;

        std::string request_id = record->request_id;
        std::string step_id = record->step_id;

        impl_->client->submit(req, [this, request_id, step_id](http::HttpResponse resp) {
            bool ok = resp.error.empty();
            if (ok) {
                impl_->queue->mark_done(request_id, resp.status_code, resp.body);
                Response out;
                out.request_id = request_id;
                out.step_id = step_id;
                out.status_code = resp.status_code;
                out.body = resp.body;
                out.ok = true;
                impl_->output->enqueue_wait(std::move(out));
                impl_->completed.fetch_add(1);
            } else {
                impl_->queue->mark_failed(request_id, resp.error);
                impl_->failed.fetch_add(1);
            }
        });

        impl_->dispatched.fetch_add(1);
    }
    return !is_done();
}

bool SourceScheduler::is_done() const {
    return impl_->queue->is_exhausted() && impl_->client->inflight_count() == 0;
}

void SourceScheduler::run() {
    impl_->running.store(true);
    while (impl_->running.load()) {
        bool keep_going = tick();
        if (!keep_going) break;
        std::this_thread::sleep_for(impl_->config.poll_interval);
    }
    impl_->running.store(false);
}

void SourceScheduler::start() {
    if (impl_->worker.joinable()) return;
    impl_->running.store(true);
    impl_->worker = std::thread([this] { run(); });
}

void SourceScheduler::stop() {
    impl_->running.store(false);
    if (impl_->worker.joinable()) {
        impl_->worker.join();
    }
}

int SourceScheduler::dispatched() const { return impl_->dispatched.load(); }
int SourceScheduler::completed() const { return impl_->completed.load(); }
int SourceScheduler::failed() const { return impl_->failed.load(); }

}  // namespace dag
}  // namespace loom
