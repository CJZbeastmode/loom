#include "test_utils.h"
#include "mock_http_server.h"
#include "loom/dag/source_scheduler.h"

#include <atomic>
#include <thread>
#include <chrono>
#include <algorithm>
#include <functional>

using namespace loom;

namespace {
void enqueue_n(queue::RequestQueue& q, int n, const std::string& base_url) {
    std::vector<queue::RequestRecord> batch;
    for (int i = 0; i < n; i++) {
        queue::RequestRecord r;
        r.request_id = "req-" + std::to_string(i);
        r.step_id = "gen";
        r.method = "GET";
        r.url = base_url + "/item/" + std::to_string(i);
        batch.push_back(r);
    }
    q.enqueue(batch);
}

bool wait_until(const std::function<bool()>& cond, int max_ms = 5000) {
    auto start = std::chrono::steady_clock::now();
    while (!cond()) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count() > max_ms) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}
}

TEST(SourceSchedulerTest, EndToEndStreaming) {
    MockHttpServer server;
    server.set_response(200, "{\"data\":1}");
    server.start();

    queue::RequestQueue q(":memory:", "run_e2e");
    queue::RateLimiter limiter(1000, 1000.0);  // effectively unlimited
    http::HttpClient::Config cfg;
    cfg.max_inflight = 10;
    http::HttpClient client(cfg);
    std::thread loop_thread([&] { client.run_event_loop(); });

    queue::StreamQueue<dag::Response>::Config sq_cfg;
    sq_cfg.max_buffered = 1000;
    sq_cfg.resume_at = 500;
    queue::StreamQueue<dag::Response> output(sq_cfg);

    dag::SourceScheduler scheduler(&q, &limiter, &client, &output);

    enqueue_n(q, 100, server.url(""));

    scheduler.start();

    // Drain output until the scheduler reports done.
    int drained = 0;
    while (!scheduler.is_done()) {
        dag::Response resp;
        if (output.dequeue(resp)) drained++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Drain any remaining items.
    dag::Response resp;
    while (output.dequeue(resp)) drained++;

    scheduler.stop();
    client.stop_event_loop();
    loop_thread.join();

    EXPECT_EQ(scheduler.dispatched(), 100);
    EXPECT_EQ(scheduler.completed(), 100);
    EXPECT_EQ(scheduler.failed(), 0);
    EXPECT_EQ(drained, 100);
    EXPECT_EQ(server.request_count(), 100);
}

TEST(SourceSchedulerTest, RespectsMaxInflight) {
    MockHttpServer server;
    server.set_response(200, "ok");
    server.set_delay_ms(20);
    server.start();

    queue::RequestQueue q(":memory:", "run_inflight");
    queue::RateLimiter limiter(1000, 1000.0);
    http::HttpClient::Config cfg;
    cfg.max_inflight = 3;
    http::HttpClient client(cfg);
    std::thread loop_thread([&] { client.run_event_loop(); });

    queue::StreamQueue<dag::Response>::Config sq_cfg;
    queue::StreamQueue<dag::Response> output(sq_cfg);

    dag::SourceScheduler scheduler(&q, &limiter, &client, &output);
    enqueue_n(q, 30, server.url(""));
    scheduler.start();

    // Poll while running; inflight must never exceed the cap.
    int max_observed = 0;
    while (!scheduler.is_done()) {
        max_observed = std::max(max_observed, client.inflight_count());
        dag::Response resp;
        while (output.dequeue(resp)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    scheduler.stop();
    client.stop_event_loop();
    loop_thread.join();

    EXPECT_TRUE(max_observed <= 3);
    EXPECT_EQ(scheduler.completed(), 30);
}

TEST(SourceSchedulerTest, BackpressurePausesDispatch) {
    MockHttpServer server;
    server.set_response(200, "{\"x\":1}");
    server.set_delay_ms(10);
    server.start();

    queue::RequestQueue q(":memory:", "run_bp");
    queue::RateLimiter limiter(1000, 1000.0);
    http::HttpClient::Config cfg;
    cfg.max_inflight = 10;
    http::HttpClient client(cfg);
    std::thread loop_thread([&] { client.run_event_loop(); });

    queue::StreamQueue<dag::Response>::Config sq_cfg;
    sq_cfg.max_buffered = 5;
    sq_cfg.resume_at = 2;
    queue::StreamQueue<dag::Response> output(sq_cfg);

    dag::SourceScheduler scheduler(&q, &limiter, &client, &output);
    enqueue_n(q, 100, server.url(""));
    scheduler.start();

    // Wait for backpressure to engage (downstream not draining).
    EXPECT_TRUE(wait_until([&] { return output.is_backpressured(); }));

    // Snapshot dispatch count; it must not grow much while backpressured.
    int dispatched_at_bp = scheduler.dispatched();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int dispatched_later = scheduler.dispatched();
    EXPECT_TRUE(dispatched_later - dispatched_at_bp <= 2);

    // Now drain; dispatch should resume and finish all requests.
    while (!scheduler.is_done()) {
        dag::Response resp;
        while (output.dequeue(resp)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    dag::Response resp;
    while (output.dequeue(resp)) {}

    scheduler.stop();
    client.stop_event_loop();
    loop_thread.join();

    EXPECT_EQ(scheduler.completed(), 100);
    EXPECT_EQ(scheduler.failed(), 0);
}

TEST(SourceSchedulerTest, FailOnStatusMarksFailed) {
    MockHttpServer server;
    server.set_response(500, "internal error");
    server.start();

    queue::RequestQueue q(":memory:", "run_fail");
    queue::RateLimiter limiter(1000, 1000.0);
    http::HttpClient::Config cfg;
    cfg.max_inflight = 5;
    cfg.fail_on_status = {500};
    http::HttpClient client(cfg);
    std::thread loop_thread([&] { client.run_event_loop(); });

    queue::StreamQueue<dag::Response>::Config sq_cfg;
    queue::StreamQueue<dag::Response> output(sq_cfg);

    dag::SourceScheduler scheduler(&q, &limiter, &client, &output);
    enqueue_n(q, 10, server.url(""));
    scheduler.start();

    while (!scheduler.is_done()) {
        dag::Response resp;
        while (output.dequeue(resp)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    scheduler.stop();
    client.stop_event_loop();
    loop_thread.join();

    EXPECT_EQ(scheduler.completed(), 0);
    EXPECT_EQ(scheduler.failed(), 10);
}
