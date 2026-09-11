#include "test_utils.h"
#include "mock_http_server.h"
#include "loom/http/client.h"

#include <atomic>
#include <thread>
#include <chrono>

using namespace loom;
using namespace loom::http;

namespace {
// Run the client event loop on a background thread for the test's duration.
struct LoopGuard {
    http::HttpClient* client;
    std::thread thread;
    explicit LoopGuard(http::HttpClient* c) : client(c) {
        thread = std::thread([c] { c->run_event_loop(); });
    }
    ~LoopGuard() {
        client->stop_event_loop();
        thread.join();
    }
};
}

TEST(HttpClientTest, GetReturnsBody) {
    MockHttpServer server;
    server.set_response(200, "{\"ok\":true}");
    server.start();

    http::HttpClient::Config cfg;
    http::HttpClient client(cfg);
    LoopGuard loop(&client);

    std::atomic<bool> done{false};
    HttpResponse result;
    http::HttpRequest req;
    req.method = "GET";
    req.url = server.url("/test");

    client.submit(req, [&](HttpResponse resp) {
        result = std::move(resp);
        done = true;
    });

    for (int i = 0; i < 100 && !done; i++) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_TRUE(done.load());
    EXPECT_EQ(result.status_code, 200);
    EXPECT_EQ(result.body, "{\"ok\":true}");
    EXPECT_EQ(result.error, "");
}

TEST(HttpClientTest, ConcurrentRequests) {
    MockHttpServer server;
    server.set_response(200, "pong");
    server.start();

    http::HttpClient::Config cfg;
    http::HttpClient client(cfg);
    LoopGuard loop(&client);

    const int N = 20;
    std::atomic<int> received{0};
    for (int i = 0; i < N; i++) {
        http::HttpRequest req;
        req.method = "GET";
        req.url = server.url("/" + std::to_string(i));
        client.submit(req, [&](HttpResponse resp) {
            if (resp.status_code == 200) received.fetch_add(1);
        });
    }

    for (int i = 0; i < 200 && received.load() < N; i++)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_EQ(received.load(), N);
    EXPECT_EQ(server.request_count(), N);
}

TEST(HttpClientTest, TimeoutFires) {
    MockHttpServer server;
    server.set_response(200, "late");
    server.set_delay_ms(500);
    server.start();

    http::HttpClient::Config cfg;
    http::HttpClient client(cfg);
    LoopGuard loop(&client);

    std::atomic<bool> done{false};
    HttpResponse result;
    http::HttpRequest req;
    req.method = "GET";
    req.url = server.url("/slow");
    req.timeout = std::chrono::milliseconds(100);

    client.submit(req, [&](HttpResponse resp) {
        result = std::move(resp);
        done = true;
    });

    for (int i = 0; i < 200 && !done; i++) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_TRUE(done.load());
    EXPECT_FALSE(result.error.empty());  // timed out
    EXPECT_EQ(result.status_code, 0);
}

TEST(HttpClientTest, InvalidUrlErrors) {
    http::HttpClient::Config cfg;
    http::HttpClient client(cfg);

    bool called = false;
    HttpResponse result;
    http::HttpRequest req;
    req.method = "GET";
    req.url = "not-a-url";

    client.submit(req, [&](HttpResponse resp) {
        result = std::move(resp);
        called = true;
    });

    EXPECT_TRUE(called);
    EXPECT_FALSE(result.error.empty());
}

TEST(HttpClientTest, HttpsRejected) {
    http::HttpClient::Config cfg;
    http::HttpClient client(cfg);

    bool called = false;
    HttpResponse result;
    http::HttpRequest req;
    req.method = "GET";
    req.url = "https://example.com/test";

    client.submit(req, [&](HttpResponse resp) {
        result = std::move(resp);
        called = true;
    });

    EXPECT_TRUE(called);
    EXPECT_FALSE(result.error.empty());
}

TEST(HttpClientTest, FailOnStatus) {
    MockHttpServer server;
    server.set_response(429, "rate limited");
    server.start();

    http::HttpClient::Config cfg;
    cfg.fail_on_status = {429, 500};
    http::HttpClient client(cfg);
    LoopGuard loop(&client);

    std::atomic<bool> done{false};
    HttpResponse result;
    http::HttpRequest req;
    req.method = "GET";
    req.url = server.url("/limited");

    client.submit(req, [&](HttpResponse resp) {
        result = std::move(resp);
        done = true;
    });

    for (int i = 0; i < 100 && !done; i++) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_TRUE(done.load());
    EXPECT_EQ(result.status_code, 429);
    EXPECT_FALSE(result.error.empty());  // marked as failure
}
