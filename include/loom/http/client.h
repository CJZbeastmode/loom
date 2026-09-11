#pragma once

#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <chrono>

namespace loom {
namespace http {

struct HttpRequest {
    std::string method = "GET";
    std::string url;
    std::string body;
    struct {
        std::string Accept = "application/json";
        std::string Authorization;
        std::string XRequestTag;
    } headers;
    std::chrono::milliseconds timeout{30000};
};

struct HttpResponse {
    int status_code = 0;
    std::string body;
    std::string error;
    std::chrono::microseconds latency;
};

using ResponseCallback = std::function<void(HttpResponse)>;

class HttpClient {
public:
    struct Config {
        std::string base_url;
        int max_inflight = 10;
        std::chrono::milliseconds timeout{30000};
        std::vector<int> fail_on_status;
    };

    explicit HttpClient(const Config& config);
    ~HttpClient();

    void submit(const HttpRequest& request, ResponseCallback callback);
    void cancel_all();
    int inflight_count() const;
    int max_inflight() const;
    void run_event_loop();
    void stop_event_loop();

    // Forward-declared (public so the .cpp's free callbacks can reference it).
    class Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace http
}  // namespace loom
