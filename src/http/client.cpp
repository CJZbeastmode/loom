#include "loom/http/client.h"

namespace loom {
namespace http {

struct HttpClient::Impl {
    Config config;
    int inflight = 0;
    bool running = false;
};

HttpClient::HttpClient(const Config& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

HttpClient::~HttpClient() = default;

void HttpClient::submit(const HttpRequest& /*request*/, ResponseCallback /*callback*/) {}

void HttpClient::cancel_all() {
    impl_->inflight = 0;
}

int HttpClient::inflight_count() const {
    return impl_->inflight;
}

void HttpClient::run_event_loop() {
    impl_->running = true;
}

void HttpClient::stop_event_loop() {
    impl_->running = false;
}

}  // namespace http
}  // namespace loom
