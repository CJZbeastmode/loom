#include "loom/http/client.h"

#include <atomic>
#include <cctype>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>

#ifdef LOOM_HAS_LIBUV
#include <uv.h>
#endif

namespace loom {
namespace http {

namespace {

struct Url {
    std::string scheme;
    std::string host;
    int port = 80;
    std::string path = "/";
};

// Parse "scheme://host[:port]/path". Only http is supported for now.
bool parse_url(const std::string& raw, Url& out) {
    size_t scheme_end = raw.find("://");
    if (scheme_end == std::string::npos) return false;
    out.scheme = raw.substr(0, scheme_end);
    if (out.scheme != "http" && out.scheme != "https") return false;

    size_t host_start = scheme_end + 3;
    size_t path_start = raw.find('/', host_start);
    std::string authority;
    if (path_start == std::string::npos) {
        authority = raw.substr(host_start);
        out.path = "/";
    } else {
        authority = raw.substr(host_start, path_start - host_start);
        out.path = raw.substr(path_start);
    }

    size_t colon = authority.rfind(':');
    if (colon != std::string::npos) {
        out.host = authority.substr(0, colon);
        try {
            out.port = std::stoi(authority.substr(colon + 1));
        } catch (...) {
            return false;
        }
    } else {
        out.host = authority;
        out.port = (out.scheme == "https") ? 443 : 80;
    }
    return !out.host.empty();
}

}  // namespace

#ifdef LOOM_HAS_LIBUV

// Per-request state. Kept alive via shared_ptr in the Impl's `active` set
// until every libuv handle associated with it has closed.
struct Request {
    HttpRequest http;
    ResponseCallback callback;
    Url url;
    HttpClient::Impl* impl = nullptr;

    uv_getaddrinfo_t resolver;
    uv_tcp_t socket;
    uv_timer_t timer;
    uv_write_t writer;

    bool socket_initialized = false;
    int closes_pending = 0;
    bool finalized = false;
    std::chrono::steady_clock::time_point start;

    std::string write_buf;
    std::string read_buf;
    std::string service;  // port string, kept alive for uv_getaddrinfo

    HttpResponse response;
};

static void finalize(Request* req, const std::string& error, int status, std::string body);

struct HttpClient::Impl {
    Config config;
    uv_loop_t loop;
    uv_async_t async;

    std::mutex pending_mutex;
    std::deque<std::shared_ptr<Request>> pending;

    std::mutex active_mutex;
    std::set<std::shared_ptr<Request>> active;

    std::atomic<int> inflight{0};
    std::atomic<bool> running{false};

    Impl() {
        uv_loop_init(&loop);
        uv_async_init(&loop, &async, on_async_wakeup);
        async.data = this;
    }
    ~Impl() {
        uv_close(reinterpret_cast<uv_handle_t*>(&async), nullptr);
        uv_run(&loop, UV_RUN_NOWAIT);
        uv_loop_close(&loop);
    }

    void decrement_inflight() { inflight.fetch_sub(1); }

    void erase(Request* req) {
        std::lock_guard<std::mutex> lock(active_mutex);
        for (auto it = active.begin(); it != active.end(); ++it) {
            if (it->get() == req) {
                active.erase(it);
                break;
            }
        }
    }

    static void on_async_wakeup(uv_async_t* handle);
};

static void on_handle_closed(uv_handle_t* handle) {
    Request* req = static_cast<Request*>(handle->data);
    if (--req->closes_pending <= 0) {
        // All handles are closed: drop the anchor reference.
        req->impl->erase(req);
    }
}

static void close_all_handles(Request* req) {
    if (req->socket_initialized) {
        req->closes_pending++;
        uv_close(reinterpret_cast<uv_handle_t*>(&req->socket), on_handle_closed);
    }
    req->closes_pending++;
    uv_close(reinterpret_cast<uv_handle_t*>(&req->timer), on_handle_closed);
}

static void finalize(Request* req, const std::string& error, int status, std::string body) {
    if (req->finalized) return;
    req->finalized = true;

    std::string err = error;
    if (err.empty() && status != 0) {
        for (int code : req->impl->config.fail_on_status) {
            if (code == status) {
                err = "HTTP status " + std::to_string(status) + " is in fail_on_status";
                break;
            }
        }
    }

    req->response.error = err;
    req->response.status_code = status;
    req->response.body = std::move(body);
    req->response.latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - req->start);

    uv_timer_stop(&req->timer);
    if (req->socket_initialized) {
        uv_read_stop(reinterpret_cast<uv_stream_t*>(&req->socket));
    }
    close_all_handles(req);

    ResponseCallback cb = std::move(req->callback);
    HttpResponse resp = req->response;
    req->impl->decrement_inflight();
    cb(std::move(resp));
}

// Find the "\r\n\r\n" boundary and parse the status code from the first line.
static int parse_status(const std::string& data, size_t* header_end) {
    size_t pos = data.find("\r\n\r\n");
    if (pos == std::string::npos) return 0;
    if (header_end) *header_end = pos + 4;
    size_t first_space = data.find(' ');
    if (first_space == std::string::npos) return 0;
    size_t second_space = data.find(' ', first_space + 1);
    std::string code = data.substr(first_space + 1, second_space - first_space - 1);
    try {
        return std::stoi(code);
    } catch (...) {
        return 0;
    }
}

static void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    Request* req = static_cast<Request*>(stream->data);

    if (nread > 0) {
        req->read_buf.append(buf->base, nread);
    }
    if (buf->base) delete[] buf->base;

    if (nread > 0) {
        size_t header_end = 0;
        int status = parse_status(req->read_buf, &header_end);
        if (status != 0) {
            size_t content_length = 0;
            size_t cl = req->read_buf.find("Content-Length:");
            if (cl == std::string::npos) cl = req->read_buf.find("content-length:");
            if (cl != std::string::npos) {
                size_t v = cl + 15;
                while (v < req->read_buf.size() && req->read_buf[v] == ' ') v++;
                size_t e = v;
                while (e < req->read_buf.size() &&
                       std::isdigit(static_cast<unsigned char>(req->read_buf[e]))) e++;
                content_length = static_cast<size_t>(std::stoull(req->read_buf.substr(v, e - v)));
            }
            bool head_only = (req->http.method == "HEAD");
            if (head_only || header_end + content_length <= req->read_buf.size()) {
                std::string body = head_only ? "" : req->read_buf.substr(header_end, content_length);
                finalize(req, "", status, std::move(body));
            }
        }
        return;
    }

    if (nread == UV_EOF) {
        size_t header_end = 0;
        int status = parse_status(req->read_buf, &header_end);
        std::string body = (status != 0) ? req->read_buf.substr(header_end) : req->read_buf;
        finalize(req, "", status, std::move(body));
    } else {
        finalize(req, "read error: " + std::string(uv_strerror(static_cast<int>(nread))), 0, "");
    }
}

static void on_write(uv_write_t* write, int status) {
    Request* req = static_cast<Request*>(write->data);
    if (status < 0) {
        finalize(req, "write error: " + std::string(uv_strerror(status)), 0, "");
        return;
    }
    uv_read_start(reinterpret_cast<uv_stream_t*>(&req->socket),
                  [](uv_handle_t*, size_t, uv_buf_t* buf) {
                      buf->base = new char[64 * 1024];
                      buf->len = 64 * 1024;
                  },
                  on_read);
}

static void on_connect(uv_connect_t* connect, int status) {
    std::shared_ptr<Request> keep = *static_cast<std::shared_ptr<Request>*>(connect->data);
    delete static_cast<std::shared_ptr<Request>*>(connect->data);
    delete connect;

    Request* req = keep.get();
    if (req->finalized) return;
    if (status < 0) {
        finalize(req, "connect error: " + std::string(uv_strerror(status)), 0, "");
        return;
    }

    std::string text;
    text += req->http.method + " " + req->url.path + " HTTP/1.1\r\n";
    text += "Host: " + req->url.host + "\r\n";
    text += "Accept: " + req->http.headers.Accept + "\r\n";
    if (!req->http.headers.Authorization.empty())
        text += "Authorization: " + req->http.headers.Authorization + "\r\n";
    if (!req->http.headers.XRequestTag.empty())
        text += "X-Request-Tag: " + req->http.headers.XRequestTag + "\r\n";
    if (!req->http.body.empty())
        text += "Content-Length: " + std::to_string(req->http.body.size()) + "\r\n";
    text += "Connection: close\r\n\r\n";
    text += req->http.body;

    req->write_buf = std::move(text);
    uv_buf_t buf = uv_buf_init(const_cast<char*>(req->write_buf.data()),
                               static_cast<unsigned int>(req->write_buf.size()));
    req->writer.data = req;
    uv_write(&req->writer, reinterpret_cast<uv_stream_t*>(&req->socket), &buf, 1, on_write);
}

static void on_resolved(uv_getaddrinfo_t* resolver, int status, struct addrinfo* res) {
    std::shared_ptr<Request> keep = *static_cast<std::shared_ptr<Request>*>(resolver->data);
    delete static_cast<std::shared_ptr<Request>*>(resolver->data);

    Request* req = keep.get();
    if (req->finalized) {
        uv_freeaddrinfo(res);
        return;
    }
    if (status < 0) {
        finalize(req, "DNS resolution failed: " + std::string(uv_strerror(status)), 0, "");
        uv_freeaddrinfo(res);
        return;
    }

    uv_tcp_init(resolver->loop, &req->socket);
    req->socket.data = req;
    req->socket_initialized = true;

    uv_connect_t* connect = new uv_connect_t;
    connect->data = new std::shared_ptr<Request>(keep);
    uv_tcp_connect(connect, &req->socket, res->ai_addr, on_connect);
    uv_freeaddrinfo(res);
}

static void on_timeout(uv_timer_t* timer) {
    Request* req = static_cast<Request*>(timer->data);
    finalize(req, "request timed out", 0, "");
}

void HttpClient::Impl::on_async_wakeup(uv_async_t* handle) {
    HttpClient::Impl* impl = static_cast<HttpClient::Impl*>(handle->data);

    std::deque<std::shared_ptr<Request>> batch;
    {
        std::lock_guard<std::mutex> lock(impl->pending_mutex);
        batch.swap(impl->pending);
    }

    for (auto& req : batch) {
        req->start = std::chrono::steady_clock::now();

        uv_timer_init(&impl->loop, &req->timer);
        req->timer.data = req.get();
        uv_timer_start(&req->timer, on_timeout,
                       static_cast<uint64_t>(req->http.timeout.count()), 0);

        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        req->resolver.data = new std::shared_ptr<Request>(req);
        req->service = std::to_string(req->url.port);
        uv_getaddrinfo(&impl->loop, &req->resolver, on_resolved,
                       req->url.host.c_str(), req->service.c_str(), &hints);
    }
}

HttpClient::HttpClient(const Config& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

HttpClient::~HttpClient() {
    cancel_all();
}

void HttpClient::submit(const HttpRequest& request, ResponseCallback callback) {
    Url url;
    if (!parse_url(request.url, url)) {
        HttpResponse resp;
        resp.error = "invalid or unsupported URL: " + request.url;
        callback(std::move(resp));
        return;
    }
    if (url.scheme == "https") {
        HttpResponse resp;
        resp.error = "HTTPS not yet supported (use http)";
        callback(std::move(resp));
        return;
    }

    auto req = std::make_shared<Request>();
    req->http = request;
    req->callback = std::move(callback);
    req->url = url;
    req->impl = impl_.get();

    impl_->inflight.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(impl_->active_mutex);
        impl_->active.insert(req);
    }
    {
        std::lock_guard<std::mutex> lock(impl_->pending_mutex);
        impl_->pending.push_back(req);
    }
    uv_async_send(&impl_->async);
}

void HttpClient::cancel_all() {
    std::deque<std::shared_ptr<Request>> pending;
    {
        std::lock_guard<std::mutex> lock(impl_->pending_mutex);
        pending.swap(impl_->pending);
    }
    for (auto& req : pending) {
        HttpResponse resp;
        resp.error = "cancelled";
        req->callback(std::move(resp));
        impl_->decrement_inflight();
        impl_->erase(req.get());
    }
}

int HttpClient::inflight_count() const {
    return impl_->inflight.load();
}

int HttpClient::max_inflight() const {
    return impl_->config.max_inflight;
}

void HttpClient::run_event_loop() {
    impl_->running.store(true);
    uv_run(&impl_->loop, UV_RUN_DEFAULT);
    impl_->running.store(false);
}

void HttpClient::stop_event_loop() {
    impl_->running.store(false);
    // Wake the loop so uv_run observes the stop flag (uv_stop alone does not
    // interrupt a loop blocked in poll).
    uv_stop(&impl_->loop);
    uv_async_send(&impl_->async);
}

#else  // !LOOM_HAS_LIBUV

struct HttpClient::Impl {
    Config config;
    std::atomic<int> inflight{0};
    std::atomic<bool> running{false};
};

HttpClient::HttpClient(const Config& config)
    : impl_(std::make_unique<Impl>()) {
    impl_->config = config;
}

HttpClient::~HttpClient() = default;

void HttpClient::submit(const HttpRequest& request, ResponseCallback callback) {
    impl_->inflight.fetch_add(1);
    std::thread([this, request, callback = std::move(callback)]() mutable {
        HttpResponse resp;
        resp.error = "libuv HTTP client not compiled in (rebuild with -DLOOM_USE_LIBUV=ON)";
        callback(std::move(resp));
        impl_->inflight.fetch_sub(1);
    }).detach();
}

void HttpClient::cancel_all() {}

int HttpClient::inflight_count() const {
    return impl_->inflight.load();
}

int HttpClient::max_inflight() const {
    return impl_->config.max_inflight;
}

void HttpClient::run_event_loop() { impl_->running.store(true); }
void HttpClient::stop_event_loop() { impl_->running.store(false); }

#endif  // LOOM_HAS_LIBUV

}  // namespace http
}  // namespace loom
