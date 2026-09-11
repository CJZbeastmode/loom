#pragma once

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// Minimal single-threaded HTTP server for tests. Responds to every request
// with a fixed status/body, optionally after a fixed delay, then closes the
// connection. Counts requests received.
class MockHttpServer {
public:
    MockHttpServer() {
        listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // OS assigns a free port
        ::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd, 128);
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &len);
        port = ntohs(addr.sin_port);
    }

    ~MockHttpServer() { stop(); }

    void set_response(int status, const std::string& body) {
        response_status = status;
        response_body = body;
    }

    void set_delay_ms(int ms) { delay_ms = ms; }

    void start() {
        running = true;
        thread = std::thread([this] { serve(); });
    }

    void stop() {
        running = false;
        if (listen_fd >= 0) ::close(listen_fd);
        if (thread.joinable()) thread.join();
    }

    int request_count() const { return count.load(); }

    std::string url(const std::string& path) const {
        return "http://127.0.0.1:" + std::to_string(port) + path;
    }

    int port = 0;
    std::atomic<int> count{0};
    std::atomic<bool> running{false};

private:
    int listen_fd = -1;
    std::thread thread;
    int response_status = 200;
    std::string response_body = "{}";
    int delay_ms = 0;

    void serve() {
        while (running.load()) {
            int fd = ::accept(listen_fd, nullptr, nullptr);
            if (fd < 0) {
                if (running.load()) continue;
                break;
            }

            // Read until the end of the request headers.
            std::string req;
            char buf[4096];
            struct timeval tv{1, 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            while (true) {
                ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                if (n <= 0) break;
                req.append(buf, n);
                if (req.find("\r\n\r\n") != std::string::npos) break;
            }

            count.fetch_add(1);

            if (delay_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            }

            std::string resp =
                "HTTP/1.1 " + std::to_string(response_status) + " OK\r\n"
                "Content-Type: application/json\r\n"
                "Content-Length: " + std::to_string(response_body.size()) + "\r\n"
                "Connection: close\r\n\r\n" + response_body;

            ::send(fd, resp.data(), resp.size(), 0);
            ::close(fd);
        }
    }
};
