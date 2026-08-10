#include "loom/queue/rate_limiter.h"
#include <mutex>
#include <thread>

namespace loom {
namespace queue {

struct RateLimiter::Impl {
    mutable std::mutex mutex;
    double tokens = 0.0;
    int64_t max_tokens = 10;
    double refill_rate_per_sec = 10.0;
    mutable std::chrono::steady_clock::time_point last_refill;

    void refill() {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - last_refill).count();
        tokens += elapsed * refill_rate_per_sec;
        if (tokens > static_cast<double>(max_tokens)) {
            tokens = static_cast<double>(max_tokens);
        }
        last_refill = now;
    }
};

RateLimiter::RateLimiter(int max_tokens, double refill_rate_per_sec)
    : impl_(std::make_unique<Impl>()) {
    impl_->max_tokens = max_tokens;
    impl_->refill_rate_per_sec = refill_rate_per_sec;
    impl_->tokens = static_cast<double>(max_tokens);
    impl_->last_refill = std::chrono::steady_clock::now();
}

RateLimiter::~RateLimiter() = default;

bool RateLimiter::try_acquire() {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->refill();
    if (impl_->tokens >= 1.0) {
        impl_->tokens -= 1.0;
        return true;
    }
    return false;
}

void RateLimiter::wait_and_acquire() {
    while (!try_acquire()) {
        std::this_thread::sleep_for(
            std::chrono::microseconds(static_cast<int64_t>(1'000'000.0 / impl_->refill_rate_per_sec)));
    }
}

void RateLimiter::set_rate(double refill_rate_per_sec) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->refill_rate_per_sec = refill_rate_per_sec;
}

int RateLimiter::available_tokens() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->refill();
    return static_cast<int>(impl_->tokens);
}

}  // namespace queue
}  // namespace loom
