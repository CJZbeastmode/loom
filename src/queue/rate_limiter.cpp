#include "loom/queue/rate_limiter.h"

#include <atomic>
#include <thread>
#include <cstdint>

namespace loom {
namespace queue {

namespace {

// Tokens are stored in fixed-point "micro-tokens" (1 token == kScale micros)
// so fractional refill rates and sub-token values can be represented with an
// integer atomic.
constexpr int64_t kScale = 1'000'000;

inline int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

struct RateLimiter::Impl {
    std::atomic<int64_t> tokens{0};           // micro-tokens currently available
    int64_t max_tokens = 0;                   // capacity in micro-tokens
    std::atomic<int64_t> refill_per_sec{0};   // micro-tokens added per second
    std::atomic<int64_t> last_refill_ns{0};   // steady-clock timestamp of last refill
};

RateLimiter::RateLimiter(int max_tokens, double refill_rate_per_sec)
    : impl_(std::make_unique<Impl>()) {
    impl_->max_tokens = static_cast<int64_t>(max_tokens) * kScale;
    impl_->tokens.store(impl_->max_tokens, std::memory_order_relaxed);
    impl_->refill_per_sec.store(
        static_cast<int64_t>(refill_rate_per_sec * kScale), std::memory_order_relaxed);
    impl_->last_refill_ns.store(now_ns(), std::memory_order_relaxed);
}

RateLimiter::~RateLimiter() = default;

bool RateLimiter::try_acquire() {
    const int64_t max = impl_->max_tokens;
    const int64_t rate = impl_->refill_per_sec.load(std::memory_order_relaxed);

    int64_t cur = impl_->tokens.load(std::memory_order_relaxed);
    while (true) {
        // Recompute refill from the elapsed time since the last banking.
        int64_t now = now_ns();
        int64_t last = impl_->last_refill_ns.load(std::memory_order_relaxed);
        int64_t elapsed = now - last;
        if (elapsed < 0) elapsed = 0;
        int64_t refill = (elapsed * rate) / 1'000'000'000;

        int64_t banked = cur + refill;
        if (banked > max) banked = max;

        int64_t after = banked - kScale;  // consume one token
        if (after < 0) {
            // Not enough credit: bank the refill and fail.
            if (impl_->tokens.compare_exchange_weak(cur, banked,
                    std::memory_order_release, std::memory_order_relaxed)) {
                impl_->last_refill_ns.compare_exchange_strong(last, now,
                    std::memory_order_relaxed, std::memory_order_relaxed);
                return false;
            }
        } else {
            // Enough credit: consume a token and bank the remainder.
            if (impl_->tokens.compare_exchange_weak(cur, after,
                    std::memory_order_release, std::memory_order_relaxed)) {
                impl_->last_refill_ns.compare_exchange_strong(last, now,
                    std::memory_order_relaxed, std::memory_order_relaxed);
                return true;
            }
        }
        // CAS failed: `cur` now holds the latest value; loop recomputes refill.
    }
}

void RateLimiter::wait_and_acquire() {
    while (!try_acquire()) {
        int64_t rate = impl_->refill_per_sec.load(std::memory_order_relaxed);
        if (rate <= 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        // One token period in nanoseconds (micro-token units cancel out).
        int64_t period_ns = (kScale * 1'000'000'000LL) / rate;
        if (period_ns <= 0) period_ns = 1'000'000;
        std::this_thread::sleep_for(std::chrono::nanoseconds(period_ns));
    }
}

void RateLimiter::set_rate(double refill_rate_per_sec) {
    impl_->refill_per_sec.store(
        static_cast<int64_t>(refill_rate_per_sec * kScale), std::memory_order_relaxed);
}

int RateLimiter::available_tokens() const {
    const int64_t max = impl_->max_tokens;
    const int64_t rate = impl_->refill_per_sec.load(std::memory_order_relaxed);
    int64_t cur = impl_->tokens.load(std::memory_order_relaxed);
    int64_t now = now_ns();
    int64_t last = impl_->last_refill_ns.load(std::memory_order_relaxed);
    int64_t elapsed = now - last;
    if (elapsed < 0) elapsed = 0;
    int64_t refill = (elapsed * rate) / 1'000'000'000;
    int64_t banked = cur + refill;
    if (banked > max) banked = max;
    return static_cast<int>(banked / kScale);
}

}  // namespace queue
}  // namespace loom
