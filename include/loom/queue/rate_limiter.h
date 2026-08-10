#pragma once

#include <chrono>
#include <memory>

namespace loom {
namespace queue {

class RateLimiter {
public:
    RateLimiter(int max_tokens, double refill_rate_per_sec);
    ~RateLimiter();

    bool try_acquire();
    void wait_and_acquire();
    void set_rate(double refill_rate_per_sec);
    int available_tokens() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace queue
}  // namespace loom
