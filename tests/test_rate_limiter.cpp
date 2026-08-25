#include "test_utils.h"
#include "loom/queue/rate_limiter.h"
#include <thread>
#include <chrono>

using namespace loom;

TEST(RateLimiterTest, InitialTokensAvailable) {
    queue::RateLimiter limiter(10, 10.0);
    EXPECT_TRUE(limiter.available_tokens() > 0);
    EXPECT_EQ(limiter.available_tokens(), 10);
}

TEST(RateLimiterTest, TryAcquireExhaustsTokens) {
    queue::RateLimiter limiter(3, 10.0);
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_FALSE(limiter.try_acquire());
}

TEST(RateLimiterTest, SetRate) {
    queue::RateLimiter limiter(5, 5.0);
    limiter.set_rate(100.0);
    EXPECT_EQ(limiter.available_tokens(), 5);
}

TEST(RateLimiterTest, RefillOverTime) {
    // 5 tokens/sec means one token every 200ms.
    queue::RateLimiter limiter(1, 5.0);
    EXPECT_TRUE(limiter.try_acquire());       // consume the initial token
    EXPECT_FALSE(limiter.try_acquire());      // empty now
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_TRUE(limiter.try_acquire());       // refilled after ~250ms
    EXPECT_FALSE(limiter.try_acquire());      // consumed again
}

TEST(RateLimiterTest, WaitAndAcquireBlocks) {
    queue::RateLimiter limiter(1, 10.0);      // one token per 100ms
    EXPECT_TRUE(limiter.try_acquire());       // drain the single token

    auto start = std::chrono::steady_clock::now();
    limiter.wait_and_acquire();               // must block until refill
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start).count();
    EXPECT_TRUE(elapsed >= 80);               // waited at least ~100ms
}

TEST(RateLimiterTest, ThroughputShaping) {
    // Burst 10, refill 10/sec. Over ~2 seconds, expect roughly 10 (burst) + 20 (refill).
    queue::RateLimiter limiter(10, 10.0);

    int acquired = 0;
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - start).count() < 2) {
        if (limiter.try_acquire()) acquired++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // Allow generous tolerance for scheduler jitter.
    EXPECT_TRUE(acquired >= 15);
    EXPECT_TRUE(acquired <= 40);
}

TEST(RateLimiterTest, ThreadSafety) {
    // 8 threads race on a single limiter; total acquisitions must never exceed burst.
    queue::RateLimiter limiter(50, 100.0);
    std::atomic<int> acquired{0};

    auto worker = [&]() {
        for (int i = 0; i < 50; i++) {
            if (limiter.try_acquire()) acquired.fetch_add(1);
        }
    };

    std::thread t1(worker), t2(worker), t3(worker), t4(worker);
    std::thread t5(worker), t6(worker), t7(worker), t8(worker);
    t1.join(); t2.join(); t3.join(); t4.join();
    t5.join(); t6.join(); t7.join(); t8.join();

    // 8 threads * 50 attempts = 400 attempts, but at most 50 (burst) can succeed.
    EXPECT_TRUE(acquired.load() <= 50);
    EXPECT_TRUE(acquired.load() > 0);
}
