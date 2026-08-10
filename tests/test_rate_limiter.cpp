#include <gtest/gtest.h>
#include "loom/queue/rate_limiter.h"

TEST(RateLimiterTest, InitialTokensAvailable) {
    loom::queue::RateLimiter limiter(10, 10.0);
    EXPECT_GT(limiter.available_tokens(), 0);
}

TEST(RateLimiterTest, TryAcquireExhaustsTokens) {
    loom::queue::RateLimiter limiter(3, 10.0);
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_TRUE(limiter.try_acquire());
    EXPECT_FALSE(limiter.try_acquire());
}

TEST(RateLimiterTest, SetRate) {
    loom::queue::RateLimiter limiter(5, 5.0);
    limiter.set_rate(100.0);
    EXPECT_EQ(limiter.available_tokens(), 5);
}
