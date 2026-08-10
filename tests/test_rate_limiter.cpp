#include "test_utils.h"
#include "loom/queue/rate_limiter.h"

using namespace loom;

TEST(RateLimiterTest, InitialTokensAvailable) {
    queue::RateLimiter limiter(10, 10.0);
    EXPECT_TRUE(limiter.available_tokens() > 0);
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
