#include <gtest/gtest.h>

#include "shard_router.h"

TEST(ShardRouterTest, ReturnsValidShard) {
    ShardRouter router(3);

    std::size_t shard = router.getShard("user123");

    EXPECT_LT(shard, 3);
}

TEST(ShardRouterTest, SameKeyMapsToSameShard) {
    ShardRouter router(5);

    EXPECT_EQ(
        router.getShard("user123"),
        router.getShard("user123")
    );
}

TEST(ShardRouterTest, DifferentKeysCanBeDistributed) {
    ShardRouter router(4);

    std::size_t shard1 = router.getShard("user1");
    std::size_t shard2 = router.getShard("user2");
    std::size_t shard3 = router.getShard("user3");

    EXPECT_LT(shard1, 4);
    EXPECT_LT(shard2, 4);
    EXPECT_LT(shard3, 4);
}

TEST(ShardRouterTest, RejectsZeroShards) {
    EXPECT_THROW(
        ShardRouter router(0),
        std::invalid_argument
    );
}

TEST(ShardRouterTest, ReportsShardCount) {
    ShardRouter router(7);

    EXPECT_EQ(router.shardCount(), 7);
}