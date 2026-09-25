#include <cstdio>

#include <gtest/gtest.h>

#include "shard_manager.h"

class ShardManagerTest : public ::testing::Test {
protected:
    const std::string wal_prefix = "test_shard";

    void SetUp() override {
        cleanup();
    }

    void TearDown() override {
        cleanup();
    }

    void cleanup() {
        for (int i = 0; i < 10; ++i) {
            std::remove(
                (wal_prefix + "_" + std::to_string(i) + ".log").c_str()
            );
        }
    }
};

TEST_F(ShardManagerTest, SetAndGet) {
    ShardManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    auto result = manager.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Khushi");
}

TEST_F(ShardManagerTest, RoutesKeyToValidShard) {
    ShardManager manager(3, wal_prefix);

    std::size_t shard = manager.getShard("name");

    EXPECT_LT(shard, 3);
}

TEST_F(ShardManagerTest, StoresDifferentKeys) {
    ShardManager manager(3, wal_prefix);

    manager.set("name", "Khushi");
    manager.set("city", "Delhi");

    auto name = manager.get("name");
    auto city = manager.get("city");

    ASSERT_TRUE(name.has_value());
    ASSERT_TRUE(city.has_value());

    EXPECT_EQ(name.value(), "Khushi");
    EXPECT_EQ(city.value(), "Delhi");
}

TEST_F(ShardManagerTest, RemoveKey) {
    ShardManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    EXPECT_TRUE(manager.remove("name"));
    EXPECT_FALSE(manager.exists("name"));
}

TEST_F(ShardManagerTest, RecoversFromShardWAL) {
    {
        ShardManager manager(3, wal_prefix);

        manager.set("name", "Khushi");
        manager.set("city", "Delhi");
    }

    {
        ShardManager manager(3, wal_prefix);

        auto name = manager.get("name");
        auto city = manager.get("city");

        ASSERT_TRUE(name.has_value());
        ASSERT_TRUE(city.has_value());

        EXPECT_EQ(name.value(), "Khushi");
        EXPECT_EQ(city.value(), "Delhi");
    }
}