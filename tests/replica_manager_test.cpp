#include <cstdio>
#include <string>

#include <gtest/gtest.h>

#include "replica_manager.h"

class ReplicaManagerTest : public ::testing::Test {
protected:
    const std::string wal_prefix = "test_replica";

    void SetUp() override {
        cleanup();
    }

    void TearDown() override {
        cleanup();
    }

    void cleanup() {
        for (int i = 0; i < 10; ++i) {
            std::remove(
                (
                    wal_prefix +
                    "_primary_" +
                    std::to_string(i) +
                    ".log"
                ).c_str()
            );

            std::remove(
                (
                    wal_prefix +
                    "_replica_" +
                    std::to_string(i) +
                    ".log"
                ).c_str()
            );
        }
    }
};

TEST_F(ReplicaManagerTest, SetReplicatesToReplica) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    auto primary = manager.get("name");
    auto replica = manager.getReplica("name");

    ASSERT_TRUE(primary.has_value());
    ASSERT_TRUE(replica.has_value());

    EXPECT_EQ(primary.value(), "Khushi");
    EXPECT_EQ(replica.value(), "Khushi");
}

TEST_F(ReplicaManagerTest, UpdateReplicatesToReplica) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");
    manager.set("name", "Rahul");

    auto primary = manager.get("name");
    auto replica = manager.getReplica("name");

    ASSERT_TRUE(primary.has_value());
    ASSERT_TRUE(replica.has_value());

    EXPECT_EQ(primary.value(), "Rahul");
    EXPECT_EQ(replica.value(), "Rahul");
}

TEST_F(ReplicaManagerTest, DeleteReplicatesToReplica) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    EXPECT_TRUE(manager.remove("name"));

    EXPECT_FALSE(manager.get("name").has_value());
    EXPECT_FALSE(manager.getReplica("name").has_value());
}

TEST_F(ReplicaManagerTest, DifferentKeysCanBeReplicated) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");
    manager.set("city", "Delhi");

    auto primary_name = manager.get("name");
    auto replica_name = manager.getReplica("name");

    auto primary_city = manager.get("city");
    auto replica_city = manager.getReplica("city");

    ASSERT_TRUE(primary_name.has_value());
    ASSERT_TRUE(replica_name.has_value());
    ASSERT_TRUE(primary_city.has_value());
    ASSERT_TRUE(replica_city.has_value());

    EXPECT_EQ(primary_name.value(), "Khushi");
    EXPECT_EQ(replica_name.value(), "Khushi");

    EXPECT_EQ(primary_city.value(), "Delhi");
    EXPECT_EQ(replica_city.value(), "Delhi");
}

TEST_F(ReplicaManagerTest, RecoversPrimaryAndReplicaFromWAL) {
    {
        ReplicaManager manager(3, wal_prefix);

        manager.set("name", "Khushi");
        manager.set("city", "Delhi");
    }

    {
        ReplicaManager manager(3, wal_prefix);

        auto primary_name = manager.get("name");
        auto replica_name = manager.getReplica("name");

        auto primary_city = manager.get("city");
        auto replica_city = manager.getReplica("city");

        ASSERT_TRUE(primary_name.has_value());
        ASSERT_TRUE(replica_name.has_value());
        ASSERT_TRUE(primary_city.has_value());
        ASSERT_TRUE(replica_city.has_value());

        EXPECT_EQ(primary_name.value(), "Khushi");
        EXPECT_EQ(replica_name.value(), "Khushi");

        EXPECT_EQ(primary_city.value(), "Delhi");
        EXPECT_EQ(replica_city.value(), "Delhi");
    }
}

TEST_F(ReplicaManagerTest, ReplicaCanBePromoted) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    EXPECT_FALSE(manager.isReplicaPromoted("name"));

    manager.promoteReplica("name");

    EXPECT_TRUE(manager.isReplicaPromoted("name"));

    auto result = manager.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Khushi");
}

TEST_F(ReplicaManagerTest, WritesContinueAfterPromotion) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    manager.promoteReplica("name");

    manager.set("name", "Rahul");

    auto result = manager.get("name");
    auto replica = manager.getReplica("name");

    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(replica.has_value());

    EXPECT_EQ(result.value(), "Rahul");
    EXPECT_EQ(replica.value(), "Rahul");
}

TEST_F(ReplicaManagerTest, DeleteWorksAfterPromotion) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");

    manager.promoteReplica("name");

    EXPECT_TRUE(manager.remove("name"));

    EXPECT_FALSE(manager.get("name").has_value());
    EXPECT_FALSE(manager.getReplica("name").has_value());
}

TEST_F(ReplicaManagerTest, PromotionIsPerShard) {
    ReplicaManager manager(3, wal_prefix);

    manager.set("name", "Khushi");
    manager.set("city", "Delhi");

    std::size_t name_shard =
        manager.getShard("name");

    std::size_t city_shard =
        manager.getShard("city");

    manager.promoteReplica("name");

    EXPECT_TRUE(manager.isReplicaPromoted("name"));

    if (name_shard != city_shard) {
        EXPECT_FALSE(manager.isReplicaPromoted("city"));
    }
}

TEST_F(ReplicaManagerTest, PromotedReplicaRecoversItsData) {
    {
        ReplicaManager manager(3, wal_prefix);

        manager.set("name", "Khushi");
        manager.promoteReplica("name");
    }

    {
        ReplicaManager manager(3, wal_prefix);

        EXPECT_FALSE(manager.isReplicaPromoted("name"));

        auto result = manager.get("name");
        auto replica = manager.getReplica("name");

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(replica.has_value());

        EXPECT_EQ(result.value(), "Khushi");
        EXPECT_EQ(replica.value(), "Khushi");
    }
}