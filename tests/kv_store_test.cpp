#include <cstdio>
#include <gtest/gtest.h>

#include "kv_store.h"

class KVStoreTest : public ::testing::Test {
protected:
    const std::string wal_file = "test_wal.log";

    void SetUp() override {
        std::remove(wal_file.c_str());
    }

    void TearDown() override {
        std::remove(wal_file.c_str());
    }
};

TEST_F(KVStoreTest, SetAndGet) {
    KVStore store(wal_file);

    store.set("name", "Khushi");

    auto result = store.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Khushi");
}

TEST_F(KVStoreTest, GetMissingKey) {
    KVStore store(wal_file);

    auto result = store.get("missing");

    EXPECT_FALSE(result.has_value());
}

TEST_F(KVStoreTest, SetUpdatesExistingKey) {
    KVStore store(wal_file);

    store.set("name", "Khushi");
    store.set("name", "Rahul");

    auto result = store.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Rahul");
}

TEST_F(KVStoreTest, RemoveKey) {
    KVStore store(wal_file);

    store.set("name", "Khushi");

    EXPECT_TRUE(store.remove("name"));
    EXPECT_FALSE(store.exists("name"));
}

TEST_F(KVStoreTest, RemoveMissingKey) {
    KVStore store(wal_file);

    EXPECT_FALSE(store.remove("missing"));
}

TEST_F(KVStoreTest, Exists) {
    KVStore store(wal_file);

    EXPECT_FALSE(store.exists("name"));

    store.set("name", "Khushi");

    EXPECT_TRUE(store.exists("name"));
}

TEST_F(KVStoreTest, RecoversDataFromWAL) {
    {
        KVStore store(wal_file);

        store.set("name", "Khushi");
        store.set("city", "Delhi");
    }

    {
        KVStore store(wal_file);

        auto name = store.get("name");
        auto city = store.get("city");

        ASSERT_TRUE(name.has_value());
        ASSERT_TRUE(city.has_value());

        EXPECT_EQ(name.value(), "Khushi");
        EXPECT_EQ(city.value(), "Delhi");
    }
}