#include <gtest/gtest.h>

#include "kv_store.h"

TEST(KVStoreTest, SetAndGet) {
    KVStore store;

    store.set("name", "Khushi");

    auto result = store.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Khushi");
}

TEST(KVStoreTest, GetMissingKey) {
    KVStore store;

    auto result = store.get("missing");

    EXPECT_FALSE(result.has_value());
}

TEST(KVStoreTest, SetUpdatesExistingKey) {
    KVStore store;

    store.set("name", "Khushi");
    store.set("name", "Rahul");

    auto result = store.get("name");

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), "Rahul");
}

TEST(KVStoreTest, RemoveKey) {
    KVStore store;

    store.set("name", "Khushi");

    EXPECT_TRUE(store.remove("name"));
    EXPECT_FALSE(store.exists("name"));
}

TEST(KVStoreTest, RemoveMissingKey) {
    KVStore store;

    EXPECT_FALSE(store.remove("missing"));
}

TEST(KVStoreTest, Exists) {
    KVStore store;

    EXPECT_FALSE(store.exists("name"));

    store.set("name", "Khushi");

    EXPECT_TRUE(store.exists("name"));
}