#include <gtest/gtest.h>

#include <filesystem>

#include "wal.h"

class WALTest : public ::testing::Test {
protected:
    const std::string file_path = "test_wal.log";

    void SetUp() override {
        std::filesystem::remove(file_path);
    }

    void TearDown() override {
        std::filesystem::remove(file_path);
    }
};

TEST_F(WALTest, AppendSetAndReplay) {
    WAL wal(file_path);

    wal.appendSet("name", "Khushi");
    wal.appendSet("age", "22");

    auto operations = wal.replay();

    ASSERT_EQ(operations.size(), 2);
    EXPECT_EQ(operations[0], "SET|name|Khushi");
    EXPECT_EQ(operations[1], "SET|age|22");
}

TEST_F(WALTest, AppendDeleteAndReplay) {
    WAL wal(file_path);

    wal.appendSet("name", "Khushi");
    wal.appendDelete("name");

    auto operations = wal.replay();

    ASSERT_EQ(operations.size(), 2);
    EXPECT_EQ(operations[0], "SET|name|Khushi");
    EXPECT_EQ(operations[1], "DELETE|name");
}

TEST_F(WALTest, EmptyLogReturnsNoOperations) {
    WAL wal(file_path);

    auto operations = wal.replay();

    EXPECT_TRUE(operations.empty());
}