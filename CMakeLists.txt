cmake_minimum_required(VERSION 3.20)

project(ShardKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(kvstore
    src/core/kv_store.cpp
    src/core/wal.cpp
)

target_include_directories(kvstore PUBLIC
    src/core
)

add_executable(shardkv
    src/main.cpp
)

target_link_libraries(shardkv PRIVATE
    kvstore
)

enable_testing()

find_package(GTest REQUIRED)

add_executable(kvstore_tests
    tests/kv_store_test.cpp
    tests/wal_test.cpp
)

target_link_libraries(kvstore_tests PRIVATE
    kvstore
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(kvstore_tests)