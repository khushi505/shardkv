cmake_minimum_required(VERSION 3.20)

project(ShardKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)

add_library(kvstore
    src/core/kv_store.cpp
    src/core/wal.cpp
)

target_include_directories(kvstore PUBLIC
    src/core
)

target_link_libraries(kvstore PUBLIC
    Threads::Threads
)

add_library(shardrouter
    src/cluster/shard_router.cpp
)

target_include_directories(shardrouter PUBLIC
    src/cluster
)

add_library(shardmanager
    src/cluster/shard_manager.cpp
)

target_include_directories(shardmanager PUBLIC
    src/cluster
)

target_link_libraries(shardmanager PUBLIC
    kvstore
    shardrouter
)

add_library(replicamanager
    src/cluster/replica_manager.cpp
    src/network/replication_client.cpp
)

target_include_directories(replicamanager PUBLIC
    src/cluster
    src/network
)

target_link_libraries(replicamanager PUBLIC
    kvstore
    shardrouter
    Threads::Threads
)

add_library(shardserver
    src/network/server.cpp
    src/cluster/cluster_manager.cpp
)

target_include_directories(shardserver PUBLIC
    src/network
    src/cluster
)

target_link_libraries(shardserver PUBLIC
    replicamanager
    Threads::Threads
)

add_executable(shardkv
    src/main.cpp
)

target_link_libraries(shardkv PRIVATE
    shardserver
)

enable_testing()

add_executable(kvstore_tests
    tests/kv_store_test.cpp
    tests/wal_test.cpp
    tests/shard_router_test.cpp
    tests/shard_manager_test.cpp
    tests/replica_manager_test.cpp
    tests/replication_client_test.cpp
    tests/replica_manager_network_test.cpp
    tests/server_integration_test.cpp
    tests/cluster_manager_test.cpp
)

target_include_directories(kvstore_tests PRIVATE
    src/cluster
    src/network
)

target_link_libraries(kvstore_tests PRIVATE
    kvstore
    shardrouter
    shardmanager
    replicamanager
    shardserver
    GTest::gtest_main
    Threads::Threads
)

include(GoogleTest)

gtest_discover_tests(kvstore_tests)