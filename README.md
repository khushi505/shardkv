
# ShardKV

**A Distributed Key-Value Store Built from Scratch in C++**

ShardKV is a systems programming project focused on building a distributed key-value storage engine using C++20. It explores the core concepts behind databases and distributed systems, including in-memory storage, Write-Ahead Logging (WAL), TCP networking, data sharding, replication, concurrency, and failure handling.

The project is being developed incrementally, with an emphasis on understanding the engineering decisions behind each component.

---

## Features

### 1. Key-Value Storage Engine
- In-memory storage using `std::unordered_map`.
- Supports `SET`, `GET`, `DELETE`, and `EXISTS`.
- Persistent storage through Write-Ahead Logging.
- WAL-based recovery after application restart.

### 2. Write-Ahead Logging (WAL)
- Records write operations before updating in-memory state.
- Replays persisted operations during initialization.
- Supports recovery of stored data after restart.
- Maintains separate WAL files for primary and replica shards.

### 3. TCP Networking
- Custom text-based request/response protocol.
- TCP server using POSIX sockets.
- Handles multiple client connections.
- Supports multiple commands over a connection.
- Handles partial socket reads and writes.
- Graceful server shutdown and client connection cleanup.

### 4. Sharding
- Partitions data across multiple logical shards.
- Routes keys to shards using a hash-based router.
- Maintains independent primary and replica stores for each shard.

### 5. Replication
- Maintains primary and replica copies of shard data.
- Supports local replication between in-process stores.
- Supports remote replication over TCP.
- Validates acknowledgements from remote replicas.
- Supports `SET` and `DELETE` replication commands.
- Exposes replication results through `ReplicationResult`.
- Reports local operation success separately from replica replication success.

### 6. Concurrency
- Supports concurrent client operations.
- Uses independent synchronization locks for individual shards.
- Serializes operations within the same shard.
- Allows operations on different shards to execute concurrently.
- Keeps the shard lock during remote replication to preserve write ordering.
- Synchronizes reads, deletes, and replica promotion with shard operations.

### 7. Replica Promotion
- Supports simulated primary failure.
- Supports manual replica promotion.
- Routes operations to the promoted replica.
- Maintains promotion state independently for each shard.

---

## Architecture

```text
                    Client
                       |
                       v
                +--------------+
                | TCP Server   |
                +--------------+
                       |
                       v
                +--------------+
                | Command      |
                | Processing   |
                +--------------+
                       |
                       v
                +--------------+
                | Replica      |
                | Manager      |
                +--------------+
                       |
             +---------+---------+
             |                   |
             v                   v
        Shard Router        Replication
             |                 Client
       +-----+-----+               |
       |     |     |               v
       v     v     v          Remote Replica
     Shard Shard Shard
       0     1     2
       |     |     |
       v     v     v
    Primary Primary Primary
       |     |     |
    Replica Replica Replica
       |     |     |
       v     v     v
      WAL   WAL   WAL
```

### Replication Flow

For a normal primary write:

1. The client sends a command to the TCP server.
2. The request is processed by the ReplicaManager.
3. The ShardRouter identifies the responsible shard.
4. The primary shard is updated.
5. The in-process replica shard is updated.
6. The ReplicationClient sends the operation to the configured remote replica.
7. The remote response is validated.
8. The operation result reports local and remote replication status.

### Per-Shard Synchronization

Each shard has its own mutex.

This design provides:

- Serialized operations within the same shard.
- Concurrent operations across different shards.
- Ordered remote replication for operations within the same shard.
- Synchronization between writes, reads, deletes, and promotion operations.

Remote network operations are performed without holding a global manager lock.

---

## Project Structure

```text
shardkv/
|
├── CMakeLists.txt
├── README.md
|
├── src/
│   ├── main.cpp
│   |
│   ├── core/
│   │   ├── kv_store.h
│   │   ├── kv_store.cpp
│   │   ├── wal.h
│   │   └── wal.cpp
│   |
│   ├── network/
│   │   ├── server.h
│   │   ├── server.cpp
│   │   ├── replication_client.h
│   │   └── replication_client.cpp
│   |
│   └── cluster/
│       ├── shard_router.h
│       ├── shard_router.cpp
│       ├── shard_manager.h
│       ├── shard_manager.cpp
│       ├── replica_manager.h
│       └── replica_manager.cpp
|
└── tests/
    ├── kv_store_test.cpp
    ├── wal_test.cpp
    ├── shard_router_test.cpp
    ├── shard_manager_test.cpp
    ├── replica_manager_test.cpp
    ├── replication_client_test.cpp
    ├── replica_manager_network_test.cpp
    └── server_integration_test.cpp
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | C++20 |
| Build System | CMake |
| Networking | POSIX TCP Sockets |
| Storage | `std::unordered_map` |
| Persistence | Write-Ahead Logging |
| Concurrency | C++ Threads, Mutexes |
| Testing | GoogleTest |
| Version Control | Git and GitHub |

---

## Getting Started

### Prerequisites

- C++20 compatible compiler
- CMake
- Git
- GoogleTest (configured through the project build)

### Clone the Repository

```bash
git clone https://github.com/khushi505/shardkv.git
cd shardkv
```

### Build

```bash
cmake -S . -B build
cmake --build build -j
```

### Run Tests

```bash
ctest --test-dir build --output-on-failure
```

### Run the Server

```bash
./build/shardkv
```

The server starts listening on its configured port.

---

## Supported Commands

ShardKV supports the following key-value operations:

| Command | Description |
|---|---|
| `SET key value` | Store or update a key |
| `GET key` | Retrieve a value |
| `DELETE key` | Delete a key |
| `EXISTS key` | Check whether a key exists |

### Example

```text
SET name Khushi
OK

GET name
Khushi

SET city Delhi
OK

GET city
Delhi

EXISTS name
YES

DELETE name
OK

GET name
NOT_FOUND
```

---

## Replication API

ReplicaManager provides result-reporting methods:

```cpp
ReplicationResult setWithResult(
    const std::string& key,
    const std::string& value
);

ReplicationResult removeWithResult(
    const std::string& key
);
```

The result structure:

```cpp
struct ReplicationResult {
    bool local_success = false;
    bool replica_success = false;

    bool success() const {
        return local_success && replica_success;
    }
};
```

This separates local storage success from remote replication success.

A successful local write does not necessarily mean that the remote replica received the operation.

---

## Testing

The project uses GoogleTest for unit, integration, networking, and concurrency tests.

Current test coverage includes:

- Key-value storage operations.
- WAL persistence and recovery.
- Shard routing and management.
- Primary and replica synchronization.
- Replica promotion.
- Concurrent writes and mixed operations.
- TCP replication.
- Replication server failures.
- TCP server integration.
- Graceful server behavior.

**Current test status: 45 tests passing.**

Run the complete suite:

```bash
ctest --test-dir build --output-on-failure
```

---

## Current Limitations

ShardKV is an evolving distributed-systems learning project. The following capabilities are not yet implemented:

- Automatic retries for failed remote replication.
- Persistent replication queues.
- Automatic leader election.
- Distributed consensus using Raft.
- Split-brain prevention.
- Replication recovery and reconciliation after network failures.
- Production-grade cluster membership and rebalancing.

Replica promotion is currently simulated rather than coordinated through a consensus protocol.

---

## Roadmap

| Phase | Feature | Status |
|---|---|---|
| 1 | In-memory Key-Value Store | Completed |
| 2 | Write-Ahead Logging | Completed |
| 3 | TCP Server and Networking | Completed |
| 4 | Sharding and Replica Management | Completed |
| 5 | Replication Reliability and Recovery | In Progress |
| 6 | Automatic Failover | Planned |
| 7 | Raft Consensus | Planned |
| 8 | Performance Benchmarking | Planned |
| 9 | Failure Testing | Planned |
| 10 | Docker-Based Multi-Node Deployment | Planned |

---

## Learning Objectives

ShardKV is designed to explore practical systems engineering concepts:

- Database internals and storage engines.
- Hash-based partitioning.
- Write-Ahead Logging and crash recovery.
- TCP networking and protocol design.
- Multithreading and synchronization.
- Primary-replica replication.
- Failure handling and distributed consistency.
- System performance and reliability.

---

## Future Improvements

- Implement a persistent replication queue.
- Add retry and reconciliation mechanisms.
- Introduce replication sequence numbers.
- Implement automatic leader election.
- Integrate the Raft consensus algorithm.
- Add cluster membership management.
- Benchmark throughput and tail latency.
- Support Docker-based multi-node deployment.

---

## Author

**Khushi Singh**

GitHub: [@khushi505](https://github.com/khushi505)

Repository: [ShardKV](https://github.com/khushi505/shardkv)

---

*ShardKV is an ongoing project built to understand and implement the fundamentals of distributed databases and systems programming from scratch.*
