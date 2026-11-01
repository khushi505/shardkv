
# ShardKV

**ShardKV** is a C++20 distributed key-value store built to explore the internals of storage engines, sharding, replication, TCP networking, concurrency, and distributed systems.

The project currently implements persistent local storage using Write-Ahead Logging (WAL), hash-based shard routing, primary-replica replication, and a concurrent TCP server.

The goal is to progressively build a reliable distributed storage system while understanding the engineering challenges behind modern databases.

---

## Features

### 1. Storage Engine
- In-memory key-value storage using `std::unordered_map`.
- Thread-safe read and write operations.
- Write-Ahead Logging (WAL).
- Recovery of stored operations from the WAL.
- Support for SET, GET, EXISTS, and DELETE operations.

### 2. Sharding
- Hash-based key distribution.
- Multiple local shard instances.
- Shard routing based on keys.
- Separation of storage and routing responsibilities.

### 3. Replication
- Primary and in-process replica storage.
- TCP-based remote replication.
- SET and DELETE replication commands.
- Replica promotion simulation.
- Thread-safe ReplicaManager operations.

### 4. TCP Networking
- TCP-based client-server communication.
- Multiple concurrent client connections.
- Dedicated client handler threads.
- Newline-delimited command protocol.
- Handling of multiple commands received in one TCP packet.
- Complete socket writes.
- Bounded incoming command buffering.
- Graceful server shutdown.
- Managed client threads instead of detached threads.

### 5. Testing
- GoogleTest-based unit testing.
- Concurrent storage operation tests.
- Replica manager tests.
- Replication client network tests.
- Real TCP server integration tests.
- Concurrent client and graceful shutdown coverage.

---

## Architecture

```text
                  TCP Clients
                       |
                       v
                +-------------+
                |   Server    |
                +-------------+
                       |
                       v
               +----------------+
               | ReplicaManager |
               +----------------+
                  |          |
                  v          v
               Primary    Replica
                  |
                  v
              ShardRouter
                  |
          +-------+-------+
          |       |       |
          v       v       v
        Shard 1 Shard 2 Shard 3
          |       |       |
          v       v       v
        KVStore KVStore KVStore
          |       |       |
          v       v       v
         WAL     WAL     WAL
```

Remote replication is handled through `ReplicationClient`, which communicates with another ShardKV server over TCP.

---

## Project Structure

```text
ShardKV/
│
├── src/
│   ├── cluster/
│   │   ├── replica_manager.cpp
│   │   ├── replica_manager.h
│   │   ├── shard_manager.cpp
│   │   ├── shard_manager.h
│   │   ├── shard_router.cpp
│   │   └── shard_router.h
│   │
│   ├── core/
│   │   ├── kv_store.cpp
│   │   ├── kv_store.h
│   │   ├── wal.cpp
│   │   └── wal.h
│   │
│   ├── network/
│   │   ├── replication_client.cpp
│   │   ├── replication_client.h
│   │   ├── server.cpp
│   │   └── server.h
│   │
│   └── main.cpp
│
├── tests/
│   ├── kv_store_test.cpp
│   ├── wal_test.cpp
│   ├── shard_router_test.cpp
│   ├── shard_manager_test.cpp
│   ├── replica_manager_test.cpp
│   ├── replication_client_test.cpp
│   ├── replica_manager_network_test.cpp
│   └── server_integration_test.cpp
│
├── CMakeLists.txt
└── README.md
```

---

## Requirements

- C++20 compatible compiler
- CMake 3.20+
- GoogleTest
- POSIX-compatible socket APIs

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/khushi505/shardkv.git
cd shardkv
```

### 2. Configure the project

```bash
cmake -S . -B build
```

### 3. Build

```bash
cmake --build build -j
```

---

## Running Tests

Run the complete test suite:

```bash
ctest --test-dir build --output-on-failure
```

Run the test executable directly:

```bash
./build/kvstore_tests
```

Run only server integration tests:

```bash
./build/kvstore_tests --gtest_filter="ServerIntegrationTest.*"
```

### Integration Test Coverage

The server integration tests currently cover:

| Test | Description |
|---|---|
| Concurrent clients | Handles eight simultaneous TCP clients |
| Idle-client shutdown | Shuts down while an idle client is connected |
| Multiple commands | Processes multiple newline-delimited commands in one TCP write |

The project also contains unit tests for storage, WAL, sharding, replication, and networking.

---

## Running the Server

Start the server using the default port:

```bash
./build/shardkv
```

Default configuration:

- Port: `8080`
- Shard count: `3`

Start on a custom port:

```bash
./build/shardkv 9090
```

Start with a remote replica:

```bash
./build/shardkv 8080 127.0.0.1 9091
```

Stop the server gracefully:

```text
Ctrl + C
```

---

## Client Protocol

ShardKV uses a newline-delimited TCP text protocol.

Each command must end with `\n`.

| Command | Description | Example |
|---|---|---|
| SET | Store a key-value pair | `SET name Khushi` |
| GET | Retrieve a value | `GET name` |
| EXISTS | Check whether a key exists | `EXISTS name` |
| DELETE | Delete a key | `DELETE name` |

### Example

Connect to the server using Netcat:

```bash
nc 127.0.0.1 8080
```

Execute commands:

```text
SET name Khushi
OK

GET name
Khushi

EXISTS name
YES

DELETE name
OK

GET name
NOT_FOUND
```

---

## Current Limitations

ShardKV is an evolving distributed systems project. The current implementation does not yet provide:

- Raft or another consensus protocol.
- Automatic failure detection and leader election.
- Consistent hashing.
- Dynamic shard rebalancing.
- Durable cluster membership and promotion state.
- Strong consistency guarantees across remote replicas.
- WAL checksums, log compaction, or explicit fsync durability.
- A bounded worker pool or production-grade connection management.

Replica promotion is currently simulated rather than driven by a consensus protocol.

---

## Roadmap

### Storage Engine
- [x] Thread-safe key-value operations.
- [x] Write-Ahead Logging.
- [x] WAL-based recovery.

### Sharding
- [x] Hash-based shard routing.
- [x] Multiple local shard instances.
- [ ] Consistent hashing.
- [ ] Dynamic shard rebalancing.

### Replication
- [x] Primary-replica management.
- [x] TCP replication client.
- [x] Replica promotion simulation.
- [ ] Reliable replication acknowledgements.
- [ ] Failure detection.
- [ ] Consensus-based leader election.

### Networking
- [x] TCP server.
- [x] Concurrent client handling.
- [x] Managed client threads.
- [x] Graceful shutdown.
- [x] Bounded incoming command buffering.
- [x] Multi-command TCP packet processing.
- [x] Real TCP integration tests.
- [ ] Bounded worker pool.
- [ ] Improved protocol framing and error handling.

### Reliability and Performance
- [ ] WAL durability improvements.
- [ ] Recovery from corrupted WAL records.
- [ ] Performance benchmarks.
- [ ] Logging and metrics.
- [ ] Docker-based multi-node deployment.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | C++20 |
| Build system | CMake |
| Networking | POSIX TCP sockets |
| Storage | `std::unordered_map` |
| Persistence | Write-Ahead Log |
| Testing | GoogleTest |
| Concurrency | C++ Threads and Mutexes |

---

## Learning Goals

ShardKV is a hands-on exploration of:

- How storage engines work internally.
- How databases distribute data across shards.
- How primary-replica replication works.
- How TCP servers handle concurrent clients.
- How to manage shared state safely with mutexes.
- How persistence and recovery work using WAL.
- How distributed systems handle failures and consistency.

---

**ShardKV — Building distributed systems, one component at a time.**
