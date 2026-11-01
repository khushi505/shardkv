#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "kv_store.h"
#include "replication_client.h"
#include "shard_router.h"

struct ReplicationResult {
    bool local_success = false;
    bool replica_success = false;

    bool success() const {
        return local_success && replica_success;
    }
};

class ReplicaManager {
public:
    ReplicaManager(
        std::size_t shard_count,
        const std::string& wal_prefix,
        const std::string& replica_host = "",
        int replica_port = 0,
        std::chrono::milliseconds retry_interval =
            std::chrono::milliseconds(1000)
    );

    ~ReplicaManager();

    ReplicaManager(const ReplicaManager&) = delete;
    ReplicaManager& operator=(const ReplicaManager&) = delete;

    void set(const std::string& key, const std::string& value);

    ReplicationResult setWithResult(
        const std::string& key,
        const std::string& value
    );

    std::optional<std::string> get(const std::string& key) const;

    bool remove(const std::string& key);

    ReplicationResult removeWithResult(const std::string& key);

    bool exists(const std::string& key) const;

    std::optional<std::string> getReplica(
        const std::string& key
    ) const;

    void promoteReplica(const std::string& key);

    bool isReplicaPromoted(const std::string& key) const;

    void simulatePrimaryFailure(const std::string& key);

    bool isPrimaryHealthy(const std::string& key) const;

    std::size_t getShard(const std::string& key) const;

    std::size_t shardCount() const;

    std::size_t retryPendingReplications();

    std::size_t pendingReplicationCount() const;

private:
    struct PendingOperation {
        enum class Type {
            SET,
            DELETE
        };

        Type type;
        std::string key;
        std::string value;
    };

    bool replicateOperation(const PendingOperation& operation);

    bool persistPendingOperation(
        std::size_t shard,
        const PendingOperation& operation
    ) const;

    bool persistPendingAcknowledgement(std::size_t shard) const;

    void loadPendingOperations();

    std::string pendingLogPath(std::size_t shard) const;

    static std::string encodeHex(const std::string& value);

    static bool decodeHex(
        const std::string& encoded,
        std::string& decoded
    );

    void startRetryWorker();
    void stopRetryWorker();
    void retryWorkerLoop();

    ShardRouter router_;

    std::string wal_prefix_;

    std::vector<std::unique_ptr<KVStore>> primary_shards_;
    std::vector<std::unique_ptr<KVStore>> replica_shards_;

    std::vector<std::unique_ptr<std::mutex>> shard_mutexes_;

    std::vector<std::deque<PendingOperation>> pending_operations_;

    std::vector<std::uint8_t> replica_promoted_;
    std::vector<std::uint8_t> primary_healthy_;

    std::unique_ptr<ReplicationClient> replication_client_;

    std::thread retry_thread_;
    std::mutex retry_mutex_;
    std::condition_variable retry_cv_;

    bool stop_retry_worker_ = false;

    std::chrono::milliseconds retry_interval_;
};