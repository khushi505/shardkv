
#include "replica_manager.h"

#include <string>
#include <utility>

ReplicaManager::ReplicaManager(
    std::size_t shard_count,
    const std::string& wal_prefix,
    const std::string& replica_host,
    int replica_port
)
    : router_(shard_count),
      replica_promoted_(shard_count, 0),
      primary_healthy_(shard_count, 1) {

    if (!replica_host.empty() && replica_port > 0) {
        replication_client_ =
            std::make_unique<ReplicationClient>(
                replica_host,
                replica_port
            );
    }

    for (std::size_t i = 0; i < shard_count; ++i) {
        shard_mutexes_.push_back(
            std::make_unique<std::mutex>()
        );

        std::string primary_wal =
            wal_prefix +
            "_primary_" +
            std::to_string(i) +
            ".log";

        std::string replica_wal =
            wal_prefix +
            "_replica_" +
            std::to_string(i) +
            ".log";

        primary_shards_.push_back(
            std::make_unique<KVStore>(primary_wal)
        );

        replica_shards_.push_back(
            std::make_unique<KVStore>(replica_wal)
        );
    }
}

void ReplicaManager::set(
    const std::string& key,
    const std::string& value
) {
    setWithResult(key, value);
}

ReplicationResult ReplicaManager::setWithResult(
    const std::string& key,
    const std::string& value
) {
    const std::size_t shard = router_.getShard(key);

    // Serialize operations belonging to this shard.
    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    const bool use_replica =
        replica_promoted_[shard] ||
        !primary_healthy_[shard];

    if (use_replica) {
        replica_shards_[shard]->set(key, value);
        return {true, true};
    }

    // Update local primary and in-process replica.
    primary_shards_[shard]->set(key, value);
    replica_shards_[shard]->set(key, value);

    // The shard lock preserves ordering while network I/O happens.
    if (!replication_client_) {
        return {true, true};
    }

    const bool remote_success =
        replication_client_->sendSet(key, value);

    return {true, remote_success};
}

std::optional<std::string> ReplicaManager::get(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    if (
        replica_promoted_[shard] ||
        !primary_healthy_[shard]
    ) {
        return replica_shards_[shard]->get(key);
    }

    return primary_shards_[shard]->get(key);
}

std::optional<std::string> ReplicaManager::getReplica(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return replica_shards_[shard]->get(key);
}

bool ReplicaManager::remove(
    const std::string& key
) {
    const ReplicationResult result =
        removeWithResult(key);

    return result.local_success;
}

ReplicationResult ReplicaManager::removeWithResult(
    const std::string& key
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    const bool use_replica =
        replica_promoted_[shard] ||
        !primary_healthy_[shard];

    if (use_replica) {
        const bool removed =
            replica_shards_[shard]->remove(key);

        return {removed, removed};
    }

    const bool primary_removed =
        primary_shards_[shard]->remove(key);

    const bool replica_removed =
        replica_shards_[shard]->remove(key);

    const bool local_success =
        primary_removed || replica_removed;

    if (!local_success) {
        return {false, false};
    }

    if (!replication_client_) {
        return {true, true};
    }

    const bool remote_success =
        replication_client_->sendDelete(key);

    return {true, remote_success};
}

bool ReplicaManager::exists(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    if (
        replica_promoted_[shard] ||
        !primary_healthy_[shard]
    ) {
        return replica_shards_[shard]->exists(key);
    }

    return primary_shards_[shard]->exists(key);
}

void ReplicaManager::promoteReplica(
    const std::string& key
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    replica_promoted_[shard] = 1;
}

bool ReplicaManager::isReplicaPromoted(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return replica_promoted_[shard] != 0;
}

void ReplicaManager::simulatePrimaryFailure(
    const std::string& key
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    primary_healthy_[shard] = 0;
    replica_promoted_[shard] = 1;
}

bool ReplicaManager::isPrimaryHealthy(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return primary_healthy_[shard] != 0;
}

std::size_t ReplicaManager::getShard(
    const std::string& key
) const {
    return router_.getShard(key);
}

std::size_t ReplicaManager::shardCount() const {
    return router_.shardCount();
}
