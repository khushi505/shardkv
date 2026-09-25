#include "replica_manager.h"

#include <string>

ReplicaManager::ReplicaManager(
    std::size_t shard_count,
    const std::string& wal_prefix,
    const std::string& replica_host,
    int replica_port
)
    : router_(shard_count),
      replica_promoted_(shard_count, false),
      primary_healthy_(shard_count, true) {

    if (!replica_host.empty() && replica_port > 0) {
        replication_client_ =
            std::make_unique<ReplicationClient>(
                replica_host,
                replica_port
            );
    }

    for (std::size_t i = 0; i < shard_count; ++i) {
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
    std::size_t shard = router_.getShard(key);

    if (replica_promoted_[shard] || !primary_healthy_[shard]) {
        replica_shards_[shard]->set(key, value);
        return;
    }

    primary_shards_[shard]->set(key, value);

    replica_shards_[shard]->set(key, value);

    if (replication_client_) {
        replication_client_->sendSet(key, value);
    }
}

std::optional<std::string> ReplicaManager::get(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    if (replica_promoted_[shard] || !primary_healthy_[shard]) {
        return replica_shards_[shard]->get(key);
    }

    return primary_shards_[shard]->get(key);
}

std::optional<std::string> ReplicaManager::getReplica(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return replica_shards_[shard]->get(key);
}

bool ReplicaManager::remove(
    const std::string& key
) {
    std::size_t shard = router_.getShard(key);

    if (replica_promoted_[shard] || !primary_healthy_[shard]) {
        return replica_shards_[shard]->remove(key);
    }

    bool primary_removed =
        primary_shards_[shard]->remove(key);

    bool replica_removed =
        replica_shards_[shard]->remove(key);

    if (replication_client_) {
        replication_client_->sendDelete(key);
    }

    return primary_removed || replica_removed;
}

bool ReplicaManager::exists(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    if (replica_promoted_[shard] || !primary_healthy_[shard]) {
        return replica_shards_[shard]->exists(key);
    }

    return primary_shards_[shard]->exists(key);
}

void ReplicaManager::promoteReplica(
    const std::string& key
) {
    std::size_t shard = router_.getShard(key);

    replica_promoted_[shard] = true;
}

bool ReplicaManager::isReplicaPromoted(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return replica_promoted_[shard];
}

void ReplicaManager::simulatePrimaryFailure(
    const std::string& key
) {
    std::size_t shard = router_.getShard(key);

    primary_healthy_[shard] = false;
    replica_promoted_[shard] = true;
}

bool ReplicaManager::isPrimaryHealthy(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return primary_healthy_[shard];
}

std::size_t ReplicaManager::getShard(
    const std::string& key
) const {
    return router_.getShard(key);
}

std::size_t ReplicaManager::shardCount() const {
    return router_.shardCount();
}