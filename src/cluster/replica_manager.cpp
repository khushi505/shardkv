#include "replica_manager.h"

#include <string>

ReplicaManager::ReplicaManager(
    std::size_t shard_count,
    const std::string& wal_prefix
)
    : router_(shard_count) {

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

    primary_shards_[shard]->set(key, value);
    replica_shards_[shard]->set(key, value);
}

std::optional<std::string> ReplicaManager::get(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return primary_shards_[shard]->get(key);
}

std::optional<std::string> ReplicaManager::getReplica(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return replica_shards_[shard]->get(key);
}

bool ReplicaManager::remove(const std::string& key) {
    std::size_t shard = router_.getShard(key);

    bool primary_removed =
        primary_shards_[shard]->remove(key);

    bool replica_removed =
        replica_shards_[shard]->remove(key);

    return primary_removed || replica_removed;
}

bool ReplicaManager::exists(const std::string& key) const {
    std::size_t shard = router_.getShard(key);

    return primary_shards_[shard]->exists(key);
}

std::size_t ReplicaManager::getShard(
    const std::string& key
) const {
    return router_.getShard(key);
}

std::size_t ReplicaManager::shardCount() const {
    return router_.shardCount();
}