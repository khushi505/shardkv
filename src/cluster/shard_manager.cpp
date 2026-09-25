#include "shard_manager.h"

#include <string>

ShardManager::ShardManager(
    std::size_t shard_count,
    const std::string& wal_prefix
)
    : router_(shard_count) {

    for (std::size_t i = 0; i < shard_count; ++i) {
        std::string wal_path =
            wal_prefix + "_" + std::to_string(i) + ".log";

        shards_.push_back(
            std::make_unique<KVStore>(wal_path)
        );
    }
}

void ShardManager::set(
    const std::string& key,
    const std::string& value
) {
    std::size_t shard = router_.getShard(key);

    shards_[shard]->set(key, value);
}

std::optional<std::string> ShardManager::get(
    const std::string& key
) const {
    std::size_t shard = router_.getShard(key);

    return shards_[shard]->get(key);
}

bool ShardManager::remove(const std::string& key) {
    std::size_t shard = router_.getShard(key);

    return shards_[shard]->remove(key);
}

bool ShardManager::exists(const std::string& key) const {
    std::size_t shard = router_.getShard(key);

    return shards_[shard]->exists(key);
}

std::size_t ShardManager::getShard(const std::string& key) const {
    return router_.getShard(key);
}

std::size_t ShardManager::shardCount() const {
    return router_.shardCount();
}