#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "kv_store.h"
#include "shard_router.h"

class ShardManager {
public:
    ShardManager(std::size_t shard_count, const std::string& wal_prefix);

    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool remove(const std::string& key);
    bool exists(const std::string& key) const;

    std::size_t getShard(const std::string& key) const;
    std::size_t shardCount() const;

private:
    ShardRouter router_;
    std::vector<std::unique_ptr<KVStore>> shards_;
};