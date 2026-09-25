#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "kv_store.h"
#include "shard_router.h"

class ReplicaManager {
public:
    ReplicaManager(std::size_t shard_count, const std::string& wal_prefix);

    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool remove(const std::string& key);
    bool exists(const std::string& key) const;

    std::optional<std::string> getReplica(
        const std::string& key
    ) const;

    void promoteReplica(const std::string& key);

    bool isReplicaPromoted(const std::string& key) const;

    std::size_t getShard(const std::string& key) const;
    std::size_t shardCount() const;

private:
    ShardRouter router_;

    std::vector<std::unique_ptr<KVStore>> primary_shards_;
    std::vector<std::unique_ptr<KVStore>> replica_shards_;

    std::vector<bool> replica_promoted_;
};