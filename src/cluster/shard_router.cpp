#include "shard_router.h"

#include <functional>
#include <stdexcept>

ShardRouter::ShardRouter(std::size_t shard_count)
    : shard_count_(shard_count) {
    if (shard_count == 0) {
        throw std::invalid_argument("Shard count must be greater than zero");
    }
}

std::size_t ShardRouter::getShard(const std::string& key) const {
    return std::hash<std::string>{}(key) % shard_count_;
}

std::size_t ShardRouter::shardCount() const {
    return shard_count_;
}