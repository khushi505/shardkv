#pragma once

#include <cstddef>
#include <string>

class ShardRouter {
public:
    explicit ShardRouter(std::size_t shard_count);

    std::size_t getShard(const std::string& key) const;
    std::size_t shardCount() const;

private:
    std::size_t shard_count_;
};