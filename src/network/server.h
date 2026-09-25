#pragma once

#include <cstdint>

#include "shard_manager.h"

class Server {
public:
    Server(std::uint16_t port, std::size_t shard_count);

    void start();

private:
    void handleClient(int client_socket);

    std::uint16_t port_;
    ShardManager shard_manager_;
};