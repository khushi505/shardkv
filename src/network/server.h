#pragma once

#include <cstdint>
#include <string>

#include "replica_manager.h"

class Server {
public:
    Server(
        std::uint16_t port,
        std::size_t shard_count,
        const std::string& replica_host = "",
        int replica_port = 0
    );

    void start();

private:
    void handleClient(int client_socket);

    std::uint16_t port_;

    ReplicaManager replica_manager_;
};