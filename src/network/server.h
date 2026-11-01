#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "replica_manager.h"

class Server {
public:
    Server(
        std::uint16_t port,
        std::size_t shard_count,
        const std::string& replica_host = "",
        int replica_port = 0
    );

    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();
    void stop();

    std::uint16_t port() const;

private:
    void handleClient(int client_socket);
    void joinClientThreads();

    std::uint16_t port_;

    std::atomic<bool> running_{false};
    std::atomic<int> server_socket_{-1};

    mutable std::mutex clients_mutex_;
    std::unordered_set<int> client_sockets_;
    std::vector<std::thread> client_threads_;

    ReplicaManager replica_manager_;
};