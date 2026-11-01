#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct ClusterPeerConfig {
    std::string node_id;
    std::string host;
    std::uint16_t port = 0;
};

enum class NodeHealth {
    UNKNOWN,
    HEALTHY,
    UNHEALTHY
};

struct NodeInfo {
    std::string node_id;
    std::string host;
    std::uint16_t port = 0;
    NodeHealth health = NodeHealth::UNKNOWN;
    std::chrono::steady_clock::time_point last_seen{};
};

class ClusterManager {
public:
    ClusterManager(
        std::string local_node_id,
        std::uint16_t local_port,
        std::vector<ClusterPeerConfig> peers = {},
        std::chrono::milliseconds heartbeat_interval =
            std::chrono::milliseconds(1000),
        std::chrono::milliseconds heartbeat_timeout =
            std::chrono::milliseconds(3500)
    );

    ~ClusterManager();

    ClusterManager(const ClusterManager&) = delete;
    ClusterManager& operator=(const ClusterManager&) = delete;

    void start();
    void stop();

    bool recordHeartbeat(const std::string& sender_node_id);

    std::vector<NodeInfo> members() const;

    bool isNodeHealthy(const std::string& node_id) const;

    std::size_t healthyNodeCount() const;

    const std::string& localNodeId() const;

    std::uint16_t localPort() const;

private:
    void heartbeatLoop();

    bool sendHeartbeat(const ClusterPeerConfig& peer);

    std::string local_node_id_;
    std::uint16_t local_port_;

    std::vector<NodeInfo> members_;

    std::chrono::milliseconds heartbeat_interval_;
    std::chrono::milliseconds heartbeat_timeout_;

    mutable std::mutex members_mutex_;
    std::mutex lifecycle_mutex_;
    std::mutex worker_mutex_;

    std::condition_variable worker_cv_;

    bool running_ = false;
    bool stop_requested_ = false;

    std::thread worker_;
};