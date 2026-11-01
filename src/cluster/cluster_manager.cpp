#include "cluster_manager.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cctype>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

bool validNodeId(const std::string& value) {
    if (value.empty() || value.size() > 128) {
        return false;
    }

    for (unsigned char ch : value) {
        if (std::isspace(ch) || ch == '\n' || ch == '\r') {
            return false;
        }
    }

    return true;
}

bool sendAll(int fd, const std::string& data) {
    std::size_t offset = 0;

    while (offset < data.size()) {
#ifdef MSG_NOSIGNAL
        constexpr int flags = MSG_NOSIGNAL;
#else
        constexpr int flags = 0;
#endif

        const ssize_t sent = send(
            fd,
            data.data() + offset,
            data.size() - offset,
            flags
        );

        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (sent == 0) {
            return false;
        }

        offset += static_cast<std::size_t>(sent);
    }

    return true;
}

bool receiveLine(int fd, std::string& line) {
    char buffer[128];

    while (
        line.size() <= 256 &&
        line.find('\n') == std::string::npos
    ) {
        const ssize_t received = recv(
            fd,
            buffer,
            sizeof(buffer),
            0
        );

        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (received == 0) {
            return false;
        }

        line.append(
            buffer,
            static_cast<std::size_t>(received)
        );
    }

    return line.size() <= 256 &&
           line.back() == '\n' &&
           line.find('\n') == line.size() - 1;
}

} // namespace

ClusterManager::ClusterManager(
    std::string local_node_id,
    std::uint16_t local_port,
    std::vector<ClusterPeerConfig> peers,
    std::chrono::milliseconds heartbeat_interval,
    std::chrono::milliseconds heartbeat_timeout
)
    : local_node_id_(std::move(local_node_id)),
      local_port_(local_port),
      heartbeat_interval_(heartbeat_interval),
      heartbeat_timeout_(heartbeat_timeout) {

    if (!validNodeId(local_node_id_)) {
        throw std::invalid_argument("Invalid local node ID");
    }

    if (
        heartbeat_interval_.count() <= 0 ||
        heartbeat_timeout_ <= heartbeat_interval_
    ) {
        throw std::invalid_argument(
            "Heartbeat timeout must be greater than a positive heartbeat interval"
        );
    }

    for (const auto& peer : peers) {
        in_addr parsed_address{};

        if (
            !validNodeId(peer.node_id) ||
            peer.node_id == local_node_id_ ||
            peer.port == 0 ||
            inet_pton(
                AF_INET,
                peer.host.c_str(),
                &parsed_address
            ) != 1
        ) {
            throw std::invalid_argument(
                "Invalid cluster peer configuration (IPv4 address required)"
            );
        }

        for (const auto& existing : members_) {
            if (existing.node_id == peer.node_id) {
                throw std::invalid_argument(
                    "Duplicate cluster peer ID"
                );
            }

            if (
                existing.host == peer.host &&
                existing.port == peer.port
            ) {
                throw std::invalid_argument(
                    "Duplicate cluster peer endpoint"
                );
            }
        }

        members_.push_back(
            NodeInfo{
                peer.node_id,
                peer.host,
                peer.port,
                NodeHealth::UNKNOWN,
                {}
            }
        );
    }
}

ClusterManager::~ClusterManager() {
    stop();
}

void ClusterManager::start() {
    std::lock_guard<std::mutex> lifecycle_lock(
        lifecycle_mutex_
    );

    std::lock_guard<std::mutex> lock(worker_mutex_);

    if (running_) {
        return;
    }

    stop_requested_ = false;
    running_ = true;

    worker_ = std::thread(
        &ClusterManager::heartbeatLoop,
        this
    );
}

void ClusterManager::stop() {
    std::lock_guard<std::mutex> lifecycle_lock(
        lifecycle_mutex_
    );

    {
        std::lock_guard<std::mutex> lock(worker_mutex_);

        if (!running_) {
            return;
        }

        stop_requested_ = true;
    }

    worker_cv_.notify_all();

    if (worker_.joinable()) {
        worker_.join();
    }

    std::lock_guard<std::mutex> lock(worker_mutex_);

    running_ = false;
}

bool ClusterManager::recordHeartbeat(
    const std::string& sender_node_id
) {
    if (
        !validNodeId(sender_node_id) ||
        sender_node_id == local_node_id_
    ) {
        return false;
    }

    std::lock_guard<std::mutex> lock(members_mutex_);

    for (auto& member : members_) {
        if (member.node_id == sender_node_id) {
            member.last_seen =
                std::chrono::steady_clock::now();

            member.health = NodeHealth::HEALTHY;

            return true;
        }
    }

    return false;
}

std::vector<NodeInfo> ClusterManager::members() const {
    std::lock_guard<std::mutex> lock(members_mutex_);

    auto copy = members_;

    const auto now = std::chrono::steady_clock::now();

    for (auto& member : copy) {
        if (
            member.last_seen ==
            std::chrono::steady_clock::time_point{}
        ) {
            member.health = NodeHealth::UNKNOWN;
        } else if (
            now - member.last_seen > heartbeat_timeout_
        ) {
            member.health = NodeHealth::UNHEALTHY;
        }
    }

    return copy;
}

bool ClusterManager::isNodeHealthy(
    const std::string& node_id
) const {
    const auto snapshot = members();

    for (const auto& member : snapshot) {
        if (member.node_id == node_id) {
            return member.health == NodeHealth::HEALTHY;
        }
    }

    return false;
}

std::size_t ClusterManager::healthyNodeCount() const {
    const auto snapshot = members();

    std::size_t count = 1;

    for (const auto& member : snapshot) {
        if (member.health == NodeHealth::HEALTHY) {
            ++count;
        }
    }

    return count;
}

const std::string& ClusterManager::localNodeId() const {
    return local_node_id_;
}

std::uint16_t ClusterManager::localPort() const {
    return local_port_;
}

bool ClusterManager::sendHeartbeat(
    const ClusterPeerConfig& peer
) {
    const int fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (fd < 0) {
        return false;
    }

    timeval timeout{};

    timeout.tv_sec = 1;

    setsockopt(
        fd,
        SOL_SOCKET,
        SO_RCVTIMEO,
        &timeout,
        sizeof(timeout)
    );

    setsockopt(
        fd,
        SOL_SOCKET,
        SO_SNDTIMEO,
        &timeout,
        sizeof(timeout)
    );

    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_port = htons(peer.port);

    if (
        inet_pton(
            AF_INET,
            peer.host.c_str(),
            &address.sin_addr
        ) != 1
    ) {
        close(fd);
        return false;
    }

    const int original_flags = fcntl(
        fd,
        F_GETFL,
        0
    );

    if (
        original_flags < 0 ||
        fcntl(
            fd,
            F_SETFL,
            original_flags | O_NONBLOCK
        ) < 0
    ) {
        close(fd);
        return false;
    }

    int connect_result = connect(
        fd,
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address)
    );

    if (
        connect_result < 0 &&
        errno == EINPROGRESS
    ) {
        pollfd descriptor{};

        descriptor.fd = fd;
        descriptor.events = POLLOUT;

        do {
            connect_result = poll(
                &descriptor,
                1,
                1000
            );
        } while (
            connect_result < 0 &&
            errno == EINTR
        );

        if (connect_result > 0) {
            int socket_error = 0;

            socklen_t error_length =
                sizeof(socket_error);

            if (
                getsockopt(
                    fd,
                    SOL_SOCKET,
                    SO_ERROR,
                    &socket_error,
                    &error_length
                ) < 0 ||
                socket_error != 0
            ) {
                connect_result = -1;
            } else {
                connect_result = 0;
            }
        } else {
            connect_result = -1;
        }
    }

    if (
        connect_result < 0 ||
        fcntl(
            fd,
            F_SETFL,
            original_flags
        ) < 0
    ) {
        close(fd);
        return false;
    }

    const bool sent = sendAll(
        fd,
        "PING " + local_node_id_ + "\n"
    );

    std::string response;

    const bool received =
        sent && receiveLine(fd, response);

    shutdown(fd, SHUT_RDWR);
    close(fd);

    const std::string expected =
        "PONG " + peer.node_id + "\n";

    if (!received || response != expected) {
        return false;
    }

    std::lock_guard<std::mutex> lock(members_mutex_);

    for (auto& member : members_) {
        if (member.node_id == peer.node_id) {
            member.last_seen =
                std::chrono::steady_clock::now();

            member.health = NodeHealth::HEALTHY;

            return true;
        }
    }

    return false;
}

void ClusterManager::heartbeatLoop() {
    std::vector<ClusterPeerConfig> peers;

    {
        std::lock_guard<std::mutex> lock(members_mutex_);

        for (const auto& member : members_) {
            peers.push_back({
                member.node_id,
                member.host,
                member.port
            });
        }
    }

    std::unique_lock<std::mutex> lock(worker_mutex_);

    while (!stop_requested_) {
        lock.unlock();

        for (const auto& peer : peers) {
            {
                std::lock_guard<std::mutex> state_lock(
                    worker_mutex_
                );

                if (stop_requested_) {
                    break;
                }
            }

            if (!sendHeartbeat(peer)) {
                std::lock_guard<std::mutex> members_lock(
                    members_mutex_
                );

                for (auto& member : members_) {
                    if (
                        member.node_id == peer.node_id &&
                        (
                            member.last_seen ==
                                std::chrono::steady_clock::time_point{} ||
                            std::chrono::steady_clock::now() -
                                member.last_seen > heartbeat_timeout_
                        )
                    ) {
                        member.health = NodeHealth::UNHEALTHY;
                    }
                }
            }
        }

        lock.lock();

        worker_cv_.wait_for(
            lock,
            heartbeat_interval_,
            [this] {
                return stop_requested_;
            }
        );
    }
}