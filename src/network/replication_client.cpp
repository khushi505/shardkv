#include "replication_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <string>

namespace {
constexpr int SOCKET_TIMEOUT_SECONDS = 3;

class SocketGuard {
public:
    explicit SocketGuard(int fd) : fd_(fd) {}

    ~SocketGuard() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    SocketGuard(const SocketGuard&) = delete;
    SocketGuard& operator=(const SocketGuard&) = delete;

    int get() const {
        return fd_;
    }

private:
    int fd_;
};

bool sendAll(int socket_fd, const std::string& data) {
    std::size_t total_sent = 0;

    while (total_sent < data.size()) {
        ssize_t bytes_sent = send(
            socket_fd,
            data.data() + total_sent,
            data.size() - total_sent,
            MSG_NOSIGNAL
        );

        if (bytes_sent < 0) {
            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (bytes_sent == 0) {
            return false;
        }

        total_sent += static_cast<std::size_t>(bytes_sent);
    }

    return true;
}
}

ReplicationClient::ReplicationClient(
    const std::string& host,
    int port
)
    : host_(host),
      port_(port) {}

bool ReplicationClient::sendSet(
    const std::string& key,
    const std::string& value
) {
    if (key.empty() || value.empty()) {
        return false;
    }

    return sendCommand(
        "SET " + key + " " + value + "\n"
    );
}

bool ReplicationClient::sendDelete(
    const std::string& key
) {
    if (key.empty()) {
        return false;
    }

    return sendCommand(
        "DELETE " + key + "\n"
    );
}

bool ReplicationClient::sendCommand(
    const std::string& command
) {
    if (
        host_.empty() ||
        port_ <= 0 ||
        port_ > 65535
    ) {
        return false;
    }

    int socket_fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (socket_fd < 0) {
        return false;
    }

    SocketGuard socket_guard(socket_fd);

    timeval timeout{};
    timeout.tv_sec = SOCKET_TIMEOUT_SECONDS;
    timeout.tv_usec = 0;

    if (
        setsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)
        ) < 0 ||
        setsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)
        ) < 0
    ) {
        return false;
    }

    sockaddr_in server_address{};

    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(
        static_cast<std::uint16_t>(port_)
    );

    if (
        inet_pton(
            AF_INET,
            host_.c_str(),
            &server_address.sin_addr
        ) != 1
    ) {
        return false;
    }

    if (
        connect(
            socket_fd,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)
        ) < 0
    ) {
        return false;
    }

    if (!sendAll(socket_fd, command)) {
        return false;
    }

    std::string response;
    char buffer[256];

    while (response.find('\n') == std::string::npos) {
        ssize_t bytes_received = recv(
            socket_fd,
            buffer,
            sizeof(buffer),
            0
        );

        if (bytes_received < 0) {
            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (bytes_received == 0) {
            return false;
        }

        response.append(
            buffer,
            static_cast<std::size_t>(bytes_received)
        );

        if (response.size() > 1024) {
            return false;
        }
    }

    return response == "OK\n";
}