
#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>

namespace {

constexpr std::size_t MAX_PENDING_DATA = 64 * 1024;
constexpr std::size_t READ_BUFFER_SIZE = 4096;

bool sendAll(int socket_fd, const std::string& data) {
    std::size_t total_sent = 0;

    while (total_sent < data.size()) {
#ifdef MSG_NOSIGNAL
        constexpr int SEND_FLAGS = MSG_NOSIGNAL;
#else
        constexpr int SEND_FLAGS = 0;
#endif

        const ssize_t bytes_sent = send(
            socket_fd,
            data.data() + total_sent,
            data.size() - total_sent,
            SEND_FLAGS
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

std::string processCommand(
    const std::string& command,
    ReplicaManager& replica_manager
) {
    std::stringstream stream(command);

    std::string operation;
    std::string key;
    std::string value;
    std::string extra;

    stream >> operation >> key;

    if (operation.empty() || key.empty()) {
        return "ERROR\n";
    }

    if (operation == "SET") {
        stream >> value;

        if (value.empty() || (stream >> extra)) {
            return "ERROR\n";
        }

        replica_manager.set(key, value);
        return "OK\n";
    }

    if (operation == "GET") {
        if (stream >> extra) {
            return "ERROR\n";
        }

        const auto result = replica_manager.get(key);

        return result.has_value()
            ? result.value() + "\n"
            : "NOT_FOUND\n";
    }

    if (operation == "EXISTS") {
        if (stream >> extra) {
            return "ERROR\n";
        }

        return replica_manager.exists(key)
            ? "YES\n"
            : "NO\n";
    }

    if (operation == "DELETE") {
        if (stream >> extra) {
            return "ERROR\n";
        }

        return replica_manager.remove(key)
            ? "OK\n"
            : "NOT_FOUND\n";
    }

    return "ERROR\n";
}

}  // namespace

Server::Server(
    std::uint16_t port,
    std::size_t shard_count,
    const std::string& replica_host,
    int replica_port
)
    : port_(port),
      replica_manager_(
          shard_count,
          "shardkv_" + std::to_string(port),
          replica_host,
          replica_port
      ) {}

Server::~Server() {
    stop();
}

std::uint16_t Server::port() const {
    return port_;
}

void Server::start() {
    bool expected = false;

    if (!running_.compare_exchange_strong(expected, true)) {
        throw std::runtime_error("Server is already running");
    }

    const int listening_socket = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (listening_socket < 0) {
        running_ = false;
        throw std::runtime_error("Failed to create server socket");
    }

    server_socket_ = listening_socket;

    int option = 1;

    if (setsockopt(
            listening_socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            &option,
            sizeof(option)
        ) < 0) {
        stop();
        throw std::runtime_error("Failed to set SO_REUSEADDR");
    }

    sockaddr_in server_address{};

    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port_);

    if (bind(
            listening_socket,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)
        ) < 0) {
        stop();
        throw std::runtime_error("Failed to bind server socket");
    }

    if (listen(listening_socket, 10) < 0) {
        stop();
        throw std::runtime_error("Failed to listen on server socket");
    }

    sockaddr_in bound_address{};
    socklen_t address_length = sizeof(bound_address);

    if (getsockname(
            listening_socket,
            reinterpret_cast<sockaddr*>(&bound_address),
            &address_length
        ) < 0) {
        stop();
        throw std::runtime_error("Failed to retrieve server port");
    }

    port_ = ntohs(bound_address.sin_port);

    std::cout
        << "ShardKV server listening on port "
        << port_
        << " with "
        << replica_manager_.shardCount()
        << " shards"
        << std::endl;

    while (running_) {
        sockaddr_in client_address{};
        socklen_t client_length = sizeof(client_address);

        const int client_socket = accept(
            listening_socket,
            reinterpret_cast<sockaddr*>(&client_address),
            &client_length
        );

        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (!running_) {
                break;
            }

            if (errno == EBADF || errno == EINVAL) {
                break;
            }

            std::cerr
                << "Accept failed: "
                << std::system_error(
                    errno,
                    std::generic_category()
                ).what()
                << std::endl;

            continue;
        }

#ifdef SO_NOSIGPIPE
        int no_sigpipe = 1;

        setsockopt(
            client_socket,
            SOL_SOCKET,
            SO_NOSIGPIPE,
            &no_sigpipe,
            sizeof(no_sigpipe)
        );
#endif

        bool thread_creation_failed = false;

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);

            if (!running_) {
                close(client_socket);
                break;
            }

            client_sockets_.insert(client_socket);

            try {
                client_threads_.emplace_back(
                    &Server::handleClient,
                    this,
                    client_socket
                );
            } catch (...) {
                client_sockets_.erase(client_socket);
                close(client_socket);
                thread_creation_failed = true;
            }
        }

        if (thread_creation_failed) {
            stop();
            throw std::runtime_error(
                "Failed to create client thread"
            );
        }
    }

    stop();
    joinClientThreads();

    std::cout << "ShardKV server stopped" << std::endl;
}

void Server::stop() {
    running_ = false;

    const int listening_socket = server_socket_.exchange(-1);

    if (listening_socket >= 0) {
        shutdown(listening_socket, SHUT_RDWR);
        close(listening_socket);
    }

    std::lock_guard<std::mutex> lock(clients_mutex_);

    for (const int client_socket : client_sockets_) {
        shutdown(client_socket, SHUT_RDWR);
    }
}

void Server::joinClientThreads() {
    for (std::thread& client_thread : client_threads_) {
        if (client_thread.joinable()) {
            client_thread.join();
        }
    }

    client_threads_.clear();
}

void Server::handleClient(int client_socket) {
    std::cout << "Client connected" << std::endl;

    char buffer[READ_BUFFER_SIZE];
    std::string pending_data;

    while (running_) {
        const ssize_t bytes_read = recv(
            client_socket,
            buffer,
            sizeof(buffer),
            0
        );

        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }

            break;
        }

        if (bytes_read == 0) {
            break;
        }

        pending_data.append(
            buffer,
            static_cast<std::size_t>(bytes_read)
        );

        if (pending_data.size() > MAX_PENDING_DATA) {
            sendAll(client_socket, "ERROR\n");
            break;
        }

        std::size_t newline_position = 0;
        bool send_failed = false;

        while (
            (newline_position = pending_data.find('\n'))
            != std::string::npos
        ) {
            std::string command = pending_data.substr(
                0,
                newline_position
            );

            pending_data.erase(
                0,
                newline_position + 1
            );

            if (!command.empty() && command.back() == '\r') {
                command.pop_back();
            }

            if (command.empty()) {
                continue;
            }

            const std::string response = processCommand(
                command,
                replica_manager_
            );

            if (!sendAll(client_socket, response)) {
                send_failed = true;
                break;
            }
        }

        if (send_failed) {
            break;
        }
    }

    shutdown(client_socket, SHUT_RDWR);
    close(client_socket);

    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        client_sockets_.erase(client_socket);
    }

    std::cout << "Client disconnected" << std::endl;
}
