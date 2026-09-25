#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <string>
#include <thread>

Server::Server(
    std::uint16_t port,
    std::size_t shard_count,
    const std::string& replica_host,
    int replica_port
)
    : port_(port),
      replica_manager_(
          shard_count,
          "shardkv",
          replica_host,
          replica_port
      ) {}

void Server::start() {
    int server_socket = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (server_socket < 0) {
        throw std::runtime_error(
            "Failed to create socket"
        );
    }

    int option = 1;

    setsockopt(
        server_socket,
        SOL_SOCKET,
        SO_REUSEADDR,
        &option,
        sizeof(option)
    );

    sockaddr_in server_address{};

    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(port_);

    if (bind(
            server_socket,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)
        ) < 0) {

        close(server_socket);

        throw std::runtime_error(
            "Failed to bind socket"
        );
    }

    if (listen(server_socket, 10) < 0) {
        close(server_socket);

        throw std::runtime_error(
            "Failed to listen"
        );
    }

    std::cout
        << "ShardKV server listening on port "
        << port_
        << " with "
        << replica_manager_.shardCount()
        << " shards"
        << std::endl;

    while (true) {
        sockaddr_in client_address{};
        socklen_t client_length =
            sizeof(client_address);

        int client_socket = accept(
            server_socket,
            reinterpret_cast<sockaddr*>(&client_address),
            &client_length
        );

        if (client_socket < 0) {
            continue;
        }

        std::thread(
            &Server::handleClient,
            this,
            client_socket
        ).detach();
    }
}

void Server::handleClient(
    int client_socket
) {
    std::cout
        << "Client connected"
        << std::endl;

    char buffer[4096];

    std::string pending_data;

    while (true) {
        ssize_t bytes_read = recv(
            client_socket,
            buffer,
            sizeof(buffer),
            0
        );

        if (bytes_read <= 0) {
            break;
        }

        pending_data.append(
            buffer,
            bytes_read
        );

        std::size_t newline_position;

        while (
            (newline_position =
                 pending_data.find('\n'))
            != std::string::npos
        ) {
            std::string command =
                pending_data.substr(
                    0,
                    newline_position
                );

            pending_data.erase(
                0,
                newline_position + 1
            );

            if (command.empty()) {
                continue;
            }

            std::stringstream stream(command);

            std::string operation;
            std::string key;
            std::string value;

            stream >> operation >> key;

            std::string response;

            if (operation == "SET") {
                stream >> value;

                if (
                    key.empty() ||
                    value.empty()
                ) {
                    response = "ERROR\n";
                } else {
                    replica_manager_.set(
                        key,
                        value
                    );

                    response = "OK\n";
                }
            } else if (operation == "GET") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else {
                    auto result =
                        replica_manager_.get(key);

                    if (result.has_value()) {
                        response =
                            result.value() +
                            "\n";
                    } else {
                        response =
                            "NOT_FOUND\n";
                    }
                }
            } else if (operation == "EXISTS") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else {
                    response =
                        replica_manager_.exists(key)
                            ? "YES\n"
                            : "NO\n";
                }
            } else if (operation == "DELETE") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else {
                    response =
                        replica_manager_.remove(key)
                            ? "OK\n"
                            : "NOT_FOUND\n";
                }
            } else {
                response = "ERROR\n";
            }

            send(
                client_socket,
                response.c_str(),
                response.size(),
                0
            );
        }
    }

    close(client_socket);

    std::cout
        << "Client disconnected"
        << std::endl;
}