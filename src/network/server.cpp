#include "server.h"

#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

Server::Server(int port)
    : port_(port),
      store_("shardkv.log") {}

void Server::start() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to configure socket");
    }

    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    if (bind(
            server_fd,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)
        ) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind socket");
    }

    if (listen(server_fd, 10) < 0) {
        close(server_fd);
        throw std::runtime_error("Failed to listen on socket");
    }

    std::cout << "ShardKV server listening on port "
              << port_
              << std::endl;

    while (true) {
        sockaddr_in client_address{};
        socklen_t client_address_length = sizeof(client_address);

        int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(&client_address),
            &client_address_length
        );

        if (client_fd < 0) {
            std::cerr << "Failed to accept client connection" << std::endl;
            continue;
        }

        std::cout << "Client connected" << std::endl;

        std::thread client_thread(
            &Server::handleClient,
            this,
            client_fd
        );

        client_thread.detach();
    }

    close(server_fd);
}

void Server::handleClient(int client_fd) {
    char buffer[1024];
    std::string pending_data;

    while (true) {
        ssize_t bytes_received = recv(
            client_fd,
            buffer,
            sizeof(buffer),
            0
        );

        if (bytes_received < 0) {
            std::cerr << "Failed to receive data" << std::endl;
            close(client_fd);
            return;
        }

        if (bytes_received == 0) {
            std::cout << "Client disconnected" << std::endl;
            close(client_fd);
            return;
        }

        pending_data.append(buffer, bytes_received);

        while (true) {
            std::size_t newline_position = pending_data.find('\n');

            if (newline_position == std::string::npos) {
                break;
            }

            std::string request = pending_data.substr(
                0,
                newline_position
            );

            pending_data.erase(
                0,
                newline_position + 1
            );

            if (!request.empty() && request.back() == '\r') {
                request.pop_back();
            }

            if (request.empty()) {
                continue;
            }

            std::stringstream stream(request);

            std::string command;
            std::string key;
            std::string value;

            stream >> command >> key;

            std::string response;

            if (command == "SET") {
                stream >> value;

                if (key.empty() || value.empty()) {
                    response = "ERROR\n";
                } else {
                    store_.set(key, value);
                    response = "OK\n";
                }
            } else if (command == "GET") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else {
                    auto result = store_.get(key);

                    if (result.has_value()) {
                        response = *result + "\n";
                    } else {
                        response = "NOT_FOUND\n";
                    }
                }
            } else if (command == "DELETE") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else if (store_.remove(key)) {
                    response = "OK\n";
                } else {
                    response = "NOT_FOUND\n";
                }
            } else if (command == "EXISTS") {
                if (key.empty()) {
                    response = "ERROR\n";
                } else if (store_.exists(key)) {
                    response = "YES\n";
                } else {
                    response = "NO\n";
                }
            } else {
                response = "ERROR\n";
            }

            ssize_t bytes_sent = send(
                client_fd,
                response.c_str(),
                response.size(),
                0
            );

            if (bytes_sent < 0) {
                std::cerr << "Failed to send response" << std::endl;
                close(client_fd);
                return;
            }
        }
    }
}