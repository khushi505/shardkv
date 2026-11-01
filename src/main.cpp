#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "server.h"

namespace {

volatile std::sig_atomic_t shutdown_requested = 0;

void handleSignal(int signal_number) {
    shutdown_requested = signal_number;
}

int parsePort(
    const std::string& value,
    const std::string& name
) {
    std::size_t parsed_length = 0;
    int port = 0;

    try {
        port = std::stoi(value, &parsed_length);
    } catch (const std::exception&) {
        throw std::invalid_argument("Invalid " + name);
    }

    if (
        parsed_length != value.size() ||
        port < 1 ||
        port > 65535
    ) {
        throw std::invalid_argument(
            name + " must be between 1 and 65535"
        );
    }

    return port;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        std::uint16_t port = 8080;

        std::string replica_host;
        int replica_port = 0;

        std::string node_id;

        std::vector<ClusterPeerConfig> peers;
        std::vector<std::string> positional_arguments;

        for (int i = 1; i < argc; ++i) {
            const std::string argument = argv[i];

            if (argument == "--node-id") {
                if (i + 1 >= argc) {
                    throw std::invalid_argument(
                        "--node-id requires a node ID"
                    );
                }

                node_id = argv[++i];

                if (node_id.empty()) {
                    throw std::invalid_argument(
                        "Node ID cannot be empty"
                    );
                }
            } else if (argument == "--peer") {
                if (i + 3 >= argc) {
                    throw std::invalid_argument(
                        "--peer requires: NODE_ID HOST PORT"
                    );
                }

                ClusterPeerConfig peer;

                peer.node_id = argv[++i];
                peer.host = argv[++i];

                peer.port = static_cast<std::uint16_t>(
                    parsePort(argv[++i], "peer port")
                );

                peers.push_back(std::move(peer));
            } else if (argument.rfind("--", 0) == 0) {
                throw std::invalid_argument(
                    "Unknown argument: " + argument
                );
            } else {
                positional_arguments.push_back(argument);
            }
        }

        if (positional_arguments.size() > 3) {
            throw std::invalid_argument(
                "Too many positional arguments"
            );
        }

        if (positional_arguments.size() == 2) {
            throw std::invalid_argument(
                "Replica configuration requires both host and port"
            );
        }

        if (!positional_arguments.empty()) {
            port = static_cast<std::uint16_t>(
                parsePort(
                    positional_arguments[0],
                    "server port"
                )
            );
        }

        if (positional_arguments.size() == 3) {
            replica_host = positional_arguments[1];

            if (replica_host.empty()) {
                throw std::invalid_argument(
                    "Replica host cannot be empty"
                );
            }

            replica_port = parsePort(
                positional_arguments[2],
                "replica port"
            );
        }

        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        Server server(
            port,
            3,
            replica_host,
            replica_port,
            node_id,
            peers
        );

        std::atomic<bool> server_failed{false};

        std::exception_ptr server_exception;

        std::mutex exception_mutex;

        std::thread server_thread([&]() {
            try {
                server.start();
            } catch (...) {
                {
                    std::lock_guard<std::mutex> lock(
                        exception_mutex
                    );

                    server_exception =
                        std::current_exception();
                }

                server_failed = true;
            }
        });

        while (
            !shutdown_requested &&
            !server_failed
        ) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        }

        server.stop();

        server_thread.join();

        if (server_exception) {
            std::rethrow_exception(server_exception);
        }

        std::cout
            << "ShardKV shutdown completed"
            << std::endl;

    } catch (const std::exception& error) {
        std::cerr
            << "Server error: "
            << error.what()
            << std::endl;

        return 1;
    }

    return 0;
}