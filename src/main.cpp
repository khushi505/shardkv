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

#include "server.h"

namespace {

volatile std::sig_atomic_t shutdown_requested = 0;

void handleSignal(int signal_number) {
    shutdown_requested = signal_number;
}

int parsePort(const std::string& value, const std::string& name) {
    std::size_t parsed_length = 0;
    int port = 0;

    try {
        port = std::stoi(value, &parsed_length);
    } catch (const std::exception&) {
        throw std::invalid_argument("Invalid " + name);
    }

    if (parsed_length != value.size() || port < 1 || port > 65535) {
        throw std::invalid_argument(name + " must be between 1 and 65535");
    }

    return port;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc != 1 && argc != 2 && argc != 4) {
            throw std::invalid_argument(
                "Usage: ./shardkv [port] [replica_host replica_port]"
            );
        }

        std::uint16_t port = 8080;
        std::string replica_host;
        int replica_port = 0;

        if (argc >= 2) {
            port = static_cast<std::uint16_t>(
                parsePort(argv[1], "server port")
            );
        }

        if (argc == 4) {
            replica_host = argv[2];

            if (replica_host.empty()) {
                throw std::invalid_argument("Replica host cannot be empty");
            }

            replica_port = parsePort(argv[3], "replica port");
        }

        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        Server server(
            port,
            3,
            replica_host,
            replica_port
        );

        std::atomic<bool> server_failed{false};
        std::exception_ptr server_exception;
        std::mutex exception_mutex;

        std::thread server_thread([&]() {
            try {
                server.start();
            } catch (...) {
                {
                    std::lock_guard<std::mutex> lock(exception_mutex);
                    server_exception = std::current_exception();
                }

                server_failed = true;
            }
        });

        while (!shutdown_requested && !server_failed) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        }

        server.stop();
        server_thread.join();

        if (server_exception) {
            std::rethrow_exception(server_exception);
        }

        std::cout << "ShardKV shutdown completed" << std::endl;

    } catch (const std::exception& error) {
        std::cerr
            << "Server error: "
            << error.what()
            << std::endl;

        return 1;
    }

    return 0;
}
