#include <iostream>
#include <string>

#include "server.h"

int main(int argc, char* argv[]) {
    try {
        std::uint16_t port = 8080;
        std::string replica_host;
        int replica_port = 0;

        if (argc >= 2) {
            port = static_cast<std::uint16_t>(
                std::stoi(argv[1])
            );
        }

        if (argc >= 4) {
            replica_host = argv[2];
            replica_port = std::stoi(argv[3]);
        }

        Server server(
            port,
            3,
            replica_host,
            replica_port
        );

        server.start();
    } catch (const std::exception& error) {
        std::cerr
            << "Server error: "
            << error.what()
            << std::endl;

        return 1;
    }

    return 0;
}