
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "server.h"

namespace {

bool sendCommand(int socket_fd, const std::string& command) {
    std::size_t sent = 0;

    while (sent < command.size()) {
#ifdef MSG_NOSIGNAL
        constexpr int SEND_FLAGS = MSG_NOSIGNAL;
#else
        constexpr int SEND_FLAGS = 0;
#endif

        const ssize_t result = send(
            socket_fd,
            command.data() + sent,
            command.size() - sent,
            SEND_FLAGS
        );

        if (result < 0) {
            return false;
        }

        if (result == 0) {
            return false;
        }

        sent += static_cast<std::size_t>(result);
    }

    return true;
}

std::string readResponse(int socket_fd) {
    std::string response;
    char character;

    while (true) {
        const ssize_t received = recv(
            socket_fd,
            &character,
            1,
            0
        );

        if (received <= 0) {
            return "";
        }

        if (character == '\n') {
            return response;
        }

        response.push_back(character);
    }
}

int connectToServer(std::uint16_t port) {
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(3);

    while (std::chrono::steady_clock::now() < deadline) {
        const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);

        if (socket_fd < 0) {
            return -1;
        }

        sockaddr_in address{};

        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        if (connect(
                socket_fd,
                reinterpret_cast<sockaddr*>(&address),
                sizeof(address)
            ) == 0) {
            return socket_fd;
        }

        close(socket_fd);

        std::this_thread::sleep_for(
            std::chrono::milliseconds(10)
        );
    }

    return -1;
}

bool waitForServerPort(
    Server& server,
    const std::atomic<bool>& server_failed
) {
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(3);

    while (
        server.port() == 0 &&
        !server_failed.load() &&
        std::chrono::steady_clock::now() < deadline
    ) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10)
        );
    }

    return !server_failed.load() && server.port() != 0;
}

}  // namespace

TEST(ServerIntegrationTest, HandlesConcurrentClientsAndShutsDown) {
    Server server(0, 3);

    std::atomic<bool> server_failed{false};

    std::thread server_thread([&]() {
        try {
            server.start();
        } catch (...) {
            server_failed = true;
        }
    });

    if (!waitForServerPort(server, server_failed)) {
        server.stop();
        server_thread.join();

        FAIL() << "Server failed to start";
    }

    constexpr int CLIENT_COUNT = 8;

    std::atomic<int> successful_clients{0};

    std::vector<std::thread> clients;

    for (int i = 0; i < CLIENT_COUNT; ++i) {
        clients.emplace_back([&, i]() {
            const int socket_fd = connectToServer(server.port());

            if (socket_fd < 0) {
                return;
            }

            const std::string key = "key" + std::to_string(i);
            const std::string value = "value" + std::to_string(i);

            bool success = true;

            success = success && sendCommand(
                socket_fd,
                "SET " + key + " " + value + "\n"
            );

            success = success && readResponse(socket_fd) == "OK";

            success = success && sendCommand(
                socket_fd,
                "GET " + key + "\n"
            );

            success = success && readResponse(socket_fd) == value;

            success = success && sendCommand(
                socket_fd,
                "EXISTS " + key + "\n"
            );

            success = success && readResponse(socket_fd) == "YES";

            success = success && sendCommand(
                socket_fd,
                "DELETE " + key + "\n"
            );

            success = success && readResponse(socket_fd) == "OK";

            close(socket_fd);

            if (success) {
                ++successful_clients;
            }
        });
    }

    for (auto& client : clients) {
        client.join();
    }

    EXPECT_EQ(successful_clients.load(), CLIENT_COUNT);

    server.stop();
    server_thread.join();

    EXPECT_FALSE(server_failed.load());
}

TEST(ServerIntegrationTest, ShutsDownWithIdleClientConnected) {
    Server server(0, 3);

    std::atomic<bool> server_failed{false};

    std::thread server_thread([&]() {
        try {
            server.start();
        } catch (...) {
            server_failed = true;
        }
    });

    if (!waitForServerPort(server, server_failed)) {
        server.stop();
        server_thread.join();

        FAIL() << "Server failed to start";
    }

    const int client_socket = connectToServer(server.port());

    ASSERT_GE(client_socket, 0);

    // Client is connected but deliberately sends no command.
    server.stop();

    server_thread.join();

    EXPECT_FALSE(server_failed.load());

    // The server should have shut down the idle client connection.
    pollfd descriptor{};

    descriptor.fd = client_socket;
    descriptor.events = POLLIN | POLLHUP;

    const int poll_result = poll(
        &descriptor,
        1,
        2000
    );

    ASSERT_GT(poll_result, 0)
        << "Idle client was not disconnected after server shutdown";

    char buffer;

    const ssize_t received = recv(
        client_socket,
        &buffer,
        1,
        0
    );

    EXPECT_LE(received, 0);

    close(client_socket);
}

TEST(ServerIntegrationTest, ProcessesMultipleCommandsInSinglePacket) {
    Server server(0, 3);

    std::atomic<bool> server_failed{false};

    std::thread server_thread([&]() {
        try {
            server.start();
        } catch (...) {
            server_failed = true;
        }
    });

    if (!waitForServerPort(server, server_failed)) {
        server.stop();
        server_thread.join();

        FAIL() << "Server failed to start";
    }

    const int client_socket = connectToServer(server.port());

    ASSERT_GE(client_socket, 0);

    // All four commands are sent in one TCP send operation.
    const std::string packet =
        "SET language cpp\n"
        "GET language\n"
        "EXISTS language\n"
        "DELETE language\n";

#ifdef MSG_NOSIGNAL
    constexpr int SEND_FLAGS = MSG_NOSIGNAL;
#else
    constexpr int SEND_FLAGS = 0;
#endif

    const ssize_t bytes_sent = send(
        client_socket,
        packet.data(),
        packet.size(),
        SEND_FLAGS
    );

    ASSERT_EQ(
        bytes_sent,
        static_cast<ssize_t>(packet.size())
    ) << "The complete command packet was not sent";

    EXPECT_EQ(readResponse(client_socket), "OK");
    EXPECT_EQ(readResponse(client_socket), "cpp");
    EXPECT_EQ(readResponse(client_socket), "YES");
    EXPECT_EQ(readResponse(client_socket), "OK");

    close(client_socket);

    server.stop();
    server_thread.join();

    EXPECT_FALSE(server_failed.load());
}
