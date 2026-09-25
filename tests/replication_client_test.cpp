#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "replication_client.h"

class ReplicationClientTest : public ::testing::Test {
protected:
    static constexpr int port = 9091;

    std::thread server_thread;

    void startServer(const std::string& expected_command) {
        server_thread = std::thread([expected_command]() {
            int server_fd = socket(
                AF_INET,
                SOCK_STREAM,
                0
            );

            ASSERT_GE(server_fd, 0);

            int option = 1;

            setsockopt(
                server_fd,
                SOL_SOCKET,
                SO_REUSEADDR,
                &option,
                sizeof(option)
            );

            sockaddr_in address{};

            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = htons(port);

            ASSERT_EQ(
                bind(
                    server_fd,
                    reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)
                ),
                0
            );

            ASSERT_EQ(
                listen(server_fd, 1),
                0
            );

            int client_fd = accept(
                server_fd,
                nullptr,
                nullptr
            );

            ASSERT_GE(client_fd, 0);

            char buffer[1024]{};

            ssize_t bytes_received = recv(
                client_fd,
                buffer,
                sizeof(buffer) - 1,
                0
            );

            ASSERT_GT(bytes_received, 0);

            buffer[bytes_received] = '\0';

            std::string received(buffer);

            EXPECT_EQ(received, expected_command);

            const std::string response = "OK\n";

            send(
                client_fd,
                response.c_str(),
                response.size(),
                0
            );

            close(client_fd);
            close(server_fd);
        });

        std::this_thread::sleep_for(
            std::chrono::milliseconds(100)
        );
    }

    void TearDown() override {
        if (server_thread.joinable()) {
            server_thread.join();
        }
    }
};

TEST_F(ReplicationClientTest, SendsSetCommand) {
    startServer("SET name Khushi\n");

    ReplicationClient client(
        "127.0.0.1",
        port
    );

    EXPECT_TRUE(
        client.sendSet(
            "name",
            "Khushi"
        )
    );
}

TEST_F(ReplicationClientTest, SendsDeleteCommand) {
    startServer("DELETE name\n");

    ReplicationClient client(
        "127.0.0.1",
        port
    );

    EXPECT_TRUE(
        client.sendDelete("name")
    );
}

TEST_F(ReplicationClientTest, ReturnsFalseWhenServerUnavailable) {
    ReplicationClient client(
        "127.0.0.1",
        9092
    );

    EXPECT_FALSE(
        client.sendSet(
            "name",
            "Khushi"
        )
    );
}