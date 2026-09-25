#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "replica_manager.h"

class ReplicaManagerNetworkTest : public ::testing::Test {
protected:
    static constexpr int port = 9093;

    std::thread server_thread;

    void startServer(
        const std::vector<std::string>& expected_commands
    ) {
        server_thread = std::thread(
            [expected_commands]() {
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
                address.sin_addr.s_addr =
                    htonl(INADDR_LOOPBACK);
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
                    listen(server_fd, 5),
                    0
                );

                for (const auto& expected_command : expected_commands) {
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

                    EXPECT_EQ(
                        received,
                        expected_command
                    );

                    const std::string response = "OK\n";

                    send(
                        client_fd,
                        response.c_str(),
                        response.size(),
                        0
                    );

                    close(client_fd);
                }

                close(server_fd);
            }
        );

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

TEST_F(
    ReplicaManagerNetworkTest,
    SetReplicatesOverTCP
) {
    startServer({
        "SET name Khushi\n"
    });

    ReplicaManager manager(
        2,
        "network_set_test",
        "127.0.0.1",
        port
    );

    manager.set(
        "name",
        "Khushi"
    );

    EXPECT_EQ(
        manager.get("name"),
        std::optional<std::string>("Khushi")
    );

    EXPECT_EQ(
        manager.getReplica("name"),
        std::optional<std::string>("Khushi")
    );
}

TEST_F(
    ReplicaManagerNetworkTest,
    DeleteReplicatesOverTCP
) {
    startServer({
        "SET name Khushi\n",
        "DELETE name\n"
    });

    ReplicaManager manager(
        2,
        "network_delete_test",
        "127.0.0.1",
        port
    );

    manager.set(
        "name",
        "Khushi"
    );

    ASSERT_TRUE(
        manager.exists("name")
    );

    EXPECT_TRUE(
        manager.remove("name")
    );

    EXPECT_FALSE(
        manager.exists("name")
    );

    EXPECT_FALSE(
        manager.getReplica("name").has_value()
    );
}

TEST_F(
    ReplicaManagerNetworkTest,
    LocalWriteWorksWhenReplicationServerUnavailable
) {
    ReplicaManager manager(
        2,
        "network_failure_test",
        "127.0.0.1",
        9094
    );

    manager.set(
        "name",
        "Khushi"
    );

    EXPECT_EQ(
        manager.get("name"),
        std::optional<std::string>("Khushi")
    );

    EXPECT_EQ(
        manager.getReplica("name"),
        std::optional<std::string>("Khushi")
    );
}