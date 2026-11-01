#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "replica_manager.h"

class ReplicaManagerNetworkTest : public ::testing::Test {
protected:
    static constexpr int port = 9093;

    std::thread server_thread;

    void startServer(const std::vector<std::string>& expected_commands) {
        server_thread = std::thread([expected_commands]() {
            int server_fd = socket(AF_INET, SOCK_STREAM, 0);
            ASSERT_GE(server_fd, 0);

            int option = 1;
            ASSERT_EQ(
                setsockopt(
                    server_fd,
                    SOL_SOCKET,
                    SO_REUSEADDR,
                    &option,
                    sizeof(option)
                ),
                0
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

            ASSERT_EQ(listen(server_fd, 5), 0);

            for (const auto& expected_command : expected_commands) {
                int client_fd = accept(server_fd, nullptr, nullptr);
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

                EXPECT_EQ(
                    std::string(buffer),
                    expected_command
                );

                const std::string response = "OK\n";

                EXPECT_EQ(
                    send(
                        client_fd,
                        response.c_str(),
                        response.size(),
                        0
                    ),
                    static_cast<ssize_t>(response.size())
                );

                close(client_fd);
            }

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

TEST_F(ReplicaManagerNetworkTest, SetReplicatesOverTCP) {
    startServer({
        "SET name Khushi\n"
    });

    ReplicaManager manager(
        2,
        "network_set_test",
        "127.0.0.1",
        port
    );

    manager.set("name", "Khushi");

    EXPECT_EQ(
        manager.get("name"),
        std::optional<std::string>("Khushi")
    );

    EXPECT_EQ(
        manager.getReplica("name"),
        std::optional<std::string>("Khushi")
    );
}

TEST_F(ReplicaManagerNetworkTest, DeleteReplicatesOverTCP) {
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

    manager.set("name", "Khushi");

    ASSERT_TRUE(manager.exists("name"));

    EXPECT_TRUE(manager.remove("name"));

    EXPECT_FALSE(manager.exists("name"));
    EXPECT_FALSE(manager.getReplica("name").has_value());
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

    manager.set("name", "Khushi");

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
    FailedReplicationIsQueued
) {
    ReplicaManager manager(
        2,
        "network_pending_test",
        "127.0.0.1",
        9094
    );

    const ReplicationResult result =
        manager.setWithResult("name", "Khushi");

    EXPECT_TRUE(result.local_success);
    EXPECT_FALSE(result.replica_success);
    EXPECT_FALSE(result.success());

    EXPECT_EQ(manager.pendingReplicationCount(), 1);

    EXPECT_EQ(
        manager.get("name"),
        std::optional<std::string>("Khushi")
    );
}

TEST_F(
    ReplicaManagerNetworkTest,
    PendingReplicationSucceedsAfterServerRecovers
) {
    ReplicaManager manager(
        2,
        "network_recovery_test",
        "127.0.0.1",
        port,
        std::chrono::hours(1)
    );

    const ReplicationResult result =
        manager.setWithResult("name", "Khushi");

    ASSERT_TRUE(result.local_success);
    ASSERT_FALSE(result.replica_success);
    ASSERT_EQ(manager.pendingReplicationCount(), 1);

    startServer({
        "SET name Khushi\n"
    });

    const std::size_t retried =
        manager.retryPendingReplications();

    EXPECT_EQ(retried, 1);
    EXPECT_EQ(manager.pendingReplicationCount(), 0);
}

TEST_F(
    ReplicaManagerNetworkTest,
    PendingOperationsAreRetriedInOrder
) {
    ReplicaManager manager(
        2,
        "network_order_test",
        "127.0.0.1",
        port,
        std::chrono::hours(1)
    );

    const ReplicationResult first =
        manager.setWithResult("name", "Khushi");

    ASSERT_TRUE(first.local_success);
    ASSERT_FALSE(first.replica_success);

    const ReplicationResult second =
        manager.setWithResult("name", "Rahul");

    EXPECT_TRUE(second.local_success);
    EXPECT_FALSE(second.replica_success);

    EXPECT_EQ(manager.pendingReplicationCount(), 2);

    startServer({
        "SET name Khushi\n",
        "SET name Rahul\n"
    });

    const std::size_t retried =
        manager.retryPendingReplications();

    EXPECT_EQ(retried, 2);
    EXPECT_EQ(manager.pendingReplicationCount(), 0);
}

TEST_F(
    ReplicaManagerNetworkTest,
    PendingSetAndDeleteAreRetriedInOrder
) {
    ReplicaManager manager(
        2,
        "network_set_delete_order_test",
        "127.0.0.1",
        port,
        std::chrono::hours(1)
    );

    const ReplicationResult set_result =
        manager.setWithResult("name", "Khushi");

    ASSERT_TRUE(set_result.local_success);
    ASSERT_FALSE(set_result.replica_success);

    const ReplicationResult delete_result =
        manager.removeWithResult("name");

    EXPECT_TRUE(delete_result.local_success);
    EXPECT_FALSE(delete_result.replica_success);

    EXPECT_EQ(manager.pendingReplicationCount(), 2);

    startServer({
        "SET name Khushi\n",
        "DELETE name\n"
    });

    const std::size_t retried =
        manager.retryPendingReplications();

    EXPECT_EQ(retried, 2);
    EXPECT_EQ(manager.pendingReplicationCount(), 0);

    EXPECT_FALSE(manager.get("name").has_value());
    EXPECT_FALSE(manager.getReplica("name").has_value());
}

TEST_F(
    ReplicaManagerNetworkTest,
    BackgroundWorkerRetriesPendingReplication
) {
    ReplicaManager manager(
        2,
        "network_background_retry_test",
        "127.0.0.1",
        port,
        std::chrono::milliseconds(100)
    );

    const ReplicationResult result =
        manager.setWithResult("name", "Khushi");

    ASSERT_TRUE(result.local_success);
    ASSERT_FALSE(result.replica_success);
    ASSERT_EQ(manager.pendingReplicationCount(), 1);

    startServer({
        "SET name Khushi\n"
    });

    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(5);

    while (
        manager.pendingReplicationCount() != 0 &&
        std::chrono::steady_clock::now() < deadline
    ) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(50)
        );
    }

    EXPECT_EQ(manager.pendingReplicationCount(), 0);
}

TEST_F(
    ReplicaManagerNetworkTest,
    PendingReplicationSurvivesManagerRestart
) {
    const std::string wal_prefix =
        "network_persistent_queue_test";

    std::string pending_log_path;

    {
        ReplicaManager manager(
            2,
            wal_prefix,
            "127.0.0.1",
            port,
            std::chrono::hours(1)
        );

        const std::size_t shard =
            manager.getShard("name");

        pending_log_path =
            wal_prefix +
            "_pending_replication_" +
            std::to_string(shard) +
            ".log";

        const ReplicationResult result =
            manager.setWithResult("name", "Khushi");

        ASSERT_TRUE(result.local_success);
        ASSERT_FALSE(result.replica_success);

        EXPECT_EQ(manager.pendingReplicationCount(), 1);
    }

    startServer({
        "SET name Khushi\n"
    });

    {
        ReplicaManager restarted_manager(
            2,
            wal_prefix,
            "127.0.0.1",
            port,
            std::chrono::hours(1)
        );

        EXPECT_EQ(
            restarted_manager.pendingReplicationCount(),
            1
        );

        const std::size_t retried =
            restarted_manager.retryPendingReplications();

        EXPECT_EQ(retried, 1);
        EXPECT_EQ(
            restarted_manager.pendingReplicationCount(),
            0
        );
    }

    if (server_thread.joinable()) {
        server_thread.join();
    }

    std::remove(pending_log_path.c_str());
}