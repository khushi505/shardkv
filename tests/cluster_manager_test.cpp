#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cluster_manager.h"

using namespace std::chrono_literals;

class ClusterManagerTest : public ::testing::Test {
protected:
    ClusterPeerConfig createPeer(
        const std::string& node_id = "node-b",
        const std::string& host = "127.0.0.1",
        std::uint16_t port = 19001
    ) {
        return ClusterPeerConfig{
            node_id,
            host,
            port
        };
    }
};

TEST_F(ClusterManagerTest, InitializesNodeIdentity) {
    ClusterManager manager("node-a", 8080);

    EXPECT_EQ(manager.localNodeId(), "node-a");
    EXPECT_EQ(manager.localPort(), 8080);
}

TEST_F(ClusterManagerTest, RejectsEmptyNodeId) {
    EXPECT_THROW(
        ClusterManager("", 8080),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsInvalidNodeId) {
    EXPECT_THROW(
        ClusterManager("node a", 8080),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsZeroHeartbeatInterval) {
    EXPECT_THROW(
        ClusterManager("node-a", 8080, {}, 0ms, 100ms),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsInvalidHeartbeatTimeout) {
    EXPECT_THROW(
        ClusterManager("node-a", 8080, {}, 100ms, 100ms),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, InitializesConfiguredPeers) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    const auto members = manager.members();

    ASSERT_EQ(members.size(), 1);

    EXPECT_EQ(members[0].node_id, "node-b");
    EXPECT_EQ(members[0].host, "127.0.0.1");
    EXPECT_EQ(members[0].port, 19001);
    EXPECT_EQ(members[0].health, NodeHealth::UNKNOWN);
}

TEST_F(ClusterManagerTest, RejectsDuplicatePeerIds) {
    EXPECT_THROW(
        ClusterManager(
            "node-a",
            8080,
            {
                createPeer("node-b", "127.0.0.1", 19001),
                createPeer("node-b", "127.0.0.1", 19002)
            }
        ),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsDuplicatePeerEndpoints) {
    EXPECT_THROW(
        ClusterManager(
            "node-a",
            8080,
            {
                createPeer("node-b", "127.0.0.1", 19001),
                createPeer("node-c", "127.0.0.1", 19001)
            }
        ),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsPeerWithSameNodeIdAsLocalNode) {
    EXPECT_THROW(
        ClusterManager(
            "node-a",
            8080,
            {createPeer("node-a")}
        ),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsInvalidPeerAddress) {
    EXPECT_THROW(
        ClusterManager(
            "node-a",
            8080,
            {createPeer("node-b", "invalid-host", 19001)}
        ),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RejectsZeroPeerPort) {
    EXPECT_THROW(
        ClusterManager(
            "node-a",
            8080,
            {createPeer("node-b", "127.0.0.1", 0)}
        ),
        std::invalid_argument
    );
}

TEST_F(ClusterManagerTest, RecordsHeartbeatFromConfiguredPeer) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_TRUE(manager.recordHeartbeat("node-b"));
    EXPECT_TRUE(manager.isNodeHealthy("node-b"));
}

TEST_F(ClusterManagerTest, RejectsHeartbeatFromUnknownNode) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_FALSE(manager.recordHeartbeat("node-unknown"));
}

TEST_F(ClusterManagerTest, RejectsHeartbeatFromLocalNode) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_FALSE(manager.recordHeartbeat("node-a"));
}

TEST_F(ClusterManagerTest, RejectsEmptyHeartbeatNodeId) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_FALSE(manager.recordHeartbeat(""));
}

TEST_F(ClusterManagerTest, InitiallyReportsPeerAsUnknown) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_FALSE(manager.isNodeHealthy("node-b"));

    const auto members = manager.members();

    ASSERT_EQ(members.size(), 1);

    EXPECT_EQ(members[0].health, NodeHealth::UNKNOWN);
}

TEST_F(ClusterManagerTest, CountsLocalNodeAsHealthy) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()}
    );

    EXPECT_EQ(manager.healthyNodeCount(), 1);
}

TEST_F(ClusterManagerTest, CountsHealthyRemoteNodes) {
    ClusterManager manager(
        "node-a",
        8080,
        {
            createPeer("node-b", "127.0.0.1", 19001),
            createPeer("node-c", "127.0.0.1", 19002)
        }
    );

    EXPECT_TRUE(manager.recordHeartbeat("node-b"));
    EXPECT_TRUE(manager.recordHeartbeat("node-c"));

    EXPECT_EQ(manager.healthyNodeCount(), 3);
}

TEST_F(ClusterManagerTest, MarksPeerUnhealthyAfterHeartbeatTimeout) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()},
        20ms,
        100ms
    );

    ASSERT_TRUE(manager.recordHeartbeat("node-b"));
    ASSERT_TRUE(manager.isNodeHealthy("node-b"));

    std::this_thread::sleep_for(150ms);

    EXPECT_FALSE(manager.isNodeHealthy("node-b"));

    const auto members = manager.members();

    ASSERT_EQ(members.size(), 1);

    EXPECT_EQ(members[0].health, NodeHealth::UNHEALTHY);
}

TEST_F(ClusterManagerTest, CanRecoverPeerAfterNewHeartbeat) {
    ClusterManager manager(
        "node-a",
        8080,
        {createPeer()},
        20ms,
        100ms
    );

    ASSERT_TRUE(manager.recordHeartbeat("node-b"));

    std::this_thread::sleep_for(150ms);

    ASSERT_FALSE(manager.isNodeHealthy("node-b"));

    EXPECT_TRUE(manager.recordHeartbeat("node-b"));
    EXPECT_TRUE(manager.isNodeHealthy("node-b"));
}

TEST_F(ClusterManagerTest, StartAndStopAreSafe) {
    ClusterManager manager(
        "node-a",
        8080,
        {},
        20ms,
        100ms
    );

    manager.start();
    manager.start();

    manager.stop();
    manager.stop();

    EXPECT_EQ(manager.localNodeId(), "node-a");
}