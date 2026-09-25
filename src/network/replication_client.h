#pragma once

#include <string>

class ReplicationClient {
public:
    ReplicationClient(
        const std::string& host,
        int port
    );

    bool sendSet(
        const std::string& key,
        const std::string& value
    );

    bool sendDelete(
        const std::string& key
    );

private:
    std::string host_;
    int port_;

    bool sendCommand(const std::string& command);
};