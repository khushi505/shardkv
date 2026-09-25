#include "replication_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>

ReplicationClient::ReplicationClient(
    const std::string& host,
    int port
)
    : host_(host),
      port_(port) {}

bool ReplicationClient::sendSet(
    const std::string& key,
    const std::string& value
) {
    return sendCommand(
        "SET " + key + " " + value + "\n"
    );
}

bool ReplicationClient::sendDelete(
    const std::string& key
) {
    return sendCommand(
        "DELETE " + key + "\n"
    );
}

bool ReplicationClient::sendCommand(
    const std::string& command
) {
    int socket_fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (socket_fd < 0) {
        return false;
    }

    sockaddr_in server_address{};

    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(port_);

    if (inet_pton(
            AF_INET,
            host_.c_str(),
            &server_address.sin_addr
        ) <= 0) {

        close(socket_fd);
        return false;
    }

    if (connect(
            socket_fd,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)
        ) < 0) {

        close(socket_fd);
        return false;
    }

    ssize_t bytes_sent = send(
        socket_fd,
        command.c_str(),
        command.size(),
        0
    );

    if (bytes_sent < 0) {
        close(socket_fd);
        return false;
    }

    char buffer[1024]{};

    ssize_t bytes_received = recv(
        socket_fd,
        buffer,
        sizeof(buffer) - 1,
        0
    );

    close(socket_fd);

    if (bytes_received <= 0) {
        return false;
    }

    buffer[bytes_received] = '\0';

    std::string response(buffer);

    return response.find("OK") != std::string::npos;
}