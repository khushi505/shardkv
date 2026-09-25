#pragma once

#include "kv_store.h"

class Server {
public:
    explicit Server(int port);

    void start();

private:
    void handleClient(int client_fd);

    int port_;
    KVStore store_;
};