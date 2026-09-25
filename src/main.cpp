#include <iostream>

#include "server.h"

int main() {
    try {
        Server server(8080, 3);

        server.start();
    } catch (const std::exception& error) {
        std::cerr << "Server error: "
                  << error.what()
                  << std::endl;

        return 1;
    }

    return 0;
}