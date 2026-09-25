#include <iostream>

#include "kv_store.h"

int main() {
    KVStore store("shardkv.log");

    store.set("name", "Khushi");

    auto value = store.get("name");

    if (value.has_value()) {
        std::cout << *value << std::endl;
    }

    return 0;
}