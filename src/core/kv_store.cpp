#include "kv_store.h"

#include <sstream>

KVStore::KVStore(const std::string& wal_path)
    : wal_(wal_path) {
    recover();
}

void KVStore::set(const std::string& key, const std::string& value) {
    wal_.appendSet(key, value);
    data_[key] = value;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    auto it = data_.find(key);

    if (it == data_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool KVStore::remove(const std::string& key) {
    if (!exists(key)) {
        return false;
    }

    wal_.appendDelete(key);
    data_.erase(key);

    return true;
}

bool KVStore::exists(const std::string& key) const {
    return data_.find(key) != data_.end();
}

void KVStore::recover() {
    for (const auto& operation : wal_.replay()) {
        std::stringstream stream(operation);

        std::string command;
        std::string key;
        std::string value;

        std::getline(stream, command, '|');
        std::getline(stream, key, '|');

        if (command == "SET") {
            std::getline(stream, value);
            data_[key] = value;
        } else if (command == "DELETE") {
            data_.erase(key);
        }
    }
}