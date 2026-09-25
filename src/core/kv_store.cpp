#include "kv_store.h"

#include <sstream>

KVStore::KVStore(const std::string& wal_path)
    : wal_(wal_path) {
    recover();
}

void KVStore::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    wal_.appendSet(key, value);
    data_[key] = value;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = data_.find(key);

    if (it == data_.end()) {
        return std::nullopt;
    }

    return it->second;
}

bool KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = data_.find(key);

    if (it == data_.end()) {
        return false;
    }

    wal_.appendDelete(key);
    data_.erase(it);

    return true;
}

bool KVStore::exists(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    return data_.find(key) != data_.end();
}

void KVStore::recover() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto operations = wal_.replay();

    for (const auto& operation : operations) {
        std::stringstream stream(operation);

        std::string type;
        std::string key;
        std::string value;

        std::getline(stream, type, '|');
        std::getline(stream, key, '|');

        if (type == "SET") {
            std::getline(stream, value);
            data_[key] = value;
        } else if (type == "DELETE") {
            data_.erase(key);
        }
    }
}