#include "kv_store.h"

void KVStore::set(const std::string& key, const std::string& value) {
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
    return data_.erase(key) > 0;
}

bool KVStore::exists(const std::string& key) const {
    return data_.find(key) != data_.end();
}