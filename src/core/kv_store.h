#pragma once

#include <optional>
#include <string>
#include <unordered_map>

#include "wal.h"

class KVStore {
public:
    explicit KVStore(const std::string& wal_path);

    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool remove(const std::string& key);
    bool exists(const std::string& key) const;

private:
    void recover();

    std::unordered_map<std::string, std::string> data_;
    WAL wal_;
};