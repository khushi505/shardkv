#pragma once

#include <string>
#include <vector>

class WAL {
public:
    explicit WAL(const std::string& file_path);

    void appendSet(const std::string& key, const std::string& value);
    void appendDelete(const std::string& key);

    std::vector<std::string> replay() const;

private:
    std::string file_path_;
};