#include "wal.h"

#include <fstream>
#include <stdexcept>

WAL::WAL(const std::string& file_path)
    : file_path_(file_path) {}

void WAL::appendSet(const std::string& key, const std::string& value) {
    std::ofstream file(file_path_, std::ios::app);

    if (!file) {
        throw std::runtime_error("Failed to open WAL file");
    }

    file << "SET|" << key << "|" << value << '\n';
}

void WAL::appendDelete(const std::string& key) {
    std::ofstream file(file_path_, std::ios::app);

    if (!file) {
        throw std::runtime_error("Failed to open WAL file");
    }

    file << "DELETE|" << key << '\n';
}

std::vector<std::string> WAL::replay() const {
    std::ifstream file(file_path_);

    if (!file) {
        return {};
    }

    std::vector<std::string> operations;
    std::string line;

    while (std::getline(file, line)) {
        if (!line.empty()) {
            operations.push_back(line);
        }
    }

    return operations;
}