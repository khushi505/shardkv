#include "replica_manager.h"

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>

#include <unistd.h>

ReplicaManager::ReplicaManager(
    std::size_t shard_count,
    const std::string& wal_prefix,
    const std::string& replica_host,
    int replica_port,
    std::chrono::milliseconds retry_interval
)
    : router_(shard_count),
      wal_prefix_(wal_prefix),
      pending_operations_(shard_count),
      replica_promoted_(shard_count, 0),
      primary_healthy_(shard_count, 1),
      retry_interval_(retry_interval) {

    if (!replica_host.empty() && replica_port > 0) {
        replication_client_ =
            std::make_unique<ReplicationClient>(
                replica_host,
                replica_port
            );
    }

    for (std::size_t i = 0; i < shard_count; ++i) {
        shard_mutexes_.push_back(
            std::make_unique<std::mutex>()
        );

        const std::string primary_wal =
            wal_prefix_ +
            "_primary_" +
            std::to_string(i) +
            ".log";

        const std::string replica_wal =
            wal_prefix_ +
            "_replica_" +
            std::to_string(i) +
            ".log";

        primary_shards_.push_back(
            std::make_unique<KVStore>(primary_wal)
        );

        replica_shards_.push_back(
            std::make_unique<KVStore>(replica_wal)
        );
    }

    loadPendingOperations();
    startRetryWorker();
}

ReplicaManager::~ReplicaManager() {
    stopRetryWorker();
}

void ReplicaManager::set(
    const std::string& key,
    const std::string& value
) {
    setWithResult(key, value);
}

ReplicationResult ReplicaManager::setWithResult(
    const std::string& key,
    const std::string& value
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    const bool use_replica =
        replica_promoted_[shard] ||
        !primary_healthy_[shard];

    if (use_replica) {
        replica_shards_[shard]->set(key, value);
        return {true, true, true};
    }

    primary_shards_[shard]->set(key, value);
    replica_shards_[shard]->set(key, value);

    if (!replication_client_) {
        return {true, true, true};
    }

    PendingOperation operation{
        PendingOperation::Type::SET,
        key,
        value
    };

    if (!pending_operations_[shard].empty()) {
        if (!persistPendingOperation(shard, operation)) {
            return {true, false, false};
        }

        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {true, false, true};
    }

    if (!replicateOperation(operation)) {
        if (!persistPendingOperation(shard, operation)) {
            return {true, false, false};
        }

        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {true, false, true};
    }

    return {true, true, true};
}

std::optional<std::string> ReplicaManager::get(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    if (
        replica_promoted_[shard] ||
        !primary_healthy_[shard]
    ) {
        return replica_shards_[shard]->get(key);
    }

    return primary_shards_[shard]->get(key);
}

std::optional<std::string> ReplicaManager::getReplica(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return replica_shards_[shard]->get(key);
}

bool ReplicaManager::remove(const std::string& key) {
    return removeWithResult(key).local_success;
}

ReplicationResult ReplicaManager::removeWithResult(
    const std::string& key
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    const bool use_replica =
        replica_promoted_[shard] ||
        !primary_healthy_[shard];

    if (use_replica) {
        const bool removed =
            replica_shards_[shard]->remove(key);

        return {removed, removed, true};
    }

    const bool primary_removed =
        primary_shards_[shard]->remove(key);

    const bool replica_removed =
        replica_shards_[shard]->remove(key);

    const bool local_success =
        primary_removed || replica_removed;

    if (!replication_client_) {
        return {local_success, true, true};
    }

    PendingOperation operation{
        PendingOperation::Type::DELETE,
        key,
        ""
    };

    if (!pending_operations_[shard].empty()) {
        if (!persistPendingOperation(shard, operation)) {
            return {local_success, false, false};
        }

        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {local_success, false, true};
    }

    if (!replicateOperation(operation)) {
        if (!persistPendingOperation(shard, operation)) {
            return {local_success, false, false};
        }

        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {local_success, false, true};
    }

    return {local_success, true, true};
}

bool ReplicaManager::exists(const std::string& key) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    if (
        replica_promoted_[shard] ||
        !primary_healthy_[shard]
    ) {
        return replica_shards_[shard]->exists(key);
    }

    return primary_shards_[shard]->exists(key);
}

void ReplicaManager::promoteReplica(const std::string& key) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    replica_promoted_[shard] = 1;
}

bool ReplicaManager::isReplicaPromoted(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return replica_promoted_[shard] != 0;
}

void ReplicaManager::simulatePrimaryFailure(
    const std::string& key
) {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    primary_healthy_[shard] = 0;
    replica_promoted_[shard] = 1;
}

bool ReplicaManager::isPrimaryHealthy(
    const std::string& key
) const {
    const std::size_t shard = router_.getShard(key);

    std::lock_guard<std::mutex> shard_lock(
        *shard_mutexes_[shard]
    );

    return primary_healthy_[shard] != 0;
}

bool ReplicaManager::replicateOperation(
    const PendingOperation& operation
) {
    if (!replication_client_) {
        return true;
    }

    if (operation.type == PendingOperation::Type::SET) {
        return replication_client_->sendSet(
            operation.key,
            operation.value
        );
    }

    return replication_client_->sendDelete(operation.key);
}

std::size_t ReplicaManager::retryPendingReplications() {
    if (!replication_client_) {
        return 0;
    }

    std::size_t successful_retries = 0;

    for (
        std::size_t shard = 0;
        shard < pending_operations_.size();
        ++shard
    ) {
        std::lock_guard<std::mutex> shard_lock(
            *shard_mutexes_[shard]
        );

        if (
            replica_promoted_[shard] ||
            !primary_healthy_[shard]
        ) {
            continue;
        }

        auto& queue = pending_operations_[shard];

        while (!queue.empty()) {
            if (!replicateOperation(queue.front())) {
                break;
            }

            if (!persistPendingAcknowledgement(shard)) {
                break;
            }

            queue.pop_front();
            ++successful_retries;
        }
    }

    return successful_retries;
}

std::size_t ReplicaManager::pendingReplicationCount() const {
    std::size_t total = 0;

    for (
        std::size_t shard = 0;
        shard < pending_operations_.size();
        ++shard
    ) {
        std::lock_guard<std::mutex> shard_lock(
            *shard_mutexes_[shard]
        );

        total += pending_operations_[shard].size();
    }

    return total;
}

std::size_t ReplicaManager::getShard(
    const std::string& key
) const {
    return router_.getShard(key);
}

std::size_t ReplicaManager::shardCount() const {
    return router_.shardCount();
}

std::string ReplicaManager::pendingLogPath(
    std::size_t shard
) const {
    return wal_prefix_ +
        "_pending_replication_" +
        std::to_string(shard) +
        ".log";
}

std::string ReplicaManager::encodeHex(
    const std::string& value
) {
    static constexpr char digits[] = "0123456789ABCDEF";

    if (value.empty()) {
        return "-";
    }

    std::string encoded;
    encoded.reserve(value.size() * 2);

    for (unsigned char character : value) {
        encoded.push_back(digits[character >> 4]);
        encoded.push_back(digits[character & 0x0F]);
    }

    return encoded;
}

bool ReplicaManager::decodeHex(
    const std::string& encoded,
    std::string& decoded
) {
    if (encoded == "-") {
        decoded.clear();
        return true;
    }

    if (encoded.empty() || encoded.size() % 2 != 0) {
        return false;
    }

    auto hexValue = [](char character) -> int {
        if (character >= '0' && character <= '9') {
            return character - '0';
        }

        if (character >= 'A' && character <= 'F') {
            return character - 'A' + 10;
        }

        if (character >= 'a' && character <= 'f') {
            return character - 'a' + 10;
        }

        return -1;
    };

    decoded.clear();
    decoded.reserve(encoded.size() / 2);

    for (std::size_t i = 0; i < encoded.size(); i += 2) {
        const int high = hexValue(encoded[i]);
        const int low = hexValue(encoded[i + 1]);

        if (high < 0 || low < 0) {
            return false;
        }

        decoded.push_back(
            static_cast<char>((high << 4) | low)
        );
    }

    return true;
}

bool ReplicaManager::appendDurably(
    const std::string& path,
    const std::string& record
) const {
    const int fd = open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_APPEND,
        0644
    );

    if (fd < 0) {
        return false;
    }

    std::size_t written = 0;
    bool success = true;

    while (written < record.size()) {
        const ssize_t result = write(
            fd,
            record.data() + written,
            record.size() - written
        );

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            success = false;
            break;
        }

        if (result == 0) {
            success = false;
            break;
        }

        written += static_cast<std::size_t>(result);
    }

    if (success && fsync(fd) != 0) {
        success = false;
    }

    if (close(fd) != 0) {
        success = false;
    }

    return success;
}

bool ReplicaManager::persistPendingOperation(
    std::size_t shard,
    const PendingOperation& operation
) const {
    std::string record;

    if (operation.type == PendingOperation::Type::SET) {
        record =
            "S " +
            encodeHex(operation.key) +
            " " +
            encodeHex(operation.value) +
            "\n";
    } else {
        record =
            "D " +
            encodeHex(operation.key) +
            "\n";
    }

    return appendDurably(pendingLogPath(shard), record);
}

bool ReplicaManager::persistPendingAcknowledgement(
    std::size_t shard
) const {
    return appendDurably(pendingLogPath(shard), "A\n");
}

void ReplicaManager::loadPendingOperations() {
    for (
        std::size_t shard = 0;
        shard < pending_operations_.size();
        ++shard
    ) {
        const std::string path = pendingLogPath(shard);

        std::ifstream input(path, std::ios::binary);

        if (!input.is_open()) {
            continue;
        }

        std::string contents(
            (std::istreambuf_iterator<char>(input)),
            std::istreambuf_iterator<char>()
        );

        std::size_t position = 0;
        std::size_t valid_bytes = 0;

        while (position < contents.size()) {
            const std::size_t newline =
                contents.find('\n', position);

            if (newline == std::string::npos) {
                break;
            }

            const std::string line =
                contents.substr(position, newline - position);

            bool valid_record = false;

            if (line == "A") {
                if (!pending_operations_[shard].empty()) {
                    pending_operations_[shard].pop_front();
                }

                valid_record = true;
            } else if (line.size() >= 3 && line[1] == ' ') {
                const char operation_type = line[0];
                const std::string payload = line.substr(2);

                if (operation_type == 'S') {
                    const std::size_t separator =
                        payload.find(' ');

                    if (separator != std::string::npos) {
                        std::string key;
                        std::string value;

                        if (
                            decodeHex(
                                payload.substr(0, separator),
                                key
                            ) &&
                            decodeHex(
                                payload.substr(separator + 1),
                                value
                            )
                        ) {
                            pending_operations_[shard].push_back({
                                PendingOperation::Type::SET,
                                std::move(key),
                                std::move(value)
                            });

                            valid_record = true;
                        }
                    }
                } else if (operation_type == 'D') {
                    std::string key;

                    if (decodeHex(payload, key)) {
                        pending_operations_[shard].push_back({
                            PendingOperation::Type::DELETE,
                            std::move(key),
                            ""
                        });

                        valid_record = true;
                    }
                }
            }

            if (!valid_record) {
                break;
            }

            valid_bytes = newline + 1;
            position = newline + 1;
        }

        if (valid_bytes < contents.size()) {
            const int fd = open(path.c_str(), O_WRONLY);

            if (fd < 0) {
                throw std::runtime_error(
                    "Unable to open pending replication log for recovery: " +
                    path
                );
            }

            const bool truncated =
                ftruncate(fd, static_cast<off_t>(valid_bytes)) == 0;

            const bool synced =
                truncated && fsync(fd) == 0;

            const bool closed = close(fd) == 0;

            if (!truncated || !synced || !closed) {
                throw std::runtime_error(
                    "Unable to repair pending replication log: " +
                    path
                );
            }
        }
    }
}

void ReplicaManager::startRetryWorker() {
    if (
        !replication_client_ ||
        retry_interval_.count() <= 0
    ) {
        return;
    }

    retry_thread_ = std::thread(
        &ReplicaManager::retryWorkerLoop,
        this
    );
}

void ReplicaManager::stopRetryWorker() {
    {
        std::lock_guard<std::mutex> lock(retry_mutex_);
        stop_retry_worker_ = true;
    }

    retry_cv_.notify_all();

    if (retry_thread_.joinable()) {
        retry_thread_.join();
    }
}

void ReplicaManager::retryWorkerLoop() {
    std::unique_lock<std::mutex> lock(retry_mutex_);

    while (!stop_retry_worker_) {
        const bool should_stop = retry_cv_.wait_for(
            lock,
            retry_interval_,
            [this]() {
                return stop_retry_worker_;
            }
        );

        if (should_stop) {
            break;
        }

        lock.unlock();

        retryPendingReplications();

        lock.lock();
    }
}