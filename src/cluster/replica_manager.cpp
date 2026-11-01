#include "replica_manager.h"

#include <string>
#include <utility>

ReplicaManager::ReplicaManager(
    std::size_t shard_count,
    const std::string& wal_prefix,
    const std::string& replica_host,
    int replica_port,
    std::chrono::milliseconds retry_interval
)
    : router_(shard_count),
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

        std::string primary_wal =
            wal_prefix +
            "_primary_" +
            std::to_string(i) +
            ".log";

        std::string replica_wal =
            wal_prefix +
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
        return {true, true};
    }

    primary_shards_[shard]->set(key, value);
    replica_shards_[shard]->set(key, value);

    if (!replication_client_) {
        return {true, true};
    }

    PendingOperation operation{
        PendingOperation::Type::SET,
        key,
        value
    };

    if (!pending_operations_[shard].empty()) {
        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {true, false};
    }

    if (!replicateOperation(operation)) {
        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {true, false};
    }

    return {true, true};
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

bool ReplicaManager::remove(
    const std::string& key
) {
    const ReplicationResult result =
        removeWithResult(key);

    return result.local_success;
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

        return {removed, removed};
    }

    const bool primary_removed =
        primary_shards_[shard]->remove(key);

    const bool replica_removed =
        replica_shards_[shard]->remove(key);

    const bool local_success =
        primary_removed || replica_removed;

    if (!replication_client_) {
        return {local_success, true};
    }

    PendingOperation operation{
        PendingOperation::Type::DELETE,
        key,
        ""
    };

    if (!pending_operations_[shard].empty()) {
        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {local_success, false};
    }

    if (!replicateOperation(operation)) {
        pending_operations_[shard].push_back(
            std::move(operation)
        );

        return {local_success, false};
    }

    return {local_success, true};
}

bool ReplicaManager::exists(
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
        return replica_shards_[shard]->exists(key);
    }

    return primary_shards_[shard]->exists(key);
}

void ReplicaManager::promoteReplica(
    const std::string& key
) {
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

    return replication_client_->sendDelete(
        operation.key
    );
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