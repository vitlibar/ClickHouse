#pragma once

#include <Backups/WithRetries.h>


namespace DB
{

/// Removes nodes from ZooKeeper after a BACKUP or RESTORE operation (successful or not).
class BackupCoordinationCleaner
{
public:
    BackupCoordinationCleaner(const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_);

    void cleanup();
    bool tryCleanupAfterError() noexcept;

private:
    bool tryRemoveAllNodes(bool throw_if_error, const WithRetries::Params & retries_params);

    const String zookeeper_path;

    /// A reference to a field of the parent object which is either BackupCoordinationOnCluster or RestoreCoordinationOnCluster.
    const WithRetries & with_retries;

    const LoggerPtr log;

    struct CleanupResult
    {
        bool succeeded = false;
        std::exception_ptr exception;
    };
    CleanupResult cleanup_result TSA_GUARDED_BY(mutex);

    std::mutex mutex;
};

}
