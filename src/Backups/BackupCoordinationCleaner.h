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
    bool tryCleanup() noexcept;

private:
    bool tryRemoveAllNodes(const WithRetries::Params & retries_params, bool throw_if_error);

    const String zookeeper_path;

    /// A reference to a field of the parent object which is either BackupCoordinationOnCluster or RestoreCoordinationOnCluster.
    const WithRetries & with_retries;

    const LoggerPtr log;

    bool succeeded TSA_GUARDED_BY(mutex) = false;
    bool failed TSA_GUARDED_BY(mutex) = false;

    std::mutex mutex;
};

}
