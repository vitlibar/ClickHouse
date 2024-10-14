#pragma once

#include <Core/UUID.h>
#include <base/scope_guard.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class BackupConcurrencyCounters;

/// Local checker for concurrent backup or restore operations.
class BackupConcurrencyCheck
{
public:
    /// Checks concurrency for a BACKUP or for a RESTORE.
    /// Keep the constructed object until the operation is done.
    BackupConcurrencyCheck(
        bool is_restore_,
        const UUID & backup_or_restore_uuid_,
        bool on_cluster_,
        bool allow_concurrency_,
        BackupConcurrencyCounters & counters_);

    ~BackupConcurrencyCheck();

    [[noreturn]] static void throwConcurrentOperationNotAllowed(bool is_restore);

private:
    const bool is_restore;
    const UUID backup_or_restore_uuid;
    const bool on_cluster;
    BackupConcurrencyCounters & counters;
};


class BackupConcurrencyCounters
{
public:
    BackupConcurrencyCounters();
    ~BackupConcurrencyCounters();

private:
    friend class BackupConcurrencyCheck;
    size_t local_backups TSA_GUARDED_BY(mutex) = 0;
    size_t local_restores TSA_GUARDED_BY(mutex) = 0;
    std::unordered_map<UUID /* backup_uuid */, size_t /* num_refs */> on_cluster_backups TSA_GUARDED_BY(mutex);
    std::unordered_map<UUID /* restore_uuid */, size_t /* num_refs */> on_cluster_restores TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
