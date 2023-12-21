#pragma once

namespace DB
{

/// Checks that there is no concurrent backups.
void checkNoConcurrentBackups(
    const String & backup_id,
    const UUID & backup_uuid,
    std::unordered_map<UUID, String> & active_backups_ids,
    std::mutex & active_backups_mutex);


/// List of the currently processed backups and restores for the concurrency check (check which is ).
class BackupsListForConcurrencyCheck
{
public:
    void addBackup(const String & backup_id, const UUID & backup_uuid, bool check_no_concurrent_backups);
    void addRestore(const String & restore_id, const UUID & restore_uuid, bool check_no_concurrent_restores);
    void remove(const UUID & backup_or_restore_uuid);

    void addOnClusterBackup(const String & backup_id, const UUID & backup_uuid, bool check_no_concurrent_backups,
                            zkutil::GetZooKeeper get_zookeeper, const String & root_zookeeper_path, const BackupKeeperSettings & keeper_settings);

    void addOnClusterRestore(const String & restore_id, const UUID & restore_uuid, bool check_no_concurrent_restores,
                             zkutil::GetZooKeeper get_zookeeper, const String & root_zookeeper_path, const BackupKeeperSettings & keeper_settings);    

private:
    std::unordered_map<UUID, String> active_ids TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};


void exposeOnClusterBackupToConcurrencyCheck(const String & backup_id, const UUID & backup_uuid, bool check_no_concurrent_backups,
                                             std::shared_ptr<BackupsListForConcurrencyCheck> active_backups_local_list,
                                             const String & current_host,
                                             zkutil::GetZooKeeper get_zookeeper,
                                             const String & root_zookeeper_path,
                                             const BackupKeeperSettings & keeper_settings)
{

}

void exposeOnClusterRestoreToConcurrencyCheck(const String & restore_id, const UUID & restore_uuid, bool check_no_concurrent_restores,
                                             std::shared_ptr<BackupsListForConcurrencyCheck> active_backups_local_list,
                                             const String & current_host,
                                             zkutil::GetZooKeeper get_zookeeper,
                                             const String & root_zookeeper_path,
                                             const BackupKeeperSettings & keeper_settings)
{

}




class BackupCoordination



    void addBackupCheckNoConcurrentBackups(const String & backup_id, const UUID & backup_uuid, const String & current_host,
                                           zkutil::GetZooKeeper get_zookeeper,
                                           const String & root_zookeeper_path,
                                           const BackupKeeperSettings & keeper_settings);

    void addRestoreCheckNoConcurrentRestores(const String & restore_id, const UUID & restore_uuid);


void checkNoConcurrentBackups(std::shared_ptr<ActiveBackupsForConcurrencyCheck> active_backups)
{

}


bool oConcurrentBackups

void checkNoConcurrentBackups(
    const String & backup_id,
    const UUID & backup_uuid,
    std::vector<std::pair<UUID, String>> & active_backups_ids,
    std::mutex & active_backups_mutex,
    const String & current_host,
    zkutil::GetZooKeeper get_zookeeper,
    const String & root_zookeeper_path,
    const BackupKeeperSettings & keeper_settings);

/// Checks that there is no concurrent backups.
void checkNoConcurrentRestores(
    const String & restore_id,
    const UUID & restore_uuid,
    std::vector<std::pair<UUID, String>> & active_restores_ids,
    std::mutex & active_restores_mutex);

void checkNoConcurrentRestores(
    const String & restore_id,
    const UUID & restore_uuid,
    std::unordered_map<UUID, String> & active_restores_ids,
    std::mutex & active_restores_mutex,
    const String & current_host,
    zkutil::GetZooKeeper get_zookeeper,
    const String & root_zookeeper_path,
    const BackupKeeperSettings & keeper_settings);





/// This class is used to find concurrent backups or restores.
class BackupConcurrencyFinderRemote
{
public:
    BackupConcurrencyFinderRemote(
        const UUID & backup_or_restore_uuid_,
        const BackupsWorker & backups_worker_,
        const String & root_zookeeper_path_,
        WithRetries & with_retries_,
        UInt64 timeout_if_no_alive_nodes_ms_,
        Poco::Logger * log_);

    /// Returns id of any concurrent backup.
    std::optional<String> getAnyConcurrentBackup() const;

    /// Returns id of any concurrent restore.
    std::optional<String> getAnyConcurrentRestore() const;

private:
    std::optional<String> getAnyConcurrentImpl(const String & type) const;

    const BackupConcurrencyFinderLocal local_finder;
    const UUID backup_or_restore_uuid;
    const String root_zookeeper_path;
    WithRetries & with_retries;
    const UInt64 timeout_if_no_alive_nodes_ms;
    Poco::Logger * const log;
};


class BackupsWorker;

class BackupConcurrencyFinderLocal
{
public:
    BackupConcurrencyFinderLocal(const UUID & backup_or_restore_uuid_, const BackupsWorker & backups_worker_);

    std::optional<String> getAnyConcurrentBackup() const;
    std::optional<String> getAnyConcurrentBackup() const;

private:
    const UUID backup_or_restore_uuid;
    const BackupsWorker & backups_worker;
};

}
