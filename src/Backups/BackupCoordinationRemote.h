#pragma once

#include <Backups/IBackupCoordination.h>
#include <Backups/BackupCoordinationFileInfos.h>
#include <Backups/BackupCoordinationReplicatedAccess.h>
#include <Backups/BackupCoordinationReplicatedSQLObjects.h>
#include <Backups/BackupCoordinationReplicatedTables.h>
#include <Backups/BackupCoordinationKeeperMapTables.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Backups/WithRetries.h>


namespace DB
{
class BackupLocalConcurrencyChecker;

/// Implementation of the IBackupCoordination interface performing coordination via ZooKeeper. It's necessary for "BACKUP ON CLUSTER".
class BackupCoordinationRemote : public IBackupCoordination
{
public:
    /// Empty string as the current host is used to mark the initiator of a BACKUP ON CLUSTER query.
    static const constexpr std::string_view kInitiator;

    BackupCoordinationRemote(
        const UUID & backup_uuid_,
        bool is_plain_backup_,
        const String & root_zookeeper_path_,
        zkutil::GetZooKeeper get_zookeeper_,
        const BackupKeeperSettings & keeper_settings_,
        const String & current_host_,
        const Strings & all_hosts_,
        BackupLocalConcurrencyChecker & concurrency_checker_,
        bool allow_concurrent_backup_,
        ThreadPoolCallbackRunnerUnsafe<void> schedule_,
        QueryStatusPtr process_list_element_);

    ~BackupCoordinationRemote() override;

    void finish(bool & all_hosts_finished) override;
    bool tryFinish(bool & all_hosts_finished) noexcept override;
    void cleanup() override;
    bool tryCleanup() noexcept override;

    void setStage(const String & new_stage, const String & message) override;
    void setError(const Exception & exception) override;
    Strings waitForStage(const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) override;
    std::chrono::seconds getOnClusterInitializationTimeout() const override;

    void addReplicatedPartNames(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<PartNameAndChecksum> & part_names_and_checksums) override;

    Strings getReplicatedPartNames(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedMutations(
        const String & table_zk_path,
        const String & table_name_for_logs,
        const String & replica_name,
        const std::vector<MutationInfo> & mutations) override;

    std::vector<MutationInfo> getReplicatedMutations(const String & table_zk_path, const String & replica_name) const override;

    void addReplicatedDataPath(const String & table_zk_path, const String & data_path) override;
    Strings getReplicatedDataPaths(const String & table_zk_path) const override;

    void addReplicatedAccessFilePath(const String & access_zk_path, AccessEntityType access_entity_type, const String & file_path) override;
    Strings getReplicatedAccessFilePaths(const String & access_zk_path, AccessEntityType access_entity_type) const override;

    void addReplicatedSQLObjectsDir(const String & loader_zk_path, UserDefinedSQLObjectType object_type, const String & dir_path) override;
    Strings getReplicatedSQLObjectsDirs(const String & loader_zk_path, UserDefinedSQLObjectType object_type) const override;

    void addKeeperMapTable(const String & table_zookeeper_root_path, const String & table_id, const String & data_path_in_backup) override;
    String getKeeperMapDataPath(const String & table_zookeeper_root_path) const override;

    void addFileInfos(BackupFileInfos && file_infos) override;
    BackupFileInfos getFileInfos() const override;
    BackupFileInfos getFileInfosForAllHosts() const override;
    bool startWritingFile(size_t data_file_index) override;

    static size_t findCurrentHostIndex(const String & current_host, const Strings & all_hosts);

private:
    void createRootNodes();

    bool tryFinishImpl() noexcept;
    bool tryFinishImpl(bool & all_hosts_finish) noexcept;
    bool tryCleanupImpl() noexcept;
    void removeAllNodes();
    bool tryRemoveAllNodes() noexcept;

    void serializeToMultipleZooKeeperNodes(const String & path, const String & value, const String & logging_name);
    String deserializeFromMultipleZooKeeperNodes(const String & path, const String & logging_name) const;

    /// Reads data of all objects from ZooKeeper that replicas have added to backup and add it to the corresponding
    /// BackupCoordinationReplicated* objects.
    /// After that, calling addReplicated* functions is not allowed and throws an exception.
    void prepareReplicatedTables() const TSA_REQUIRES(replicated_tables_mutex);
    void prepareReplicatedAccess() const TSA_REQUIRES(replicated_access_mutex);
    void prepareReplicatedSQLObjects() const TSA_REQUIRES(replicated_sql_objects_mutex);
    void prepareKeeperMapTables() const TSA_REQUIRES(keeper_map_tables_mutex);
    void prepareFileInfos() const TSA_REQUIRES(file_infos_mutex);

    const String root_zookeeper_path;
    const String zookeeper_path;
    const BackupKeeperSettings keeper_settings;
    const UUID backup_uuid;
    const Strings all_hosts;
    const String current_host;
    const size_t current_host_index;
    const bool plain_backup;
    LoggerPtr const log;

    const WithRetries with_retries;
    scope_guard concurrency_check TSA_GUARDED_BY(concurrency_check_mutex);
    BackupCoordinationStageSync stage_sync;

    std::atomic<bool> all_nodes_removed = false;
    std::atomic<bool> failed_to_remove_all_nodes = false;

    mutable std::optional<BackupCoordinationReplicatedTables> replicated_tables TSA_GUARDED_BY(replicated_tables_mutex);
    mutable std::optional<BackupCoordinationReplicatedAccess> replicated_access TSA_GUARDED_BY(replicated_access_mutex);
    mutable std::optional<BackupCoordinationReplicatedSQLObjects> replicated_sql_objects TSA_GUARDED_BY(replicated_sql_objects_mutex);
    mutable std::optional<BackupCoordinationFileInfos> file_infos TSA_GUARDED_BY(file_infos_mutex);
    mutable std::optional<BackupCoordinationKeeperMapTables> keeper_map_tables TSA_GUARDED_BY(keeper_map_tables_mutex);
    std::unordered_set<size_t> writing_files TSA_GUARDED_BY(writing_files_mutex);

    mutable std::mutex concurrency_check_mutex;
    mutable std::mutex zookeeper_mutex;
    mutable std::mutex replicated_tables_mutex;
    mutable std::mutex replicated_access_mutex;
    mutable std::mutex replicated_sql_objects_mutex;
    mutable std::mutex file_infos_mutex;
    mutable std::mutex writing_files_mutex;
    mutable std::mutex keeper_map_tables_mutex;
};

}
