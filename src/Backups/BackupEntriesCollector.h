#pragma once

#include <Backups/BackupSettings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Core/QualifiedTableName.h>
#include <Parsers/ASTBackupQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/MergeTree/ZooKeeperRetries.h>
#include <filesystem>
#include <queue>


namespace DB
{

class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
using BackupEntries = std::vector<std::pair<String, BackupEntryPtr>>;
class IBackupCoordination;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
struct StorageID;
enum class AccessEntityType;

/// Collects backup entries for all databases and tables which should be put to a backup.
class BackupEntriesCollector : private boost::noncopyable
{
public:
    BackupEntriesCollector(const ASTBackupQuery::Elements & backup_query_elements_,
                           const BackupSettings & backup_settings_,
                           std::shared_ptr<IBackupCoordination> backup_coordination_,
                           const ReadSettings & read_settings_,
                           const ContextPtr & context_,
                           ThreadPool & threadpool_);
    ~BackupEntriesCollector();

    /// Collects backup entries and returns the result.
    /// This function first generates a list of databases and then call IDatabase::getTablesForBackup() for each database from this list.
    /// Then it calls IStorage::backupData() to build a list of backup entries.
    BackupEntries run();

    const BackupSettings & getBackupSettings() const { return backup_settings; }
    std::shared_ptr<IBackupCoordination> getBackupCoordination() const { return backup_coordination; }
    const ReadSettings & getReadSettings() const { return read_settings; }
    ContextPtr getContext() const { return context; }
    const ZooKeeperRetriesInfo & getZooKeeperRetriesInfo() const { return global_zookeeper_retries_info; }

    /// Adds a backup entry which will be later returned by run().
    /// These function can be called by implementations of IStorage::backupData() in inherited storage classes.
    void addBackupEntry(const String & file_name, BackupEntryPtr backup_entry);
    void addBackupEntry(const std::pair<String, BackupEntryPtr> & backup_entry);
    void addBackupEntries(const BackupEntries & backup_entries_);
    void addBackupEntries(BackupEntries && backup_entries_);

    /// Adds a function which must be called after all IStorage::backupData() have finished their work on all hosts.
    /// This function is designed to help making a consistent in some complex cases like
    /// 1) we need to join (in a backup) the data of replicated tables gathered on different hosts.
    void addPostTask(std::function<void()> task);

    /// Returns an incremental counter used to backup access control.
    size_t getAccessCounter(AccessEntityType type);

private:
    void calculateRootPathInBackup();

    void collectMetadata();
    void collectDatabasesMetadata();
    void collectTablesMetadataInOtherDBs();
    void collectTablesMetadataInOtherDBs_FindTables(const String & database_name, std::unordered_set<StoragePtr> & table_ptrs);
    void collectTablesMetadataInOtherDBs_GetCreateTableQueries();
    void collectTablesMetadataInReplicatedDBs();
    void collectTablesMetadataInReplicatedDB(const String & database_name);
    void collectTablesMetadataInReplicatedDBs_Sync();
    void collectTablesMetadataInReplicatedDB_Sync(const String & database_name);
    static std::function<bool(const String &)> makeTableNameFilter(const DatabaseInfo & database_info) const;
    void checkRequiredDatabasesFound();
    void checkRequiredTablesFound();
    void removeDatabaseInfosForDroppedDatabases();
    void removeTableInfosForDroppedTables();
    void calculateDatabasesPathsInBackup();
    void calculateTablesPathsInBackup();
    bool checkIsDatabaseDropped(const String & database_name);
    bool checkIsTableDropped(const QualifiedTableName & table_name);
    void checkStorageSupportForPartitions();
    void lockTablesForReading();

    void makeBackupEntriesForDatabasesDefs();
    void makeBackupEntriesForTablesDefs();
    void makeBackupEntriesForTablesData();
    void makeBackupEntriesForTableData(const QualifiedTableName & table_name);

    void addBackupEntryUnlocked(const String & file_name, BackupEntryPtr backup_entry);

    void runPostTasks();

    Strings setStage(const String & new_stage, const String & message = "");

    const ASTBackupQuery::Elements backup_query_elements;
    const BackupSettings backup_settings;
    std::shared_ptr<IBackupCoordination> backup_coordination;
    const ReadSettings read_settings;
    ContextPtr context;

    /// The time a BACKUP ON CLUSTER or RESTORE ON CLUSTER command will wait until all the nodes receive the BACKUP (or RESTORE) query and start working.
    /// This setting is similar to `distributed_ddl_task_timeout`.
    const std::chrono::milliseconds on_cluster_first_sync_timeout;

    Poco::Logger * log;
    /// Unfortunately we can use ZooKeeper for collecting information for backup
    /// and we need to retry...
    ZooKeeperRetriesInfo global_zookeeper_retries_info;

    Strings all_hosts;
    DDLRenamingMap renaming_map;
    std::filesystem::path root_path_in_backup;

    struct DatabaseInfo
    {
        bool throw_if_database_not_exists = false;
        DatabasePtr database;
        bool is_replicated_database = false;
        String replication_zk_path;
        ASTPtr create_database_query;
        bool should_backup_create_database_query = false;
        String metadata_path_in_backup;
        std::unordered_set<String> table_names;
        bool all_tables = false;
        std::unordered_set<String> except_tables;
    };

    std::unordered_set<StoragePtr> tables_by_ptr;

    struct TableInfo
    {
        bool throw_if_table_not_exists = false;
        DatabasePtr database;
        bool is_replicated_database = false;
        StoragePtr storage;
        bool table_found = false;
        TableLockHolder table_lock;
        ASTPtr create_table_query;
        String metadata_path_in_backup;
        std::filesystem::path data_path_in_backup;
        std::optional<String> replicated_table_shared_id;
        std::optional<ASTs> partitions;
    };

    String current_stage;

    std::unordered_map<String, DatabaseInfo> database_infos;
    std::unordered_map<QualifiedTableName, TableInfo> table_infos;

    BackupEntries backup_entries;
    std::queue<std::function<void()>> post_tasks;
    std::vector<size_t> access_counters;

    ThreadPool & threadpool;
    std::mutex mutex;
};

}
