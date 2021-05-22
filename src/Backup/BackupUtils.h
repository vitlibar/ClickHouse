#pragma once

#include <Core/Types.h>
#include <unordered_set>


namespace DB
{
class IBackup;
class IBackupEntry;
using BackupEntries = std::vector<std::pair<String, std::unique_ptr<IBackupEntry>>>;
using RestoreTasks = std::vector<std::function<void()>>;
class Context;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using DatabaseAndTableName = std::pair<String, String>;
class RenamingInBackup;
class RestoreWithReplaceMode;


/// Creates a new backup and open it for writing.
std::shared_ptr<IBackup> createBackup(
    const Context & context, const String & backup_name, UInt64 estimated_backup_size, const std::shared_ptr<const IBackup> & base_backup);

/// Opens an existing backup for reading.
std::shared_ptr<const IBackup> readBackup(const Context & context, const String & backup_name);

/// Estimate total size of the backup which can be written from the specified entries.
UInt64 estimateBackupSize(const BackupEntries & backup_entries, const std::shared_ptr<const IBackup> & base_backup);

/// Prepares entries to make a backup of a specified table.
BackupEntries backupTable(
    const Context & context,
    const DatabaseAndTableName & name,
    const Strings & partitions,
    const RenamingInBackup & renaming,
    DatabasePtr database = nullptr,
    StoragePtr table = nullptr);

/// Restore a specified table from a backup.
RestoreTasks restoreTableFromBackup(
    Context & context,
    const IBackup & backup,
    const DatabaseAndTableName & name_in_backup,
    const Strings & partitions,
    const RenamingInBackup & renaming,
    const RestoreWithReplaceMode & replace_mode,
    DatabasePtr database = nullptr,
    StoragePtr table = nullptr);

/// Prepares entries to make a backup of a specified database.
BackupEntries backupDatabase(
    const Context & context,
    const String & name,
    const std::unordered_set<String> & except_tables,
    const RenamingInBackup & renaming,
    DatabasePtr database = nullptr);

/// Restores a specified database from a backup.
RestoreTasks restoreDatabaseFromBackup(
    Context & context,
    const IBackup & backup,
    const String & name_in_backup,
    const std::unordered_set<String> & except_tables,
    const RenamingInBackup & renaming,
    const RestoreWithReplaceMode & replace_mode,
    DatabasePtr database = nullptr);

/// Prepares entries to make a backup of all databases (except the system one).
BackupEntries backupAllDatabases(const Context & context,
                                 const std::unordered_set<String> & except_databases,
                                 const RenamingInBackup & renaming);

RestoreTasks restoreAllDatabasesFromBackup(
    Context & context,
    const IBackup & backup,
    const std::unordered_set<String> & except_databases,
    const RenamingInBackup & renaming,
    const RestoreWithReplaceMode & replace_mode);

BackupEntries backupTemporaryTable(
        const Context & context,
        const String & name,
        const RenamingInBackup & renaming,
        DatabasePtr temporary_database = nullptr,
        StoragePtr table = nullptr);

RestoreTasks restoreTemporaryTable(
        Context & context,
        const IBackup & backup,
        const String & name_in_backup,
        const RenamingInBackup & renaming,
        const RestoreWithReplaceMode & replace_mode,
        DatabasePtr temporary_database = nullptr,
        StoragePtr table = nullptr);

BackupEntries backupAllTemporaryTables(
        const Context & context,
        const std::unordered_set<String> & except_tables,
        const RenamingInBackup & renaming,
        DatabasePtr temporary_database = nullptr);

RestoreTasks restoreAllTemporaryTables(
        Context & context,
        const IBackup & backup,
        const std::unordered_set<String> & except_tables,
        const RenamingInBackup & renaming,
        const RestoreWithReplaceMode & replace_mode,
        DatabasePtr temporary_database = nullptr);

}
