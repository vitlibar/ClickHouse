#pragma once

#include <Core/Types.h>


namespace DB
{
class IBackup;
class IBackupEntry;
using BackupEntries = std::map<String, std::unique_ptr<IBackupEntry>>;
class RenamingInBackup;
using RenamingInBackupPtr = std::shared_ptr<const RenamingInBackup>;
class Context;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
using DatabaseAndTableName = std::pair<String, String>;


/// Creates a new backup and open it for writing.
std::unique_ptr<IBackup> createBackup(
    const String & backup_name, UInt64 estimated_backup_size, const Context & context, const std::shared_ptr<const IBackup> & base_backup);

/// Opens an existing backup for reading.
std::shared_ptr<const IBackup> readBackup(const String & backup_name, const Context & context);

/// Estimate total size of the backup which can be written from the specified entries.
UInt64 estimateBackupSize(const BackupEntries & backup_entries, const std::shared_ptr<const IBackup> & base_backup);

/// Prepares entries to make a backup of all the database except the system one.
void backupEverything(BackupEntries & out_backup_entries, const Context & context, const RenamingInBackupPtr & renaming);

/// Prepares entries to make a backup of the specified database.
void backupDatabase(
    BackupEntries & out_backup_entries,
    const Context & context,
    const RenamingInBackupPtr & renaming,
    const String & database_name,
    DatabasePtr database = nullptr);

/// Prepares entries to make a backup of the specified table.
void backupTable(
    BackupEntries & out_backup_entries,
    const Context & context,
    const RenamingInBackupPtr & renaming,
    const DatabaseAndTableName & database_and_table_name,
    DatabasePtr database = nullptr,
    StoragePtr table = nullptr,
    const Strings & partition_ids = {});

/// Prepares entries to make a backup of the specified dictionary.
void backupDictionary(
    BackupEntries & out_backup_entries,
    const Context & context,
    const RenamingInBackupPtr & renaming,
    const DatabaseAndTableName & database_and_dictionary_name,
    DatabasePtr database = nullptr);

/// Restores everything from the backup.
void restoreEverythingFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming);

/// Restores a database from the backup.
void restoreDatabaseFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming,
    const String & database_name,
    DatabasePtr database = nullptr);

/// Restores a table from the backup.
void restoreTableFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming,
    const DatabaseAndTableName & database_and_table_name,
    DatabasePtr database = nullptr,
    StoragePtr table = nullptr,
    const Strings & partition_ids = {});

/// Restores a dictionary from the backup.
void restoreDictionaryFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming,
    const DatabaseAndTableName & database_and_dictionary_name,
    DatabasePtr database = nullptr);

}
