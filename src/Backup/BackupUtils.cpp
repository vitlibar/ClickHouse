#include <Backup/BackupUtils.h>
#include <Backup/BackupEntryFromAttachQuery.h>
#include <Backup/BackupOnDisk.h>
#include <Backup/IBackupEntry.h>
#include <Backup/RenamingInBackup.h>
#include <Common/escapeForFileName.h>
#include <Databases/IDatabase.h>
#include <Disks/IVolume.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int UNKNOWN_DATABASE;
}

namespace
{
    void executeCreateQuery(
        Context & context,
        const IBackup & backup,
        const String & database_name,
        const String & table_name,
        const RenamingInBackup & renaming)
    {
        String path_to_metadata = "metadata/" + escapeForFileName(database_name);
        if (table_name.empty())
            path_to_metadata += ".sql";
        else
            path_to_metadata = "/" + escapeForFileName(table_name) + ".sql";

        auto metadata_entry = backup.read(path_to_metadata);
        auto read_buffer = metadata_entry->getReadBuffer();
        String query;
        readStringUntilEOF(query, *read_buffer);
        read_buffer.reset();

        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            query.data(),
            query.data() + query.size(),
            "in file " + path_to_metadata + " from backup",
            0,
            context.getSettingsRef().max_parser_depth);

        auto & ast_create_query = ast->as<ASTCreateQuery &>();

        String new_database_name, new_table_name;
        if (table_name.empty())
            new_database_name = renaming.getNewName(database_name);
        else
            std::tie(new_database_name, new_table_name) = renaming.getNewName(DatabaseAndTableName{database_name, table_name});

        ast_create_query.database = new_database_name;
        if (!ast_create_query.table.empty())
            ast_create_query.table = new_table_name;

        InterpreterCreateQuery interpreter(ast, context);
        interpreter.setInternal(true);
        interpreter.setForceAttach(true);
        //interpreter.setForceRestoreData(has_force_restore_data_flag);
        interpreter.execute();
    }
}

std::unique_ptr<IBackup> createBackup(
    const String & backup_name, UInt64 estimated_backup_size, const Context & context, const std::shared_ptr<const IBackup> & base_backup)
{
    auto backups_volume = context.getBackupsVolume();
    if (!backups_volume)
        throw Exception("No backups volume", ErrorCodes::LOGICAL_ERROR);

    auto reservation = backups_volume->reserve(estimated_backup_size);
    if (!reservation)
        throw Exception("Not enough space for a new backup", ErrorCodes::NOT_ENOUGH_SPACE);

    return std::make_unique<BackupOnDisk>(IBackup::OpenMode::CREATE, reservation->getDisk(), backup_name, base_backup);
}


std::shared_ptr<const IBackup> readBackup(const Context & context, const String & backup_name)
{
    auto backups_volume = context.getBackupsVolume();
    if (!backups_volume)
        throw Exception("No backups volume", ErrorCodes::LOGICAL_ERROR);

    for (const auto & disk : backups_volume->getDisks())
    {
        if (disk->exists(backup_name))
            return std::make_shared<BackupOnDisk>(IBackup::OpenMode::READ, disk, backup_name);
    }

    throw Exception("Couldn't find the backup " + quoteString(backup_name), ErrorCodes::BAD_ARGUMENTS);
}


UInt64 estimateBackupSize(const BackupEntries & entries, const std::shared_ptr<const IBackup> & base_backup)
{
    UInt64 total_size = 0;
    for (const auto & entry : entries)
    {
        UInt64 data_size = entry->getDataSize();
        if (base_backup)
        {
            const String & path_in_backup = entry->getPathInBackup();
            if (base_backup->exists(path_in_backup) && (data_size == base_backup->getDataSize(path_in_backup)))
            {
                auto checksum = entry->tryGetChecksumFast();
                if (checksum && (*checksum == base_backup->getChecksum(path_in_backup)))
                    continue;
            }
        }
        total_size += data_size;
    }
    return total_size;
}


void backupAllDatabases(BackupEntries & out_backup_entries, const Context & context, const RenamingInBackupPtr & renaming)
{
    auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if ((database_name == DatabaseCatalog::TEMPORARY_DATABASE) ||
            (database_name == DatabaseCatalog::SYSTEM_DATABASE))
            continue;
        backupDatabase(out_backup_entries, context, renaming, database_name, database);
    }
}


void backupDatabase(BackupEntries & out_backup_entries, const Context & context, const RenamingInBackupPtr & renaming, const String & database_name, DatabasePtr database)
{
    if (!database)
        database = DatabaseCatalog::instance().getDatabase(database_name);

    auto new_database_name = renaming->getNewName(database_name);
    auto path_in_backup = escapeForFileName(new_database_name);

    auto it = database->getTablesIterator(context);
    while (it->isValid())
    {
        backupTable(
            out_backup_entries,
            context,
            renaming,
            DatabaseAndTableName{database_name, it->name()},
            database,
            it->table(),
            /*partition_ids = */ {});
        it->next();
    }

    out_backup_entries.push_back(
        std::make_unique<BackupEntryFromAttachQuery>("metadata/" + path_in_backup + ".sql", database->getCreateDatabaseQuery(), renaming));
}


void backupTable(
    BackupEntries & out_backup_entries,
    const Context & context,
    const RenamingInBackupPtr & renaming,
    const DatabaseAndTableName & database_and_table_name,
    DatabasePtr database,
    StoragePtr table,
    const Strings & partition_ids)
{
    const auto & database_name = database_and_table_name.first;
    const auto & table_name = database_and_table_name.second;
    if (!database)
    {
        database = DatabaseCatalog::instance().getDatabase(database_name);
    }
    if (!table)
    {
        table = database->tryGetTable(table_name, context);
        if (!table)
            throw Exception("Table " + backQuote(table_name) + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
    }

    auto new_name = renaming->getNewName(database_and_table_name);
    auto path_in_backup = escapeForFileName(new_name.first) + "/" + escapeForFileName(new_name.second);

    if (partition_ids.empty())
        table->backup(out_backup_entries, context, "data/" + path_in_backup + "/");
    else
        table->backupPartitions(out_backup_entries, context, "data/" + path_in_backup + "/", partition_ids);

    out_backup_entries.push_back(std::make_unique<BackupEntryFromAttachQuery>(
        "metadata/" + path_in_backup + ".sql", database->getCreateTableQuery(table_name, context), renaming));
}


void restoreEverythingFromBackup(
    Context & context, const IBackup & backup, const RenamingInBackup & renaming, RestoreMode restore_mode)
{
    auto metadata_contents = backup.list("metadata/");
    for (const auto & metadata_element : metadata_contents)
    {
        if (metadata_element.ends_with(".sql"))
        {
            String database_name = unescapeForFileName(std::filesystem::path(metadata_element).stem());
            restoreDatabaseFromBackup(context, backup, renaming, restore_mode, database_name);
        }
    }
}


void restoreDatabaseFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming,
    RestoreMode restore_mode,
    const String & database_name,
    DatabasePtr database)
{
    String new_name = renaming.getNewName(database_name);
    if (!database)
    {
        database = DatabaseCatalog::instance().tryGetDatabase(new_name);
        if (!database)
        {
            executeCreateQuery(context, backup, database_name, {}, renaming);
            database = DatabaseCatalog::instance().getDatabase(new_name);
        }
    }

    auto metadata_contents = backup.list("metadata/" + escapeForFileName(database_name) + "/");
    for (const auto & metadata_element : metadata_contents)
    {
        if (metadata_element.ends_with(".sql"))
        {
            String table_name = unescapeForFileName(std::filesystem::path(metadata_element).stem());
            restoreTableFromBackup(context, backup, renaming, restore_mode, DatabaseAndTableName{database_name, table_name}, database, {});
        }
    }
}

void restoreTableFromBackup(
    Context & context,
    const IBackup & backup,
    const RenamingInBackup & renaming,
    RestoreMode restore_mode,
    const DatabaseAndTableName & database_and_table_name,
    DatabasePtr database,
    StoragePtr table,
    const Strings & partition_ids)
{
    const String & database_name = database_and_table_name.first;
    const String & table_name = database_and_table_name.second;
    DatabaseAndTableName new_name = renaming.getNewName(database_and_table_name);

    if (!database)
    {
        database = DatabaseCatalog::instance().tryGetDatabase(new_name.first);
        if (!database)
        {
            executeCreateQuery(context, backup, database_name, {}, renaming);
            database = DatabaseCatalog::instance().getDatabase(new_name.first);
        }
    }

    if (!table)
    {
        table = database->tryGetTable(new_name.second, context);
        if (!table)
        {
            executeCreateQuery(context, backup, database_name, table_name, renaming);
            table = database->tryGetTable(new_name.second, context);
            if (!table)
                throw Exception("Table " + backQuote(new_name.second) + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
        }
    }

    String path_in_backup = "metadata/" + escapeForFileName(database_name) + "/" + escapeForFileName(table_name) + "/";
    if (partition_ids.empty())
        table->restoreFromBackup(context, backup, path_in_backup, restore_mode);
    else
        table->restorePartitionsFromBackup(context, backup, path_in_backup, restore_mode, partition_ids);
}

}
