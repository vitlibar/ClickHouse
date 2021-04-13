#include <Interpreters/InterpreterBackupQuery.h>
#include <Interpreters/Context.h>
#include <Backup/IBackupEntry.h>
#include <Backup/BackupOnDisk.h>
#include <Parsers/ASTBackupQuery.h>
#include <Disks/IVolume.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

namespace
{
    using TableInfo = ASTBackupQuery::TableInfo;
    using DatabaseInfo = ASTBackupQuery::DatabaseInfo;

    std::shared_ptr<const IBackup> openBackupForReading(const Context & context, const String & backup_name)
    {
        auto backups_volume = context.getBackupsVolume();
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

    BackupEntries backupTable(const Context & context, const TableInfo & info)
    {
        auto [database, table] = DatabaseCatalog::instance().getDatabaseAndTable(StorageID{info.database_name, info.table_name});


        table->backup(context);

        auto get_attach_query = [&]
        {
            auto ast = typeid_cast<std::shared_ptr<ASTCreateQuery>>(getCreateTableQuery(table_name, context));
            ast->attach = true;
            ast->uuid = UUIDHelpers::Nil;
            return serializeAST(*ast);
        };

        String attach_query = get_attach_query();
        BackupEntries entries;

        while (true)
        {
            auto entries = storage->backup(context);
            String new_attach_query = get_attach_query();
            if (new_attach_query == attach_query)
                break;
            attach_query = std::move(new_attach_query);
        }

        entries.push_back(std::make_unique<BackupEntryFromMemory>(
                    "metadata/" + escapeForFileName(getDatabaseName()) + "/" + escapeForFileName(table_name) + ".sql",
                    attach_query));

        return entries;
    }

    BackupEntries backupDatabase(const Context & context, const DatabaseInfo & info)
    {

    }

    BackupEntries backupAllDatabases(const Context & context)
    {

    }
}


BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<ASTBackupQuery &>();

    auto backups_volume = context.getBackupsVolume();
    if (!backups_volume)
        throw Exception("No backups volume", ErrorCodes::LOGICAL_ERROR);

    if (query.kind == ASTBackupQuery::Kind::BACKUP)
    {
        BackupEntries entries;
        if (query.all_databases)
            boost::range::push_back(entries, backupAllDatabases(context));
        for (const auto & database : query.databases)
            boost::range::push_back(entries, backupDatabase(context, database));
        for (const auto & table : query.tables)
            boost::range::push_back(entries, backupTable(context, table));
        if (entries.empty())
            throw Exception("No entries to put to backup", ErrorCodes::BACKUP_IS_EMPTY);

        std::shared_ptr<const IBackup> base_backup;
        if (query.use_incremental_backup)
            base_backup = openBackupForReading(context, query.base_backup_name);

        UInt64 new_backup_size = estimateBackupSize(entries, base_backup);
        auto reservation = backups_volume->reserve(new_backup_size);
        if (!reservation)
            throw Exception("Not enough space for a new backup", ErrorCodes::NOT_ENOUGH_SPACE);

        BackupOnDisk backup{IBackup::OpenMode::CREATE, reservation->getDisk(), query.backup_name, base_backup};

        for (auto & entry : entries)
            backup.write(std::move(entry));

        backup.finishWriting();
    }
    else
    {
    }
    return {};
}

}

#if 0
BackupEntries DatabaseWithOwnTablesBase::backupTable(const String & table_name, const Context & context) const
{
    auto storage = tryGetTable(table_name, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                        backQuote(getDatabaseName()), backQuote(table_name));

    auto get_attach_query = [&]
    {
        auto ast = typeid_cast<std::shared_ptr<ASTCreateQuery>>(getCreateTableQuery(table_name, context));
        ast->attach = true;
        ast->uuid = UUIDHelpers::Nil;
        return serializeAST(*ast);
    };

    String attach_query = get_attach_query();
    BackupEntries entries;

    while (true)
    {
        auto entries = storage->backup(context);
        String new_attach_query = get_attach_query();
        if (new_attach_query == attach_query)
            break;
        attach_query = std::move(new_attach_query);
    }

    entries.push_back(std::make_unique<BackupEntryFromMemory>(
                "metadata/" + escapeForFileName(getDatabaseName()) + "/" + escapeForFileName(table_name) + ".sql",
                attach_query));

    return entries;
}

BackupEntries DatabaseOnDisk::backupTable(const String & table_name) const
{
    auto storage = tryGetTable(table_name, global_context);
    if (!storage)
        return {};
    BackupEntries entries = storage->backup();
    String metadata_entry_name = "metadata/" + getDatabaseName() + escapeForFileName(table_name) + ".sql";
    entries.push_back(std::make_unique<BackupEntryFromFile>(metadata_entry_name, getObjectMetadataPath(table_name)));
    return entries;
}

void DatabaseOnDisk::restoreTable(const IBackup & backup, const String & table_name)
{
    String metadata_entry_name = "metadata/" + getDatabaseName() + escapeForFileName(table_name) + ".sql";
    if (!backup.exists(metadata_entry_name))
        return;
    auto entry = backup.read(metadata_entry_name);
    auto
    copyData(entry.getReadBuffer(),

    auto storage = tryGetTable(table_name, global_context);
    if (!storage)
        return {};
    BackupEntries entries = storage->backup();
    entries.push_back(std::make_unique<BackupEntryFromFile>(
        "metadata/" + getDatabaseName() + escapeForFileName(object_name) + ".sql", getObjectMetadataPath(table_name)));
    return entries;
}
#endif
