#include <Interpreters/InterpreterBackupQuery.h>
#include <Backup/IBackup.h>
#include <Backup/IBackupEntry.h>
#include <Backup/RenamingInBackup.h>
#include <Backup/BackupUtils.h>
#include <Parsers/ASTBackupQuery.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_IS_EMPTY;
}

BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<ASTBackupQuery &>();

    auto renaming = std::make_shared<RenamingInBackup>();
    for (const auto & database : query.databases)
    {
        if (!database.new_database_name.empty())
            renaming->add(database.database_name, database.new_database_name);
    }
    for (const auto & table : query.tables)
    {
        if (!table.new_table_name.second.empty())
            renaming->add(table.table_name, table.new_table_name);
    }

    if (query.kind == ASTBackupQuery::Kind::BACKUP)
    {
        BackupEntries entries;
        if (query.all_databases)
            backupAllDatabases(entries, context, renaming);
        else
        {
            for (const auto & database : query.databases)
                backupDatabase(entries, context, renaming, database.database_name);

            for (const auto & table : query.tables)
                backupTable(entries, context, renaming, table.table_name, {}, {}, table.partitions);
        }

        if (entries.empty())
            throw Exception("No entries to put to backup", ErrorCodes::BACKUP_IS_EMPTY);

        std::shared_ptr<const IBackup> base_backup;
        if (query.use_incremental_backup)
            base_backup = readBackup(query.base_backup_name, context);

        UInt64 estimated_backup_size = estimateBackupSize(entries, base_backup);
        auto backup = createBackup(query.backup_name, estimated_backup_size, context, base_backup);

        for (auto & backup_entry : entries)
            backup->write(std::move(backup_entry));

        backup->finishWriting();
    }
    else if (query.kind == ASTBackupQuery::Kind::RESTORE)
    {
        auto backup = readBackup(query.backup_name, context);

        if (query.all_databases)
            restoreEverythingFromBackup(context, *backup, *renaming, query.restore_mode);
        else
        {
            for (const auto & database : query.databases)
                restoreDatabaseFromBackup(context, *backup, *renaming, query.restore_mode, database.database_name);

            for (const auto & table : query.tables)
                restoreTableFromBackup(context, *backup, *renaming, query.restore_mode, table.table_name, {}, {}, table.partitions);
        }
    }

    return {};
}

}
