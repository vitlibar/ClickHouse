#include <Interpreters/InterpreterBackupQuery.h>
#include <Backup/IBackup.h>
#include <Backup/IBackupEntry.h>
#include <Backup/RenamingInBackup.h>
#include <Backup/BackupUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/formatAST.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void replaceEmptyDatabaseWithCurrentDatabase(ASTBackupQuery & query, const Context & context)
    {
        String current_database = context.getCurrentDatabase();
        for (auto & entry : query.entries)
        {
            switch (entry.type)
            {
                case ASTBackupQuery::TABLE: [[fallthrough]];
                case ASTBackupQuery::DICTIONARY:
                {
                    if (entry.name.first.empty())
                        entry.name.first = current_database;
                    if (entry.name_in_backup.first.empty())
                        entry.name_in_backup.first = current_database;
                    break;
                }
                default:
                    break;
            }
        }
    }


    void replaceEverythingEntry(ASTBackupQuery & query)
    {
        for (auto & entry : query.entries)
        {
            if (entry.type == ASTBackupQuery::EVERYTHING)
            {
                entry.type = ASTBackupQuery::ALL_DATABASES;
                query.entries.emplace_back().type = ASTBackupQuery::ALL_TEMPORARY_TABLES;
                break;
            }
        }
    }


    void insertMissedDatabaseEntries(ASTBackupQuery & query)
    {
        bool all_databases_found = false;
        for (auto & entry : query.entries)
        {
            if (entry.type == ASTBackupQuery::ALL_DATABASES)
            {
                all_databases_found = true;
                break;
            }
        }

        if (!all_databases_found)
            return;

        std::unordered_set<String> database_names;
        for (auto & entry : query.entries)
        {
            if ((entry.type == ASTBackupQuery::TABLE) || (entry.type == ASTBackupQuery::DICTIONARY))
                database_names.emplace(entry.name.first);
        }

        for (auto & entry : query.entries)
        {
            if (entry.type == ASTBackupQuery::DATABASE)
                database_names.erase(entry.name.first);
        }

        for (const auto & database_name : database_names)
        {
            auto & new_entry = query.entries.emplace_back();
            new_entry.type = ASTBackupQuery::DATABASE;
            new_entry.name.first = database_name;
        }
    }


#if 0
    void sortEntriesForRestore(ASTBackupQuery & query)
    {
        /// Databases should be restored before tables and dictionaries.
        for (auto & entry : query.entries)
        {
            if (entry.type == ASTBackupQuery::DATABASE)
                database_names.erase(entry.name.first);
        }
    }
#endif


    void makeBackup(const ASTBackupQuery & query, const Context & context)
    {
        assert (query.kind == ASTBackupQuery::Kind::BACKUP);
        RenamingInBackup renaming{query};

        size_t max_threads_for_backup = context.getSettingsRef().max_backup_threads;
        if (!max_threads_for_backup)
            max_threads_for_backup = 1;

        BackupEntries backup_entries;
        for (const auto & entry : query.entries)
        {
            switch (entry.type)
            {
                case ASTBackupQuery::TABLE: [[fallthrough]];
                case ASTBackupQuery::DICTIONARY:
                {
                    boost::range::push_back(backup_entries, backupTable(context, entry.name, entry.partitions, renaming));
                    break;
                }
                case ASTBackupQuery::DATABASE:
                {
                    boost::range::push_back(backup_entries, backupDatabase(context, entry.name.first, renaming.getTableNames(entry.name.first), renaming));
                    break;
                }
                case ASTBackupQuery::ALL_DATABASES:
                {
                    boost::range::push_back(backup_entries, backupAllDatabases(context, renaming.getDatabaseNames(), renaming));
                    break;
                }
                case ASTBackupQuery::TEMPORARY_TABLE:
                {
                    boost::range::push_back(backup_entries, backupTemporaryTable(context, entry.name.second, renaming));
                    break;
                }
                case ASTBackupQuery::ALL_TEMPORARY_TABLES:
                {
                    boost::range::push_back(backup_entries, backupAllTemporaryTables(context, renaming.getTemporaryTableNames(), renaming));
                    break;
                }
                default:
                    throw Exception("Cannot backup entry type " + std::to_string(static_cast<int>(entry.type)), ErrorCodes::LOGICAL_ERROR);
            }
        }

        if (backup_entries.empty())
            throw Exception("No entries to put to backup", ErrorCodes::BACKUP_IS_EMPTY);

        std::shared_ptr<const IBackup> base_backup;
        if (!query.base_backup_name.empty())
            base_backup = readBackup(context, query.base_backup_name);

        UInt64 estimated_backup_size = estimateBackupSize(backup_entries, base_backup);
        auto backup = createBackup(context, query.backup_name, estimated_backup_size, base_backup);

        std::vector<ThreadFromGlobalPool> threads;
        size_t num_active_threads = 0;
        std::mutex mutex;
        std::condition_variable cond;
        std::exception_ptr exception;

        for (auto & [name, backup_entry] : backup_entries)
        {
            {
                std::unique_lock lock{mutex};
                if (exception)
                    break;
                cond.wait(lock, [&] { return num_active_threads < max_threads_for_backup; });
                if (exception)
                    break;
                ++num_active_threads;
            }

            threads.emplace_back([backup, name = name, backup_entry = std::move(backup_entry), &query, &mutex, &num_active_threads, &exception]() mutable
            {
                try
                {
                    backup->write(name, std::move(backup_entry));
                }
                catch (...)
                {
                    std::lock_guard lock{mutex};
                    if (!exception)
                        exception = std::current_exception();
                }

                {
                    std::lock_guard lock{mutex};
                    --num_active_threads;
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        if (exception)
        {
            backup_entries.clear();
            backup.reset();
            std::rethrow_exception(exception);
        }

        backup->finishWriting();
    }


    void restoreFromBackup(const ASTBackupQuery & query, Context & context)
    {
        assert (query.kind == ASTBackupQuery::Kind::RESTORE);
        size_t max_threads_for_restore = context.getSettingsRef().max_backup_threads;
        if (!max_threads_for_restore)
            max_threads_for_restore = 1;

        auto backup = readBackup(context, query.backup_name);
        RestoreTasks restore_tasks;

        for (const auto & entry : query.entries)
        {
            switch (entry.type)
            {
                case ASTBackupQuery::EntryType::TABLE:
                {
                    boost::range::push_back(restore_tasks, restoreTableFromBackup(context, entry.name, nullptr, nullptr, entry.partitions, *backup, entry.name_in_backup, query.replace_table_if_exists));
                    break;
                }
                case ASTBackupQuery::EntryType::DICTIONARY:
                {
                    boost::range::push_back(restore_tasks, restoreDictionaryFromBackup(context, entry.name, nullptr, *backup, entry.name_in_backup));
                    break;
                }
                case ASTBackupQuery::EntryType::DATABASE:
                {
                    boost::range::push_back(restore_tasks, restoreDatabaseFromBackup(context, entry.name.first, nullptr, *backup, entry.name_in_backup.first, query.replace_database_if_exists, query.replace_table_if_exists));
                    break;
                }
                case ASTBackupQuery::EntryType::EVERYTHING:
                {
                    boost::range::push_back(restore_tasks, restoreEverythingFromBackup(context, *backup, query.replace_database_if_exists, query.replace_table_if_exists));
                    break;
                }
                default:
                    throw Exception("Cannot restore entry type " + std::to_string(static_cast<int>(entry.type)), ErrorCodes::LOGICAL_ERROR);
            }
        }

        std::vector<ThreadFromGlobalPool> threads;
        size_t num_active_threads = 0;
        std::mutex mutex;
        std::condition_variable cond;
        std::exception_ptr exception;

        for (auto & restore_task : restore_tasks)
        {
            {
                std::unique_lock lock{mutex};
                if (exception)
                    break;
                cond.wait(lock, [&] { return num_active_threads < max_threads_for_restore; });
                if (exception)
                    break;
                ++num_active_threads;
            }

            threads.emplace_back([backup, restore_task = std::move(restore_task), &query, &mutex, &num_active_threads, &exception]() mutable
            {
                try
                {
                    std::move(restore_task)();
                }
                catch (...)
                {
                    std::lock_guard lock{mutex};
                    if (!exception)
                        exception = std::current_exception();
                }

                {
                    std::lock_guard lock{mutex};
                    --num_active_threads;
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        if (exception)
        {
            restore_tasks.clear();
            backup.reset();
            std::rethrow_exception(exception);
        }
    }
}


BlockIO InterpreterBackupQuery::execute()
{
    auto & query = query_ptr->as<ASTBackupQuery &>();
    replaceEmptyDatabaseWithCurrentDatabase(query, context);
    insertMissedDatabaseEntries(query);

    if (query.kind == ASTBackupQuery::BACKUP)
        makeBackup(query, context);
    else if (query.kind == ASTBackupQuery::RESTORE)
    {
        sortEntriesForRestore(query);
        restoreFromBackup(query, context);
    }

    return {};
}

}
