#include <Interpreters/InterpreterBackupQuery.h>
#include <Backup/IBackup.h>
#include <Backup/IBackupEntry.h>
#include <Backup/BackupEntryFromMemory.h>
#include <Backup/BackupUtils.h>
#include <Backup/RenamingInBackup.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/formatAST.h>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_ELEMENT_DUPLICATE;
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using Elements = ASTBackupQuery::Elements;
    using ElementType = ASTBackupQuery::ElementType;

    /// Replaces an empty database with the current database inside a backup query.
    void replaceEmptyDatabaseWithCurrentDatabase(Elements & elements, const Context & context)
    {
        String current_database = context.getCurrentDatabase();
        for (auto & element : elements)
        {
            switch (element.type)
            {
                case ElementType::TABLE: [[fallthrough]];
                case ElementType::DICTIONARY:
                {
                    if (element.name.first.empty())
                        element.name.first = current_database;
                    if (element.name_in_backup.first.empty())
                        element.name_in_backup.first = current_database;
                    break;
                }
                default:
                    break;
            }
        }
    }

    /// Replaces an element of type EVERYTHING with two elements of types ALL_DATABASES and ALL_TEMPORARY_TABLES.
    void replaceElementTypeEverything(Elements & elements)
    {
        for (auto & element : elements)
        {
            if (element.type == ElementType::EVERYTHING)
            {
                element.type = ElementType::ALL_DATABASES;
                elements.emplace_back().type = ElementType::ALL_TEMPORARY_TABLES;
                break;
            }
        }
    }

    /// Removes duplications in the elements of a backup query by removing some excessive elements and by updating
    /// except_lists in some other elements.
    /// This function helps deduplicate elements in queries like "BACKUP ALL DATABASES, DATABASE xxx USING NAME yyy"
    /// (we need a deduplication for that query because `ALL DATABASES` includes `xxx` however we don't want
    /// to backup/restore the same database twice while executing the same query).
    void deduplicateElements(Elements & elements)
    {
        struct DatabaseInfo
        {
            size_t index = static_cast<size_t>(-1);
            std::unordered_map<String, size_t> tables;
        };
        std::unordered_map<String, DatabaseInfo> databases;
        std::unordered_map<String, size_t> temporary_tables;

        std::set<size_t> skip_indices;
        size_t index_all_databases = static_cast<size_t>(-1);
        size_t index_all_temporary_tables = static_cast<size_t>(-1);

        for (size_t i : ext::range(elements.size()))
        {
            auto & element = elements[i];
            switch (element.type)
            {
                case ElementType::ALL_DATABASES:
                {
                    if (index_all_databases == static_cast<size_t>(-1))
                    {
                        index_all_databases = i;
                    }
                    else
                    {
                        size_t prev_index = index_all_databases;
                        if (elements[i].except_list == elements[prev_index].except_list)
                            skip_indices.emplace(i);
                        else
                            throw Exception("The tag ALL DATABASES was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                    }
                    break;
                }

                case ElementType::DATABASE:
                {
                    auto it = databases.find(element.name.first);
                    if (it == databases.end())
                    {
                        databases.emplace(element.name.first, DatabaseInfo{.index = i});
                    }
                    else if (it->second.index == static_cast<size_t>(-1))
                    {
                        it->second.index = i;
                    }
                    else
                    {
                        size_t prev_index = it->second.index;
                        if ((elements[i].name_in_backup == elements[prev_index].name_in_backup)
                            && (elements[i].except_list == elements[prev_index].except_list))
                        {
                            skip_indices.emplace(i);
                        }
                        else
                        {
                            throw Exception("Database " + backQuote(element.name.first) + " was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                        }

                    }
                    break;
                }

                case ElementType::TABLE: [[fallthrough]];
                case ElementType::DICTIONARY:
                {
                    auto & tables = databases.emplace(element.name.first, DatabaseInfo{}).first->second.tables;
                    auto it = tables.find(element.name.second);
                    if (it == tables.end())
                    {
                        tables.emplace(element.name.second, i);
                    }
                    else
                    {
                        size_t prev_index = it->second;
                        if ((elements[i].name_in_backup == elements[prev_index].name_in_backup)
                            && (elements[i].partitions.empty() == elements[prev_index].partitions.empty()))
                        {
                            elements[prev_index].partitions.insert(elements[i].partitions.begin(), elements[i].partitions.end());
                            skip_indices.emplace(i);
                        }
                        else
                        {
                            throw Exception(
                                "Table " + backQuote(element.name.first) + "." + backQuote(element.name.second) + " was specified twice",
                                ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                        }
                    }
                    break;
                }

                case ElementType::ALL_TEMPORARY_TABLES:
                {
                    if (index_all_temporary_tables == static_cast<size_t>(-1))
                    {
                        index_all_temporary_tables = i;
                    }
                    else
                    {
                        size_t prev_index = index_all_temporary_tables;
                        if (elements[i].except_list == elements[prev_index].except_list)
                            skip_indices.emplace(i);
                        else
                            throw Exception("The tag ALL DATABASES was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);
                    }
                    break;
                }

                case ElementType::TEMPORARY_TABLE:
                {
                    auto it = temporary_tables.find(element.name.second);
                    if (it == temporary_tables.end())
                    {
                        temporary_tables.emplace(element.name.second, i);
                    }
                    else
                    {
                        size_t prev_index = it->second;
                        if (elements[i].name_in_backup == elements[prev_index].name_in_backup)
                            skip_indices.emplace(i);
                        else
                            throw Exception("Temporary table " + backQuote(element.name.second) + " was specified twice", ErrorCodes::BACKUP_ELEMENT_DUPLICATE);

                    }
                    break;
                }

                default:
                    assert(false);
            }
        }

        if (index_all_databases != static_cast<size_t>(-1))
        {
            for (auto & [database_name, database] : databases)
            {
                if (database.index == static_cast<size_t>(-1))
                {
                    elements.push_back(Element{.name = DatabaseAndTableName{database_name, ""}, .type = ElementType::DATABASE});
                    database.index = elements.size() - 1;
                }
            }
        }

        for (const auto & [database_name, database] : databases)
        {
            if (index_all_databases != static_cast<size_t>(-1))
                elements[index_all_databases].except_list.emplace(database_name);

            for (auto & [table_name, table_index] : database.tables)
                elements[database.index].except_list.emplace(table_name);
        }

        for (auto skip_index : skip_indices | boost::adaptors::reversed)
            elements.erase(elements.begin() + skip_index);
    }


    std::pair<String, std::unique_ptr<IBackupEntry>> makeBackupEntryForCreateQuery(const ASTPtr & create_query, const RenamingInBackup & renaming)
    {
        ASTPtr create_query_after_renaming = renaming.renameInCreateQuery(create_query);
        auto backup_entry = std::make_unique<BackupEntryFromMemory>(serializeAST(*create_query));
        auto ASTCreateQuery
    };


    BackupEntries makeBackupEntryForDictionary(const DatabasePtr & database, const String & dictionary_name, const DatabaseAndTableName & name_in_backup, const Context & context)
    {

    }

    BackupEntries makeBackupEntryForTable(const DatabaseAndTable & database_and_table, const String & table_name, const DatabaseAndTableName & name_in_backup, const std::set<String> & partitions, const Context & context)
    {

    }

    BackupEntries makeBackupEntryForDatabase(const DatabasePtr & database, const String & name_in_backup, const std::set<String> & exception_list, const Context & context)
    {

    }

    BackupEntries makeBackupEntryForTemporaryTable(const DatabasePtr & temporary_database, const StoragePtr & temporary_table, const String & table_name, const String & name_in_backup, const Context & context)
    {

    }

    BackupEntries makeBackupEntries(const Elements & elements, const Context & context)
    {
        assert (query.kind == ASTBackupQuery::Kind::BACKUP);
        RenamingInBackup renaming{query};
        BackupEntries backup_entries;

        for (const auto & element : query.elements)
        {
            switch (element.type)
            {
                case ElementType::ALL_DATABASES:
                {
                    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases())
                    {
                        if (element.except_list.contains(database_name))
                            continue;
                        backup_entries.emplace_back(createBackupEntryForCreateQuery(database->getCreateDatabaseQuery(), renaming));
                        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
                        {
                            backup_entries.emplace_back(createBackupEntryForCreateQuery(database->getCreateTableQuery(it->name(), context), renaming));
                            auto table = it->table;
                            BackupEntries table_backup;
                            if (element.partitions.empty())
                                table_backup = table->backup(context, element.name_in_backup);
                            else
                                table_backup = table->backupPartitions(context, element.name_in_backup, element.partitions);
                            boost::range::push_back(backup_entries, std::move(table_backup));
                        }
                    }
                    break;
                }

                case ElementType::DATABASE:
                {
                    const String & database_name = element.name.first;
                    auto database = DatabaseCatalog::instance().getDatabase(database_name);
                    backup_entries.emplace_back(createBackupEntryForCreateQuery(database->getCreateDatabaseQuery(), renaming));
                    for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
                    {
                        if (element.except_list.contains(it->name()))
                            continue;
                        backup_entries.emplace_back(createBackupEntryForCreateQuery(database->getCreateTableQuery(it->name(), context), renaming));
                        auto table = it->table;
                        BackupEntries table_backup;
                        if (element.partitions.empty())
                            table_backup = table->backup(context, element.name_in_backup);
                        else
                            table_backup = table->backupPartitions(context, element.name_in_backup, element.partitions);
                        boost::range::push_back(backup_entries, std::move(table_backup));
                    }
                    break;
                }

                case ElementType::TABLE:
                {
                    const String & database_name = element.name.first;
                    const String & table_name = element.name.second;
                    auto [database, table] = DatabaseCatalog::instance().getDatabaseAndTable(database_name, table_name);
                    backup_entries.emplace_back(createBackupEntryForCreateQuery(database->getCreateTableQuery(it->name(), context), renaming));
                    BackupEntries table_backup;
                    if (element.partitions.empty())
                        table_backup = table->backup(context, element.name_in_backup);
                    else
                        table_backup = table->backupPartitions(context, element.name_in_backup, element.partitions);
                    boost::range::push_back(backup_entries, std::move(table_backup));
                    break;
                }
            }

        }

        /// Check that all backup entries are unique.
        std::sort(backup_entries.begin(), backup_entries.end(), [](const std::pair<String, std::unique_ptr<IBackupEntry>> & lhs, const std::pair<String, std::unique_ptr<IBackupEntry>> & rhs)
                    { return lhs.first < rhs.first; });
        auto adjancent = std::adjancent_find(backup_entries.begin(), backup_entries.end());
        if (adjancent != backup_entries.end())
            throw Exception("Cannot write multiple entries with the same name " + quoteString(adjancent->first), ErrorCodes::BACKUP_ELEMENT_DUPLICATE);

        if (backup_entries.empty())
            throw Exception("No entries to put to backup", ErrorCodes::BACKUP_IS_EMPTY);

        return backup_entries;
    }


    void makeBackup(const ASTBackupQuery & query, const Context & context)
    {
        assert (query.kind == ASTBackupQuery::Kind::BACKUP);
        RenamingInBackup renaming{query};

        BackupEntries backup_entries = makeBackupEntries(query.elements, context);
        std::shared_ptr<const IBackup> base_backup;
        if (!query.base_backup_name.empty())
            base_backup = readBackup(context, query.base_backup_name);

        UInt64 estimated_backup_size = estimateBackupSize(backup_entries, base_backup);
        auto backup = createBackup(context, query.backup_name, estimated_backup_size, base_backup);

        size_t max_threads_for_backup = context.getSettingsRef().max_backup_threads;
        if (!max_threads_for_backup)
            max_threads_for_backup = 1;
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

        for (const auto & entry : query.elements)
        {
            switch (entry.type)
            {
                case ASTBackupQuery::ElementType::TABLE:
                {
                    boost::range::push_back(restore_tasks, restoreTableFromBackup(context, entry.name, nullptr, nullptr, entry.partitions, *backup, entry.name_in_backup, query.replace_table_if_exists));
                    break;
                }
                case ASTBackupQuery::ElementType::DICTIONARY:
                {
                    boost::range::push_back(restore_tasks, restoreDictionaryFromBackup(context, entry.name, nullptr, *backup, entry.name_in_backup));
                    break;
                }
                case ASTBackupQuery::ElementType::DATABASE:
                {
                    boost::range::push_back(restore_tasks, restoreDatabaseFromBackup(context, entry.name.first, nullptr, *backup, entry.name_in_backup.first, query.replace_database_if_exists, query.replace_table_if_exists));
                    break;
                }
                case ASTBackupQuery::ElementType::EVERYTHING:
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
    replaceEmptyDatabaseWithCurrentDatabase(query.elements, context);
    replaceElementTypeEverything(query.elements);
    deduplicateElements(query.elements);

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
