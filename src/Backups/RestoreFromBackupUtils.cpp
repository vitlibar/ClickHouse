#include <Backups/BackupUtils.h>
#include <Backups/BackupRenamingConfig.h>
#include <Backups/IRestoreFromBackupTask.h>
#include <Backups/hasCompatibleDataToRestoreTable.h>
#include <Backups/renameInCreateQuery.h>
#include <Common/escapeForFileName.h>
#include <Databases/IDatabase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <base/insertAtEnd.h>
#include <boost/range/adaptor/reversed.hpp>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_ELEMENT_DUPLICATE;
    extern const int BACKUP_IS_EMPTY;
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using Elements = ASTBackupQuery::Elements;
    using ElementType = ASTBackupQuery::ElementType;


    class RestoreDatabaseFromBackupTask : public IRestoreFromBackupTask
    {
    public:
        RestoreDatabaseFromBackupTask(ContextMutablePtr context_, const ASTPtr & create_query_, bool throw_if_exists_)
            : context(context_), create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_))
        {
            if (create_query->if_not_exists == !throw_if_exists)
            {
                create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
                create_query->if_not_exists = !throw_if_exists;
            }
        }

        RestoreDatabasesFromBackupTask(ContextMutablePtr context_, const String & new_database_name_, bool throw_if_exists_)
            : context(context_)
        {
            create_query = std::make_shared<ASTCreateQuery>();
            create_query->setDatabase(new_database_name_);
            create_query->if_not_exists = !throw_if_exists;
        }

        RestoreFromBackupTasks run() override
        {
            createDatabase();
            return {};
        }

        bool isSequential() const override { return true; }

    private:
        void createDatabase()
        {
            InterpreterCreateQuery create_interpreter{create_query, context};
            create_interpreter.execute();
        }

        ContextMutablePtr context;
        std::shared_ptr<ASTCreateQuery> create_query;
    };


    class RestoreTableFromBackupTask : public IRestoreFromBackupTask
    {
    public:
        RestoreTableFromBackupTask(
            ContextMutablePtr context_,
            const ASTPtr & create_query_,
            bool throw_if_exists_,
            const ASTs & partitions_,
            const BackupPtr & backup_,
            const DatabaseAndTableName & table_name_in_backup_)
            : context(context_), create_query(typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query_)),
              partitions(partitions_), backup(backup_), table_name_in_backup(table_name_in_backup_)
        {
            if (create_query->if_not_exists == !throw_if_exists)
            {
                create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(create_query->clone());
                create_query->if_not_exists = !throw_if_exists;
            }

            table_name = DatabaseAndTableName{create_query->getDatabase(), create_query->getTable()};
            if (create_query->temporary)
                table_name.first = DatabaseCatalog::TEMPORARY_DATABASE;
        }

        RestoreFromBackupTasks run() override
        {
            auto storage = getOrCreateStorage();
            RestoreFromBackupTasks tasks;
            if (auto task = insertDataIntoStorage())
                tasks.push_back(insertDataIntoStorage());
            return tasks;
        }

        bool isSequential() const override { return true; }

    private:
        void createStorage()
        {
            InterpreterCreateQuery create_interpreter{create_query, context};
            create_interpreter.execute();
        }

        StoragePtr getStorage()
        {
            if (!DatabaseCatalog::instance().isTableExist({table_name.first, table_name.second}, context))
                return nullptr;

            DatabasePtr existing_database;
            StoragePtr existing_storage;
            std::tie(existing_database, existing_storage) = DatabaseCatalog::instance().tryGetDatabaseAndTable({new_table_name.first, new_table_name.second}, context);
            if (!existing_storage)
                return nullptr;

            auto existing_table_create_query = existing_database->tryGetCreateTableQuery(new_table_name.second, context);
            if (!existing_table_create_query)
                return nullptr;

            if (!hasCompatibleDataToRestoreTable(*create_query, existing_table_create_query->as<ASTCreateQuery &>()))
            {
                String error_message = (table_name.first == DatabaseCatalog::TEMPORARY_DATABASE)
                    ? ("Temporary table " + backQuoteIfNeed(table_name.second) + " already exists")
                    : ("Table " + backQuoteIfNeed(table_name.first) + "." + backQuoteIfNeed(table_name.second)
                       + " already exists");
                throw Exception(error_message, ErrorCodes::CANNOT_RESTORE_TABLE);
            }

            return existing_storage;
        }

        StoragePtr getOrCreateStorage()
        {
            createStorage();
            if (auto storage = getStorage())
                return storage;

            String error_message = (table_name.first == DatabaseCatalog::TEMPORARY_DATABASE)
                ? ("Could not create temporary table " + backQuoteIfNeed(table_name.second) + " for restoring")
                : ("Could not create table " + backQuoteIfNeed(table_name.first) + "." + backQuoteIfNeed(table_name.second)
                   + " for restoring");
            throw Exception(error_message, ErrorCodes::CANNOT_RESTORE_TABLE);
        }

        RestoreFromBackupTaskPtr insertDataIntoStorage(StoragePtr storage)
        {
            context->checkAccess(AccessType::INSERT, table_name.first, table_name.second);
            String data_path_in_backup = getDataPathInBackup(table_name_in_backup);
            return storage->restoreFromBackup(backup, data_path_in_backup, partitions, context);
        }

        ContextMutablePtr context;
        ASTPtr create_query;
        DatabaseAndTableName table_name;
        ASTs partitions;
        BackupPtr backup;
        DatabaseAndTableName table_name_in_backup;
    };


    class RestoreTasksBuilder
    {
    public:
        RestoreTasksBuilder(ContextMutablePtr context_, const BackupPtr & backup_)
            : context(context_), backup(backup_) {}

        void makeTasks(const ASTBackupQuery::Elements & elements)
        {
            initBackupRenamingConfig(renaming_config, context, elements);

            for (const auto & element : elements)
            {
                switch (element.type)
                {
                    case ElementType::TABLE: [[fallthrough]];
                    case ElementType::DICTIONARY:
                    {
                        String database_name = element.name.first;
                        if (database_name.empty())
                            database_name = context->getCurrentDatabase();
                        const String & table_name = element.name.second;
                        prepareTasksToRestoreTable(DatabaseAndTableName{database_name, table_name}, element.partitions);
                        break;
                    }

                    case ElementType::TEMPORARY_TABLE:
                    {
                        String database_name = DatabaseCatalog::TEMPORARY_DATABASE;
                        const String & table_name = element.name.second;
                        prepareTasksToRestoreTable(DatabaseAndTableName{database_name, table_name}, element.partitions);
                        break;
                    }

                    case ElementType::DATABASE:
                    {
                        const String & database_name = element.name.first;
                        prepareTasksToRestoreDatabase(database_name, element.except_list);
                        break;
                    }

                    case ElementType::ALL_TEMPORARY_TABLES:
                    {
                        prepareTasksToRestoreDatabase(DatabaseCatalog::TEMPORARY_DATABASE, element.except_list);
                        break;
                    }

                    case ElementType::ALL_DATABASES:
                    {
                        prepareTasksToRestoreAllDatabases(element.except_list);
                        break;
                    }

                    case ElementType::EVERYTHING:
                    {
                        prepareTasksToRestoreAllDatabases({});
                        prepareTasksToRestoreDatabase(DatabaseCatalog::TEMPORARY_DATABASE, {});
                        break;
                    }
                }
            }
        }

        RestoreFromBackupTasks getTasks()
        {
            for (const auto & db_and_table_name : tasks_to_restore_tables | boost::adaptors::map_keys)
            {
                const String & database_name = db_and_table_name.first;
                if (tasks_to_restore_databases.contains(database_name))
                    continue;
                tasks_to_restore_databases[database_name] =
                        std::make_unique<RestoreDatabaseFromBackupTask>(context, database_name, /* throw_if_exists = */ false);
            }

            RestoreFromBackupTasks res;
            for (auto & task : tasks_to_restore_databases | boost::adaptors::map_values)
                res.push_back(std::move(task));

            for (auto & task : tasks_to_restore_tables | boost::adaptors::map_values)
                res.push_back(std::move(task));

            return res;
        }

    private:
        void prepareTasksToRestoreTable(const DatabaseAndTableName & table_name_, const ASTs & partitions_)
        {
            ASTPtr create_query = readCreateQueryFromBackup(table_name, backup);
            auto new_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(renameInCreateQuery(create_query, renaming_config, context));

            DatabaseAndTableName new_table_name{new_create_query->getDatabase(), new_create_query->getTable()};
            if (new_create_query->temporary)
                new_table_name.first = DatabaseCatalog::TEMPORARY_DATABASE;

            if (tasks_to_restore_tables.contains(new_table_name))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Table {}.{} cannot be restored twice",
                    backQuoteIfNeed(new_table_name.first), backQuoteIfNeed(new_table_name.second));

            auto task = std::make_unique<RestoreTableFromBackup>(context, new_create_query, false, partitions_, backup, table_name_);
            tasks_to_restore_tables[new_table_name] = std::move(task);
        }

        void prepareTasksToRestoreDatabase(const String & database_name_,
                                           const std::set<String> & except_list_)
        {
            ASTPtr create_query = readCreateQueryFromBackup(database_name_, backup);
            auto new_create_query = typeid_cast<std::shared_ptr<ASTCreateQuery>>(renameInCreateQuery(create_query, renaming_config, context));

            const String & new_database_name = new_create_query->getDatabase();
            if (tasks_to_restore_databases.contains(new_database_name))
                throw Exception(ErrorCodes::CANNOT_RESTORE_DATABASE, "Database {} cannot be restored twice", backQuoteIfNeed(new_database_name));

            auto task = std::make_unique<RestoreDatabaseFromBackup>(context, new_create_query, false);
            tasks_to_restore_databases[new_database_name] = std::move(task);

            Strings table_metadata_filenames = backup->listFiles("metadata/" escapeForFileName(database_name) + "/", "/");
            for (const String & table_metadata_filename : table_metadata_filenames)
            {
                String table_name = unescapeForFileName(fs::path{table_metadata_filename}.stem());
                if (except_list.contains(table_name))
                    continue;
                prepareTasksToRestoreTable(DatabaseAndTableName{database_name, table_name}, ASTs{});
            }

            return restore_tables_tasks;
        }

        void prepareTasksToRestoreAllDatabases()
        {
            Strings database_metadata_filenames = backup->listFiles("metadata/", "/");
            for (const String & database_metadata_filename : database_metadata_filenames)
            {
                String database_name = unescapeForFileName(fs::path{database_metadata_filename}.stem());
                if (except_list.contains(database_name))
                    continue;
                prepareTaskToRestoreDatabase(database_name, std::set<String>{});
            }
        }

        ASTPtr readCreateQueryFromBackup(const DatabaseAndTableName & table_name, const BackupPtr & backup)
        {
            String create_query_path = getMetadataPathInBackup(table_name);
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }

        ASTPtr readCreateQueryFromBackup(const String & database_name, const BackupPtr & backup)
        {
            String create_query_path = getMetadataPathInBackup(database_name);
            auto read_buffer = backup->readFile(create_query_path)->getReadBuffer();
            String create_query_str;
            readStringUntilEOF(create_query_str, *read_buffer);
            read_buffer.reset();
            ParserCreateQuery create_parser;
            return parseQuery(create_parser, create_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }

        ContextMutablePtr context;
        BackupPtr backup;
        BackupRenamingConfig renaming_config;
        std::map<String, RestoreFromBackupTaskPtr> tasks_to_restore_databases;
        std::map<DatabaseAndTableName, RestoreFromBackupTaskPtr> tasks_to_restore_tables;
    };


    void rollbackRestoreTasks(RestoreFromBackupTasks && restore_tasks)
    {
        for (auto it = restore_tasks.rbegin(); it != restore_tasks.rend(); ++it)
        {
            try
            {
                (*it)->rollback();
            }
            catch (...)
            {
                tryLogCurrentException("Restore", "Couldn't rollback changes after failed RESTORE");
            }
        }
    }
}


RestoreFromBackupTasks makeRestoreTasks(const Elements & elements, ContextMutablePtr context, const BackupPtr & backup)
{
    RestoreTasksBuilder builder{context, backup};
    builder.makeTasks(elements);
    return builder.getTasks();
}


void executeRestoreTasks(RestoreFromBackupTasks && restore_tasks, size_t num_threads)
{
    if (!num_threads)
        num_threads = 1;

    RestoreFromBackupTasks completed_tasks;
    bool need_rollback_completed_tasks = true;

    SCOPE_EXIT({
        if (need_rollback_completed_tasks)
            rollbackRestoreTasks(std::move(completed_tasks));
    });

    std::deque<std::unique_ptr<IRestoreFromBackupTask>> sequential_tasks;
    std::deque<std::unique_ptr<IRestoreFromBackupTask>> enqueued_tasks;

    /// There are two kinds of restore tasks: sequential and non-sequential ones.
    /// Sequential tasks are executed first and always in one thread.
    for (auto & task : restore_tasks)
    {
        if (task->isSequential())
            sequential_tasks.push_back(std::move(task));
        else
            enqueued_tasks.push_back(std::move(task));
    }

    /// Sequential tasks.
    while (!sequential_tasks.empty())
    {
        auto current_task = std::move(sequential_tasks.front());
        sequential_tasks.pop_front();

        RestoreFromBackupTasks new_tasks = current_task->run();

        completed_tasks.push_back(std::move(current_task));
        for (auto & task : new_tasks)
        {
            if (task->isSequential())
                sequential_tasks.push_back(std::move(task));
            else
                enqueued_tasks.push_back(std::move(task));
        }
    }

    /// Non-sequential tasks.
    std::unordered_map<IRestoreFromBackupTask *, std::unique_ptr<IRestoreFromBackupTask>> running_tasks;
    std::vector<ThreadFromGlobalPool> threads;
    std::mutex mutex;
    std::condition_variable cond;
    std::exception_ptr exception;

    while (true)
    {
        IRestoreFromBackupTask * current_task = nullptr;
        {
            std::unique_lock lock{mutex};
            cond.wait(lock, [&]
            {
                if (exception)
                    return true;
                if (enqueued_tasks.empty())
                    return running_tasks.empty();
                return (running_tasks.size() < num_threads);
            });

            if (exception || enqueued_tasks.empty())
                break;

            auto current_task_ptr = std::move(enqueued_tasks.front());
            current_task = current_task_ptr.get();
            enqueued_tasks.pop_front();
            running_tasks[current_task] = std::move(current_task_ptr);
        }

        assert(current_task);
        threads.emplace_back([current_task, &mutex, &cond, &enqueued_tasks, &running_tasks, &completed_tasks, &exception]() mutable
        {
            {
                std::lock_guard lock{mutex};
                if (exception)
                    return;
            }

            RestoreFromBackupTasks new_tasks;
            std::exception_ptr new_exception;
            try
            {
                new_tasks = current_task->run();
            }
            catch (...)
            {
                new_exception = std::current_exception();
            }

            {
                std::lock_guard lock{mutex};
                auto current_task_it = running_tasks.find(current_task);
                auto current_task_ptr = std::move(current_task_it->second);
                running_tasks.erase(current_task_it);

                if (!new_exception)
                {
                    completed_tasks.push_back(std::move(current_task_ptr));
                    enqueued_tasks.insert(
                        enqueued_tasks.end(), std::make_move_iterator(new_tasks.begin()), std::make_move_iterator(new_tasks.end()));
                }

                if (!exception)
                    exception = new_exception;

                cond.notify_all();
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    if (exception)
        std::rethrow_exception(exception);
    else
        need_rollback_completed_tasks = false;
}

}
