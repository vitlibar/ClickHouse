#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Backups/RestoreSettings.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>


namespace DB
{

DDLRenamingMap makeRenamingMapFromBackupQuery(const ASTBackupQuery::Elements & elements)
{
    DDLRenamingMap map;

    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                const String & table_name = element.table_name;
                const String & database_name = element.database_name;
                const String & new_table_name = element.new_table_name;
                const String & new_database_name = element.new_database_name;
                assert(!table_name.empty());
                assert(!new_table_name.empty());
                assert(!database_name.empty());
                assert(!new_database_name.empty());
                map.setNewTableName({database_name, table_name}, {new_database_name, new_table_name});
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const String & table_name = element.table_name;
                const String & new_table_name = element.new_table_name;
                assert(!table_name.empty());
                assert(!new_table_name.empty());
                map.setNewTableName({DatabaseCatalog::TEMPORARY_DATABASE, table_name}, {DatabaseCatalog::TEMPORARY_DATABASE, new_table_name});
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const String & database_name = element.database_name;
                const String & new_database_name = element.new_database_name;
                assert(!database_name.empty());
                assert(!new_database_name.empty());
                map.setNewDatabaseName(database_name, new_database_name);
                break;
            }

            case ASTBackupQuery::ALL: break;
        }
    }
    return map;
}


void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries)
{
    std::mutex mutex;
    size_t num_entries_requested = 0;
    size_t num_entries_responded;
    std::exception_ptr error;
    Poco::Event all_finished;

    auto on_entry_written = [&](std::exception_ptr error_)
    {
        std::lock_guard lock{mutex};
        if (!error)
            error = error_;
        ++num_entries_responded;
        size_t expect_num_entries_responded = error ? num_entries_requested : backup_entries.size();
        if (num_entries_responded == expect_num_entries_responded)
            all_finished.set();
    };

    for (auto & [name, entry] : backup_entries)
    {
        {
            std::lock_guard lock{mutex};
            if (error)
                break;
            ++num_entries_requested;
        }
        backup->writeFileAsync(name, entry, on_entry_written);
    }

    all_finished.wait();

    {
        std::lock_guard lock{mutex};
        if (error)
            std::rethrow_exception(error);
    }
}


void restoreTablesData(DataRestoreTasks && tasks, ThreadPool & thread_pool)
{
    size_t num_active_jobs = 0;
    std::mutex mutex;
    std::condition_variable event;
    std::exception_ptr exception;

    auto thread_group = CurrentThread::getGroup();

    for (auto & task : tasks)
    {
        {
            std::unique_lock lock{mutex};
            if (exception)
                break;
            ++num_active_jobs;
        }

        auto job = [&](bool async)
        {
            SCOPE_EXIT_SAFE(
                std::lock_guard lock{mutex};
                if (!--num_active_jobs)
                    event.notify_all();
                if (async)
                    CurrentThread::detachQueryIfNotDetached();
            );

            try
            {
                if (async && thread_group)
                    CurrentThread::attachTo(thread_group);

                if (async)
                    setThreadName("RestoreWorker");

                {
                    std::lock_guard lock{mutex};
                    if (exception)
                        return;
                }

                std::move(task)();
            }
            catch (...)
            {
                std::lock_guard lock{mutex};
                if (!exception)
                    exception = std::current_exception();
            }
        };

        if (!thread_pool.trySchedule([job] { job(true); }))
            job(false);
    }

    {
        std::unique_lock lock{mutex};
        event.wait(lock, [&] { return !num_active_jobs; });
        if (exception)
            std::rethrow_exception(exception);
    }
}


/// Returns access required to execute BACKUP query.
AccessRightsElements getRequiredAccessToBackup(const ASTBackupQuery::Elements & elements)
{
    AccessRightsElements required_access;
    for (const auto & element : elements)
    {
        switch (element.type)
        {
            case ASTBackupQuery::TABLE:
            {
                required_access.emplace_back(AccessType::BACKUP, element.database_name, element.table_name);
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                /// It's always allowed to backup temporary tables.
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                /// TODO: It's better to process `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP, element.database_name);
                break;
            }

            case ASTBackupQuery::ALL:
            {
                /// TODO: It's better to process `element.except_databases` & `element.except_tables` somehow.
                required_access.emplace_back(AccessType::BACKUP);
                break;
            }
        }
    }
    return required_access;
}

}
