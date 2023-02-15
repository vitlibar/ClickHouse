#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Backups/RestoreSettings.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/runAsyncWithOnFinishCallback.h>
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


void writeBackupEntries(
    BackupMutablePtr backup, BackupEntries && backup_entries, std::function<void(std::exception_ptr)> on_finish_callback)
{
    auto tracker = std::make_shared<MultipleTasksTrackerWithOnFinishCallback>(backup_entries.size(), std::move(on_finish_callback));
    auto on_task_finished = [tracker](std::exception_ptr error_) { tracker->onTaskFinished(error_); };

    for (auto & [name, entry] : backup_entries)
    {
        if (!tracker->onTaskStarted())
            break;
        backup->writeFileAsync(name, entry, on_task_finished);
    }
}


void writeBackupEntries(BackupMutablePtr backup, BackupEntries && backup_entries)
{
    Poco::Event finished;
    std::exception_ptr error;

    auto on_finish_callback = [&finished, &error](std::exception_ptr error_)
    {
        error = error_;
        finished.set();
    };

    writeBackupEntries(backup, std::move(backup_entries), on_finish_callback);

    finished.wait();
    if (error)
        std::rethrow_exception(error);
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
