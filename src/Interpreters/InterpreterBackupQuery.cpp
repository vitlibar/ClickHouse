#include <Interpreters/InterpreterBackupQuery.h>
#include <Backups/BackupFactory.h>
#include <Backups/BackupUtils.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntry.h>
#include <Backups/IRestoreFromBackupTask.h>
#include <Backups/RestoreFromBackupSettings.h>
#include <Backups/RestoreFromBackupUtils.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace
{
    BackupMutablePtr createBackup(const ContextPtr & context, const ASTBackupQuery & query)
    {
        BackupFactory::CreateParams params;
        params.open_mode = (query.kind == ASTBackupQuery::BACKUP) ? IBackup::OpenMode::WRITE : IBackup::OpenMode::READ;
        params.context = context;

        params.backup_info = BackupInfo::fromAST(*query.backup_name);
        if (query.base_backup_name)
            params.base_backup_info = BackupInfo::fromAST(*query.base_backup_name);

        return BackupFactory::instance().createBackup(params);
    }

    RestoreFromBackupSettings getRestoreSettings(const ASTBackupQuery & query)
    {
        RestoreFromBackupSettings settings;
        if (query.settings)
            settings.applyChanges(query.settings->as<const ASTSetQuery &>().changes);
        return settings;
    }

    void executeBackup(const ContextPtr & context, const ASTBackupQuery & query)
    {
        BackupMutablePtr backup = createBackup(context, query);
        auto backup_entries = makeBackupEntries(context, query.elements);
        writeBackupEntries(backup, std::move(backup_entries), context->getSettingsRef().max_backup_threads);
    }

    void executeRestore(ContextMutablePtr context, const ASTBackupQuery & query)
    {
        BackupPtr backup = createBackup(context, query);
        auto settings = getRestoreSettings(query);
        auto restore_tasks = makeRestoreTasks(context, backup, query.elements, settings);
        executeRestoreTasks(std::move(restore_tasks), context->getSettingsRef().max_backup_threads);
    }
}

BlockIO InterpreterBackupQuery::execute()
{
    const auto & query = query_ptr->as<const ASTBackupQuery &>();
    if (query.kind == ASTBackupQuery::BACKUP)
        executeBackup(context, query);
    else if (query.kind == ASTBackupQuery::RESTORE)
        executeRestore(context, query);
    return {};
}

}
