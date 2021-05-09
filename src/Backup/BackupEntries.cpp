#include <Backup/BackupEntries.h>
#include <Backup/IBackupEntry.h>


namespace DB
{

BackupEntries::BackupEntries() = default;
BackupEntries::~BackupEntries() = default;
BackupEntries::BackupEntries(BackupEntries && src) = default;
BackupEntries & BackupEntries::operator =(BackupEntries && src) = default;

bool BackupEntries::add(std::unique_ptr<IBackupEntry> entry)
{
    if (!entries_by_path.emplace(entry->getPathInBackup(), entry.get()).second)
        return false;

    entries.push_back(std::move(entry));
    return true;
}

std::vector<std::unique_ptr<IBackupEntry>> BackupEntries::extractAll() &&
{
    entries_by_path.clear();
    return std::move(entries);
}

}
