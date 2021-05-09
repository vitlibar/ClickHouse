#pragma once

#include <Core/Types.h>
#include <unordered_map>

namespace DB
{
class IBackupEntry;


/// Container for backup entries, very like to std::vector<std::unique_ptr<IBackupEntry>>
/// but with ability to search by path.
class BackupEntries
{
public:
    BackupEntries();
    ~BackupEntries();
    BackupEntries(BackupEntries && src);
    BackupEntries & operator =(BackupEntries && src);

    bool empty() const { return entries.empty(); }
    size_t size() const { return entries.size(); }
    bool contains(const String & path) const { return entries_by_path.count(path); }

    bool add(std::unique_ptr<IBackupEntry> entry);

    std::vector<std::unique_ptr<IBackupEntry>> extractAll() &&;

private:
    std::vector<std::unique_ptr<IBackupEntry>> entries;
    std::unordered_map<std::string_view, IBackupEntry *> entries_by_path;
};

}
