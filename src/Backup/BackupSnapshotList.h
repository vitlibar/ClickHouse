#pragma once

#include <Backup/IBackupSnapshot.h>
#include <vector>


namespace DB
{
class BackupSnapshotList : public IBackupSnapshot
{
public:
    BackupSnapshotList(std::vector<std::unique_ptr<IBackupSnapshot>> nested_snapshots_)
        : nested_snapshots(std::move(nested_snapshots_)) {}

    bool getNextEntry(BackupEntry & entry) override
    {
        while (pos != nested_snapshots.size())
        {
            if (nested_snapshots[pos]->getNextEntry(entry))
                return true;
            nested_snapshots[pos].reset();
            ++pos;
        }
        return false;
    }

private:
    std::vector<std::unique_ptr<IBackupSnapshot>> nested_snapshots;
    size_t pos = 0;
};

}
