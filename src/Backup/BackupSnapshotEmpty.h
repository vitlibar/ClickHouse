#pragma once

#include <Backup/IBackupSnapshot.h>


namespace DB
{
/// Backup snapshot which returns no entries.
class BackupSnapshotEmpty : public IBackupSnapshot
{
public:
    BackupSnapshotEmpty() = default;
    bool getNextEntry(BackupEntry &) override { return false; }
};

}
