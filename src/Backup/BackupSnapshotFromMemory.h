#pragma once

#include <Backup/IBackupSnapshot.h>
#include <Backup/BackupEntry.h>


namespace DB
{
struct BackupEntry;

/// Represents small preloaded data to be included in a backup.
class BackupSnapshotFromMemory : public IBackupSnapshot
{
public:
    BackupSnapshotFromMemory(const String & name_, const void * data_, size_t size_, const BackupSnapshotParams & params_);
    BackupSnapshotFromMemory(const String & name_, String data_, const BackupSnapshotParams & params_);

    bool getNextEntry(BackupEntry & entry) override;

private:
    const String name;
    const String data;
    const bool calculate_checksum = false;
    bool backup_entry_generated = false;
};

}
