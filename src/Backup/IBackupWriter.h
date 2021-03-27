#pragma once

#include <Backup/BackupEntry.h>
#include <Backup/IBackupSnapshot.h>


namespace DB
{
struct BackupEntry;

class IBackupWriter
{
public:
    virtual ~IBackupWriter() = default;

    /// Adds an entry to this backup.
    virtual void write(BackupEntry && entry) = 0;

    void writeSnapshot(std::unique_ptr<IBackupSnapshot> snapshot)
    {
        BackupEntry entry;
        while (snapshot->getNextEntry(entry))
            write(std::move(entry));
    }
};

using BackupWriterPtr = std::unique_ptr<IBackupWriter>;

}
