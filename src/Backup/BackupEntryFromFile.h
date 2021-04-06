#pragma once

#include <Backup/IBackupEntry.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup
/// which cannot be changed and cannot be removed.
class BackupEntryFromFile : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});
    ~BackupEntryFromFile() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    UInt64 calculateDataSize() const override;

    String file_path;
};


/// The same as BackupEntryFromFile except the source file is located on a disk implementing the IDisk interface.
class BackupEntryFromFileOnDisk : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromFileOnDisk(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});
    ~BackupEntryFromFileOnDisk() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    UInt64 calculateDataSize() const override;

    DiskPtr disk;
    String file_path;
};

}
