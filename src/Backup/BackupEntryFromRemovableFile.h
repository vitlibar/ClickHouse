#pragma once

#include <Backup/IBackupEntry.h>

namespace Poco { class TemporaryFile; }

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using TemporaryFile = Poco::TemporaryFile;

/// Represents a file prepared to be included in a backup
/// which can be removed but cannot be changed.
/// Due to the file is considered as immutable we make a hard link to it in the constructor
/// and that's enough.
class BackupEntryFromRemovableFile : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromRemovableFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});
    ~BackupEntryFromRemovableFile() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    std::unique_ptr<TemporaryFile> temp_file;
};


/// The same as BackupEntryFromRemovableFile except the source file is located on a disk implementing the IDisk interface.
class BackupEntryFromRemovableFileOnDisk : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromRemovableFileOnDisk(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});
    ~BackupEntryFromRemovableFileOnDisk() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    DiskPtr disk;
    String temp_file_path;
};

}
