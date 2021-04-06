#pragma once

#include <Backup/BackupEntryFromRemovableFile.h>


namespace DB
{

/// Represents a file prepared to be included in a backup which can be appended with new data.
class BackupEntryFromAppendableFile : public BackupEntryFromRemovableFile
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromAppendableFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
};


/// The same as BackupEntryFromAppendableFile except the source file is located on a disk implementing the IDisk interface.
class BackupEntryFromAppendableFileOnDisk : public BackupEntryFromRemovableFileOnDisk
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromAppendableFileOnDisk(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
};

}
