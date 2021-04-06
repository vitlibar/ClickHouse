#pragma once

#include <Backup/IBackupEntry.h>

namespace Poco { class TemporaryFile; }

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using TemporaryFile = Poco::TemporaryFile;

/// Represents a file prepared to be included in a backup which can be changed or removed.
/// Use this class for small files only, for large files BackupEntryFromImmutableFile
/// or BackupEntryFromAppendableFile should be preferred.
/// This class makes a full copy of the file or reads it into memory.
class BackupEntryFromMutableFile : public IBackupEntry
{
public:
    /// It's allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromMutableFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});

    ~BackupEntryFromMutableFile() override;
    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    String data;
    std::unique_ptr<TemporaryFile> temp_file;
};


/// The same as BackupEntryFromMutableFile except the source file is located on a disk implementing the IDisk interface.
class BackupEntryFromMutableFileOnDisk : public IBackupEntry
{
public:
    /// It's allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromMutableFileOnDisk(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_ = {},
        const std::optional<UInt128> & checksum_ = {});

    ~BackupEntryFromMutableFileOnDisk() override;
    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    String data;
    std::unique_ptr<TemporaryFile> temp_file;
};

}
