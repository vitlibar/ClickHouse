#include <Backup/BackupEntryFromAppendableFile.h>
#include <IO/LimitReadBuffer.h>


namespace DB
{
BackupEntryFromAppendableFile::BackupEntryFromAppendableFile(
    const String & path_in_backup_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromRemovableFile(path_in_backup_, file_path_, file_size_, checksum_)
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromAppendableFile::getReadBuffer() const
{
    return std::make_unique<LimitReadBuffer>(BackupEntryFromRemovableFile::getReadBuffer(), getDataSize(), false);
}


BackupEntryFromAppendableFileOnDisk::BackupEntryFromAppendableFileOnDisk(
    const String & path_in_backup_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_)
    : BackupEntryFromRemovableFileOnDisk(path_in_backup_, disk_, file_path_, file_size_, checksum_)
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromAppendableFileOnDisk::getReadBuffer() const
{
    return std::make_unique<LimitReadBuffer>(BackupEntryFromRemovableFileOnDisk::getReadBuffer(), getDataSize(), false);
}
}
