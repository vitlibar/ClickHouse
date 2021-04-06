#include <Backup/BackupEntryFromFile.h>
#include <Common/filesystemHelpers.h>
#include <Disks/IDisk.h>
#include <IO/createReadBufferFromFileBase.h>


namespace DB
{
BackupEntryFromFile::BackupEntryFromFile(
    const String & path_in_backup_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_, checksum_), file_path(file_path_)
{
}

BackupEntryFromFile::~BackupEntryFromFile() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromFile::getReadBuffer() const
{
    return createReadBufferFromFileBase(file_path, 0, 0, 0);
}

UInt64 BackupEntryFromFile::calculateDataSize() const
{
    return Poco::File(file_path).getSize();
}


BackupEntryFromFileOnDisk::BackupEntryFromFileOnDisk(
    const String & path_in_backup_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_, checksum_), disk(disk_), file_path(file_path_)
{
}

BackupEntryFromFileOnDisk::~BackupEntryFromFileOnDisk() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromFileOnDisk::getReadBuffer() const
{
    return disk->readFile(file_path);
}

UInt64 BackupEntryFromFileOnDisk::calculateDataSize() const
{
    return disk->getFileSize(file_path);
}

}
