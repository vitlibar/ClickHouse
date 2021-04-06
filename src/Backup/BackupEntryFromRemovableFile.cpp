#include <Backup/BackupEntryFromRemovableFile.h>
#include <Common/createHardLink.h>
#include <Common/filesystemHelpers.h>
#include <Core/UUID.h>
#include <Disks/IDisk.h>
#include <IO/createReadBufferFromFileBase.h>
#include <IO/WriteHelpers.h>


namespace DB
{
BackupEntryFromRemovableFile::BackupEntryFromRemovableFile(
    const String & path_in_backup_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_ ? *file_size_ : Poco::File(file_path_).getSize(), checksum_)
{
    temp_file = createTemporaryFile("");
    ::DB::createHardLink(file_path_, temp_file->path());
}

BackupEntryFromRemovableFile::~BackupEntryFromRemovableFile() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromRemovableFile::getReadBuffer() const
{
    return createReadBufferFromFileBase(temp_file->path(), 0, 0, 0);
}


BackupEntryFromRemovableFileOnDisk::BackupEntryFromRemovableFileOnDisk(const String & path_in_backup_, const DiskPtr & disk_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_ ? *file_size_ : disk_->getFileSize(file_path_), checksum_)
    , disk(disk_)
{
    disk->createDirectories("tmp");
    temp_file_path = "tmp-backup-" + toString(UUIDHelpers::generateV4());
    disk->createHardLink(file_path_, temp_file_path);
}

BackupEntryFromRemovableFileOnDisk::~BackupEntryFromRemovableFileOnDisk()
{
    if (!temp_file_path.empty())
        disk->removeFileIfExists(temp_file_path);
}

std::unique_ptr<ReadBuffer> BackupEntryFromRemovableFileOnDisk::getReadBuffer() const
{
    return disk->readFile(temp_file_path);
}

}
