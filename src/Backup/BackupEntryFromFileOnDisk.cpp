#include <Backup/BackupEntryFromFileOnDisk.h>
#include <Disks/IDisk.h>
#include <IO/createReadBufferFromFileBase.h>


namespace DB
{

BackupEntryFromFileOnDisk::BackupEntryFromFileOnDisk(
    const String & path_in_backup_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    Flags flags_)
    : BackupEntryFromFile(path_in_backup_, file_size_, checksum_, NoInitTag{})
    , disk(disk_)
    , disk_is_local(disk_->getType() == DiskType::Type::Local)
{
    init(file_path_, flags_);
}

void BackupEntryFromFileOnDisk::init(const String & file_path_, Flags flags_)
{
    if (disk_is_local)
        BackupEntryFromFile::init(fullPath(disk, file_path_), flags_);
}

BackupEntryFromFileOnDisk::~BackupEntryFromFileOnDisk() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromFileOnDisk::getReadBuffer() const
{
    if (disk_is_local)
        return BackupEntryFromFile::getReadBuffer();
    return disk->readFile(file_path);
}

UInt64 BackupEntryFromFileOnDisk::calculateDataSize() const
{
    if (disk_is_local)
        return BackupEntryFromFile::calculateDataSize();
    return disk->getFileSize(file_path);
}

}
