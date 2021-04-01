#include <Backup/BackupSnapshotFromFileOnDisk.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

BackupSnapshotFromFileOnDisk::BackupSnapshotFromFileOnDisk(
    const String & name_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt128> & checksum_,
    PossibleChanges possible_changes_,
    const BackupSnapshotParams & params_)
    : BackupSnapshotFromFile(name_, file_path_, checksum_, possible_changes_, params_)
    , disk(disk_)
{
}

BackupSnapshotFromFileOnDisk::~BackupSnapshotFromFileOnDisk() = default;

void BackupSnapshotFromFileOnDisk::utilCopyFile(const String & src_file_path, const String & dest_file_path) const
{
    disk->copy(src_file_path, disk, dest_file_path);
}

void BackupSnapshotFromFileOnDisk::utilCreateHardLink(const String & src_file_path, const String & dest_file_path) const
{
    disk->createHardLink(src_file_path, dest_file_path);
}

void BackupSnapshotFromFileOnDisk::utilRemoveFile(const String & file_path) const
{
    disk->removeFile(file_path);
}

size_t BackupSnapshotFromFileOnDisk::utilGetFileSize(const String & file_path) const
{
    return disk->getFileSize(file_path);
}

std::unique_ptr<ReadBuffer> BackupSnapshotFromFileOnDisk::utilReadFile(const String & file_path) const
{
    return disk->readFile(file_path);
}

}
