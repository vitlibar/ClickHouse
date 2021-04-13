#include <Backup/BackupEntryFromFile.h>
#include <Backup/BackupEntryFromLocalFile.h>
#include <Common/typeid_cast.h>
#include <Disks/DiskLocal.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

BackupEntryFromFile::BackupEntryFromFile(
    const String & path_in_backup_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    Flags flags_,
    const VolumePtr & temporary_volume_,
    const String & temp_directory_on_disk_)
    : IBackupEntry(path_in_backup_)
{
    if (disk_->getType() != DiskType::Type::Local)
        throw Exception("Disk " + DiskType::toString(disk_->getType()) + " is not supported",
                        ErrorCodes::NOT_IMPLEMENTED);

    impl = std::make_unique<BackupEntryFromLocalFile>(
        path_in_backup_,
        typeid_cast<std::shared_ptr<DiskLocal>>(disk_),
        file_path_,
        file_size_,
        checksum_,
        flags_,
        temporary_volume_,
        temp_directory_on_disk_);
}

BackupEntryFromFile::~BackupEntryFromFile() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromFile::getReadBuffer() const
{
    return impl->getReadBuffer();
}

UInt64 BackupEntryFromFile::getDataSize() const
{
    return impl->getDataSize();
}

UInt128 BackupEntryFromFile::getChecksum() const
{
    return impl->getChecksum();
}

std::optional<UInt128> BackupEntryFromFile::tryGetChecksumFast() const
{
    return impl->tryGetChecksumFast();
}

}
