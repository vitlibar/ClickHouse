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
    const DiskPtr & disk_,
    const String & file_path_,
    Flags flags_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const VolumePtr & temporary_volume_,
    const String & temp_directory_on_disk_)
{
    if (disk_->getType() != DiskType::Type::Local)
        throw Exception("Disk type " + DiskType::toString(disk_->getType()) + " is not supported",
                        ErrorCodes::NOT_IMPLEMENTED);

    impl = std::make_unique<BackupEntryFromLocalFile>(
        typeid_cast<std::shared_ptr<DiskLocal>>(disk_),
        file_path_,
        flags_,
        file_size_,
        checksum_,
        temporary_volume_,
        temp_directory_on_disk_);
}

BackupEntryFromFile::~BackupEntryFromFile() = default;

UInt64 BackupEntryFromFile::getSize() const
{
    return impl->getSize();
}

std::optional<UInt128> BackupEntryFromFile::getChecksum() const
{
    return impl->getChecksum();
}

std::unique_ptr<ReadBuffer> BackupEntryFromFile::getReadBuffer() const
{
    return impl->getReadBuffer();
}

}
