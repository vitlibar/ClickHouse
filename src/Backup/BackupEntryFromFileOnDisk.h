#pragma once

#include <Backup/BackupEntryFromFile.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// The same as BackupEntryFromFile except the source file is located on a disk implementing the IDisk interface.
class BackupEntryFromFileOnDisk : public BackupEntryFromFile
{
public:
    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in this case they will calculated from the data.
    BackupEntryFromFileOnDisk(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_,
        const std::optional<UInt128> & checksum_,
        Flags flags);
    ~BackupEntryFromFileOnDisk() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

private:
    void init(const String & file_path_, Flags flags_);
    UInt64 calculateDataSize() const override;

    const DiskPtr disk;
    const bool disk_is_local = false;
};

}
