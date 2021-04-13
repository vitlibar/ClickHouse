#pragma once

#include <Backup/IBackupEntry.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;


/// Represents a file prepared to be included in a backup.
class BackupEntryFromFile : public IBackupEntry
{
public:
    /// Flags must combine two of the following constants:
    /// the first one is one from the list {MUTABLE, IMMUTABLE, APPENDABLE},
    /// and the second one is one from the list {REMOVABLE, ALWAYS_EXISTS}.
    enum Flags
    {
        /// File can be changed.
        MUTABLE = 0x01,

        /// File cannot be changed.
        IMMUTABLE = 0x02,

        /// File can be appended with new data and that's the only way how it can be changed.
        APPEND_ONLY = 0x04,

        /// File can be removed.
        REMOVABLE = 0x08,

        /// File cannot be removed.
        ALWAYS_EXISTS = 0x10,

        ALWAYS_EXISTS_IMMUTABLE = ALWAYS_EXISTS | IMMUTABLE,
        REMOVABLE_IMMUTABLE = REMOVABLE | IMMUTABLE,
        REMOVABLE_APPEND_ONLY = REMOVABLE | APPEND_ONLY,
        REMOVABLE_MUTABLE = REMOVABLE | MUTABLE,
    };

    /// The constructor is allowed to not set `file_size_` or `checksum_`,
    /// in this case they will calculated from the data.
    BackupEntryFromFile(
        const String & path_in_backup_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_,
        const std::optional<UInt128> & checksum_,
        Flags flags_,
        const VolumePtr & temporary_volume_,
        const String & temp_directory_on_disk_);

    ~BackupEntryFromFile() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
    UInt64 getDataSize() const override;
    UInt128 getChecksum() const override;
    std::optional<UInt128> tryGetChecksumFast() const override;

private:
    std::unique_ptr<IBackupEntry> impl;
};

}
