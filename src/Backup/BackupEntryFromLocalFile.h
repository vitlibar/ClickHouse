#pragma once

#include <Backup/BackupEntryFromFile.h>

namespace Poco { class TemporaryFile; }

namespace DB
{
class DiskLocal;
using TemporaryFile = Poco::TemporaryFile;


/// Represents a local file prepared to be included in a backup.
class BackupEntryFromLocalFile : public IBackupEntry
{
public:
    using Flags = BackupEntryFromFile::Flags;

    /// The constructor is allowed to not set `file_size_` or `checksum_`,
    /// in that case they will calculated from the data.
    BackupEntryFromLocalFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_,
        const std::optional<UInt128> & checksum_,
        Flags flags_,
        const VolumePtr & temporary_volume_);

    BackupEntryFromLocalFile(
        const String & path_in_backup_,
        const std::shared_ptr<DiskLocal> & disk_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_,
        const std::optional<UInt128> & checksum_,
        Flags flags_,
        const VolumePtr & temporary_volume_,
        const String & temp_directory_on_disk_);

    ~BackupEntryFromLocalFile() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
    UInt64 getDataSize() const override;
    UInt128 getChecksum() const override;
    std::optional<UInt128> tryGetChecksumFast() const override;

protected:
    void init();
    void createTemporaryFile(const std::optional<dev_t> & device_id);

    const String file_path;
    const Flags flags;
    const VolumePtr temporary_volume;
    const String temp_directory;
    std::optional<String> data;
    std::unique_ptr<TemporaryFile> temporary_file;
    mutable std::optional<UInt64> file_size;
    mutable std::optional<UInt128> checksum;
};

}
