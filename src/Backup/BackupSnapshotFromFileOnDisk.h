#pragma once

#include <Backup/BackupSnapshotFromFile.h>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Represents a file prepared to be included in a backup.
class BackupSnapshotFromFileOnDisk : public BackupSnapshotFromFile
{
public:
    BackupSnapshotFromFileOnDisk(
        const String & name_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt128> & checksum_,
        PossibleChanges possible_changes_,
        const BackupSnapshotParams & params_);

    ~BackupSnapshotFromFileOnDisk() override;

protected:
    void utilCopyFile(const String & src_file_path, const String & dest_file_path) const override;
    void utilCreateHardLink(const String & src_file_path, const String & dest_file_path) const override;
    void utilRemoveFile(const String & file_path) const override;
    size_t utilGetFileSize(const String & file_path) const override;
    std::unique_ptr<ReadBuffer> utilReadFile(const String & file_path) const override;

private:
    const DiskPtr disk;
};

}
