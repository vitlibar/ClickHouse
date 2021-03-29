#pragma once

#include <Backup/IBackupSnapshot.h>
#include <Common/UInt128.h>
#include <Core/Types.h>
#include <memory>
#include <optional>


namespace DB
{
struct BackupEntry;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;


/// Represents a file prepared to be included in a backup.
class BackupSnapshotFromFile : public IBackupSnapshot
{
public:
    /// Specifies how much the file be changed after creating this BackupSnapshotFromFile object.
    /// The enum's constants describes possible changes of the file,
    /// and it's not related to a possible removing of the file. Any file can be removed.
    enum class PossibleChanges
    {
        /// File never changes, for example MergeTree's parts.
        NONE,

        /// File can be appended with new data, for example data.bin for the StripeLog table engine.
        APPEND,

        /// File can be changed in any way, for example metadata.
        /// Do NOT use PossibleChanges::ANY for large files because it causes copying of the file.
        ANY,
    };

    BackupSnapshotFromFile(
        const String & name_,
        const DiskPtr & disk_,
        const String & file_path_,
        const std::optional<UInt128> & checksum_,
        PossibleChanges possible_changes_,
        const BackupSnapshotParams & params_);
    ~BackupSnapshotFromFile() override;

    bool getNextEntry(BackupEntry & entry) override;

private:
    const String name;
    const DiskPtr disk;
    const PossibleChanges possible_changes;
    String temp_file_path;
    std::optional<String> data;
    std::optional<size_t> data_size;
    std::optional<UInt128> checksum;
    bool reading_size_is_limited = false;
    bool backup_entry_generated = false;
};

}
