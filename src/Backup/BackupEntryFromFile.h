#pragma once

#include <Backup/IBackupEntry.h>

namespace Poco { class TemporaryFile; }

namespace DB
{
using TemporaryFile = Poco::TemporaryFile;

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
        APPENDABLE = 0x04,

        /// File can be removed.
        REMOVABLE = 0x08,

        /// File cannot be removed.
        ALWAYS_EXISTS = 0x10,

        IMMUTABLE_ALWAYS_EXISTS = IMMUTABLE | ALWAYS_EXISTS,
        IMMUTABLE_REMOVABLE = IMMUTABLE | REMOVABLE,
        APPENDABLE_REMOVABLE = APPENDABLE | REMOVABLE,
        MUTABLE_REMOVABLE = MUTABLE | REMOVABLE,
    };

    /// The constructor is allowed to not set `file_size` or `checksum`,
    /// in that case they will calculated from the data.
    BackupEntryFromFile(
        const String & path_in_backup_,
        const String & file_path_,
        const std::optional<UInt64> & file_size_,
        const std::optional<UInt128> & checksum_,
        Flags flags);
    ~BackupEntryFromFile() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

protected:
    struct NoInitTag {};
    BackupEntryFromFile(const String & path_in_backup_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_, NoInitTag);
    void init(const String & file_path_, Flags flags_);

    UInt64 calculateDataSize() const override;

    struct StatInfo
    {
        UInt64 size;
        dev_t device_id;
    };

    const StatInfo & getStatInfo() const;
    void createTemporaryFile(bool same_device_is_required);

    String file_path;
    Flags flags;
    mutable std::optional<StatInfo> stat_info;
    std::optional<String> data;
    std::unique_ptr<TemporaryFile> temporary_file;
};

}
