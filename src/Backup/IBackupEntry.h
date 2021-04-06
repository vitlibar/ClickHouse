#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>
#include <optional>

namespace DB
{
class ReadBuffer;

/// A backup entry consists of:
/// 1) Non-empty name. Usually it looks like a path and starts with "data/" or "metadata/".
/// 2) Data with optional checksum. If the checksum isn't specified it will be calculated automatically later.
class IBackupEntry
{
public:
    IBackupEntry(const String & path_in_backup_, const std::optional<UInt64> & data_size_, const std::optional<UInt128> & checksum_ = {})
        : path_in_backup(path_in_backup_), data_size(data_size_), checksum(checksum_)
    {
    }
    virtual ~IBackupEntry() = default;

    const String & getPathInBackup() const { return path_in_backup; }

    /// Returns the size of the data if it's known.
    const std::optional<UInt64> & tryGetDataSize() const { return data_size; }

    /// Returns the checksum of the data if it's known.
    const std::optional<UInt128> & tryGetChecksum() const { return checksum; }

    /// Returns the size of the data, calculates it if it's not known.
    UInt64 getDataSize() const;

    /// Returns the checksum of the data, calculates it if it's not known.
    UInt128 getChecksum() const;

    virtual std::unique_ptr<ReadBuffer> getReadBuffer() const = 0;

protected:
    virtual UInt64 calculateDataSize() const;
    virtual UInt128 calculateChecksum() const;

private:
    String path_in_backup;
    mutable std::optional<UInt64> data_size;
    mutable std::optional<UInt128> checksum;
};

using BackupEntries = std::vector<std::unique_ptr<IBackupEntry>>;

}
