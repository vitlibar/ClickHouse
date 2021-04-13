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
    IBackupEntry(const String & path_in_backup_) : path_in_backup(path_in_backup_) {}
    virtual ~IBackupEntry() = default;

    const String & getPathInBackup() const { return path_in_backup; }

    /// Returns the data.
    virtual std::unique_ptr<ReadBuffer> getReadBuffer() const = 0;

    /// Returns the size of the data.
    virtual UInt64 getDataSize() const = 0;

    /// Returns the checksum of the data.
    /// The default implementation first calls tryGetChecksumFast(), then calculates
    /// the checksum from the buffer returned by getReadBuffer().
    virtual UInt128 getChecksum() const;

    /// Returns the checksum of the data only if it's precalculated.
    virtual std::optional<UInt128> tryGetChecksumFast() const { return {}; }

private:
    String path_in_backup;
};

using BackupEntries = std::vector<std::unique_ptr<IBackupEntry>>;

}
