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

    /// Returns the size of the data.
    virtual UInt64 getSize() = 0;

    /// Returns the checksum for the data.
    /// Can return nullopt which means the checksum should be calculated from the read buffer.
    virtual std::optional<UInt128> getChecksum() { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<ReadBuffer> getReadBuffer() = 0;

private:
    const String path_in_backup;
};

using BackupEntries = std::vector<std::unique_ptr<IBackupEntry>>;

}
