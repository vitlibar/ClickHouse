#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>
#include <optional>
#include <unordered_map>

namespace DB
{
class ReadBuffer;

/// A backup entry represents some data which should be written to the backup
/// or has been read from the backup. A backup entry is stored in the backup with its name.
/// That name looks like a path and starts with "data/" or "metadata/".
class IBackupEntry
{
public:
    virtual ~IBackupEntry() = default;

    /// Returns the size of the data.
    virtual UInt64 getSize() const = 0;

    /// Returns the checksum for the data.
    /// Can return nullopt which means the checksum should be calculated from the read buffer.
    virtual std::optional<UInt128> getChecksum() const { return {}; }

    /// Returns a read buffer for reading the data.
    virtual std::unique_ptr<ReadBuffer> getReadBuffer() const = 0;
};

using BackupEntryPtr = std::unique_ptr<IBackupEntry>;
using BackupEntries = std::unordered_map<String, BackupEntryPtr>;

}
