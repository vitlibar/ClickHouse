#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>


namespace DB
{
struct BackupEntry;

class IBackup
{
public:
    virtual ~IBackup() = default;

    enum class OpenMode
    {
        READ_ONLY,
        CREATE,
        APPEND,
    };

    virtual OpenMode getOpenMode() const = 0;

    /// Returns the base of this incremental backup, or nullptr if it's not an incremental backup.
    virtual std::shared_ptr<const IBackup> getIncrementalBase() const = 0;

    /// Returns pathes of all the entries stored in the backup.
    virtual Strings list() const = 0;

    /// Checks if an entry with a specified path exists.
    virtual bool exists(const String & name) const = 0;

    /// Get the size of the entry's data.
    virtual size_t getDataSize(const String & name) const = 0;

    /// Get the checksum of the entry's data.
    virtual UInt128 getChecksum(const String & name) const = 0;

    /// Reads an entry from the backup.
    virtual BackupEntry read(const String & name) const = 0;

    /// Adds a new entry to the backup.
    virtual void write(BackupEntry && entry) = 0;
};

}
