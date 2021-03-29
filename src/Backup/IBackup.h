#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>


namespace DB
{
struct BackupEntry;

/// Represents a backup, i.e. a storage of BackupEntries which can be accessed by their names.
/// A backup can be either incremental or non-incremental. An incremental backup doesn't store
/// the data of the entries which are not changed compared to its base backup.
class IBackup
{
public:
    virtual ~IBackup() = default;

    enum class OpenMode
    {
        CREATE,
        READ,
    };

    /// A backup can be open either in CREATE or READ mode.
    virtual OpenMode getOpenMode() const = 0;

    /// Returns the disk's name and the path to the backup on that disk. Can be empty.
    virtual String getDiskName() const { return ""; }
    virtual String getPath() const { return ""; }

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
