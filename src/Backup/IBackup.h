#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>


namespace DB
{
class IBackupEntry;
using BackupEntryPtr = std::unique_ptr<const IBackupEntry>;

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

    /// Returns the path to the backup. Can be empty.
    virtual String getPath() const { return ""; }

    /// Returns the name of the disk where that backup is located. Can be empty.
    virtual String getDisk() const { return ""; }

    /// Returns information about names of entries stored in the backup.
    /// If `prefix` isn't empty the function will return only the names starting with
    /// the prefix (but without the prefix itself).
    /// If the `terminator` isn't empty the function will returns only parts of the names
    /// before the terminator. For example, list("", "") returns names of all the entries
    /// in the backup; and list("data/", "/") return something like a list of folders and
    /// files stored in the "data/" directory inside the backup.
    virtual Strings list(const String & prefix, const String & terminator) const = 0;

    /// Checks if an entry with a specified name exists.
    virtual bool exists(const String & name) const = 0;

    /// Get the size of the entry's data.
    virtual size_t getSize(const String & name) const = 0;

    /// Get the checksum of the entry's data.
    virtual UInt128 getChecksum(const String & name) const = 0;

    /// Reads an entry from the backup.
    virtual BackupEntryPtr read(const String & name) const = 0;

    /// Adds a new entry to the backup.
    virtual void write(const String & name, BackupEntryPtr entry) = 0;

    /// Finish writing the backup.
    virtual void finishWriting() = 0;
};

}
