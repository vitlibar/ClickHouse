#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>


namespace DB
{
class IBackupEntry;

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

    /// Returns pathes of all the entries stored in the backup.
    virtual Strings list() const = 0;

    /// Checks if an entry with a specified path exists.
    virtual bool exists(const String & path_in_backup) const = 0;

    /// Get the size of the entry's data.
    virtual size_t getDataSize(const String & path_in_backup) const = 0;

    /// Get the checksum of the entry's data.
    virtual UInt128 getChecksum(const String & path_in_backup) const = 0;

    /// Reads an entry from the backup.
    virtual std::unique_ptr<IBackupEntry> read(const String & path_in_backup) const = 0;

    /// Adds a new entry to the backup.
    virtual void write(std::unique_ptr<IBackupEntry> entry) = 0;

    /// Finish writing the backup.
    virtual void finishWriting() = 0;
};

}
