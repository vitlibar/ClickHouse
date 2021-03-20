#pragma once

#include <Storages/BackupEntry.h>


namespace DB
{
/// Interface for backups.
class IBackup
{
public:
    virtual ~IBackup() = default;

    /// Returns the path to where this backup is stored, usually it's a directory path.
    virtual const String & getBackupDirectory() const = 0;

    /// Returns a base backup if this is an incremental backup. Returns nullptr otherwise.
    virtual const IBackup * getBaseBackup() const = 0;

    /// Returns the time when this backup was created.
    virtual std::chrono::file_clock::time_point getCreationTime() const = 0;

    /// Returns the time when this backup was modified last time.
    virtual std::chrono::file_clock::time_point getLastModificationTime() const = 0;

    enum class OpenMode
    {
        Read,
        Write,
    };
    virtual OpenMode getOpenMode() const = 0;

    /// Adds an entry to this backup.
    virtual void write(BackupEntry && entry, bool overwrite) = 0;

    /// Reads an entry from this backup. Throws an exception if there is no entry with this path.
    virtual BackupEntry read(const String & path) const = 0;

    /// Reads an entry from this backup. Returns false if there is no entry with this path.
    virtual bool tryRead(const String & path, BackupEntry & entry) const = 0;

    /// Get checksum of an entry in this backup.
    virtual UInt128 getChecksum(const String & path) const = 0;
    virtual bool tryGetChecksum(const String & path, UInt128 & checksum) const = 0;

    /// Checks if an entry with a specified path exists in this backup.
    virtual bool exists(const String & path) const = 0;

    /// Returns pathes to all entries stored in the backup.
    virtual Strings list() const = 0;

    /// Removes an entry from this backup.
    virtual void remove(const String & path) = 0;

    /// Subscribes for changes in this backup.
    using OnChangedHandler = std::function<void(const String & path, bool added, bool removed)>;
    virtual void subscribeForChanges(const OnChangedHandler & handler) const = 0;
};

}
