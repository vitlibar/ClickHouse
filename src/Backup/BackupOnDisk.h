#pragma once

#include <Backup/IBackup.h>
#include <map>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class Context;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory.
/// Also three system files are stored:
/// 1) ".base" is an XML file with information about the base backup.
/// 2) ".contents" is a binary file containg a list of all entries along with their sizes
/// and checksums and information whether the base backup should be used for each entry
/// 3) ".write_lock" is a temporary empty file which is created before writing of a backup
/// and deleted after finishing that writing.
class BackupOnDisk : public IBackup
{
public:
    BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_, const Context & context_);
    BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_, const std::shared_ptr<const IBackup> & base_backup_);
    ~BackupOnDisk() override;

    OpenMode getOpenMode() const override;
    String getDisk() const override;
    String getPath() const override;
    Strings list(const String & prefix, const String & terminator) const override;
    bool exists(const String & name) const override;
    size_t getSize(const String & name) const override;
    UInt128 getChecksum(const String & name) const override;
    BackupEntryPtr read(const String & name) const override;
    void write(const String & name, BackupEntryPtr entry) override;
    void finishWriting() override;

private:
    void open();
    void close();
    void writeLockFile();
    void removeLockFile();
    void writeBaseBackupInfo();
    void readBaseBackupInfo();
    void writeContents();
    void readContents();

    struct EntryInfo
    {
        UInt64 size = 0;
        UInt128 checksum{0, 0};

        /// for incremental backups
        UInt64 base_size = 0;
        UInt128 base_checksum{0, 0};
    };

    const OpenMode open_mode;
    const DiskPtr disk;
    const String directory;
    const Context * context = nullptr;
    std::shared_ptr<const IBackup> base_backup;
    std::map<String, EntryInfo> infos;
    String lock_file_path;
    bool directory_was_empty = false;
    bool writing_finished = false;
    std::mutex mutex;
};

}
