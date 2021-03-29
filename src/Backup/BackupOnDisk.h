#pragma once

#include <Backup/IBackup.h>
#include <unordered_map>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class DiskSelector;

/// Represents a backup stored on a disk.
/// A backup is stored as a directory, each entry is stored as a file in that directory,
/// and also the ".header" file is stored which contains list of the files with their sizes and checksums.
/// While BackupOnDisk is writing a backup it creates the ".write_lock" file and removes it afterwards.
class BackupOnDisk : public IBackup
{
public:
    BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_);
    BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_, const std::shared_ptr<const IBackup> & base_backup_);
    BackupOnDisk(OpenMode open_mode_, const String & disk_name_, const String & directory_, const DiskSelector & disk_selector_);
    ~BackupOnDisk() override;

    OpenMode getOpenMode() const override;
    String getDiskName() const override;
    String getPath() const override;
    Strings list() const override;
    bool exists(const String & name) const override;
    size_t getDataSize(const String & name) const override;
    UInt128 getChecksum(const String & name) const override;
    BackupEntry read(const String & name) const override;
    void write(BackupEntry && entry) override;
    void finishWriting() override;

private:
    void open();
    void close();
    void writeLockFile();
    void removeLockFile();
    void writeHeader();
    void readHeader();

    struct Entry
    {
        size_t data_size;
        UInt128 checksum;

        /// For incremental backups. This is an index in the vector `incremental_bases`, 0 means this backup.
        bool read_from_base = false;
    };

    const OpenMode open_mode;
    const DiskPtr disk;
    const DiskSelector * disk_selector = nullptr;
    const String directory;
    std::shared_ptr<const IBackup> base_backup;
    std::unordered_map<String, Entry> entries;
    String lock_file_path;
    bool directory_was_empty = false;
    bool writing_finished = false;
    std::mutex mutex;
};

}
