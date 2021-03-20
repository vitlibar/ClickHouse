#pragma once

#include <Storages/IBackup.h>
#include <unordered_map>


namespace DB
{
class IDisk;

class BackupOnDisk : public IBackup
{
public:
    BackupOnDisk(const String & backup_directory_, OpenMode open_mode_, const String & base_backup_directory_);
    ~BackupOnDisk() override;

    const String & getBackupDirectory() const override;
    const IBackup * getBaseBackup() const override;
    std::chrono::file_clock::time_point getCreationTime() const override;
    std::chrono::file_clock::time_point getLastModificationTime() const override;
    void write(BackupEntry && entry, bool overwrite) override;
    BackupEntry read(const String & path) const override;
    bool tryRead(const String & path, BackupEntry & entry) const override;
    UInt128 getChecksum(const String & path) const override;
    bool tryGetChecksum(const String & path, UInt128 & checksum) const override;
    bool exists(const String & path) const override;
    Strings list() const override;
    void remove(const String & path) override;
    void subscribeForChanges(const OnChangedHandler & handler) const override;

private:
    struct Header
    {
        std::chrono::file_clock::time_point creation_time;
        std::chrono::file_clock::time_point last_modification_time;
        String base_backup_path;
    };

    struct Entry
    {
        UInt128 checksum;
        bool is_symlink = false;
        bool is_hard_link = false;

        /// For incremental backups. This is an index in the vector `base_backups`, -1 means this backup.
        size_t backup_index = static_cast<size_t>(-1);
    };

    std::shared_ptr<IDisk> disk;
    const String backup_root_dir;
    std::vector<std::shared_ptr<const IBackup>> base_backups;
    const OpenMode open_mode;
    Header header;
    std::unordered_map<String, Entry> entries;
    std::vector<OnChangedHandler> on_changed_handlers;
    std::mutex mutex;
};

}
