#pragma once

#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/BackupInfo.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/runAsyncWithOnFinishCallback.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class IBackupCoordination;
class IBackupReader;
class IBackupWriter;
class SeekableReadBuffer;
class IArchiveReader;
class IArchiveWriter;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Implementation of IBackup.
/// Along with passed files it also stores backup metadata - a single file named ".backup" in XML format
/// which contains a list of all files in the backup with their sizes and checksums and information
/// whether the base backup should be used for each entry.
class BackupImpl : public IBackup
{
public:
    struct ArchiveParams
    {
        String archive_name;
        String password;
        String compression_method;
        int compression_level = 0;
        size_t max_volume_size = 0;
    };

    BackupImpl(
        const String & backup_name_for_logging_,
        const ArchiveParams & archive_params_,
        const std::optional<BackupInfo> & base_backup_info_,
        std::shared_ptr<IBackupReader> reader_,
        const ContextPtr & context_);

    BackupImpl(
        const String & backup_name_for_logging_,
        const ArchiveParams & archive_params_,
        const std::optional<BackupInfo> & base_backup_info_,
        std::shared_ptr<IBackupWriter> writer_,
        const ContextPtr & context_,
        const ThreadPoolCallbackRunner<void> & scheduler_,
        bool is_internal_backup_,
        const std::shared_ptr<IBackupCoordination> & coordination_,
        const std::optional<UUID> & backup_uuid_,
        bool deduplicate_files_);

    ~BackupImpl() override;

    const String & getNameForLogging() const override { return backup_name_for_logging; }
    OpenMode getOpenMode() const override { return open_mode; }
    time_t getTimestamp() const override;
    UUID getUUID() const override;
    std::shared_ptr<const IBackup> getBaseBackup() const;
    size_t getNumFiles() const override;
    UInt64 getTotalSize() const override;
    size_t getNumEntries() const override;
    UInt64 getSizeOfEntries() const override;
    UInt64 getUncompressedSize() const override;
    UInt64 getCompressedSize() const override;
    size_t getNumReadFiles() const override;
    UInt64 getNumReadBytes() const override;
    Strings listFiles(const String & directory, bool recursive) const override;
    bool hasFiles(const String & directory) const override;
    bool fileExists(const String & file_name) const override;
    bool fileExists(const SizeAndChecksum & size_and_checksum) const override;
    UInt64 getFileSize(const String & file_name) const override;
    UInt128 getFileChecksum(const String & file_name) const override;
    SizeAndChecksum getFileSizeAndChecksum(const String & file_name) const override;
    BackupEntryPtr readFile(const String & file_name) const override;
    BackupEntryPtr readFile(const SizeAndChecksum & size_and_checksum) const override;
    void writeFileAsync(const String & file_name, BackupEntryPtr entry, const std::function<void(std::exception_ptr)> & on_finish_callback) override;
    void finalizeWriting() override;

private:
    using FileInfo = IBackupCoordination::FileInfo;
    class BackupEntryFromBackupImpl;

    void openBackup(const ContextPtr & context);
    void closeBackup();
    void closeBackupNoLock() TSA_REQUIRES(mutex);

    /// Closes an archive if this backup is an archive.
    void closeArchives() TSA_REQUIRES(mutex);

    /// Writes the file ".backup" containing backup's metadata.
    void writeBackupMetadata() TSA_REQUIRES(mutex);
    void readBackupMetadata() TSA_REQUIRES(mutex);

    /// Checks this backup doesn't exist yet.
    void checkBackupDoesntExist() const TSA_REQUIRES(mutex);

    /// Checks this backup is not finalized yet.
    void checkWritingNotFinalized();

    /// Lock file named ".lock" and containing the UUID of a backup is used to own the place where we're writing the backup.
    /// Thus it will not be allowed to put any other backup to the same place (even if the BACKUP command is executed on a different node).
    void createLockFile() TSA_REQUIRES(mutex);
    bool checkLockFile(bool throw_if_failed) const TSA_REQUIRES(mutex);
    void removeLockFile() TSA_REQUIRES(mutex);

    std::shared_ptr<IBackupReader> getReader() const;
    std::shared_ptr<IBackupWriter> getWriter() const;
    std::shared_ptr<IBackupCoordination> getCoordination() const;   

    void writeFile(const String & file_name, BackupEntryPtr entry, const std::shared_ptr<OnFinishCallbackRunnerForAsyncJob<void>> & on_finish);
    void addFileToArchive(const String & archive_suffix, const String & file_name, BackupEntryPtr backup_entry, size_t offset, size_t size, const std::function<void(std::exception_ptr)> & on_finish_callback);

    std::shared_ptr<IArchiveReader> getArchiveReader(const String & archive_suffix) const;
    std::shared_ptr<IArchiveReader> getArchiveReaderNoLock(const String & archive_suffix) const TSA_REQUIRES(mutex);
    std::shared_ptr<IArchiveWriter> getArchiveWriter(const String & archive_suffix);
    std::shared_ptr<IArchiveWriter> getArchiveWriterNoLock(const String & archive_suffix) TSA_REQUIRES(mutex);
    String getArchiveNameWithSuffix(const String & archive_suffix) const;

    /// Calculates and sets `compressed_size`.
    void setCompressedSize() TSA_REQUIRES(mutex);

    /// Removes all files of this backup after a failure.
    void removeAllFilesAfterFailure() TSA_REQUIRES(mutex);

    const String backup_name_for_logging;
    const ArchiveParams archive_params;
    const bool use_archives;
    const OpenMode open_mode;
    const ThreadPoolCallbackRunner<void> scheduler;
    const bool is_internal_backup;
    const bool deduplicate_files = true;
    const Poco::Logger * log;

    mutable std::mutex mutex;

    std::shared_ptr<IBackupWriter> TSA_GUARDED_BY(mutex) writer;
    std::shared_ptr<IBackupReader> TSA_GUARDED_BY(mutex) reader;
    std::shared_ptr<IBackupCoordination> TSA_GUARDED_BY(mutex) coordination;

    struct ArchiveWritingQueueItem
    {
        String file_name;
        BackupEntryPtr backup_entry;
        size_t offset;
        size_t size;
        std::function<void(std::exception_ptr)> on_finish_callback;
    };
    struct ArchiveWritingQueue
    {
        std::queue<ArchiveWritingQueueItem> queue;
        bool writing_now = false;
    };
    std::unordered_map<String /* archive_suffix */, ArchiveWritingQueue> TSA_GUARDED_BY(mutex) archive_writing_queues;

    std::optional<UUID> TSA_GUARDED_BY(mutex) uuid;
    time_t TSA_GUARDED_BY(mutex) timestamp = 0;
    size_t TSA_GUARDED_BY(mutex) num_files = 0;
    UInt64 TSA_GUARDED_BY(mutex) total_size = 0;
    size_t TSA_GUARDED_BY(mutex) num_entries = 0;
    UInt64 TSA_GUARDED_BY(mutex) size_of_entries = 0;
    UInt64 TSA_GUARDED_BY(mutex) uncompressed_size = 0;
    UInt64 TSA_GUARDED_BY(mutex) compressed_size = 0;
    mutable size_t TSA_GUARDED_BY(mutex) num_read_files = 0;
    mutable UInt64 TSA_GUARDED_BY(mutex) num_read_bytes = 0;
    int TSA_GUARDED_BY(mutex) version;
    std::optional<BackupInfo> TSA_GUARDED_BY(mutex) base_backup_info;
    std::shared_ptr<const IBackup> TSA_GUARDED_BY(mutex) base_backup;
    std::optional<UUID> TSA_GUARDED_BY(mutex) base_backup_uuid;
    mutable std::unordered_map<String /* archive_suffix */, std::shared_ptr<IArchiveReader>> TSA_GUARDED_BY(mutex) archive_readers;
    mutable std::unordered_map<String /* archive_suffix */, std::shared_ptr<IArchiveWriter>> TSA_GUARDED_BY(mutex) archive_writers;
    String TSA_GUARDED_BY(mutex) current_archive_suffix;
    String TSA_GUARDED_BY(mutex) lock_file_name;
    bool TSA_GUARDED_BY(mutex) writing_finalized = false;
};

}
