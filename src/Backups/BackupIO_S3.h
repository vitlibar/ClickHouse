#pragma once

#include "config.h"

#if USE_AWS_S3
#include <Backups/BackupIO.h>
#include <IO/ReadSettings.h>
#include <IO/S3Common.h>
#include <Storages/StorageS3Settings.h>


namespace DB
{

/// Represents a backup stored to AWS S3.
class BackupReaderS3 : public IBackupReader
{
public:
    BackupReaderS3(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_);
    ~BackupReaderS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<SeekableReadBuffer> readFile(const String & file_name) override;
    DataSourceDescription getDataSourceDescription() const override;

private:
    S3::URI s3_uri;
    std::shared_ptr<S3::Client> client;
    ReadSettings read_settings;
    S3Settings::RequestSettings request_settings;
};


class BackupWriterS3 : public IBackupWriter
{
public:
    BackupWriterS3(const S3::URI & s3_uri_, const String & access_key_id_, const String & secret_access_key_, const ContextPtr & context_);
    ~BackupWriterS3() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    bool fileContentsEqual(const String & file_name, const String & expected_file_contents) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void removeFile(const String & file_name) override;
    void removeFiles(const Strings & file_names) override;

    void copyDataToFile(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name,
                        const ThreadPoolCallbackRunner<void> & scheduler) override;

    void copyDataToFileAsync(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name,
                             const ThreadPoolCallbackRunner<void> & scheduler, const std::function<void(std::exception_ptr)> & on_finish_callback) override;

    DataSourceDescription getDataSourceDescription() const override;
    bool supportNativeCopy(DataSourceDescription data_source_description) const override;

    void copyFileNative(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name,
                        const ThreadPoolCallbackRunner<void> & scheduler) override;

    void copyFileNativeAsync(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name,
                             const ThreadPoolCallbackRunner<void> & scheduler, const std::function<void(std::exception_ptr)> & on_finish_callback) override;

private:
    void removeFilesBatch(const Strings & file_names);

    void copyDataToFileImpl(const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name,
                            const ThreadPoolCallbackRunner<void> & scheduler, bool async, const std::function<void(std::exception_ptr)> & on_finish_callback);

    void copyFileNativeImpl(DiskPtr src_disk, const String & src_file_name, UInt64 src_offset, UInt64 src_size, const String & dest_file_name,
                            const ThreadPoolCallbackRunner<void> & scheduler, bool async, const std::function<void(std::exception_ptr)> & on_finish_callback);

    S3::URI s3_uri;
    std::shared_ptr<S3::Client> client;
    ReadSettings read_settings;
    S3Settings::RequestSettings request_settings;
    Poco::Logger * log;
    std::optional<bool> supports_batch_delete;
};

}

#endif
