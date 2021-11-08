#pragma once

#include <Backups/BackupImpl.h>
#include <IO/S3Common.h>


namespace DB
{

/// Represents a backup stored in S3.
class BackupS3 : public BackupImpl
{
public:
    BackupS3(
        const S3::URI & s3_uri,
        const String & access_key_id,
        const String & secret_access_key,
        const String & backup_name_,
        OpenMode open_mode_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});
    ~BackupS3() override;

private:
    bool backupExists() const override;
    void startWriting() override;
    void removeAllFilesAfterFailure() override;
    ReadBufferCreator readFileImpl(const String & file_name) const override;
    void addFileImpl(const String & file_name, ReadBuffer & read_buffer) override;

    DiskPtr disk;
    String path;
    String dir_path; /// `path` without terminating slash
};

}
