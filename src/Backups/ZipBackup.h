#pragma once

#include <Backups/BackupImpl.h>
#include <IO/ZipArchiveWriter.h>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class ZipArchiveReader;

/// Stores a backup as a single .zip file.
class ZipBackup : public BackupImpl
{
public:
    /// `disk`_ is allowed to be nullptr and that means the `path_` is a path in the local filesystem.
    ZipBackup(
        const String & backup_name_,
        const DiskPtr & disk_,
        const String & path_,
        const ContextPtr & context_,
        const std::optional<BackupInfo> & base_backup_info_ = {});

    ~ZipBackup() override;

    using CompressionMethod = ZipArchiveWriter::CompressionMethod;
    using CompressionLevel = ZipArchiveWriter::CompressionLevel;

    /// Sets compression method and level.
    void setCompression(CompressionMethod method_, int level_ = static_cast<int>(CompressionLevel::kDefault));
    void setCompression(const String & method_, int level_ = static_cast<int>(CompressionLevel::kDefault));

    /// Sets password.
    void setPassword(const String & password_);

private:
    friend class ReadBufferFromZipBackup;
    friend class WriteBufferFromZipBackup;

    bool backupExists() const override;
    void openImpl(OpenMode open_mode_) override;
    void closeImpl(bool writing_finalized_) override;
    bool supportsWritingInMultipleThreads() const override { return false; }
    std::unique_ptr<ReadBuffer> readFileImpl(const String & file_name) const override;
    std::unique_ptr<WriteBuffer> addFileImpl(const String & file_name) override;

    const DiskPtr disk;
    const String path;
    std::shared_ptr<ZipArchiveReader> reader;
    std::shared_ptr<ZipArchiveWriter> writer;
    CompressionMethod compression_method = CompressionMethod::kDeflate;
    int compression_level = static_cast<int>(CompressionLevel::kDefault);
    String password;
};

}
