#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_SSL
#include <IO/WriteBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>


namespace DB
{

/// Encrypts data and writes the encrypted data to the underlying write buffer.
class WriteBufferFromFileEncrypted : public WriteBufferFromFileBase
{
public:
    /// `old_file_size` should be set to non-zero if we're going to append the file.
    WriteBufferFromFileEncrypted(
        size_t buffer_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const String & key_,
        const FileEncryption::InitVector & init_vector_,
        size_t old_file_size = 0);
    ~WriteBufferFromFileEncrypted() override;

    void finalize() override;

    void sync() override { out->sync(); }
    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override;

    bool finalized = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    FileEncryption::InitVector iv;
    bool flush_iv = false;

    FileEncryption::Encryptor encryptor;
};

}

#endif
