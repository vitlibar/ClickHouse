#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_SSL
#include <IO/WriteBufferFromFileBase.h>
#include <IO/FileEncryptionCommon.h>


namespace DB
{

class WriteBufferFromFileEncrypted : public WriteBufferFromFileBase
{
public:
    WriteBufferFromFileEncrypted(
        size_t buf_size_,
        std::unique_ptr<WriteBufferFromFileBase> out_,
        const String & init_vector_,
        const FileEncryption::EncryptionKey & key_,
        const size_t & file_size);
    ~WriteBufferFromFileEncrypted() override;

    void finalize() override;

    void sync() override { out->sync(); }
    std::string getFileName() const override { return out->getFileName(); }

private:
    void nextImpl() override;

    bool finalized = false;
    std::unique_ptr<WriteBufferFromFileBase> out;

    bool flush_iv;
    String iv;
    FileEncryption::Encryptor encryptor;
};

}

#endif
