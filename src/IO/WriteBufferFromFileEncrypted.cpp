#include <IO/WriteBufferFromFileEncrypted.h>


namespace DB
{

WriteBufferFromFileEncrypted::WriteBufferFromFileEncrypted(
    size_t buf_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const String & init_vector_,
    const FileEncryption::EncryptionKey & key_,
    const size_t & file_size)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , out(std::move(out_))
    , flush_iv(!file_size)
    , iv(init_vector_)
    , encryptor(FileEncryption::Encryptor(init_vector_, key_, file_size))
{
}

WriteBufferFromFileEncrypted::~WriteBufferFromFileEncrypted()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void WriteBufferFromFileEncrypted::finalize()
{
    if (finalized)
        return;

    next();
    out->finalize();

    finalized = true;
}

void WriteBufferFromFileEncrypted::nextImpl()
{
    if (!offset())
        return;
    if (flush_iv)
    {
        FileEncryption::writeIV(iv, *out);
        flush_iv = false;
    }

    encryptor.encrypt(working_buffer.begin(), *out, offset());
}
}
