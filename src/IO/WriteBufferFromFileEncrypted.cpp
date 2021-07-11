#include <IO/WriteBufferFromFileEncrypted.h>


namespace DB
{

using InitVector = FileEncryption::InitVector;

WriteBufferFromFileEncrypted::WriteBufferFromFileEncrypted(
    size_t buffer_size_,
    std::unique_ptr<WriteBufferFromFileBase> out_,
    const String & key_,
    const InitVector & init_vector_,
    size_t old_file_size)
    : WriteBufferFromFileBase(buffer_size_, nullptr, 0)
    , out(std::move(out_))
    , iv(init_vector_)
    , flush_iv(!old_file_size)
    , encryptor(key_, init_vector_)
{
    encryptor.setOffset(old_file_size);
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
        iv.write(*out);
        flush_iv = false;
    }

    encryptor.encrypt(working_buffer.begin(), offset(), *out);
}
}
