#include <IO/ReadBufferFromFileEncrypted.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

using InitVector = FileEncryption::InitVector;

ReadBufferFromFileEncrypted::ReadBufferFromFileEncrypted(
    size_t buffer_size_,
    std::unique_ptr<ReadBufferFromFileBase> in_,
    const String & key_,
    const InitVector & init_vector_)
    : ReadBufferFromFileBase(buffer_size_, nullptr, 0)
    , in(std::move(in_))
    , encrypted_buffer(buffer_size_)
    , encryptor(key_, init_vector_)
{
    in->seek(InitVector::kSize, SEEK_SET);
}

off_t ReadBufferFromFileEncrypted::seek(off_t off, int whence)
{
    off_t new_pos;
    if (whence == SEEK_SET)
    {
        if (off < 0)
            throw Exception("SEEK_SET underflow: off = " + std::to_string(off), ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        new_pos = off;
    }
    else if (whence == SEEK_CUR)
    {
        if (off < 0 && -off > getPosition())
            throw Exception("SEEK_CUR shift out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        new_pos = getPosition() + off;
    }
    else
        throw Exception("ReadBufferFromFileEncrypted::seek expects SEEK_SET or SEEK_CUR as whence", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if ((offset - static_cast<off_t>(working_buffer.size()) <= new_pos) && (new_pos <= offset))
    {
        /// Position is still inside buffer.
        pos = working_buffer.end() - offset + new_pos;
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());
    }
    else
    {
        offset = new_pos;
        need_seek = true;
        pos = working_buffer.end();
    }

    encryptor.setOffset(offset);
    return new_pos;
}

off_t ReadBufferFromFileEncrypted::getPosition()
{
    return offset - available();
}

bool ReadBufferFromFileEncrypted::nextImpl()
{
    if (need_seek)
    {
        off_t raw_offset = offset + InitVector::kSize;
        if (in->seek(raw_offset, SEEK_SET) != raw_offset)
            return false;
        need_seek = false;
    }

    if (in->eof())
        return false;

    /// Read up to the size of `encrypted_buffer`.
    size_t bytes_read = 0;
    while (bytes_read < encrypted_buffer.size() && !in->eof())
    {
        bytes_read += in->read(encrypted_buffer.data() + bytes_read, encrypted_buffer.size() - bytes_read);
    }

    /// The used cipher algorithms generate the same number of bytes in output as it were in input,
    /// so after deciphering the numbers of bytes will be still `bytes_read`.
    working_buffer.resize(bytes_read);
    encryptor.decrypt(encrypted_buffer.data(), bytes_read, working_buffer.begin());

    pos = working_buffer.begin();
    return true;
}

}
