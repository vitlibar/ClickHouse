#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
class HashingReadBuffer : public IHashingBuffer<ReadBuffer>
{
public:
    explicit HashingReadBuffer(ReadBuffer & in_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IHashingBuffer<ReadBuffer>(block_size_), in(in_)
    {
        working_buffer = in.buffer();
        pos = in.position();

        /// We ignore the data which already read from the buffer.
        hash_start = pos;
    }

    uint128 getHash()
    {
        calculateHash(hash_start, pos - hash_start);
        hash_start = pos;
        return IHashingBuffer<ReadBuffer>::getHash();
    }

private:
    bool nextImpl() override
    {
        calculateHash(hash_start, pos - hash_start);

        in.position() = pos;
        bool res = in.next();
        working_buffer = in.buffer();

        // `pos` may be different from working_buffer.begin() when using AIO.
        pos = in.position();
        hash_start = pos;

        return res;
    }

    ReadBuffer & in;

    /// Start position for the hash calculation.
    Position hash_start;
};

}
