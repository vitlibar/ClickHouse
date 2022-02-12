#pragma once

#include <base/extended_types.h>


namespace DB
{
class ReadBuffer;
class WriteBuffer;
enum class CompressionMethodByte : uint8_t;

struct CompressedBufferHeader
{
    UInt128 checksum;
    CompressionMethodByte compression_method;
    UInt32 compressed_size;
    UInt32 decompressed_size;

    static const constexpr size_t kSize
        = sizeof(checksum) + sizeof(compression_method) + sizeof(compressed_size) + sizeof(decompressed_size);

    void read(ReadBuffer & in);
    void write(WriteBuffer & out);

    size_t getCompressedDataSize() const
    {
        return compressed_size - sizeof(compression_method) - sizeof(compressed_size) - sizeof(decompressed_size);
    }

    struct EqualChecksum
    {
        bool operator()(const CompressedBufferHeader & lhs, const CompressedBufferHeader & rhs) const
        {
            return (lhs.compressed_size < rhs.compressed_size) || ((lhs.size == rhs.size) && (lhs.checksum < rhs.checksum));
        }
    };
};

using CompressedBufferHeaders = std::vector<CompressedBufferHeader>;

}
