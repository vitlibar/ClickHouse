#include <Formats/ProtobufZeroCopyOutputStreamFromWriteBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SEEK_POSITION_OUT_OF_BOUND;
}


ProtobufZeroCopyOutputStreamFromWriteBuffer::ProtobufZeroCopyOutputStreamFromWriteBuffer(WriteBuffer & buffer_)
    : buffer(buffer_) {}

ProtobufZeroCopyOutputStreamFromWriteBuffer::~ProtobufZeroCopyOutputStreamFromWriteBuffer()
{
    flush();
}

void ProtobufZeroCopyOutputStreamFromWriteBuffer::flush()
{
    buffer.next();
}

bool ProtobufZeroCopyOutputStreamFromWriteBuffer::Next(void** data, int* size)
{
    buffer.nextIfAtEnd();
    *data = buffer.position();
    *size = buffer.available();
    buffer.position() += *size;
    return true;
}

void ProtobufZeroCopyOutputStreamFromWriteBuffer::BackUp(int count)
{
    if (count > static_cast<int>(buffer.offset()))
        throw Exception("Cannot back up " + std::to_string(count) + " bytes because it's greater than "
                        "the current offset " + std::to_string(buffer.offset()) + " in the write buffer.",
                        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    buffer.position() -= count;
}

int64_t ProtobufZeroCopyOutputStreamFromWriteBuffer::ByteCount() const
{
    return buffer.count();
}

bool ProtobufZeroCopyOutputStreamFromWriteBuffer::WriteAliasedRaw(const void* data, int size)
{
    buffer.write(reinterpret_cast<const char*>(data), size);
    return true;
}

bool ProtobufZeroCopyOutputStreamFromWriteBuffer::AllowsAliasing() const {
    return true;
}

}
