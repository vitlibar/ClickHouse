#pragma once

#include <google/protobuf/io/zero_copy_stream.h>


namespace DB
{
class WriteBuffer;


class ProtobufZeroCopyOutputStreamFromWriteBuffer : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    ProtobufZeroCopyOutputStreamFromWriteBuffer(WriteBuffer & buffer_);
    ~ProtobufZeroCopyOutputStreamFromWriteBuffer() override;

    void flush();

    bool Next(void** data, int* size) override;
    void BackUp(int count) override;
    int64_t ByteCount() const override;
    bool WriteAliasedRaw(const void* data, int size);
    bool AllowsAliasing() const;

private:
    WriteBuffer & buffer;
};

}
