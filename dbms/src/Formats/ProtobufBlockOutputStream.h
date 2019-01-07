#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufZeroCopyOutputStreamFromWriteBuffer.h>
#include <Core/Block.h>


namespace google
{
namespace protobuf
{
class Message;
}
}


namespace DB
{

/** TODO */
class ProtobufBlockOutputStream : public IBlockOutputStream
{
public:
    ProtobufBlockOutputStream(WriteBuffer & buffer_,
                              const Block & header_,
                              const google::protobuf::Message* message_prototype_,
                              const FormatSettings & format_settings_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void flush() override;
    std::string getContentType() const override { return "application/octet-stream"; }

private:
    ProtobufZeroCopyOutputStreamFromWriteBuffer ostrm;
    const Block header;
    const google::protobuf::Message* message_prototype;
    const FormatSettings format_settings;
};

}
