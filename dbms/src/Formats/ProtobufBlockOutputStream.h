#pragma once

#include <string>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatSettings.h>
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

class WriteBuffer;

/** TODO */
class ProtobufBlockOutputStream : public IBlockOutputStream
{
public:
    ProtobufBlockOutputStream(WriteBuffer & buffer_,
                              const Block & header_,
                              const google::protobuf::Message* format_prototype_,
                              const FormatSettings & format_settings_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void flush() override;
    std::string getContentType() const override { return "application/octet-stream"; }

private:
    class OutputStream;
    std::unique_ptr<OutputStream> stream;
    const Block header;
    const google::protobuf::Message* format_prototype;
    const FormatSettings format_settings;
};

}
