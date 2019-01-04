#include <Formats/FormatFactory.h>
#include <Formats/ProtobufBlockOutputStream.h>
#include <Formats/ProtobufSchemas.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SEEK_POSITION_OUT_OF_BOUND;
}

class ProtobufBlockOutputStream::OutputStream : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    OutputStream(WriteBuffer & buffer_)
        : buffer(buffer_) {}

    ~OutputStream() override
    {
        flush();
    }

    void flush()
    {
        buffer.next();
    }

    bool Next(void** data, int* size) override
    {
        buffer.nextIfAtEnd();
        *data = buffer.position();
        *size = buffer.available();
        buffer.position() += *size;
        return true;
    }

    void BackUp(int count)
    {
        if (count > static_cast<int>(buffer.offset()))
            throw Exception("Cannot back up " + std::to_string(count) + " bytes because it's greater than "
                            "the current offset " + std::to_string(buffer.offset()) + " in the write buffer.",
                            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        buffer.position() -= count;
    }

    Int64 ByteCount() const override
    {
        return buffer.count();
    }

    bool WriteAliasedRaw(const void* data, int size)
    {
        buffer.write(reinterpret_cast<const char*>(data), size);
        return true;
    }

    bool AllowsAliasing() const { return true; }

private:
    WriteBuffer & buffer;
};


ProtobufBlockOutputStream::ProtobufBlockOutputStream(WriteBuffer & buffer_,
                                                     const Block & header_,
                                                     const google::protobuf::Message* format_prototype_,
                                                     const FormatSettings & format_settings_)
    : stream(new OutputStream(buffer_)), header(header_),
      format_prototype(format_prototype_), format_settings(format_settings_)
{
}

void ProtobufBlockOutputStream::flush()
{
    stream->flush();
}

void ProtobufBlockOutputStream::write(const Block & block)
{
    for (size_t row = 0; row != block.rows(); ++row)
    {
        std::unique_ptr<google::protobuf::Message> message(format_prototype->New());
        google::protobuf::util::SerializeDelimitedToZeroCopyStream(*message, stream.get());
    }
}

void registerOutputFormatProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormat("Protobuf", [](
        WriteBuffer & buf,
        const Block & header,
        const Context & context,
        const FormatSettings & format_settings)
    {
        const String format_schema = context.getSettingsRef().format_schema.toString();
        const auto* format_prototype = format_schema.empty()
                ? ProtobufSchemas::instance().getAppropriatePrototypeForColumns(header.getColumnsWithTypeAndName())
                : ProtobufSchemas::instance().getPrototypeForFormatSchema(format_schema, context.getFormatSchemaPath());
        return std::make_shared<ProtobufBlockOutputStream>(buf, header, format_prototype, format_settings);
    });
}

}
