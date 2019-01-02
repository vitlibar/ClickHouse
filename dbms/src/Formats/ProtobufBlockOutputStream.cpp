#include <Formats/FormatFactory.h>
#include <Formats/ProtobufBlockOutputStream.h>
#include <Formats/ProtobufSchemas.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

ProtobufBlockOutputStream::ProtobufBlockOutputStream(WriteBuffer & out_, const Block & header_,
                                                     const google::protobuf::Message* format_prototype_,
                                                     const FormatSettings & format_settings_)
    : out(out_), header(header_), format_prototype(format_prototype_), format_settings(format_settings_)
{
}

void ProtobufBlockOutputStream::flush()
{
    out.next();
}

void ProtobufBlockOutputStream::write(const Block &/* block*/)
{
}

void ProtobufBlockOutputStream::writePrefix()
{
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
