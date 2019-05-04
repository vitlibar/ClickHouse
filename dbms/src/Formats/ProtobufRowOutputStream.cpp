#include <Common/config.h>
#if USE_PROTOBUF

#include "ProtobufRowOutputStream.h"

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaLoader.h>
#include <Formats/ProtobufSchemas.h>
#include <google/protobuf/descriptor.h>


namespace DB
{
ProtobufRowOutputStream::ProtobufRowOutputStream(WriteBuffer & out, const Block & header, const google::protobuf::Descriptor * message_type)
    : data_types(header.getDataTypes()), writer(out, message_type, header.getNames())
{
    value_indices.resize(header.columns());
}

void ProtobufRowOutputStream::write(const Block & block, size_t row_num)
{
    writer.startMessage();
    std::fill(value_indices.begin(), value_indices.end(), 0);
    size_t column_index;
    while (writer.writeField(column_index))
        data_types[column_index]->serializeProtobuf(
            *block.getByPosition(column_index).column, row_num, writer, value_indices[column_index]);
    writer.endMessage();
}


void registerOutputFormatProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings &)
        {
            const String format_schema_setting = context.getSettingsRef().format_schema.toString();
            const auto * message_type = context.getFormatSchemaLoader().getProtobufSchema(format_schema_setting);
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<ProtobufRowOutputStream>(buf, header, message_type), header);
        });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatProtobuf(FormatFactory &) {}
}

#endif
