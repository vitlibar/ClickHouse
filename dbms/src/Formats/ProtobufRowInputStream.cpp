#include <Common/config.h>
#if USE_PROTOBUF

#include <Formats/ProtobufRowInputStream.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaLoader.h>
#include <Formats/ProtobufSchemas.h>


namespace DB
{

ProtobufRowInputStream::ProtobufRowInputStream(ReadBuffer & in_, const Block & header, const google::protobuf::Descriptor * message_type)
    : data_types(header.getDataTypes()), reader(in_, message_type, header.getNames())
{
}

ProtobufRowInputStream::~ProtobufRowInputStream() = default;

bool ProtobufRowInputStream::read(MutableColumns & columns, RowReadExtension & extra)
{
    if (!reader.startMessage())
        return false; // EOF reached, no more messages.

    // Set of columns for which the values were read. The rest will be filled with default values.
    auto & read_columns = extra.read_columns;
    read_columns.assign(columns.size(), false);

    // Read values from this message and put them to the columns while it's possible.
    size_t column_index;
    while (reader.readColumnIndex(column_index))
    {
        bool allow_add_row = !static_cast<bool>(read_columns[column_index]);
        do
        {
            bool row_added;
            data_types[column_index]->deserializeProtobuf(*columns[column_index], reader, allow_add_row, row_added);
            if (row_added)
            {
                read_columns[column_index] = true;
                allow_add_row = false;
            }
        } while (reader.maybeCanReadValue());
    }

    // Fill non-visited columns with the default values.
    for (column_index = 0; column_index < read_columns.size(); ++column_index)
        if (!read_columns[column_index])
            data_types[column_index]->insertDefaultInto(*columns[column_index]);

    reader.endMessage();
    return true;
}

bool ProtobufRowInputStream::allowSyncAfterError() const
{
    return true;
}

void ProtobufRowInputStream::syncAfterError()
{
    reader.endMessage();
}


void registerInputFormatProtobuf(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Protobuf",
        [](ReadBuffer & buf, const Block & sample, const Context & context, UInt64 max_block_size, const FormatSettings & settings)
        {
            const String format_schema_setting = context.getSettingsRef().format_schema.toString();
            const auto * message_type = context.getFormatSchemaLoader().getProtobufSchema(format_schema_setting);
            return std::make_shared<BlockInputStreamFromRowInputStream>(
                std::make_shared<ProtobufRowInputStream>(buf, sample, message_type), sample, max_block_size, settings);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &) {}
}

#endif
