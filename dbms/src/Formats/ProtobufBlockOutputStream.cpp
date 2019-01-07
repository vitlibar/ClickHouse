#include <boost/algorithm/string.hpp>
#include <Formats/FormatFactory.h>
#include <Formats/ProtobufBlockOutputStream.h>
#include <Formats/ProtobufField.h>
#include <Formats/ProtobufSchemas.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
extern const int REQUIRED_FIELD_OF_PROTOBUF_MESSAGE;
}


ProtobufBlockOutputStream::ProtobufBlockOutputStream(WriteBuffer & buffer_,
                                                     const Block & header_,
                                                     const google::protobuf::Message* message_prototype_,
                                                     const FormatSettings & format_settings_)
    : ostrm(buffer_), header(header_),
      message_prototype(message_prototype_), format_settings(format_settings_)
{
}

void ProtobufBlockOutputStream::flush()
{
    ostrm.flush();
}

void ProtobufBlockOutputStream::write(const Block & block)
{
    std::vector<std::pair<const ColumnWithTypeAndName*, std::unique_ptr<ProtobufField>>> mapping;
    const auto* descriptor = message_prototype->GetDescriptor();
    for (int i = 0; i != descriptor->field_count(); ++i)
    {
        const auto* field = descriptor->field(i);
        if (block.has(field->name()))
        {
            const ColumnWithTypeAndName & column = block.getByName(field->name());
            mapping.emplace_back(&column, std::make_unique<ProtobufField>(field));
        }
        else if (field->is_required())
        {
            throw Exception("Output doesn't have a column named " + field->name() +
                            " which is required to write the output in the protobuf format "
                            " (message type: " + descriptor->name() + ").",
                            ErrorCodes::REQUIRED_FIELD_OF_PROTOBUF_MESSAGE);
        }
    }

    for (size_t row_num = 0; row_num != block.rows(); ++row_num)
    {
        std::unique_ptr<google::protobuf::Message> message(message_prototype->New());

        for (const auto & pr : mapping)
        {
            const auto & column = *pr.first;
            const auto & field = *pr.second;
            column.type->serializeProtobuf(*column.column, row_num, field, *message);
        }

        if (!message->IsInitialized())
        {
            std::vector<std::string> not_initialized_fields;
            message->FindInitializationErrors(&not_initialized_fields);
            throw Exception("Fields " + boost::algorithm::join(not_initialized_fields, ",") +
                            " are required but not initialized "
                            " (message type: " + descriptor->name() + ").",
                            ErrorCodes::REQUIRED_FIELD_OF_PROTOBUF_MESSAGE);
        }

        google::protobuf::util::SerializeDelimitedToZeroCopyStream(*message, &ostrm);
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
        const auto* message_prototype = format_schema.empty()
                ? ProtobufSchemas::instance().getAppropriatePrototypeForColumns(header.getColumnsWithTypeAndName())
                : ProtobufSchemas::instance().getPrototypeForFormatSchema(format_schema, context.getFormatSchemaPath());
        return std::make_shared<ProtobufBlockOutputStream>(buf, header, message_prototype, format_settings);
    });
}

}
