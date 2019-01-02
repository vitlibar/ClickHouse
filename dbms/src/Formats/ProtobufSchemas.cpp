#include "Formats/ProtobufSchemas.h"
#include <boost/algorithm/string.hpp>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <google/protobuf/dynamic_message.h>
#include <Poco/Path.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_FORMAT_SCHEMA;
}

ProtobufSchemas::ProtobufSchemas()
    : disk_source_tree(new google::protobuf::compiler::DiskSourceTree()),
      importer(new google::protobuf::compiler::Importer(disk_source_tree.get(), this)),
      dynamic_message_factory(new google::protobuf::DynamicMessageFactory()) {}

ProtobufSchemas::~ProtobufSchemas() = default;

const google::protobuf::Message* ProtobufSchemas::getPrototypeForFormatSchema(
        const String & format_schema, const String & format_schema_path)
{
    auto it = format_schema_to_prototype_map.find(format_schema);
    if (it != format_schema_to_prototype_map.end())
        return it->second;

    std::vector<String> tokens;
    boost::split(tokens, format_schema, boost::is_any_of(":"));
    if ((tokens.size() != 2) || tokens[0].empty() || tokens[1].empty())
        throw Exception("Format Protobuf requires 'format_schema' setting to have a schema_file:message_name format, "
                        "e.g. 'schema.proto:Message'",
            ErrorCodes::BAD_ARGUMENTS);

    Poco::Path proto_file_path = Poco::Path(format_schema_path).resolve(tokens[0]);
    if (proto_file_path.getExtension().empty())
        proto_file_path.setExtension(".proto");

    const auto* prototype = readPrototypeFromProtoFile(proto_file_path.toString(), tokens[1]);
    format_schema_to_prototype_map[format_schema] = prototype;  // Cache found prototype for fast access.
    return prototype;
}

const google::protobuf::Message* ProtobufSchemas::readPrototypeFromProtoFile(
        const String & proto_file_path, const String & message_name)
{
    const auto* file_descriptor = importer->Import(proto_file_path);

    // If there parsing errors AddError() throws an exception and in this case the following line
    // isn't executed.
    assert(file_descriptor);

    const auto* descriptor = file_descriptor->FindMessageTypeByName(message_name);
    if (!descriptor)
        throw Exception("Not found a message named '" + message_name + "' in the schema file '" + proto_file_path + "'.",
                        ErrorCodes::BAD_ARGUMENTS);

    const auto* prototype = dynamic_message_factory->GetPrototype(descriptor);
    assert(prototype);
    return prototype;
}

const google::protobuf::Message* ProtobufSchemas::getAppropriatePrototypeForColumns(
        const std::vector<ColumnWithTypeAndName> & /*columns*/)
{
    return nullptr;  // TODO
}

void ProtobufSchemas::AddError(const String & filename, int line, int column, const String & message)
{
    throw Exception("Cannot parse '" + filename + "' file, found an error at line " + std::to_string(line) +
                    ", column " + std::to_string(column) + ". " + message,
                    ErrorCodes::CANNOT_PARSE_FORMAT_SCHEMA);
}

}
