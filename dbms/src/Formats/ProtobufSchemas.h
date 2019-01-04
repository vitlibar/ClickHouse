#pragma once

#include <Core/Types.h>
#include <google/protobuf/compiler/importer.h>
#include <ext/singleton.h>

namespace google
{
namespace protobuf
{
class Message;
class DynamicMessageFactory;
}
}

namespace DB
{
class Block;
struct ColumnWithTypeAndName;

/** Keeps parsed google protobuf schemas either parsed from files or generated from DB columns.
  * This class is used to handle the "Protobuf" input/output formats.
  */
class ProtobufSchemas : public ext::singleton<ProtobufSchemas>,
                        public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    ProtobufSchemas();
    ~ProtobufSchemas();

    /// Parses the format schema, then parses the corresponding proto file, and returns the default (prototype) message
    /// of that type. You can then call that message's New() method to construct a mutable message of that type.
    const google::protobuf::Message* getPrototypeForFormatSchema(const String & format_schema, const String & format_schema_path);

    /// Parses a proto file, then find a message type by a specified name, and returns the default (prototype) message of that type.
    /// You can then call that message's New() method to construct a mutable message of that type.
    const google::protobuf::Message* readPrototypeFromProtoFile(const String & proto_file_name, const String & message_name);

    /// Generates a message type with suitable types of fields to store a block with |header|, then returns the
    /// default (prototype) message of that type. You can then call that message's New() method to construct
    /// a mutable message of that type.
    const google::protobuf::Message* getAppropriatePrototypeForColumns(const std::vector<ColumnWithTypeAndName> & columns);

private:
    // Can be called by |importer| while parsing *.proto files.
    void AddError(const String & filename, int line, int column, const String & message) override;

    String proto_directory;
    std::unique_ptr<google::protobuf::compiler::DiskSourceTree> disk_source_tree;
    std::unique_ptr<google::protobuf::compiler::Importer> importer;
    std::unique_ptr<google::protobuf::DynamicMessageFactory> dynamic_message_factory;
    std::map<String, const google::protobuf::Message*> prototype_cache;
};

}
