#pragma once

#include <Common/config.h>
#if USE_PROTOBUF

#include <functional>
#include <google/protobuf/compiler/importer.h>
#include <Core/Types.h>

namespace DB
{

/// Parses and caches protobuf schemas.
class ProtobufSchemaParser : public google::protobuf::compiler::SourceTree, public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    using ReadFileFunction = std::function<String(const String &)>;

    ProtobufSchemaParser(const ReadFileFunction & read_file_);
    ~ProtobufSchemaParser() override;

    const google::protobuf::Descriptor * getProtobufSchema(const String & path, const String & message_type);

private:
    // Overrides google::protobuf::compiler::SourceTree:
    google::protobuf::io::ZeroCopyInputStream* Open(const std::string& filename) override;

    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override;

    ReadFileFunction read_file;
    google::protobuf::compiler::Importer importer;
};

}
#endif
