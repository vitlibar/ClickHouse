#include <Common/config.h>
#if USE_PROTOBUF

#include <Formats/ProtobufSchemaParser.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}


namespace
{
    struct StringHolder
    {
        StringHolder(const String & str_) : str(str_) {}
        String str;
    };

    class InputStream : public StringHolder, public google::protobuf::io::ArrayInputStream
    {
    public:
        InputStream(const String & str_) : StringHolder(str_), google::protobuf::io::ArrayInputStream(str.data(), str.size()) {}
    };
}


ProtobufSchemaParser::ProtobufSchemaParser(const ReadFileFunction & read_file_)
    : read_file(read_file_), importer(this, this)
{
}


ProtobufSchemaParser::~ProtobufSchemaParser() = default;


const google::protobuf::Descriptor * ProtobufSchemaParser::getProtobufSchema(const String & path, const String & message_type)
{
    // Search the message type among already imported ones.
    const auto * descriptor = importer.pool()->FindMessageTypeByName(message_type);
    if (descriptor)
        return descriptor;

    const auto * file_descriptor = importer.Import(path);

    // If there are parsing errors AddError() throws an exception, so `file_descriptor` is never null.
    descriptor = file_descriptor->FindMessageTypeByName(message_type);
    if (!descriptor)
        throw Exception(
            "Not found a message '" + message_type + "' in the file '" + path + "'", ErrorCodes::BAD_ARGUMENTS);

    return descriptor;
}


google::protobuf::io::ZeroCopyInputStream* ProtobufSchemaParser::Open(const String & path)
{
    return new InputStream(read_file(path));
}


void ProtobufSchemaParser::AddError(const String & filename, int line, int column, const String & message)
{
    throw Exception(
        "Cannot parse '" + filename + "' file, found an error at line " + std::to_string(line) + ", column " + std::to_string(column)
            + ", " + message,
        ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA);
}

}
#endif
