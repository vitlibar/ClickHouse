#pragma once
#if 0

namespace DB
{
    class CapnProtoSchemaParser
    {
    public:
        using ReadFileFunction = std::function<String(const String &)>;

        CapnProtoSchemaParser(const ReadFileFunction & read_file_);
        ~CapnProtoSchemaParser() override;

        const capnp::SchemaParser::ParsedSchema & getCapnpSchema(const String & path, const String & message_type);

    private:
        capnp::SchemaParser parser;
    };
}
#endif
