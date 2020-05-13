#include <Common/config.h>

#if USE_PROTOBUF
#include <google/protobuf/compiler/importer.h>
#include <Formats/FormatSchemaLoader.h>

namespace DB
{

/// Loads a protobuf schema. Throws an exception if failed to load or parse.
/// `format_schema_setting` is supposed to be a path to a schema file and a message type's name delimited by colon `:`.
class ProtobufSchemaLoader : public FormatSchemaLoader::IFormatSpecific
{
public:
    static ProtobufSchemaLoader & get(ProtobufSchemaLoader & loader)
    {
        return loader.get<ProtobufSchemaLoader>();
    }

    static ProtobufSchemaLoader & get(const Context & context)
    {
        return get(context.getFormatSchemaLoader());
    }

    const google::protobuf::Descriptor * getProtobufSchema(const String & format_schema_setting)
    {
        SchemaLocation location = splitFormatSchemaSetting(format_schema_setting);
        auto it = parsed_files_by_paths.find(location.path);
        if (it == parsed_files_by_paths.end())
            it = parsed_files_by_paths.try_emplace(location.path, {loader, location.path}).first;
        const ParsedFile & parsed_file = it->second;
        return parsed_file.findMessageType(location.message_type);
    }

private:
    friend class FormatSchemaLoader;
    ProtobufSchemaLoader(FormatSchemaLoader & loader)
        : IFormatSpecific(loader, "Protobuf", "proto") {}

    class ParsedFile;
    std::unordered_map<String, ParsedFile> parsed_files_by_paths;
};

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


    class ParsedFile : public google::protobuf::compiler::SourceTree, public google::protobuf::compiler::MultiFileErrorCollector
    {
    public:
        ParsedFile(FormatSchemaLoader & loader_, const Poco::Path & path)
            : loader(loader_), basedir(Poco::Path(path).makeParent()), filename(path.getFileName()), importer(this, this)
        {
            importer.Import(filename);
        }

        const google::protobuf::Descriptor * findMessageType(const String & message_type) const
        {
            const auto * descriptor = importer.pool()->FindMessageTypeByName(message_type);
            if (!descriptor)
                throw Exception(
                    "Not found a message named '" + message_type + "' in the schema file '" + filename + "'", ErrorCodes::BAD_ARGUMENTS);
            return descriptor;
        }

    private:
        google::protobuf::io::ZeroCopyInputStream* Open(const String & filename)
        {
            return new InputStream(loader.getSchemaFile(Poco::Path(basedir).resolve(filename)));
        }

        void AddError(const String & filename_, int line, int column, const String & message)
        {
            throw Exception(
                "Cannot parse '" + filename_ + "' file, found an error at line " + std::to_string(line) + ", column "
                    + std::to_string(column) + ", " + message,
                ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA);
        }

        FormatSchemaLoader & loader;
        Poco::Path basedir;
        String filename;
        google::protobuf::compiler::Importer importer;
    };

}


class FormatSchemaLoader::ProtobufCache : public Cache
{
public:
    ProtobufCache(FormatSchemaLoader & loader_) : loader(loader_) {}

    const google::protobuf::Descriptor * getProtobufSchema(const String & format_schema_setting)
    {
        SchemaLocation location = loader.splitFormatSchemaSetting(format_schema_setting, format_traits);
        auto it = parsed_files_by_paths.find(location.path);
        if (it == parsed_files_by_paths.end())
            it = parsed_files_by_paths.try_emplace(location.path, {loader, location.path}).first;
        const ParsedFile & parsed_file = it->second;
        return parsed_file.findMessageType(location.message_type);
    }

private:
    FormatSchemaLoader & loader;
    const FormatTraits format_traits{"Protobuf", "proto"};
    std::unordered_map<String, ParsedFile> parsed_files_by_paths;
};


const google::protobuf::Descriptor * FormatSchemaLoader::getProtobufSchema(const String & path)
{
    if (!protobuf_cache)
        protobuf_cache = std::make_unique<ProtobufCache>(*this);
    return static_cast<ProtobufCache *>(loader.protobuf_cache.get())->getProtobufSchema(path);
}

}
#endif
