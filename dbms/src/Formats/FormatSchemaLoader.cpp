#include <Formats/FormatSchemaLoader.h>

#include <Poco/File.h>
#include <Poco/DirectoryIterator.h>
#include <Common/config.h>
#include <Common/typeid_cast.h>
#include <Formats/ProtobufSchemaParser.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}


namespace
{
    void splitFormatSchemaSetting(
        const String & format_schema_setting,
        const String & format_name,
        const String & default_file_extension,
        String & directory,
        String & filename,
        String & message_type)
    {
        if (format_schema_setting.empty())
            throw Exception(
                "Format '" + format_name + "' requires the 'format_schema' setting to be set. "
                "This setting should have the following format: 'filename:message', "
                "for example 'my" + format_name + "." + default_file_extension + ":Message'",
                ErrorCodes::BAD_ARGUMENTS);

        size_t colon_pos = format_schema_setting.find(':');
        Poco::Path p;
        if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema_setting.length() - 1)
            || p.assign(format_schema_setting.substr(0, colon_pos)).makeFile().getFileName().empty())
        {
            throw Exception(
                "Format '" + format_name + "' requires the 'format_schema' setting in the following format: 'filename:message', "
                "for example 'My" + format_name + "." + default_file_extension + ":Message'. Got " + format_schema_setting,
                ErrorCodes::BAD_ARGUMENTS);
        }

        if (p.getExtension().empty() && !default_file_extension.empty())
            p.setExtension(default_file_extension);

        path = p.toString();
        message_type = format_schema_setting.substr(colon_pos + 1);
    }

    void findAllPathsInDirectoryTree(const Poco::Path & directory, const Poco::Path & subdirectory, std::vector<String> & all_paths)
    {
        Poco::DirectoryIterator it(Poco::Path(directory).append(subdirectory));
        Poco::DirectoryIterator end;
        for (; it != end; ++it)
        {
            if (it->isFile())
                all_paths.emplace_back(Poco::Path{subdirectory}.setFileName(it.name()).toString());
            else if (it->isDirectory())
                findAllPathsInDirectoryTree(directory, Poco::Path{subdirectory}.pushDirectory(it.name()), all_paths);
        }
    }
}


FormatSchemaLoader::FormatSchemaLoader(const Context & context_) : context(context_) {}
FormatSchemaLoader::~FormatSchemaLoader() = default;


void FormatSchemaLoader::setDirectory(const String & directory_, bool allow_paths_break_out_)
{
    std::cout << "setDirectory(" << directory_ << ")" << std::endl;
    std::lock_guard lock{mutex};
    std::optional<Poco::Path> new_directory;
    if (!directory_.empty())
    {
        new_directory = Poco::Path(directory_).makeAbsolute().makeDirectory();
        Poco::File(*new_directory).createDirectories();
    }
    directory = new_directory;
    allow_paths_break_out = allow_paths_break_out_;
}


void FormatSchemaLoader::setAbsolutePathsAreAllowed(bool allowed)
{
    allow_absolute_paths = allowed;
}


void FormatSchemaLoader::setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_)
{
    std::lock_guard lock{mutex};
    host = host_;
    port = port_;
    user = user_;
    password = password_;
}


void FormatSchemaLoader::clearCaches()
{
    std::lock_guard lock{mutex};
    schemas_by_path.clear();
#if USE_PROTOBUF
    protobuf_parsers.reset();
#endif
}


Strings FormatSchemaLoader::getAllPaths()
{
    std::lock_guard lock{mutex};
    Strings all_paths;

    if (directory)
    {
        std::cout << "FormatSchemaLoader: Searching files in directory " << directory->toString() << std::endl;
        findAllPathsInDirectoryTree(*directory, {}, all_paths);
    }

    if (!host.empty())
    {
        String query = "SELECT path FROM system.format_schemas";
        sendQueryToConnection(query, all_paths);
    }

    return all_paths;
}


String FormatSchemaLoader::getRawSchema(const String & path)
{
    std::lock_guard lock{mutex};
    return getRawSchemaImpl(path);
}

String FormatSchemaLoader::getRawSchemaImpl(const String & path)
{
    String schema;
    if (!tryGetRawSchemaImpl(path, schema))
        throw Exception("Format schema '" + path + "' not found", ErrorCodes::FILE_DOESNT_EXIST);
    return schema;
}

bool FormatSchemaLoader::tryGetRawSchema(const String & path, String & schema)
{
    std::lock_guard lock{mutex};
    return tryGetRawSchemaImpl(path, schema);
}

bool FormatSchemaLoader::tryGetRawSchemaImpl(const String & path, String & schema)
{
    std::cout << "tryGetRawSchemaImpl(" << path << ")" << std::endl;
    auto it = schemas_by_path.find(path);
    if (it != schemas_by_path.end())
    {
        schema = it->second;
        return true;
    }

    Poco::Path p(path);
    if (!tryGetRawSchemaFromDirectory(p, schema) && !tryGetRawSchemaFromConnection(p, schema))
        return false;

    schemas_by_path.try_emplace(path, schema);
    return true;
}


bool FormatSchemaLoader::tryGetRawSchemaFromDirectory(const Poco::Path & path, String & schema)
{
    std::cout << "tryGetRawSchemaFromDirectory(" << path.toString() << ")" << std::endl;
    if (!directory)
        return false;

    if (path.isAbsolute())
    {
        if (!allow_absolute_paths)
            throw Exception("Path '" + path.toString() + "' to format_schema should be relative", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        if (path.depth() >= 1 && path.directory(0) == ".." && !allow_paths_break_out)
            throw Exception("Path '" + path.toString() + "' to format_schema should be inside the format_schema_path directory specified in the setting", ErrorCodes::BAD_ARGUMENTS);
    }

    Poco::Path resolved_path = Poco::Path(*directory).resolve(path);
    std::cout << "FormatSchemaLoader: Reading file " << resolved_path.toString() << std::endl;
    Poco::File file(resolved_path);
    if (!file.exists() || !file.isFile())
        return false;

    ReadBufferFromFile buf(resolved_path.toString());
    readStringUntilEOF(schema, buf);
    return true;
}


bool FormatSchemaLoader::tryGetRawSchemaFromConnection(const Poco::Path & path, String & schema)
{
    std::cout << "tryGetRawSchemaFromConnection(" << path.toString() << ")" << std::endl;
    if (host.empty())
        return false;

    if (path.isAbsolute() || (path.depth() >= 1 && path.directory(0) == ".."))
        return false;

    WriteBufferFromOwnString escaped_path;
    writeQuotedString(path.toString(), escaped_path);
    String query = "SELECT schema FROM system.format_schemas WHERE path=" + escaped_path.str();
    Strings result;
    sendQueryToConnection(query, result);
    if (result.empty())
        return false;

    schema = result.front();
    return true;
}


size_t FormatSchemaLoader::sendQueryToConnection(const String & query, Strings & result)
{
    std::cout << "sendQueryToConnection(" << query << ")" << std::endl;
    if (!connection)
        connection = std::make_unique<Connection>(
            host, port, "default", user, password, ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context.getSettingsRef()));

    Block header;
    RemoteBlockInputStream in(*connection, query, header, context);

    Block block = in.read();
    if (!block)
        return 0;

    if (block.columns() != 1)
        throw Exception("Wrong number of columns received for query to read format schemas", ErrorCodes::LOGICAL_ERROR);

    const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);
    size_t num_rows = block.rows();
    result.reserve(result.size() + num_rows);
    for (size_t i = 0; i != num_rows; ++i)
        result.emplace_back(column.getDataAt(i).toString());
    return num_rows;
}

String FormatSchemaLoader::readSchemaFile(const String & basedir, const String & filename)
{

}

#if USE_PROTOBUF
const google::protobuf::Descriptor * FormatSchemaLoader::getProtobufSchema(const String & format_schema_setting)
{
    std::lock_guard lock{mutex};
    String path, message_type;
    splitFormatSchemaSetting(format_schema_setting, "Protobuf", "proto", path, message_type);
    if (!protobuf_schema_parser)
        protobuf_schema_parser = std::make_unique<ProtobufSchemaParser>(std::bind(&FormatSchemaLoader::readSchemaFile, this, std::placeholders::_1));
    return protobuf_schema_parser->getProtobufSchema(path, message_type);
}
#endif

}
