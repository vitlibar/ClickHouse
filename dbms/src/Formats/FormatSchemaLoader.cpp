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


void FormatSchemaLoader::setDirectory(const String & directory_)
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
}


void FormatSchemaLoader::setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_)
{
    std::lock_guard lock{mutex};
    host = host_;
    port = port_;
    user = user_;
    password = password_;
}


/// Enables using absolute paths for retrieving schema files from the local file system. By default it's disabled.
void FormatSchemaLoader::enableAbsolutePaths(bool enable)
{
    std::lock_guard lock{mutex};
    enable_absolute_paths = enable;
}

/// Enables expanding environment variables in a path (relative or absolute) to a schema file. By default it's disabled.
void FormatSchemaLoader::enableEnvironmentVariables(bool enable)
{
    std::lock_guard lock{mutex};
    enable_environment_variables = enable;
}


bool FormatSchemaLoader::schemaFileExists(const String & path)
{
    std::lock_guard lock{mutex};
    const Poco::Path p = resolvePath(path);
    if (p.isAbsolute())
    {
        Poco::File file(p);
        return file.exists() && file.isFile();
    }

    if (directory)
    {
        Poco::File file(Poco::Path(*directory).resolve(p));
        if (file.exists() && file.isFile())
            return true;
    }

    if (!host.empty())
    {
        WriteBufferFromOwnString escaped_path;
        writeQuotedString(p, escaped_path);
        const String query = "SELECT ANY(path) FROM system.format_schemas WHERE path = " + escaped_path;
        return !sendQueryToConnection(query).empty();
    }
}


String FormatSchemaLoader::getSchemaFile(const String & path)
{
    std::lock_guard lock{mutex};
    const Poco::Path p = resolvePath(path);
    if (p.isAbsolute())
    {
        ReadBufferFromFile buf(p.toString());
        String file_contents;
        readStringUntilEOF(file_contents, buf);
        return file_contents;
    }

    if (directory)
    {
        Poco::File file(Poco::Path(*directory).resolve(p));
        if (file.exists() && file.isFile())
        {
            ReadBufferFromFile buf(p.toString());
            String file_contents;
            readStringUntilEOF(file_contents, buf);
            return file_contents;
        }
    }

    if (!host.empty())
    {
        WriteBufferFromOwnString escaped_path;
        writeQuotedString(p, escaped_path);
        const String query = "SELECT file_contents FROM system.format_schemas WHERE path = " + escaped_path;
        Strings query_result = sendQueryToConnection(query);
        if (!query_result.empty())
            return query_result.front();
    }

    throw Exception("Format schema '" + path + "' not found", ErrorCodes::FILE_DOESNT_EXIST);
}


Strings FormatSchemaLoader::getAllPaths()
{
    return getPathsStartingWith("");
}

Strings FormatSchemaLoader::getPathsStartingWith(const String & prefix)
{
    std::lock_guard lock{mutex};
    Strings paths;

    if (directory)
    {
        std::cout << "FormatSchemaLoader: Searching files in directory " << directory->toString() << std::endl;
        if (endsWith(prefix, "/"))
            findAllPathsInDirectoryTree(*directory, Poco::Path(prefix), all_paths);
        else
        {
            findAllPathsInDirectoryTree(*directory, {}, paths);
            paths.erase(std::remove_if(paths.begin(), paths.end(), [&prefix](const String & path) { return startsWith(prefix); }),
                        paths.end());
        }
    }

    if (!host.empty())
    {
        String pattern = prefix + "%";
        WriteBufferFromOwnString escaped_pattern;
        writeQuotedString(pattern, escaped_pattern);
        String query = "SELECT path FROM system.format_schemas WHERE path LIKE " + escaped_pattern;
        sendQueryToConnection(query, paths);
    }

    return all_paths;
}


void FormatSchemaLoader::clearCaches()
{
    std::lock_guard lock{mutex};
    protobuf_cache.reset();
    capnproto_cache.reset();
}


Poco::Path FormatSchemaLoader::resolvePath(const String & path, const FormatTraits & format_traits)
{
    Poco::Path resolved_path(enable_environment_variables ? Poco::Path::expand(path) : path);

    if (resolved_path.getFileName().empty())
        throw Exception("Path '" + path + "' to a format schema should contain file name", ErrorCodes::BAD_ARGUMENTS);

    if (resolved_path.isAbsolute())
    {
        if (!enable_absolute_paths)
            throw Exception("Absolute path '" + path + "' to a format schema is not allowed", ErrorCodes::BAD_ARGUMENTS);
    }
    else
    {
        if (resolved_path.depth() >= 1 && resolved_path[0] == "..")
        {
            if (!directory || !enable_absolute_paths)
                throw Exception("Path '" + path + "' to a format schema should not break out to the parent directory (it shouldn't starts with '../')", ErrorCodes::BAD_ARGUMENTS);
            resolved_path = Poco::Path(*directory).resolve(resolved_path);
        }
    }

    if (resolved_path.getExtension().empty() && !format_traits.default_file_extension.empty())
        resolved_path.setExtension(format_traits.default_file_extension);

    return resolved_path;
}


FormatSchemaLoader::SchemaLocation FormatSchemaLoader::splitFormatSchemaSetting(const String & format_schema_setting, const FormatTraits & format_traits)
{
    if (format_schema_setting.empty())
        throw Exception(
            "Format '" + format_traits.format_name + "' requires the 'format_schema' setting to be set. "
            "This setting should have the following format: 'filename:message', "
            "for example 'my" + format_traits.format_name + "." + format_traits.default_file_extension + ":Message'",
            ErrorCodes::BAD_ARGUMENTS);

    size_t colon_pos = format_schema_setting.find(':');
    if ((colon_pos == String::npos) || (colon_pos == 0) || (colon_pos == format_schema_setting.length() - 1))
    {
        throw Exception(
            "Format '" + format_traits.format_name + "' requires the 'format_schema' setting in the following format: 'filename:message', "
            "for example 'my" + format_traits.format_name + "." + format_traits.default_file_extension + ":Message'. Got " + format_schema_setting,
            ErrorCodes::BAD_ARGUMENTS);
    }

    SchemaLocation schema_location;
    schema_location.path = resolvePath(format_schema_setting.substr(0, colon_pos));
    schema_location.message_type = format_schema_setting.substr(colon_pos + 1);
    return schema_location;
}


Strings FormatSchemaLoader::sendQueryToConnection(const String & query)
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
    Strings result;
    result.reserve(num_rows);
    for (size_t i = 0; i != num_rows; ++i)
        result.emplace_back(column.getDataAt(i).toString());
    return result;
}

}
