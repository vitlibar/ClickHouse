#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <Poco/Path.h>
#include <Common/config.h>
#include <Core/Types.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
}
}

namespace DB
{
class Connection;
class Context;
class ProtobufSchemaParser;

/// Loads and caches format schemas.
/// This class tries to load a format schema from a couple of sources, in the following order:
/// 1) the local directory specified by calling setDirectory(),
/// 2) from the remote server specified by calling setConnectionParameters().
/// After a format schema is loaded successfully it's cached in memory.
/// This class is thread-safe.
class FormatSchemaLoader : private boost::noncopyable
{
public:
    FormatSchemaLoader(const Context & context_);
    ~FormatSchemaLoader();

    /// Sets a local directory to search schemas.
    void setDirectory(const String & directory_, bool allow_paths_break_out_ = false);

    /// Enables using absolute paths (by default they're disabled).
    void setAbsolutePathsAreAllowed(bool allowed);

    /// Sets connection parameters to load schema from a remote server.
    void setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_);

    /// Clears caches with loaded schemas.
    void clearCaches();

    /// Finds all paths we have schemas for.
    Strings getAllPaths();

    /// Loads a schema as a raw text.
    String getRawSchema(const String & path);
    bool tryGetRawSchema(const String & path, String & schema);

#if USE_PROTOBUF
    /// Loads a protobuf schema. Throws an exception if failed to load or parse.
    const google::protobuf::Descriptor * getProtobufSchema(const String & format_schema_setting);
#endif

    /// Loads a cap'n proto schema. Throws an exception if failed to load or parse.
    //const capnp::SchemaParser::ParsedSchema & getCapnpSchema(const String & format_schema_setting);

private:
    String getRawSchemaImpl(const String & path);
    bool tryGetRawSchemaImpl(const String & path, String & schema);
    bool tryGetRawSchemaFromDirectory(const Poco::Path & path, String & schema);
    bool tryGetRawSchemaFromConnection(const Poco::Path & path, String & schema);
    size_t sendQueryToConnection(const String & query, Strings & result);

    const Context & context;
    std::optional<Poco::Path> directory;
    bool allow_paths_break_out = false;
    bool allow_absolute_paths = false;
    String host;
    UInt16 port;
    String user;
    String password;
    std::unique_ptr<Connection> connection;
    std::unordered_map<String, String> schemas_by_path;
#if USE_PROTOBUF
    std::unordered_map<String, ProtobufSchemaParser> protobuf_parsers;
#endif
    //std::unique_ptr<CapnProtoSchemaParserForFormatSchemaLoader> capnproto_schema_parser;
    std::mutex mutex;
};

}
