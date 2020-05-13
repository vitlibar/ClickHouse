#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <boost/noncopyable.hpp>
#include <Poco/Path.h>
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

/// Loads and parses format schemas.
/// If a path to a schema file is relative it's searched in the local directory (if set) and then in the directory at server (if set).
/// If a path to a schema file is absolute it's searched in the local file system (if enabled).
/// Successfully loaded schemas are cached in memory.
/// This class is thread-safe.
class FormatSchemaLoader : private boost::noncopyable
{
public:
    explicit FormatSchemaLoader(const Context & context_);
    ~FormatSchemaLoader();

    /// Sets a local directory for retrieving schema files specified by relative paths.
    void setDirectory(const String & directory_);

    /// Sets connection parameters for retrieving schema files specified by relative paths from a remote server.
    void setConnectionParameters(const String & host_, UInt16 port_, const String & user_, const String & password_);

    /// Enables using absolute paths for retrieving schema files from the local file system. By default it's disabled.
    void enableAbsolutePaths(bool enable);

    /// Enables expanding environment variables in a path (relative or absolute) to a schema file. By default it's disabled.
    void enableEnvironmentVariables(bool enable);

    /// Whether there is a schema file accessible by the specified path.
    bool fileExists(const String & path);

    /// Loads a schema file as a raw text. Throws an exception if failed to load.
    String getFileContents(const String & path);

    /// Finds paths to schema files.
    /// This function returns list of all relative paths inside the directory specified by call setDirectory(),
    /// and all relative paths retrieved from the remote server specified by call setConnectionParameters().
    Strings getAllPaths() { return getPathsStartingWith(""); }
    Strings getPathsStartingWith(const String & prefix);

    class IFormatSpecific
    {
    public:
        IFormatSpecific(FormatSchemaLoader & loader_, const String & format_, const String & file_extension_)
            : loader(loader_), format(format_), file_extension(file_extension_) {}
        virtual ~IFormatSpecific() {}

        struct SchemaLocation
        {
            Poco::Path path;
            String message_type;
        };
        SchemaLocation splitFormatSchemaSetting(const String & format_schema_setting);
        Poco::Path addMissingFileExtension(const Poco::Path & path);

        FormatSchemaLoader & loader;
        const String format;
        const String file_extension;
    };

    template <typename FormatSpecificType,
              typename std::enable_if_t<std::is_base_of_v<IFormatSpecific, FormatSpecificType>, int> = 0>
    FormatSpecificType & get()
    {
        std::type_index type_index = typeid(FormatSpecificType);
        auto it = format_specifics.find(type_index);
        if (it == format_specifics.end())
            it = format_specifics.emplace(type_index, std::make_unique<FormatSpecificType>(*this)).first;
        return static_cast<FormatSpecificType &>(*it->second);
    }

private:
    Poco::Path resolvePath(const String & path);
    Strings sendQueryToConnection(const String & query);

    const Context & context;
    std::optional<Poco::Path> directory;
    bool enable_absolute_paths = false;
    bool enable_environment_variables = false;
    String host;
    UInt16 port;
    String user;
    String password;
    std::unique_ptr<Connection> connection;
    std::unordered_map<std::type_index, std::unique_ptr<IFormatSpecific>> format_specifics;
    std::mutex mutex;
};

}
