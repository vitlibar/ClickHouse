#include <Access/UsersConfigFileAttributesStorage.h>
#include <Access/SettingsProfile.h>
#include <Access/User.h>
#include <Common/StringUtils/StringUtils.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
}


UsersConfigFileAttributesStorage::UsersConfigFileAttributesStorage()
    : SourceSyncAttributesStorage("UsersConfigFile") {}
UsersConfigFileAttributesStorage::~UsersConfigFileAttributesStorage() {}


void UsersConfigFileAttributesStorage::loadFromConfig(const Poco::AutoPtr<Poco::Util::AbstractConfiguration> & config_)
{
    config = config_;
    updateFromSource();
}


UsersConfigFileAttributesStorage::NamesAndTypes UsersConfigFileAttributesStorage::readNamesAndTypesFromSource() const
{
    NamesAndTypes names_and_types;
    for (String & name : readNamesOfSettingsProfiles())
        names_and_types.push_back({std::move(name), &SettingsProfile::TYPE});
    for (String & name : readNamesOfUsers())
        names_and_types.push_back({std::move(name), &User::TYPE});
    return names_and_types;
}


Strings UsersConfigFileAttributesStorage::readNamesOfSettingsProfiles() const
{
    Poco::Util::AbstractConfiguration::Keys profile_names;
    config->keys("profiles", profile_names);
    return profile_names;
}


Strings UsersConfigFileAttributesStorage::readNamesOfUsers() const
{
    Poco::Util::AbstractConfiguration::Keys user_names;
    config->keys("users", user_names);
    return user_names;
}


AttributesPtr UsersConfigFileAttributesStorage::readFromSource(const String & name, const Type & type) const
{
    if (type == SettingsProfile::TYPE)
        return readSettingsProfile(name);
    if (type == User::TYPE)
        return readUser(name);
    throw Exception("Unexpected attributes type in UsersConfig: " + String(type.name), ErrorCodes::LOGICAL_ERROR);
}


AttributesPtr UsersConfigFileAttributesStorage::readSettingsProfile(const String & name) const
{
    auto profile = std::make_shared<SettingsProfile>();
    profile->name = name;

    String profile_config = "profiles." + name;
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys(profile_config, keys);
    for (const String & key : keys)
    {
        if (key == "profile")
        {
            profile->parent_profile = config->getString(profile_config + "." + key);
        }
        else if (key == "constraints")
        {
            profile->constraints.loadFromConfig(profile_config + ".constraints", *config);
        }
        else
        {
            const auto & setting_name = key;
            Field value = Settings::castValueWithoutApplying(key, config->getString(profile_config + "." + setting_name));
            profile->settings.push_back({setting_name, value});
        }
    }
    return profile;
}


AttributesPtr UsersConfigFileAttributesStorage::readUser(const String & name) const
{
    auto user = std::make_shared<User>();
    user->name = name;

    String users_config = "users." + name;
    bool has_password = config->has(users_config + ".password");
    bool has_password_sha256_hex = config->has(users_config + ".password_sha256_hex");
    bool has_password_double_sha1_hex = config->has(users_config + ".password_double_sha1_hex");

    if (has_password + has_password_sha256_hex + has_password_double_sha1_hex > 1)
        throw Exception("More than one field of 'password', 'password_sha256_hex', 'password_double_sha1_hex' is used to specify password for user " + name + ". Must be only one of them.",
            ErrorCodes::BAD_ARGUMENTS);

    if (!has_password && !has_password_sha256_hex && !has_password_double_sha1_hex)
        throw Exception("Either 'password' or 'password_sha256_hex' or 'password_double_sha1_hex' must be specified for user " + name + ".", ErrorCodes::BAD_ARGUMENTS);

    if (has_password)
    {
        user->authentication = Authentication{Authentication::PLAINTEXT_PASSWORD};
        user->authentication.setPassword(config->getString(users_config + ".password"));
    }
    else if (has_password_sha256_hex)
    {
        user->authentication = Authentication{Authentication::SHA256_PASSWORD};
        user->authentication.setPasswordHashHex(config->getString(users_config + ".password_sha256_hex"));
    }
    else if (has_password_double_sha1_hex)
    {
        user->authentication = Authentication{Authentication::DOUBLE_SHA1_PASSWORD};
        user->authentication.setPasswordHashHex(config->getString(users_config + ".password_double_sha1_hex"));
    }

    user->profile = config->getString(users_config + ".profile");
    user->quota = config->getString(users_config + ".quota");

    /// Fill list of allowed hosts.
    const auto config_networks = users_config + ".networks";
    if (config->has(config_networks))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config->keys(config_networks, config_keys);
        for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
        {
            String value = config->getString(config_networks + "." + *it);
            if (startsWith(*it, "ip"))
                user->allowed_client_hosts.addSubnet(value);
            else if (startsWith(*it, "host_regexp"))
                user->allowed_client_hosts.addHostRegexp(value);
            else if (startsWith(*it, "host"))
                user->allowed_client_hosts.addHostName(value);
            else
                throw Exception("Unknown address pattern type: " + *it, ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE);
        }
    }

    /// Fill list of allowed databases.
    const auto config_sub_elem = users_config + ".allow_databases";
    if (config->has(config_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config->keys(config_sub_elem, config_keys);

        user->databases.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto database_name = config->getString(config_sub_elem + "." + key);
            user->databases.insert(database_name);
        }
    }

    /// Fill list of allowed dictionaries.
    const auto config_dictionary_sub_elem = users_config + ".allow_dictionaries";
    if (config->has(config_dictionary_sub_elem))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config->keys(config_dictionary_sub_elem, config_keys);

        user->dictionaries.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto dictionary_name = config->getString(config_dictionary_sub_elem + "." + key);
            user->dictionaries.insert(dictionary_name);
        }
    }

    /// Read properties per "database.table"
    /// Only tables are expected to have properties, so that all the keys inside "database" are table names.
    const auto config_databases = users_config + ".databases";
    if (config->has(config_databases))
    {
        Poco::Util::AbstractConfiguration::Keys database_names;
        config->keys(config_databases, database_names);

        /// Read tables within databases
        for (const auto & database : database_names)
        {
            const auto config_database = config_databases + "." + database;
            Poco::Util::AbstractConfiguration::Keys table_names;
            config->keys(config_database, table_names);

            /// Read table properties
            for (const auto & table : table_names)
            {
                const auto config_filter = config_database + "." + table + ".filter";
                if (config->has(config_filter))
                {
                    const auto filter_query = config->getString(config_filter);
                    user->table_props[database][table]["filter"] = filter_query;
                }
            }
        }
    }
    return user;
}
}
