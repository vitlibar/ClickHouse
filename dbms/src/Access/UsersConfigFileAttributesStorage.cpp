#include <Access/UsersConfigFileAttributesStorage.h>
#include <Access/SettingsProfile.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
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
    return names_and_types;
}


Strings UsersConfigFileAttributesStorage::readNamesOfSettingsProfiles() const
{
    Poco::Util::AbstractConfiguration::Keys profile_names;
    config->keys("profiles", profile_names);
    return profile_names;
}


AttributesPtr UsersConfigFileAttributesStorage::readFromSource(const String & name, const Type & type) const
{
    if (type == SettingsProfile::TYPE)
        return readSettingsProfile(name);
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
}
