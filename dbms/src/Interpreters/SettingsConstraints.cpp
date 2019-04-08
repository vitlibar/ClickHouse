#include <Interpreters/SettingsConstraints.h>

#include <Common/SettingsChanges.h>
#include <Common/FieldVisitors.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int SETTING_CONSTRAINT_VIOLATION;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


SettingsConstraints::SettingsConstraints() = default;
SettingsConstraints::SettingsConstraints(const SettingsConstraints & src) = default;
SettingsConstraints & SettingsConstraints::operator=(const SettingsConstraints & src) = default;
SettingsConstraints::SettingsConstraints(SettingsConstraints && src) = default;
SettingsConstraints & SettingsConstraints::operator=(SettingsConstraints && src) = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::setMaxValue(const String & name, const Field & max_value)
{
    // In order to compare `max_value` correctly we need to adjust its type.
    max_values[name] = SettingChange::adjustFieldTypeForSetting(max_value, name);
}

void SettingsConstraints::check(const Settings & settings, const SettingChange & change) const
{
    Field current_value;
    if (!settings.tryGet(change.getName(), current_value))
        return;

    /// Setting isn't checked if value wasn't changed.
    if (current_value == change.getValue())
        return;

    if (!settings.allow_ddl && change.getName() == "allow_ddl")
        throw Exception("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);

    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */
    if (settings.readonly == 1)
        throw Exception("Cannot modify '" + change.getName() + "' setting in readonly mode", ErrorCodes::READONLY);

    if (settings.readonly > 1 && change.getName() == "readonly")
        throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);

    auto it = max_values.find(change.getName());
    if (it != max_values.end())
    {
        const Field & max_value = it->second;
        if (change.getValue() > max_value)
            throw Exception(
                "Setting " + change.getName() + " shouldn't be greater than " + applyVisitor(FieldVisitorToString(), max_value),
                ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
    }
}


void SettingsConstraints::check(const Settings & settings, const SettingsChanges & changes) const
{
    for (const SettingChange & change : changes)
        check(settings, change);
}


void SettingsConstraints::setProfile(const String & profile_name, const Poco::Util::AbstractConfiguration & config)
{
    String parent_profile = "profiles." + profile_name + ".profile";
    if (config.has(parent_profile))
        setProfile(parent_profile, config); // Inheritance of one profile from another.

    String path_to_constraints = "profiles." + profile_name + ".constraints";
    if (config.has(path_to_constraints))
        loadFromConfig(path_to_constraints, config);
}


void SettingsConstraints::loadFromConfig(const String & path_to_constraints, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(path_to_constraints))
        throw Exception("There is no path '" + path_to_constraints + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys names;
    config.keys(path_to_constraints, names);

    for (const String & name : names)
    {
        String path_to_name = path_to_constraints + "." + name;
        Poco::Util::AbstractConfiguration::Keys constraint_types;
        config.keys(path_to_name, constraint_types);
        for (const String & constraint_type : constraint_types)
        {
            String path_to_type = path_to_name + "." + constraint_type;
            if (constraint_type == "max")
                setMaxValue(name, config.getString(path_to_type));
            else
                throw Exception("Setting " + constraint_type + " value for " + name + " isn't supported", ErrorCodes::NOT_IMPLEMENTED);
        }
    }
}


}
