#include <Access/SettingsConstraints.h>
#include <Core/Settings.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int READONLY;
    extern const int QUERY_IS_PROHIBITED;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

SettingsConstraints::SettingsConstraints() = default;
SettingsConstraints::SettingsConstraints(const SettingsConstraints & src) = default;
SettingsConstraints & SettingsConstraints::operator=(const SettingsConstraints & src) = default;
SettingsConstraints::SettingsConstraints(SettingsConstraints && src) = default;
SettingsConstraints & SettingsConstraints::operator=(SettingsConstraints && src) = default;
SettingsConstraints::~SettingsConstraints() = default;


void SettingsConstraints::clear()
{
    constraints.clear();
}


void SettingsConstraints::setMinValue(const std::string_view & setting_name, const Field & min_value)
{
    getConstraintRef(setting_name).min_value = Settings::valueToCorrespondingType(setting_name, min_value);
}

Field SettingsConstraints::getMinValue(const std::string_view & setting_name) const
{
    const auto * ptr = tryGetConstraint(setting_name);
    if (ptr)
        return ptr->min_value;
    else
        return {};
}


void SettingsConstraints::setMaxValue(const std::string_view & setting_name, const Field & max_value)
{
    getConstraintRef(setting_name).max_value = Settings::valueToCorrespondingType(setting_name, max_value);
}

Field SettingsConstraints::getMaxValue(const std::string_view & setting_name) const
{
    const auto * ptr = tryGetConstraint(setting_name);
    if (ptr)
        return ptr->max_value;
    else
        return {};
}


void SettingsConstraints::setReadOnly(const std::string_view & setting_name, bool read_only)
{
    getConstraintRef(setting_name).read_only = read_only;
}

bool SettingsConstraints::isReadOnly(const std::string_view & setting_name) const
{
    const auto * ptr = tryGetConstraint(setting_name);
    if (ptr)
        return ptr->read_only;
    else
        return false;
}


void SettingsConstraints::set(const std::string_view & setting_name, const Field & min_value, const Field & max_value, bool read_only)
{
    auto & ref = getConstraintRef(setting_name);
    ref.min_value = Settings::valueToCorrespondingType(setting_name, min_value);
    ref.max_value = Settings::valueToCorrespondingType(setting_name, max_value);
    ref.read_only = read_only;
}

void SettingsConstraints::get(const std::string_view & setting_name, Field & min_value, Field & max_value, bool & read_only) const
{
    const auto * ptr = tryGetConstraint(setting_name);
    if (ptr)
    {
        min_value = ptr->min_value;
        max_value = ptr->max_value;
        read_only = ptr->read_only;
    }
    else
    {
        min_value = Field{};
        max_value = Field{};
        read_only = false;
    }
}

void SettingsConstraints::merge(const SettingsConstraints & other)
{
    for (const auto & [other_name, other_constraint] : other.constraints)
    {
        auto & constraint = getConstraintRef(other_name);
        if (!other_constraint.min_value.isNull())
            constraint.min_value = other_constraint.min_value;
        if (!other_constraint.max_value.isNull())
            constraint.max_value = other_constraint.max_value;
        if (other_constraint.read_only)
            constraint.read_only = true;
    }
}


void SettingsConstraints::check(const Settings & current_settings, const SettingChange & change) const
{
    checkImpl(current_settings, const_cast<SettingChange &>(change), THROW_ON_VIOLATION);
}

void SettingsConstraints::check(const Settings & current_settings, const SettingsChanges & changes) const
{
    for (const auto & change : changes)
        check(current_settings, change);
}

void SettingsConstraints::clamp(const Settings & current_settings, SettingChange & change) const
{
    checkImpl(current_settings, change, CLAMP_ON_VIOLATION);
}

void SettingsConstraints::clamp(const Settings & current_settings, SettingsChanges & changes) const
{
    for (auto & change : changes)
        clamp(current_settings, change);
}


void SettingsConstraints::checkImpl(const Settings & current_settings, SettingChange & change, ReactionOnViolation reaction) const
{
    const String & setting_name = change.name;
    Field current_value;
    if (!current_settings.tryGet(setting_name, current_value))
        return;

    /// Setting isn't checked if value has not been changed.
    if (change.value == current_value)
        return;

    Field new_value = Settings::valueToCorrespondingType(setting_name, new_value);
    if (applyVisitor(FieldVisitorAccurateEquals{}, new_value, current_value))
        return;

    if (!current_settings.allow_ddl && setting_name == "allow_ddl")
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception("Cannot modify 'allow_ddl' setting when DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
        else
        {
            change.value = current_value;
            return;
        }
    }

    /** The `readonly` value is understood as follows:
      * 0 - everything allowed.
      * 1 - only read queries can be made; you can not change the settings.
      * 2 - You can only do read queries and you can change the settings, except for the `readonly` setting.
      */
    if (current_settings.readonly == 1)
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception("Cannot modify '" + setting_name + "' setting in readonly mode", ErrorCodes::READONLY);
        else
        {
            change.value = current_value;
            return;
        }
    }

    if (current_settings.readonly > 1 && setting_name == "readonly")
    {
        if (reaction == THROW_ON_VIOLATION)
            throw Exception("Cannot modify 'readonly' setting in readonly mode", ErrorCodes::READONLY);
        else
        {
            change.value = current_value;
            return;
        }
    }

    const Constraint * constraint = tryGetConstraint(setting_name);
    if (constraint)
    {
        if (constraint->read_only)
        {
            if (reaction == THROW_ON_VIOLATION)
                throw Exception("Setting " + setting_name + " should not be changed", ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
            else
            {
                change.value = current_value;
                return;
            }
        }

        Field min_value = constraint->min_value;
        Field max_value = constraint->max_value;
        if (!min_value.isNull())
            min_value = Settings::valueToCorrespondingType(setting_name, min_value);
        if (!max_value.isNull())
            max_value = Settings::valueToCorrespondingType(setting_name, max_value);

        if (!min_value.isNull() && !max_value.isNull() &&
            applyVisitor(FieldVisitorAccurateLess{}, max_value, min_value))
        {
            if (reaction == THROW_ON_VIOLATION)
                throw Exception("Setting " + setting_name + " should not be changed", ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
            else
            {
                change.value = current_value;
                return;
            }
        }

        if (!min_value.isNull() &&
                applyVisitor(FieldVisitorAccurateLess{}, new_value, min_value))
        {
            if (reaction == THROW_ON_VIOLATION)
            {
                throw Exception(
                    "Setting " + setting_name + " shouldn't be less than " + applyVisitor(FieldVisitorToString(), constraint->min_value),
                    ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
            }
            else
            {
                change.value = min_value;
                return;
            }
        }

        if (!max_value.isNull() &&
                applyVisitor(FieldVisitorAccurateLess{}, max_value, new_value))
        {
            if (reaction == THROW_ON_VIOLATION)
            {
                throw Exception(
                    "Setting " + setting_name + " shouldn't be greater than " + applyVisitor(FieldVisitorToString(), constraint->max_value),
                    ErrorCodes::SETTING_CONSTRAINT_VIOLATION);
            }
            else
            {
                change.value = max_value;
                return;
            }
        }
    }
}


SettingsConstraints::Constraint & SettingsConstraints::getConstraintRef(const std::string_view & setting_name)
{
    auto it = constraints.find(setting_name);
    if (it == constraints.end())
    {
        auto setting_name_ptr = std::make_shared<const String>(setting_name);
        Constraint new_constraint;
        new_constraint.setting_name = setting_name_ptr;
        it = constraints.emplace(*setting_name_ptr, std::move(new_constraint)).first;
    }
    return it->second;
}

const SettingsConstraints::Constraint * SettingsConstraints::tryGetConstraint(const std::string_view & setting_name) const
{
    auto it = constraints.find(setting_name);
    if (it == constraints.end())
        return nullptr;
    return &it->second;
}


bool SettingsConstraints::Constraint::operator==(const Constraint & other) const
{
    return (read_only == other.read_only) && (min_value == other.min_value) && (max_value == other.max_value)
        && (*setting_name == *other.setting_name);
}


bool operator ==(const SettingsConstraints & left, const SettingsConstraints & right)
{
    return left.constraints == right.constraints;
}
}
