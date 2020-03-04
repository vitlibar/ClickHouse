#include <Access/SettingsProfileElement.h>
#include <Access/SettingsConstraints.h>
#include <Core/Settings.h>


namespace DB
{
bool operator==(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs)
{
    return (lhs.parent_profile == rhs.parent_profile) && (lhs.name == rhs.name) && (lhs.value == rhs.value)
        && (lhs.min_value == rhs.min_value) && (lhs.max_value == rhs.max_value) && (lhs.readonly == rhs.readonly);
}


void SettingsProfileElements::merge(const SettingsProfileElements & other)
{
    insert(end(), other.begin(), other.end());
}


Settings SettingsProfileElements::toSettings() const
{
    Settings res;
    for (const auto & elem : *this)
    {
        if (!elem.name.empty() && !elem.value.isNull())
            res.set(elem.name, elem.value);
    }
    return res;
}

SettingsChanges SettingsProfileElements::toSettingsChanges() const
{
    SettingsChanges res;
    for (const auto & elem : *this)
    {
        if (!elem.name.empty() && !elem.value.isNull())
            res.emplace_back(elem.name, elem.value);
    }
    return res;
}

SettingsConstraints SettingsProfileElements::toSettingsConstraints() const
{
    SettingsConstraints res;
    for (const auto & elem : *this)
    {
        if (!elem.name.empty())
        {
            if (!elem.min_value.isNull())
                res.setMinValue(elem.name, elem.min_value);
            if (!elem.max_value.isNull())
                res.setMaxValue(elem.name, elem.max_value);
            if (elem.readonly)
                res.setReadOnly(elem.name, *elem.readonly);
        }
    }
    return res;
}

}
