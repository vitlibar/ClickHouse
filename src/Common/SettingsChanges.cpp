#include <Common/SettingsChanges.h>


namespace DB
{
namespace
{
    SettingChange * find(SettingsChanges & changes, const std::string_view & name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }

    const SettingChange * find(const SettingsChanges & changes, const std::string_view & name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }
}

bool SettingsChanges::tryGet(const std::string_view & name, Field & result) const
{
    auto * change = find(*this, name);
    if (!change)
        return false;
    result = change->value;
    return true;
}

const Field * SettingsChanges::tryGet(const std::string_view & name) const
{
    auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

Field * SettingsChanges::tryGet(const std::string_view & name)
{
    auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

}
