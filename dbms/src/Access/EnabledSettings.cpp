#include <Access/EnabledSettings.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
EnabledSettings::EnabledSettings(
    const UUID & user_id_,
    const std::vector<UUID> & enabled_roles_,
    const SettingsProfileElements & settings_from_user_and_enabled_roles_)
    : user_id(user_id_)
    , enabled_roles(enabled_roles_)
    , settings_from_user_and_enabled_roles(settings_from_user_and_enabled_roles_)
{
}

EnabledSettings::~EnabledSettings() = default;


std::shared_ptr<const SettingsChanges> EnabledSettings::getSettings() const
{
    std::lock_guard lock{mutex};
    return settings;
}


std::shared_ptr<const SettingsConstraints> EnabledSettings::getConstraints() const
{
    std::lock_guard lock{mutex};
    return constraints;
}


void EnabledSettings::setSettingsAndConstraints(
    const std::shared_ptr<const SettingsChanges> & settings_, const std::shared_ptr<const SettingsConstraints> & constraints_)
{
    std::vector<OnChangeHandler> handlers_to_notify;
    {
        std::lock_guard lock{mutex};
        bool same_settings = (settings == settings_) || (settings && settings_ && (*settings == *settings_));
        bool same_constraints = (constraints == constraints_) || (constraints && constraints_ && (*constraints == *constraints_));
        if (same_settings && same_constraints)
            return;

        boost::range::copy(handlers, std::back_inserter(handlers_to_notify));
    }

    for (const auto & handler : handlers_to_notify)
        handler(settings_, constraints_);
}


ext::scope_guard EnabledSettings::subscribeForChanges(const OnChangeHandler & handler) const
{
    std::lock_guard lock{mutex};
    handlers.push_back(handler);
    auto it = std::prev(handlers.end());

    return [this, it]
    {
        std::lock_guard lock2{mutex};
        handlers.erase(it);
    };
}

}
