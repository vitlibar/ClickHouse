#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Common/SettingsChanges.h>
#include <Access/SettingsConstraints.h>
#include <Access/SettingsProfileElement.h>
#include <ext/scope_guard.h>
#include <list>


namespace DB
{
/// Watches settings profiles for a specific user and roles.
class EnabledSettings
{
public:
    ~EnabledSettings();

    /// Returns the default settings come from settings profiles defined for the user
    /// and the roles passed in the constructor.
    std::shared_ptr<const SettingsChanges> getSettings() const;

    /// Returns the constraints come from settings profiles defined for the user
    /// and the roles passed in the constructor.
    std::shared_ptr<const SettingsConstraints> getConstraints() const;

    using OnChangeHandler = std::function<void(const std::shared_ptr<const SettingsChanges> & settings_, const std::shared_ptr<const SettingsConstraints> & constraints_)>;

    /// Called after either the default settings or the constraints have been changed.
    ext::scope_guard subscribeForChanges(const OnChangeHandler & handler) const;

private:
    friend class SettingsProfilesCache;
    EnabledSettings(
        const UUID & user_id_,
        const std::vector<UUID> & enabled_roles_,
        const SettingsProfileElements & settings_from_user_and_enabled_roles_);

    void setSettingsAndConstraints(
        const std::shared_ptr<const SettingsChanges> & settings_, const std::shared_ptr<const SettingsConstraints> & constraints_);

    const UUID user_id;
    const std::vector<UUID> enabled_roles;
    const SettingsProfileElements settings_from_user_and_enabled_roles;
    std::shared_ptr<const SettingsChanges> settings;
    std::shared_ptr<const SettingsConstraints> constraints;
    mutable std::list<OnChangeHandler> handlers;
    mutable std::mutex mutex;
};
}
