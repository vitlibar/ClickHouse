#pragma once

#include <Core/UUID.h>
#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <unordered_map>


namespace DB
{
class AccessControlManager;
struct SettingsProfile;
using SettingsProfilePtr = std::shared_ptr<const SettingsProfile>;
class SettingsProfileElements;
class EnabledSettings;


/// Reads and caches all the settings profiles.
class SettingsProfilesCache
{
public:
    SettingsProfilesCache(const AccessControlManager & manager_);
    ~SettingsProfilesCache();

    SettingsProfilePtr getProfile(const String & name);

    std::shared_ptr<const EnabledSettings> getEnabledSettings(
        const UUID & user_id,
        const std::vector<UUID> & enabled_roles,
        const SettingsProfileElements & settings_from_user_and_enabled_roles_,
        const String & default_profile_name);

private:
    void ensureAllProfilesRead();
    void profileAddedOrChanged(const UUID & profile_id, const SettingsProfilePtr & new_profile);
    void profileRemoved(const UUID & profile_id);
    void setDefaultProfile(const String & default_profile_name);
    void mergeSettingsAndConstraints();
    void mergeSettingsAndConstraintsFor(EnabledSettings & enabled) const;
    void substituteProfiles(SettingsProfileElements & elements) const;

    const AccessControlManager & manager;
    std::unordered_map<UUID, SettingsProfilePtr> all_profiles;
    std::unordered_map<String, UUID> profiles_by_name;
    bool all_profiles_read = false;
    ext::scope_guard subscription;
    std::vector<std::weak_ptr<EnabledSettings>> enabled_settings;
    std::optional<UUID> default_profile_id;
    mutable std::mutex mutex;
};
}
