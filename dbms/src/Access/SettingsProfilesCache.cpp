#include <Access/SettingsProfilesCache.h>
#include <Access/AccessControlManager.h>
#include <Access/SettingsProfile.h>
#include <Access/EnabledSettings.h>
#include <Common/quoteString.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_PROFILE;
}


SettingsProfilesCache::SettingsProfilesCache(const AccessControlManager & manager_)
    : manager(manager_) {}

SettingsProfilesCache::~SettingsProfilesCache() = default;


void SettingsProfilesCache::ensureAllProfilesRead()
{
    /// `mutex` is already locked.
    if (all_profiles_read)
        return;
    all_profiles_read = true;

    subscription = manager.subscribeForChanges<SettingsProfile>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                profileAddedOrChanged(id, typeid_cast<SettingsProfilePtr>(entity));
            else
                profileRemoved(id);
        });

    for (const UUID & id : manager.findAll<SettingsProfile>())
    {
        auto profile = manager.tryRead<SettingsProfile>(id);
        if (profile)
        {
            all_profiles.emplace(id, profile);
            profiles_by_name[profile->getName()] = id;
        }
    }
}


void SettingsProfilesCache::profileAddedOrChanged(const UUID & profile_id, const SettingsProfilePtr & new_profile)
{
    std::lock_guard lock{mutex};
    auto it = all_profiles.find(profile_id);
    if (it == all_profiles.end())
    {
        all_profiles.emplace(profile_id, new_profile);
        profiles_by_name[new_profile->getName()] = profile_id;
    }
    else
    {
        auto old_profile = it->second;
        it->second = new_profile;
        if (old_profile->getName() != new_profile->getName())
            profiles_by_name.erase(old_profile->getName());
        profiles_by_name[new_profile->getName()] = profile_id;
    }
    mergeSettingsAndConstraints();
}


void SettingsProfilesCache::profileRemoved(const UUID & profile_id)
{
    std::lock_guard lock{mutex};
    auto it = all_profiles.find(profile_id);
    if (it != all_profiles.end())
    {
        profiles_by_name.erase(it->second->getName());
        all_profiles.erase(it);
    }
    mergeSettingsAndConstraints();
}


void SettingsProfilesCache::setDefaultProfile(const String & default_profile_name)
{
    if (default_profile_id || default_profile_name.empty())
        return;

    auto it = profiles_by_name.find(default_profile_name);
    if (it == profiles_by_name.end())
        throw Exception("Settings profile " + backQuote(default_profile_name) + " not found", ErrorCodes::THERE_IS_NO_PROFILE);

    default_profile_id = it->second;
}

void SettingsProfilesCache::mergeSettingsAndConstraints()
{
    /// `mutex` is already locked.
    boost::range::remove_erase_if(
        enabled_settings,
        [&](const std::weak_ptr<EnabledSettings> & elem)
        {
            auto enabled_settings = elem.lock();
            if (!enabled_settings)
                return true; // remove from the `enabled_settings` list.
            mergeSettingsAndConstraintsFor(*enabled_settings);
            return false; // keep in the `enabled_settings` list.
        });
}


void SettingsProfilesCache::mergeSettingsAndConstraintsFor(EnabledSettings & enabled) const
{
    SettingsProfileElements merged_settings;
    if (default_profile_id)
    {
        SettingsProfileElement new_element;
        new_element.parent_profile = *default_profile_id;
        merged_settings.emplace_back(new_element);
    }

    for (const auto & [profile_id, profile] : all_profiles)
        if (profile->to_roles.match(enabled.user_id, enabled.enabled_roles))
        {
            SettingsProfileElement new_element;
            new_element.parent_profile = profile_id;
            merged_settings.emplace_back(new_element);
        }

    merged_settings.merge(enabled.settings_from_user_and_enabled_roles);

    substituteProfiles(merged_settings);

    enabled.setSettingsAndConstraints(
                std::make_shared<SettingsChanges>(merged_settings.toSettings()),
                std::make_shared<SettingsConstraints>(merged_settings.toSettingsConstraints()));
}


void SettingsProfilesCache::substituteProfiles(SettingsProfileElements & elements) const
{
    bool stop_substituting = false;
    boost::container::flat_set<UUID> already_substituted;
    while (!stop_substituting)
    {
        stop_substituting = true;
        for (size_t i = 0; i != elements.size(); ++i)
        {
            auto & element = elements[i];
            if (!element.parent_profile)
                continue;

            auto parent_profile_id = *element.parent_profile;
            element.parent_profile.reset();
            if (already_substituted.contains(parent_profile_id))
                continue;

            already_substituted.insert(parent_profile_id);
            auto parent_profile = all_profiles.find(parent_profile_id);
            if (parent_profile == all_profiles.end())
                continue;

            const auto & parent_profile_elements = parent_profile->second->elements;
            elements.insert(elements.begin() + i, parent_profile_elements.begin(), parent_profile_elements.end());
            i += parent_profile_elements.size();
            stop_substituting = false;
        }
    }
}


std::shared_ptr<const SettingsProfile> SettingsProfilesCache::getProfile(const String & name)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();
    auto it = profiles_by_name.find(name);
    if (it == profiles_by_name.end())
        throw Exception("Settings profile " + backQuote(name) + " not found", ErrorCodes::THERE_IS_NO_PROFILE);
    return all_profiles[it->second];
}


std::shared_ptr<const EnabledSettings> SettingsProfilesCache::getEnabledSettings(
    const UUID & user_id,
    const std::vector<UUID> & enabled_roles,
    const SettingsProfileElements & settings_from_user_and_enabled_roles,
    const String & default_profile_name)
{
    std::lock_guard lock{mutex};
    ensureAllProfilesRead();
    setDefaultProfile(default_profile_name);
    std::shared_ptr<EnabledSettings> res(new EnabledSettings(user_id, enabled_roles, settings_from_user_and_enabled_roles));
    enabled_settings.emplace_back(res);
    mergeSettingsAndConstraintsFor(*res);
    return res;
}

}
