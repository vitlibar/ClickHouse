#include <Access/SettingsProfile.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t SETTINGS_PROFILE_NAMESPACE_IDX;
}


const IAttributes::Type SettingsProfile::TYPE{"Profile",
                                              AccessControlNames::SETTINGS_PROFILE_NAMESPACE_IDX,
                                              nullptr};


bool SettingsProfile::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    const auto & other_profile = *other.cast<SettingsProfile>();
    return (parent_profile == other_profile.parent_profile) && (settings == other_profile.settings)
        && (constraints == other_profile.constraints);
}
}
