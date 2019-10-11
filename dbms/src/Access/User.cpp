#include <Access/User.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t USER_NAMESPACE_IDX;
}


const IAttributes::Type User::TYPE{"User", AccessControlNames::USER_NAMESPACE_IDX, nullptr};


bool User::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    const auto & other_user = *other.cast<User>();
    return (authentication == other_user.authentication) && (profile == other_user.profile) && (quota == other_user.quota)
        && (allowed_client_hosts == other_user.allowed_client_hosts) && (databases == other_user.databases)
        && (dictionaries == other_user.dictionaries) && (table_props == other_user.table_props);
}


bool User::hasAccessToDatabase(const String & database_name) const
{
    return databases.empty() || databases.count(database_name);
}


bool User::hasAccessToDictionary(const String & dictionary_name) const
{
    return dictionaries.empty() || dictionaries.count(dictionary_name);
}
}
