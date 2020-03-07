#include <Access/RoleCache.h>
#include <boost/container/flat_set.hpp>


namespace DB
{

RoleCache::RoleCache(const AccessControlManager & manager_)
    : manager(manager_), cache(600000 /* 10 minutes */) {}


RoleCache::~RoleCache() = default;


std::shared_ptr<const EnabledRoles> RoleCache::getEnabledRoles(
    const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    if (roles.size() == 1 && roles_with_admin_option.empty())
        return getEnabledRolesImpl(roles[0], false);

    if (roles.size() == 1 && roles_with_admin_option == roles)
        return getEnabledRolesImpl(roles[0], true);

    std::vector<std::shared_ptr<const EnabledRoles>> children;
    children.reserve(roles.size());
    for (const auto & role : roles_with_admin_option)
        children.push_back(getEnabledRolesImpl(role, true));

    boost::container::flat_set<UUID> roles_with_admin_option_set{roles_with_admin_option.begin(), roles_with_admin_option.end()};
    for (const auto & role : roles)
    {
        if (!roles_with_admin_option_set.contains(role))
            children.push_back(getEnabledRolesImpl(role, false));
    }

    auto res = std::shared_ptr<const EnabledRoles>(new EnabledRoles(std::move(children)));
    return res;
}


std::shared_ptr<const EnabledRoles> RoleCache::getEnabledRolesImpl(const UUID & id, bool with_admin_option)
{
    std::lock_guard lock{mutex};
    auto key = std::make_pair(id, with_admin_option);
    auto x = cache.get(key);
    if (x)
        return *x;
    auto res = std::shared_ptr<const EnabledRoles>(new EnabledRoles(manager, id, with_admin_option));
    cache.add(key, res);
    return res;
}

}
