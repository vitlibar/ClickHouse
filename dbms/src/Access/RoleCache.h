#pragma once

#include <Access/EnabledRoles.h>
#include <Poco/ExpireCache.h>
#include <mutex>


namespace DB
{
class AccessControlManager;


class RoleCache
{
public:
    RoleCache(const AccessControlManager & manager_);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(const std::vector<UUID> & current_roles, const std::vector<UUID> & current_roles_with_admin_option);

private:
    std::shared_ptr<const EnabledRoles> getEnabledRolesImpl(const UUID & role_id, bool with_admin_option);

    const AccessControlManager & manager;
    Poco::ExpireCache<std::pair<UUID /* role_id */, bool /* with_admin_option */>, std::shared_ptr<const EnabledRoles>> cache;
    std::mutex mutex;
};

}
