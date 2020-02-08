#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Role;
using RolePtr = std::shared_ptr<const Role>;
using EnabledRolesPtr = std::shared_ptr<const std::vector<std::pair<UUID, RolePtr>>>;
class AccessControlManager;


class EnabledRolesWatcher
{
public:
    EnabledRolesWatcher(
        const AccessControlManager & manager_,
        const std::vector<UUID> & current_roles_,
        std::function<void(const EnabledRolesPtr &)> on_change_ = {});

    EnabledRolesWatcher(
        const AccessControlManager & manager_,
        const std::shared_ptr<const std::vector<UUID>> & current_roles_,
        std::function<void(const EnabledRolesPtr &)> on_change_ = {});

    ~EnabledRolesWatcher();

    EnabledRolesPtr getEnabledRoles() const;

private:
    void update();
    void updateNoNotify();
    RolePtr getRole(const UUID & id);

    const AccessControlManager & manager;
    const std::shared_ptr<const std::vector<UUID>> current_roles;
    const std::function<void(const EnabledRolesPtr &)> on_change;

    struct MapEntry
    {
        RolePtr role;
        ext::scope_guard subscription;
        bool in_use = true;
    };
    std::unordered_map<UUID, MapEntry> enabled_roles_map;

    EnabledRolesPtr enabled_roles;
    mutable std::mutex mutex;
};

}
