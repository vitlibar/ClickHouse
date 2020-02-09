#pragma once

#include <Access/EnabledRole.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <unordered_map>
#include <vector>


namespace DB
{
class AccessControlManager;
struct User;
using UserPtr = std::shared_ptr<const User>;


class EnabledRolesWatcher
{
public:
    EnabledRolesWatcher(
        const AccessControlManager & manager_,
        const UserPtr & user_,
        const std::vector<UUID> & current_roles_,
        std::function<void(const EnabledRolesPtr &)> on_change_ = {});

    EnabledRolesWatcher(
        const AccessControlManager & manager_,
        const UserPtr & user_,
        const std::shared_ptr<const std::vector<UUID>> & current_roles_,
        std::function<void(const EnabledRolesPtr &)> on_change_ = {});

    ~EnabledRolesWatcher();

    EnabledRolesPtr getEnabledRoles() const;

private:
    void update();
    void updateNoNotify();

    static constexpr size_t npos = static_cast<size_t>(-1);

    struct Entry
    {
        RolePtr role;
        ext::scope_guard subscription;
        size_t index = npos;
    };
    Entry * getEntry(const UUID & id);

    const AccessControlManager & manager;
    const UserPtr user;
    const std::shared_ptr<const std::vector<UUID>> current_roles;
    const std::function<void(const EnabledRolesPtr &)> on_change;

    std::unordered_map<UUID, Entry> enabled_roles_map;

    EnabledRolesPtr enabled_roles;
    mutable std::mutex mutex;
};

}
