#pragma once

#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Role;
using RolePtr = std::shared_ptr<const Role>;
struct EnabledRolesInfo;
class AccessControlManager;


class EnabledRoles
{
public:
    ~EnabledRoles();

    /// Returns all the roles specified in the constructor.
    std::shared_ptr<const EnabledRolesInfo> getInfo() const;

    using OnChangeHandler = std::function<void(const std::shared_ptr<const EnabledRolesInfo> & info)>;

    /// Called when either the specified roles or the roles granted to the specified roles are changed.
    ext::scope_guard subscribeForChanges(const OnChangeHandler & handler) const;

private:
    friend class RoleCache;
    EnabledRoles(const AccessControlManager & manager_, const UUID & current_role_, bool with_admin_option_);
    EnabledRoles(std::vector<std::shared_ptr<const EnabledRoles>> && children_);

    void update();
    void updateImpl();

    void traverseRoles(const UUID & id_, bool with_admin_option_);

    const AccessControlManager * manager = nullptr;
    std::optional<UUID> current_role;
    bool with_admin_option = false;
    std::vector<std::shared_ptr<const EnabledRoles>> children;
    std::vector<ext::scope_guard> subscriptions_for_change_children;

    struct RoleEntry
    {
        RolePtr role;
        ext::scope_guard subscription_for_change_role;
        bool with_admin_option = false;
        bool in_use = false;
    };
    mutable std::unordered_map<UUID, RoleEntry> roles_map;
    mutable std::shared_ptr<const EnabledRolesInfo> info;
    mutable std::list<OnChangeHandler> handlers;
    mutable std::mutex mutex;
};

}
