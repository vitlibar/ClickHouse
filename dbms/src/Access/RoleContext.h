#pragma once

#include <Access/AccessRights.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{
struct Role;
using RolePtr = std::shared_ptr<const Role>;
class AccessControlManager;


/// Information about a role.
struct EnabledRolesInfo
{
    std::unordered_set<UUID> current_roles;
    std::unordered_set<UUID> enabled_roles;
    std::unordered_set<UUID> enabled_roles_with_admin_option;
    std::unordered_map<UUID, String> names_of_roles;
    AccessRights access;
    AccessRights access_with_grant_option;

    friend bool operator ==(const EnabledRolesInfo & lhs, const EnabledRolesInfo & rhs);
    friend bool operator !=(const EnabledRolesInfo & lhs, const EnabledRolesInfo & rhs) { return !(lhs == rhs); }
};

using EnabledRolesInfoPtr = std::shared_ptr<const EnabledRolesInfo>;


class RoleContext
{
public:
    ~RoleContext();

    /// Returns all the roles specified in the constructor.
    EnabledRolesInfoPtr getInfo() const;

    using OnChangeHandler = std::function<void(const EnabledRolesInfoPtr & info)>;

    /// Called when either the specified roles or the roles granted to the specified roles are changed.
    ext::scope_guard subscribeForChanges(const OnChangeHandler & handler) const;

private:
    friend class RoleContextFactory;
    RoleContext(const AccessControlManager & manager_, const UUID & current_role_, bool with_admin_option_);
    RoleContext(std::vector<std::shared_ptr<const RoleContext>> && children_);

    void update();
    void updateNoNotify();
    RolePtr getRole(const UUID & id);

    const AccessControlManager * manager = nullptr;
    UUID current_role = UUID(UInt128(0));
    bool with_admin_option = false;
    std::vector<std::shared_ptr<const RoleContext>> children;
    std::vector<ext::scope_guard> subscriptions_for_change_children;

    struct RoleEntry
    {
        RolePtr role;
        ext::scope_guard subscription_for_change_role;
        bool in_use = false;
    };
    mutable std::unordered_map<UUID, RoleEntry> roles_map;
    mutable EnabledRolesInfoPtr info;
    mutable std::list<OnChangeHandler> handlers;
    mutable std::mutex mutex;
};

using RoleContextPtr = std::shared_ptr<const RoleContext>;
}
