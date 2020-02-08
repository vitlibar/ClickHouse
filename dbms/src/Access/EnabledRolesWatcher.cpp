#include <Access/EnabledRolesWatcher.h>
#include <Access/Role.h>
#include <Access/AccessControlManager.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace
{
    using EnabledRoles = std::vector<std::pair<UUID, RolePtr>>;

    bool exists(const EnabledRoles & vec, const UUID & id)
    {
        return std::find_if(vec.begin(), vec.end(), [&id](const std::pair<UUID, RolePtr> & elem)
        {
            return elem.first == id;
        }) != vec.end();
    }
}


EnabledRolesWatcher::EnabledRolesWatcher(
    const AccessControlManager & manager_,
    const std::vector<UUID> & current_roles_,
    std::function<void(const EnabledRolesPtr &)> on_change_)
    : EnabledRolesWatcher(manager_, current_roles_.empty() ? nullptr : std::make_shared<std::vector<UUID>>(current_roles_), on_change_)
{
}


EnabledRolesWatcher::EnabledRolesWatcher(const AccessControlManager & manager_, const std::shared_ptr<const std::vector<UUID>> & current_roles_, std::function<void(const EnabledRolesPtr &)> on_change_)
    : manager(manager_), current_roles(current_roles_), on_change(on_change_)
{
    updateNoNotify();
}


EnabledRolesWatcher::~EnabledRolesWatcher() = default;


void EnabledRolesWatcher::update()
{
    std::lock_guard lock{mutex};
    auto old_enabled_roles = enabled_roles;

    updateNoNotify();

    if (!on_change)
        return;

    auto new_enabled_roles = enabled_roles;
    if ((old_enabled_roles && new_enabled_roles && (*old_enabled_roles == *new_enabled_roles))
        || (!old_enabled_roles && !new_enabled_roles))
        return;

    on_change(new_enabled_roles);
}

void EnabledRolesWatcher::updateNoNotify()
{
    if (!current_roles)
        return;

    for (auto & entry : enabled_roles_map | boost::adaptors::map_values)
        entry.in_use = false;

    auto new_enabled_roles = std::make_shared<EnabledRoles>();

    for (const auto & id : *current_roles)
    {
        RolePtr role;
        if (!exists(*new_enabled_roles, id) && (role = getRole(id)))
            new_enabled_roles->emplace_back(id, std::move(role));
    }

    size_t num_processed_enabled_roles = 0;
    while (num_processed_enabled_roles != new_enabled_roles->size())
    {
        for (size_t i = num_processed_enabled_roles; i != new_enabled_roles->size(); ++i)
        {
            const auto & role = (*new_enabled_roles)[i].second;
            for (const auto & granted_role_id : role->granted_roles)
            {
                RolePtr granted_role;
                if (!exists(*new_enabled_roles, granted_role_id) && (granted_role = getRole(granted_role_id)))
                    new_enabled_roles->emplace_back(granted_role_id, std::move(granted_role));
            }
        }
    }

    for (auto it = enabled_roles_map.begin(); it != enabled_roles_map.end();)
    {
        if (it->second.in_use)
            ++it;
        else
            it = enabled_roles_map.erase(it);
    }

    enabled_roles = std::move(new_enabled_roles);
}


RolePtr EnabledRolesWatcher::getRole(const UUID & id)
{
    auto it = enabled_roles_map.find(id);
    if (it != enabled_roles_map.end())
    {
        it->second.in_use = true;
        return it->second.role;
    }

    auto subscription = manager.subscribeForChanges(id, [this, id](const UUID &, const AccessEntityPtr & entity)
    {
        auto it2 = enabled_roles_map.find(id);
        if (it2 == enabled_roles_map.end())
            return;
        if (entity)
            it2->second.role = typeid_cast<RolePtr>(entity);
        else
            enabled_roles_map.erase(it2);
        update();
    });

    auto role = manager.tryRead<Role>(id);
    if (!role)
        return nullptr;

    MapEntry new_entry;
    new_entry.role = role;
    new_entry.subscription = std::move(subscription);
    new_entry.in_use = true;
    enabled_roles_map.emplace(id, std::move(new_entry));
    return role;
}


EnabledRolesPtr EnabledRolesWatcher::getEnabledRoles() const
{
    std::lock_guard lock{mutex};
    return enabled_roles;
}

}
