#include <Access/EnabledRolesWatcher.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/AccessControlManager.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace
{
    bool exists(const std::vector<UUID> & vec, const UUID & id) { return std::find(vec.begin(), vec.end(), id) != vec.end(); }
}


EnabledRolesWatcher::EnabledRolesWatcher(
    const AccessControlManager & manager_,
    const UserPtr & user_,
    const std::vector<UUID> & current_roles_,
    std::function<void(const EnabledRolesPtr &)> on_change_)
    : EnabledRolesWatcher(manager_, user_, current_roles_.empty() ? nullptr : std::make_shared<std::vector<UUID>>(current_roles_), on_change_)
{
}


EnabledRolesWatcher::EnabledRolesWatcher(const AccessControlManager & manager_, const UserPtr & user_, const std::shared_ptr<const std::vector<UUID>> & current_roles_, std::function<void(const EnabledRolesPtr &)> on_change_)
    : manager(manager_), user(user_), current_roles(current_roles_), on_change(on_change_)
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
        entry.index = npos;

    auto new_enabled_roles = std::make_shared<EnabledRoles>();
    auto & new_enabled_roles_ref = *new_enabled_roles;

    for (const auto & id : *current_roles)
    {
        Entry * entry = getEntry(id);
        if (!entry)
            continue;

        if (entry->index == npos)
        {
            entry->index = new_enabled_roles_ref.size();
            new_enabled_roles_ref.emplace_back(entry->role, id, false);
        }

        if (!new_enabled_roles_ref[entry->index].admin_option && exists(user->granted_roles_with_admin_option, id))
            new_enabled_roles_ref[entry->index].admin_option = true;
    }

    size_t num_processed_enabled_roles = 0;
    while (num_processed_enabled_roles != new_enabled_roles->size())
    {
        for (size_t i = num_processed_enabled_roles; i != new_enabled_roles_ref.size(); ++i)
        {
            const auto & role = new_enabled_roles_ref[i].role;
            for (const auto & granted_role_id : role->granted_roles)
            {
                Entry * entry = getEntry(granted_role_id);
                if (!entry)
                    continue;

                if (entry->index == npos)
                {
                    entry->index = new_enabled_roles_ref.size();
                    new_enabled_roles_ref.emplace_back(entry->role, granted_role_id, false);
                }
            }
            for (const auto & granted_role_id : role->granted_roles_with_admin_option)
            {
                Entry * entry = getEntry(granted_role_id);
                if (!entry || (entry->index == npos))
                    continue;

                new_enabled_roles_ref[entry->index].admin_option = true;
            }
        }
    }

    for (auto it = enabled_roles_map.begin(); it != enabled_roles_map.end();)
    {
        if (it->second.index == npos)
            it = enabled_roles_map.erase(it);
        else
            ++it;
    }

    enabled_roles = std::move(new_enabled_roles);
}


EnabledRolesWatcher::Entry * EnabledRolesWatcher::getEntry(const UUID & id)
{
    auto it = enabled_roles_map.find(id);
    if (it != enabled_roles_map.end())
        return &it->second;

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

    Entry new_entry;
    new_entry.role = role;
    new_entry.subscription = std::move(subscription);
    it = enabled_roles_map.emplace(id, std::move(new_entry)).first;
    return &it->second;
}


EnabledRolesPtr EnabledRolesWatcher::getEnabledRoles() const
{
    std::lock_guard lock{mutex};
    return enabled_roles;
}

}
