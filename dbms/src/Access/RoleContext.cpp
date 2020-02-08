#include <Access/RoleContext.h>
#include <Access/Role.h>
#include <Access/AccessControlManager.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{

RoleContext::RoleContext(const AccessControlManager & manager_, const UUID & current_role_, bool with_admin_option_)
    : manager(&manager_), current_role(current_role_), with_admin_option(with_admin_option_)
{
    updateNoNotify();
}


RoleContext::RoleContext(std::vector<RoleContextPtr> && children_)
    : children(std::move(children_))
{
    updateNoNotify();
}


void RoleContext::update()
{
    std::lock_guard lock{mutex};
    auto old_info = info;

    updateNoNotify();

    if (!handlers.empty() && (!old_info || (*old_info != *info)))
    {
        for (const auto & handler : handlers)
            handler(info);
    }
}


void RoleContext::updateNoNotify()
{
    if (!children.empty())
    {
        if (subscriptions_for_change_children.empty())
        {
            for (const auto & child : children)
                child->subscribeForChanges([this](const InfoPtr &) { update(); });
        }

        auto new_info = std::make_shared<Info>();
        auto & new_info_ref = *new_info;

        for (const auto & child : children)
        {
            auto child_info = child->getInfo();
            new_info_ref.access.merge(child_info->access);
            new_info_ref.access_with_grant_option.merge(child_info->access_with_grant_option);
            boost::range::copy(child_info->current_roles, std::inserter(new_info_ref.current_roles, new_info_ref.current_roles.end()));
            boost::range::copy(child_info->enabled_roles, std::inserter(new_info_ref.enabled_roles, new_info_ref.enabled_roles.end()));
            boost::range::copy(child_info->enabled_roles_with_admin_option, std::inserter(new_info_ref.enabled_roles_with_admin_option, new_info_ref.enabled_roles_with_admin_option.end()));
            boost::range::copy(child_info->names_of_roles, std::inserter(new_info_ref.names_of_roles, new_info_ref.names_of_roles.end()));
        }
        info = new_info;
        return;
    }

    for (auto it = roles_map.begin(); it != roles_map.end();)
    {
        auto & entry = it->second;
        if (!entry.in_use)
        {
            it = roles_map.erase(it);
            continue;
        }
        entry.in_use = false;
        ++it;
    }

    auto new_info = std::make_shared<Info>();
    auto & new_info_ref = *new_info;

    struct PendingRole
    {
        UUID id;
        bool is_current_role;
        bool with_admin_option;
    };

    std::vector<PendingRole> pending_roles;
    pending_roles.push_back({current_role, true, with_admin_option});

    while (!pending_roles.empty())
    {
        size_t old_num_pending_roles = pending_roles.size();
        for (const auto & pending_role : pending_roles)
        {
            if (new_info_ref.enabled_roles_with_admin_option.contains(pending_role.id))
                continue;

            RolePtr role = getRole(pending_role.id);
            if (!role)
                continue;

            new_info_ref.enabled_roles.insert(pending_role.id);
            if (pending_role.is_current_role)
                new_info_ref.current_roles.insert(pending_role.id);
            if (pending_role.with_admin_option)
                new_info_ref.enabled_roles_with_admin_option.insert(pending_role.id);

            new_info_ref.access.merge(role->access);
            new_info_ref.access_with_grant_option.merge(role->access_with_grant_option);
            new_info_ref.names_of_roles[pending_role.id] = role->getName();

            for (const auto & granted_role : role->granted_roles_with_admin_option)
                pending_roles.push_back({granted_role, false, true});

            for (const auto & granted_role : role->granted_roles)
                pending_roles.push_back({granted_role, false, false});
        }
        pending_roles.erase(pending_roles.begin(), pending_roles.begin() + old_num_pending_roles);
    }

    info = new_info;
}


RolePtr RoleContext::getRole(const UUID & id)
{
    auto it = roles_map.find(id);
    if (it != roles_map.end())
    {
        it->second.in_use = true;
        return it->second.role;
    }

    auto subscription = manager->subscribeForChanges(id, [this, id](const UUID &, const AccessEntityPtr & entity)
    {
        auto it2 = roles_map.find(id);
        if (it2 == roles_map.end())
            return;
        if (entity)
            it2->second.role = typeid_cast<RolePtr>(entity);
        else
            roles_map.erase(it2);
        update();
    });

    auto role = manager->tryRead<Role>(id);
    if (!role)
        return nullptr;

    RoleEntry new_entry;
    new_entry.role = role;
    new_entry.subscription_for_change_role = std::move(subscription);
    new_entry.in_use = true;
    roles_map.emplace(id, std::move(new_entry));
    return role;
}


RoleContext::InfoPtr RoleContext::getInfo() const
{
    std::lock_guard lock{mutex};
    return info;
}


ext::scope_guard RoleContext::subscribeForChanges(const OnChangeHandler & handler) const
{
    std::lock_guard lock{mutex};
    handlers.push_back(handler);
    auto it = std::prev(handlers.end());

    return [this, it]
    {
        std::lock_guard lock2{mutex};
        handlers.erase(it);
    };
}


bool operator ==(const RoleContext::Info & lhs, const RoleContext::Info & rhs)
{
    return (lhs.current_roles == rhs.current_roles) && (lhs.enabled_roles == rhs.enabled_roles)
        && (lhs.enabled_roles_with_admin_option == rhs.enabled_roles_with_admin_option) && (lhs.names_of_roles == rhs.names_of_roles)
        && (lhs.access == rhs.access) && (lhs.access_with_grant_option == rhs.access_with_grant_option);
}
}
