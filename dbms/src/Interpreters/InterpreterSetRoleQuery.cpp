#include <Interpreters/InterpreterSetRoleQuery.h>
#include <Parsers/ASTSetRoleQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>


namespace DB
{
BlockIO InterpreterSetRoleQuery::execute()
{
    const auto & query = query_ptr->as<const ASTSetRoleQuery &>();
    if (query.kind == ASTSetRoleQuery::Kind::SET_DEFAULT_ROLE)
        setDefaultRole(query);
    else
        setRole(query);
    return {};
}


void InterpreterSetRoleQuery::setRole(const ASTSetRoleQuery & query)
{
    auto & access_control = context.getAccessControlManager();
    auto & session_context = context.getSessionContext();
    auto user = session_context.getUser();

    std::vector<UUID> role_ids;
    if (query.kind == ASTSetRoleQuery::Kind::SET_ROLE_DEFAULT)
        role_ids = user->default_roles;
    else
        role_ids = access_control.getGrantedRolesFromList(user, query.roles);

    session_context.setCurrentRoles(role_ids);
}

void InterpreterSetRoleQuery::setDefaultRole(const ASTSetRoleQuery & query)
{
    auto & access_control = context.getAccessControlManager();
    std::vector<UUID> to_users;
    if (query.to_users->all_roles)
    {
        for (const auto & id : access_control.findAll<User>())
        {
            if (!query.to_users->except_roles.empty())
            {

            }
        }
    }
}

    auto & access_control = context.getAccessControlManager();
    context.checkAccess(query.alter ? AccessType::ALTER_USER : AccessType::CREATE_USER);

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_user = typeid_cast<std::shared_ptr<User>>(entity->clone());
            updateUserFromQuery(*updated_user, query);
            return updated_user;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<User>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<User>(query.name), update_func);
    }
    else
    {
        auto new_user = std::make_shared<User>();
        updateUserFromQuery(*new_user, query);

        if (query.if_not_exists)
            access_control.tryInsert(new_user);
        else if (query.or_replace)
            access_control.insertOrReplace(new_user);
        else
            access_control.insert(new_user);
    }

    return {};
}


void InterpreterCreateUserQuery::updateUserFromQuery(User & user, const ASTCreateUserQuery & query)
{
    if (query.alter)
    {
        if (!query.new_name.empty())
            user.setName(query.new_name);
    }
    else
        user.setName(query.name);

    if (query.authentication)
        user.authentication = *query.authentication;

    if (query.hosts)
        user.allowed_client_hosts = *query.hosts;
    if (query.remove_hosts)
        user.allowed_client_hosts.remove(*query.remove_hosts);
    if (query.add_hosts)
        user.allowed_client_hosts.add(*query.add_hosts);

    if (query.profile)
        user.profile = *query.profile;
}
}
