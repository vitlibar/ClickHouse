#include <Interpreters/Access/InterpreterCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/formatAST.h>
#include <Access/AccessControlManager.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
namespace
{
    void updateRowPolicyFromQueryImpl(
        RowPolicy & policy,
        const ASTCreateRowPolicyQuery & query,
        const RowPolicyName & override_name,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            policy.setName(override_name);
        else if (!query.new_short_name.empty())
            policy.setShortName(query.new_short_name);
        else if (query.names->names.size() == 1)
            policy.setName(query.names->names.front());

        if (query.is_restrictive)
            policy.setRestrictive(*query.is_restrictive);

        for (const auto & [condition_type, condition] : query.conditions)
            policy.setCondition(condition_type, condition ? serializeAST(*condition) : String{});

        if (override_to_roles)
            policy.to_roles = *override_to_roles;
        else if (query.roles)
            policy.to_roles = *query.roles;
    }
}


BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateRowPolicyQuery &>();
    auto & access_control = getContext()->getAccessControlManager();
    getContext()->checkAccess(query.alter ? AccessType::ALTER_ROW_POLICY : AccessType::CREATE_ROW_POLICY);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(query_ptr, getContext());
    }

    assert(query.names->cluster.empty());
    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles, access_control, getContext()->getUserID()};

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQueryImpl(*updated_policy, query, {}, roles_from_query);
            return updated_policy;
        };
        Strings names = query.names->toStrings();
        if (query.if_exists)
        {
            auto ids = access_control.find<RowPolicy>(names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<RowPolicy>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_policies;
        for (const auto & name : query.names->names)
        {
            auto new_policy = std::make_shared<RowPolicy>();
            updateRowPolicyFromQueryImpl(*new_policy, query, name, roles_from_query);
            new_policies.emplace_back(std::move(new_policy));
        }

        if (query.if_not_exists)
            access_control.tryInsert(new_policies);
        else if (query.or_replace)
            access_control.insertOrReplace(new_policies);
        else
            access_control.insert(new_policies);
    }

    return {};
}


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query)
{
    updateRowPolicyFromQueryImpl(policy, query, {}, {});
}

}
