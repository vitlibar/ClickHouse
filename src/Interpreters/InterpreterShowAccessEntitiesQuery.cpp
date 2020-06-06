#include <Interpreters/InterpreterShowAccessEntitiesQuery.h>
#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/executeQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Access/AccessFlags.h>
#include <Access/AccessControlManager.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using EntityType = IAccessEntity::Type;


InterpreterShowAccessEntitiesQuery::InterpreterShowAccessEntitiesQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_), ignore_quota(query_ptr->as<ASTShowAccessEntitiesQuery &>().type == EntityType::QUOTA)
{
}


BlockIO InterpreterShowAccessEntitiesQuery::execute()
{
    auto & query = query_ptr->as<ASTShowAccessEntitiesQuery &>();
    if (query.all_access)
    {
        BlockIO res;
        res.in = getInputStreamFromAllCreateAndGrantQueries();
        return res;
    }

    return executeQuery(getRewrittenQuery(), context, true);
}


String InterpreterShowAccessEntitiesQuery::getRewrittenQuery() const
{
    const auto & query = query_ptr->as<ASTShowAccessEntitiesQuery &>();
    String origin;
    String expr = "*";
    String filter, order;

    if (!query.type)
        throw Exception("Type of ASTShowAccessEntitiesQuery not set", ErrorCodes::LOGICAL_ERROR);

    switch (*query.type)
    {
        case EntityType::ROW_POLICY:
        {
            origin = "row_policies";
            expr = "name";
            const String & table_name = query.table_name;
            if (!table_name.empty())
            {
                String database = query.database;
                if (database.empty())
                    database = context.getCurrentDatabase();
                filter = "database = " + quoteString(database) + " AND table = " + quoteString(table_name);
                expr = "short_name";
            }
            break;
        }

        case EntityType::QUOTA:
        {
            if (query.current_quota)
            {
                origin = "quota_usage";
                order = "duration";
            }
            else
            {
                origin = "quotas";
                expr = "name";
            }
            break;
        }

        case EntityType::SETTINGS_PROFILE:
        {
            origin = "settings_profiles";
            expr = "name";
            break;
        }

        case EntityType::USER:
        {
            origin = "users";
            expr = "name";
            break;
        }

        case EntityType::ROLE:
        {
            if (query.current_roles)
            {
                origin = "current_roles";
                order = "role_name";
            }
            else if (query.enabled_roles)
            {
                origin = "enabled_roles";
                order = "role_name";
            }
            else
            {
                origin = "roles";
                expr = "name";
            }
            break;
        }

        case EntityType::MAX:
            break;
    }

    if (origin.empty())
        throw Exception(toString(*query.type) + ": type is not supported by SHOW query", ErrorCodes::LOGICAL_ERROR);

    if (order.empty() && expr != "*")
        order = expr;

    return "SELECT " + expr + " from system." + origin +
            (filter.empty() ? "" : " WHERE " + filter) +
            (order.empty() ? "" : " ORDER BY " + order);
}


BlockInputStreamPtr InterpreterShowAccessEntitiesQuery::getInputStreamFromAllCreateAndGrantQueries() const
{
    /// Build a create queries.
    ASTs queries = getCreateAndGrantQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    std::stringstream ss;
    for (const auto & query : queries)
    {
        formatAST(*query, ss, false, true);
        column->insert(ss.str());
        ss.str("");
    }

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), "ACCESS"}});
}


ASTs InterpreterShowAccessEntitiesQuery::getCreateAndGrantQueries() const
{
    const auto & access_control = context.getAccessControlManager();
    context.checkAccess(AccessType::SHOW_ACCESS);

    ASTs list;

    std::vector<AccessEntityPtr> users_and_roles;
    for (auto type : ext::range(EntityType::MAX))
    {
        auto ids = access_control.findAll(type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
            {
                list.push_back(InterpreterShowCreateAccessEntityQuery::getCreateQuery(*entity, access_control));
                if (entity->isTypeOf(EntityType::USER) || entity->isTypeOf(EntityType::USER))
                    users_and_roles.push_back(entity);
            }
        }
    }

    for (const auto & entity : users_and_roles)
        boost::range::push_back(list, InterpreterShowGrantsQuery::getGrantQueries(*entity, access_control));

    return list;
}

}
