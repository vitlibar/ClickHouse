#include <Interpreters/InterpreterShowQuotaQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowQuotaQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/System/StorageSystemQuotas.h>
#include <Access/Quota.h>
#include <Common/quoteString.h>
#include <Common/StringUtils/StringUtils.h>
#include <ext/range.h>


namespace DB
{
InterpreterShowQuotaQuery::InterpreterShowQuotaQuery(const ASTPtr & query_ptr_, Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}


String InterpreterShowQuotaQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowQuotaQuery &>();

    /// Transform the query into some kind of "SELECT from system.quotas" query.
    String filter;
    if (query.current)
    {
        filter = "(id = currentQuotaID()) AND (key = currentQuotaKey())";
    }
    else if (!query.all)
    {
        if (!query.name.empty())
            filter = "name = " + quoteString(query.name);
        if (query.key)
        {
            if (!filter.empty())
                filter += " AND ";
            filter += " key = " + quoteString(*query.key);
        }
    }

    String expr = "name || if(isNull(key), '', ' KEY=\\'' || key || '\\'' || if(isNull(end_of_interval), '', ' INTERVAL=[' || toString(end_of_interval - duration) || ' .. ' || "
                  "toString(end_of_interval) || ']'";
    for (auto resource_type : ext::range_with_static_cast<Quota::ResourceType>(Quota::MAX_RESOURCE_TYPE))
    {
        String column_name = Quota::resourceTypeToColumnName(resource_type);
        expr += String{" || ' "} + Quota::getNameOfResourceType(resource_type) + "=' || toString(" + column_name + ")";
        expr += String{" || if(max_"} + column_name + "=0, '', '/' || toString(max_" + column_name + "))";
    }
    expr += "))";

    /// Prepare description of the result column.
    std::stringstream ss;
    formatAST(query, ss, false, true);
    String desc = ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    /// Build a new query.
    return "SELECT DISTINCT " + expr + " AS " + backQuote(desc) + " FROM system.quotas" + (filter.empty() ? "" : (" WHERE " + filter))
        + " ORDER BY name, key, duration";
}


BlockIO InterpreterShowQuotaQuery::execute()
{
    return executeQuery(getRewrittenQuery(), context, true);
}

}
