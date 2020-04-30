#include <Access/RowPolicy.h>
#include <Interpreters/Context.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{

String RowPolicy::FullNameParts::getFullName() const
{
    String full_name;
    full_name.reserve(database.length() + table_name.length() + policy_name.length() + 6);
    full_name += backQuoteIfNeed(policy_name);
    full_name += " ON ";
    if (!database.empty())
    {
        full_name += backQuoteIfNeed(database);
        full_name += '.';
    }
    full_name += backQuoteIfNeed(table_name);
    return full_name;
}


void RowPolicy::setDatabase(const String & database)
{
    full_name_parts.database = database;
    full_name = full_name_parts.getFullName();
}


void RowPolicy::setTableName(const String & table_name)
{
    full_name_parts.table_name = table_name;
    full_name = full_name_parts.getFullName();
}


void RowPolicy::setName(const String & policy_name)
{
    full_name_parts.policy_name = policy_name;
    full_name = full_name_parts.getFullName();
}


void RowPolicy::setFullName(const String & database, const String & table_name, const String & policy_name)
{
    full_name_parts.database = database;
    full_name_parts.table_name = table_name;
    full_name_parts.policy_name = policy_name;
    full_name = full_name_parts.getFullName();
}


void RowPolicy::setFullName(const FullNameParts & parts)
{
    full_name_parts = parts;
    full_name = full_name_parts.getFullName();
}


bool RowPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_policy = typeid_cast<const RowPolicy &>(other);
    return (full_name_parts == other_policy.full_name_parts) && boost::range::equal(conditions, other_policy.conditions)
        && restrictive == other_policy.restrictive && (to_roles == other_policy.to_roles);
}


const char * RowPolicy::conditionTypeToString(ConditionType index)
{
    switch (index)
    {
        case SELECT_FILTER: return "SELECT_FILTER";
#if 0 /// INSERT, UPDATE, DELETE are not supported yet
        case INSERT_CHECK: return "INSERT_CHECK";
        case UPDATE_FILTER: return "UPDATE_FILTER";
        case UPDATE_CHECK: return "UPDATE_CHECK";
        case DELETE_FILTER: return "DELETE_FILTER";
#endif
    }
    __builtin_unreachable();
}
}
