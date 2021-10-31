#include <Access/RowPolicy.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/equal.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


void RowPolicy::setDatabase(const String & database)
{
    row_policy_name.database = database;
    IAccessEntity::setName(row_policy_name.toString());
}

void RowPolicy::setTableName(const String & table_name)
{
    row_policy_name.table_name = table_name;
    IAccessEntity::setName(row_policy_name.toString());
}

void RowPolicy::setShortName(const String & short_name)
{
    row_policy_name.short_name = short_name;
    IAccessEntity::setName(row_policy_name.toString());
}

void RowPolicy::setName(const String & short_name, const String & database, const String & table_name)
{
    row_policy_name.short_name = short_name;
    row_policy_name.database = database;
    row_policy_name.table_name = table_name;
    IAccessEntity::setName(row_policy_name.toString());
}

void RowPolicy::setName(const RowPolicyName & name_)
{
    row_policy_name = name_;
    IAccessEntity::setName(row_policy_name.toString());
}

void RowPolicy::setName(const String &)
{
    throw Exception("RowPolicy::setName() is not implemented", ErrorCodes::NOT_IMPLEMENTED);
}


bool RowPolicy::equal(const IAccessEntity & other) const
{
    if (!IAccessEntity::equal(other))
        return false;
    const auto & other_policy = typeid_cast<const RowPolicy &>(other);
    return (row_policy_name == other_policy.row_policy_name) && boost::range::equal(conditions, other_policy.conditions)
        && restrictive == other_policy.restrictive && (to_roles == other_policy.to_roles);
}

}
