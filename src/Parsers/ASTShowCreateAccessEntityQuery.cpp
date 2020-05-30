#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Common/quoteString.h>


namespace DB
{
using EntityTypeInfo = IAccessEntity::TypeInfo;


String ASTShowCreateAccessEntityQuery::getID(char) const
{
    return String("SHOW CREATE ") + toString(type) + " query";
}


ASTPtr ASTShowCreateAccessEntityQuery::clone() const
{
    return std::make_shared<ASTShowCreateAccessEntityQuery>(*this);
}


void ASTShowCreateAccessEntityQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW CREATE " << EntityTypeInfo::get(type).name
                  << (settings.hilite ? hilite_none : "");

    if (current_user || current_quota)
    {
    }
    else if (type == EntityType::ROW_POLICY)
    {
        settings.ostr << " ";
        row_policy_name->format(settings);
    }
    else
        settings.ostr << " " << backQuoteIfNeed(name);
}


void ASTShowCreateAccessEntityQuery::replaceEmptyDatabaseWithCurrent(const String & current_database)
{
    if (row_policy_name)
        row_policy_name->replaceEmptyDatabaseWithCurrent(current_database);
}

}
