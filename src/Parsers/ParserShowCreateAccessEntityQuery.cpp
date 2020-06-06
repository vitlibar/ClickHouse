#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserRowPolicyName.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Parsers/parseUserName.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <ext/range.h>
#include <assert.h>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, EntityType & type)
    {
        for (auto i : ext::range(EntityType::MAX))
        {
            const auto & type_info = EntityTypeInfo::get(i);
            if (ParserKeyword{type_info.name.c_str()}.ignore(pos, expected)
                || (!type_info.alias.empty() && ParserKeyword{type_info.alias.c_str()}.ignore(pos, expected)))
            {
                type = i;
                return true;
            }
        }
        return false;
    }
}


bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    EntityType type;
    if (!parseEntityType(pos, expected, type))
        return false;

    Strings names;
    bool current_quota = false;
    bool current_user = false;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    bool all = false;
    String all_on_database, all_on_table_name;

    if (ParserKeyword{"ALL"}.ignore(pos, expected))
    {
        all = true;
        if ((type == EntityType::ROW_POLICY) && ParserKeyword("ON").ignore(pos, expected))
        {
            if (!parseDatabaseAndTableName(pos, expected, all_on_database, all_on_table_name))
                return false;
        }
    }
    else if (type == EntityType::USER)
    {
        if (parseCurrentUserTag(pos, expected) || !parseUserNames(pos, expected, names))
            current_user = true;
    }
    else if (type == EntityType::ROLE)
    {
        if (!parseRoleNames(pos, expected, names))
            return false;
    }
    else if (type == EntityType::ROW_POLICY)
    {
        ASTPtr ast;
        if (!ParserRowPolicyNames{}.parse(pos, ast, expected))
            return false;
        row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
    }
    else if (type == EntityType::QUOTA)
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
        {
            /// SHOW CREATE QUOTA
            current_quota = true;
        }
    }
    else if (type == EntityType::SETTINGS_PROFILE)
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
            return false;
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>();
    node = query;

    query->type = type;
    query->names = std::move(names);
    query->current_quota = current_quota;
    query->current_user = current_user;
    query->row_policy_names = std::move(row_policy_names);
    query->all = all;
    query->all_on_database = all_on_database;
    query->all_on_table_name = all_on_table_name;

    return true;
}
}
