#include <Parsers/ParserShowGrantsQuery.h>
#include <Parsers/ParserRolesOrUsersSet.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Parsers/ASTShowGrantsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{
bool ParserShowGrantsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW GRANTS"}.ignore(pos, expected))
        return false;

    std::shared_ptr<ASTRolesOrUsersSet> for_whom;

    if (ParserKeyword{"FOR"}.ignore(pos, expected))
    {
        ASTPtr for_roles_ast;
        ParserRolesOrUsersSet for_roles_p;
        for_roles_p.allowUserNames().allowRoleNames().allowAll().allowCurrentUser();
        if (!for_roles_p.parse(pos, for_roles_ast, expected))
            return false;

        for_whom = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(for_roles_ast);
    }

    auto query = std::make_shared<ASTShowGrantsQuery>();
    query->for_whom = std::move(for_whom);
    node = query;

    return true;
}
}
