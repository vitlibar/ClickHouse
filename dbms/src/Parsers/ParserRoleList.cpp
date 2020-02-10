#include <Parsers/ParserRoleList.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/parseUserName.h>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
ParserRoleList::ParserRoleList(bool allow_all_, bool allow_current_user_)
    : allow_all(allow_all_), allow_current_user(allow_current_user_)
{
}


bool ParserRoleList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Strings names;
    bool current_user = false;
    bool all = false;
    Strings except_names;
    bool except_current_user = false;

    bool except_mode = false;
    while (true)
    {
        if (ParserKeyword{"NONE"}.ignore(pos, expected))
        {
        }
        else if (
            allow_current_user
            && (ParserKeyword{"CURRENT_USER"}.ignore(pos, expected) || ParserKeyword{"currentUser"}.ignore(pos, expected)))
        {
            if (ParserToken{TokenType::OpeningRoundBracket}.ignore(pos, expected))
            {
                if (!ParserToken{TokenType::ClosingRoundBracket}.ignore(pos, expected))
                    return false;
            }
            if (except_mode)
                except_current_user = true;
            else
                current_user = true;
        }
        else if (allow_all && ParserKeyword{"ALL"}.ignore(pos, expected))
        {
            all = true;
            if (ParserKeyword{"EXCEPT"}.ignore(pos, expected))
            {
                except_mode = true;
                continue;
            }
        }
        else
        {
            String name;
            if (!parseUserName(pos, expected, name))
                return false;
            if (except_mode)
                except_names.push_back(name);
            else
                names.push_back(name);
        }

        if (!ParserToken{TokenType::Comma}.ignore(pos, expected))
            break;
    }

    if (all)
    {
        current_user = false;
        names.clear();
    }

    auto result = std::make_shared<ASTRoleList>();
    result->names = std::move(names);
    result->current_user = current_user;
    result->all = all;
    result->except_names = std::move(except_names);
    result->except_current_user = except_current_user;
    node = result;
    return true;
}

}
