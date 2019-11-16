#include <Parsers/ParserShowQuotaQuery.h>
#include <Parsers/ASTShowQuotaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
namespace
{
    bool parseWithKey(IParserBase::Pos & pos, Expected & expected, std::optional<String> & key)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"WITH KEY"}.ignore(pos, expected))
                return false;
            if (!ParserToken{TokenType::Equals}.ignore(pos, expected))
                return false;
            ASTPtr key_ast;
            if (!ParserStringLiteral{}.parse(pos, key_ast, expected))
                return false;
            key = key_ast->as<ASTLiteral &>().value.safeGet<String>();
            return true;
        });
    }
}


bool ParserShowQuotaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool plural = false;
    if (ParserKeyword{"SHOW QUOTA"}.ignore(pos, expected))
    {
    }
    else if (ParserKeyword{"SHOW QUOTAS"}.ignore(pos, expected))
    {
        plural = true;
    }
    else
        return false;

    String name;
    std::optional<String> key;
    bool current = false;
    bool all = false;

    if (ParserKeyword{"CURRENT"}.ignore(pos, expected))
    {
        /// SHOW QUOTA CURRENT
        current = true;
    }
    else if (ParserKeyword{"ALL"}.ignore(pos, expected))
    {
        /// SHOW QUOTA ALL
        all = true;
    }
    else if (parseWithKey(pos, expected, key))
    {
        /// SHOW QUOTA WITH KEY = key
    }
    else if (parseIdentifierOrStringLiteral(pos, expected, name))
    {
        /// SHOW QUOTA name [WITH KEY = key]
        parseWithKey(pos, expected, key);
    }
    else
    {
        if (plural)
            all = true; /// SHOW QUOTAS
        else
            current = true; /// SHOW QUOTA
    }

    auto query = std::make_shared<ASTShowQuotaQuery>();
    node = query;

    query->name = std::move(name);
    query->key = std::move(key);
    query->current = current;
    query->all = all;

    return true;
}
}
