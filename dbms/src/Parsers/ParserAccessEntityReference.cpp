#include <Parsers/ParserAccessEntityReference.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
bool ParserAccessEntityReference::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    String name;
    if (id_mode)
    {
        if (!ParserKeyword{"ID"}.ignore(pos, expected))
            return false;
        if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
            return false;
        ASTPtr ast;
        if (!ParserStringLiteral{}.parse(pos, ast, expected))
            return false;
        name = ast->as<ASTLiteral &>().value.safeGet<String>();
        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;
    }
    else
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;
    }

    auto result = std::make_shared<ASTAccessEntityReference>();
    result->name = std::move(name);
    result->id_mode = id_mode;
    node = result;
    return true;
}

}
