#include <Parsers/ParserViewTargets.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/ReadHelpers.h>


namespace DB
{

bool ParserViewTargets::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserStringLiteral literal_p;
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);

    std::shared_ptr<ASTViewTargets> res;

    auto result = [&] -> ASTViewTargets &
    {
        if (!res)
            res = std::make_shared<ASTViewTargets>();
        return *res;
    };

    for (;;)
    {
        auto start = pos;
        for (auto kind : magic_enum::enum_values<TargetKind>())
        {
            if ((kind != accept_kind1) && (kind != accept_kind2) && (kind != accept_kind3))
                continue;

            auto current = pos;

            auto keyword = ASTViewTargets::kindToKeywordForInnerUUID(kind);
            if (ParserKeyword{keyword}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (literal_p.parse(pos, ast, expected))
                {
                    result().setInnerUUID(kind, parseFromString<UUID>(ast->as<ASTLiteral>()->value.safeGet<String>()));
                    break;
                }
            }
            pos = current;

            auto prefix = ASTViewTargets::kindToPrefixForInnerStorage(kind);
            if (ParserKeyword{prefix}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (storage_p.parse(pos, ast, expected))
                {
                    result().setTableEngine(kind, ast);
                    break;
                }
            }
            pos = current;

            keyword = ASTViewTargets::kindToKeywordForTableID(kind);
            if (ParserKeyword{keyword}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (table_name_p.parse(pos, ast, expected))
                {
                    result().setTableID(kind, ast->as<ASTTableIdentifier>()->getTableId());
                    break;
                }
            }
            pos = current;
        }
        if (pos == start)
            break;
    }

    if (!res || res->targets.empty())
        return false;

    node = res;
    return true;
}

}
