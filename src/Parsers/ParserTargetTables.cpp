#include <Parsers/ParserTargetTables.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTargetTables.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <IO/ReadHelpers.h>


namespace DB
{

bool ParserTargetTables::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserStringLiteral literal_p;
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);

    std::shared_ptr<ASTTargetTables> res;

    auto result = [&] -> ASTTargetTables &
    {
        if (!res)
            res = std::make_shared<ASTTargetTables>();
        return *res;
    };

    auto min_kind = accept_kinds_min.value_or(magic_enum::enum_value<TargetTableKind>(0));
    auto max_kind = accept_kinds_max.value_or(magic_enum::enum_value<TargetTableKind>(magic_enum::enum_count<TargetTableKind>() - 1));

    for (;;)
    {
        auto start = pos;
        for (auto kind = min_kind; kind <= max_kind; kind = static_cast<TargetTableKind>(static_cast<int>(kind) + 1))
        {
            auto current = pos;

            auto keyword = ASTTargetTables::kindToKeywordForInnerUUID(kind);
            if (keyword && ParserKeyword{*keyword}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (literal_p.parse(pos, ast, expected))
                {
                    result().setInnerUUID(kind, parseFromString<UUID>(ast->as<ASTLiteral>()->value.safeGet<String>()));
                    break;
                }
            }
            pos = current;

            auto prefix = ASTTargetTables::kindToPrefixForStorage(kind);
            if (prefix && ParserKeyword{*prefix}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (storage_p.parse(pos, ast, expected))
                {
                    result().setStorage(kind, ast);
                    break;
                }
            }
            pos = current;

            keyword = ASTTargetTables::kindToKeywordForTableId(kind);
            if (keyword && ParserKeyword{*keyword}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (table_name_p.parse(pos, ast, expected))
                {
                    result().setTableId(kind, ast->as<ASTTableIdentifier>()->getTableId());
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
