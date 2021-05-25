#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    bool parseName(IParser::Pos & pos, Expected & expected, ElementType type, DatabaseAndTableName & name)
    {
        switch (type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                return parseDatabaseAndTableName(pos, expected, name.first, name.second);
            }

            case ElementType::DATABASE:
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name.first = getIdentifierName(ast);
                name.second.clear();
                return true;
            }

            case ElementType::TEMPORARY_TABLE:
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name.second = getIdentifierName(ast);
                name.first.clear();
                return true;
            }

            default:
                return true;
        }
    }

    bool parsePartitions(IParser::Pos & pos, Expected & expected, std::set<String> & partitions)
    {
        if (!ParserKeyword{"PARTITION"}.ignore(pos, expected))
            return false;

        std::set<String> result;
        auto parse_list_element = [&]
        {
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            result.emplace(ast->as<ASTLiteral &>().value.safeGet<String>());
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        partitions = std::move(result);
        return true;
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, Element & entry)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ElementType type;
            if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                type = ElementType::TABLE;
            else if (ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
                type = ElementType::DICTIONARY;
            else if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                type = ElementType::DATABASE;
            else if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
                type = ElementType::ALL_DATABASES;
            else if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
                type = ElementType::TEMPORARY_TABLE;
            else if (ParserKeyword{"ALL TEMPORARY TABLES"}.ignore(pos, expected))
                type = ElementType::ALL_TEMPORARY_TABLES;
            else if (ParserKeyword{"EVERYTHING"}.ignore(pos, expected))
                type = ElementType::EVERYTHING;
            else
                return false;

            DatabaseAndTableName name;
            if (!parseName(pos, expected, type, name))
                return false;

            std::set<String> partitions;
            if (type == ElementType::TABLE)
                parsePartitions(pos, expected, partitions);

            DatabaseAndTableName name_in_backup;
            if (ParserKeyword{"USING NAME"}.ignore(pos, expected))
            {
                if (!parseName(pos, expected, type, name_in_backup))
                    return false;
            }

            if ((type == ElementType::TABLE) && partitions.empty())
                parsePartitions(pos, expected, partitions);

            entry.type = type;
            entry.name = std::move(name);
            entry.name_in_backup = std::move(name_in_backup);
            entry.partitions = std::move(partitions);
            return true;
        });
    }

    bool parseElements(
        IParser::Pos & pos,
        Expected & expected,
        std::vector<Element> & elements)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<Element> result;

            auto parse_element = [&]
            {
                Element element;
                if (parseElement(pos, expected, element))
                {
                    result.emplace_back(std::move(element));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            elements = std::move(result);
            return true;
        });
    }
}


bool ParserBackupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Kind kind;
    if (ParserKeyword{"BACKUP"}.ignore(pos, expected))
        kind = Kind::BACKUP;
    else if (ParserKeyword{"RESTORE"}.ignore(pos, expected))
        kind = Kind::RESTORE;
    else
        return false;

    std::vector<Element> elements;
    if (!parseElements(pos, expected, elements))
        return false;

    String base_backup_name;
    if (kind == Kind::BACKUP)
    {
        if (ParserKeyword{"WITH BASE"}.ignore(pos, expected))
        {
            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            base_backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();
        }
    }

    if (!ParserKeyword{(kind == Kind::BACKUP) ? "TO" : "FROM"}.ignore(pos, expected))
        return false;
    ASTPtr ast;
    if (!ParserStringLiteral{}.parse(pos, ast, expected))
        return false;
    String backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    query->backup_name = std::move(backup_name);
    query->base_backup_name = std::move(base_backup_name);

    return true;
}

}
