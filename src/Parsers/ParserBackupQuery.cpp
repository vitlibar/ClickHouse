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
    using TableInfo = ASTBackupQuery::TableInfo;
    using DictionaryInfo = ASTBackupQuery::DictionaryInfo;
    using DatabaseInfo = ASTBackupQuery::DatabaseInfo;

    bool parseTableInfo(IParser::Pos & pos, Expected & expected, TableInfo & out_info)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"TABLE"}.ignore(pos, expected))
                return false;

            TableInfo info;
            if (!parseDatabaseAndTableName(pos, expected, info.table_name.first, info.table_name.second))
                return false;

            if (ParserKeyword{"AS"}.ignore(pos, expected))
            {
                if (!parseDatabaseAndTableName(pos, expected, info.new_table_name.first, info.new_table_name.second))
                    return false;
            }

            if (ParserKeyword{"PARTITION"}.ignore(pos, expected))
            {
                auto parse_element = [&]
                {
                    ASTPtr ast;
                    if (!ParserStringLiteral{}.parse(pos, ast, expected))
                        return false;
                    info.partitions.emplace_back(ast->as<ASTLiteral &>().value.safeGet<String>());
                    return true;
                };
                if (!ParserList::parseUtil(pos, expected, parse_element, false))
                    return false;
            }

            out_info = std::move(info);
            return true;
        });
    }

    bool parseDictionaryInfo(IParser::Pos & pos, Expected & expected, DictionaryInfo & out_info)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
                return false;

            DictionaryInfo info;
            if (!parseDatabaseAndTableName(pos, expected, info.dictionary_name.first, info.dictionary_name.second))
                return false;

            if (ParserKeyword{"AS"}.ignore(pos, expected))
            {
                if (!parseDatabaseAndTableName(pos, expected, info.new_dictionary_name.first, info.new_dictionary_name.second))
                    return false;
            }

            out_info = std::move(info);
            return true;
        });
    }

    bool parseDatabaseInfo(IParser::Pos & pos, Expected & expected, DatabaseInfo & out_info)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"DATABASE"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserIdentifier{}.parse(pos, ast, expected))
                return false;

            DatabaseInfo info;
            info.database_name = getIdentifierName(ast);

            if (ParserKeyword{"AS"}.ignore(pos, expected))
            {
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;

                info.new_database_name = getIdentifierName(ast);
            }

            out_info = std::move(info);
            return true;
        });
    }

    bool parseInfos(
        IParser::Pos & pos,
        Expected & expected,
        std::vector<TableInfo> & out_tables,
        std::vector<DictionaryInfo> & out_dictionaries,
        std::vector<DatabaseInfo> & out_databases)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<TableInfo> tables;
            std::vector<DictionaryInfo> dictionaries;
            std::vector<DatabaseInfo> databases;

            auto parse_element = [&]
            {
                TableInfo table;
                if (parseTableInfo(pos, expected, table))
                {
                    tables.emplace_back(std::move(table));
                    return true;
                }
                DictionaryInfo dictionary;
                if (parseDictionaryInfo(pos, expected, dictionary))
                {
                    dictionaries.emplace_back(std::move(dictionary));
                    return true;
                }
                DatabaseInfo database;
                if (parseDatabaseInfo(pos, expected, database))
                {
                    databases.emplace_back(std::move(database));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            out_databases = std::move(databases);
            out_tables = std::move(tables);
            out_dictionaries = std::move(dictionaries);
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

    bool all_databases = false;
    if (kind == Kind::BACKUP)
    {
        if (ParserKeyword{"ALL DATABASES EXCEPT SYSTEM"}.ignore(pos, expected) || ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
            all_databases = true;
    }

    std::vector<TableInfo> tables;
    std::vector<DictionaryInfo> dictionaries;
    std::vector<DatabaseInfo> databases;
    if (!all_databases)
    {
        if (!parseInfos(pos, expected, tables, dictionaries, databases))
        {
            if (kind == Kind::BACKUP)
                throw Exception("Objects to backup are not specified", ErrorCodes::SYNTAX_ERROR);
        }
    }

    String base_backup_name;
    bool replace_table_if_exists = false;
    bool replace_database_if_exists = false;
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
    else
    {
        assert(kind == Kind::RESTORE);
        if (ParserKeyword{"WITH REPLACE IF TABLE EXISTS"}.ignore(pos, expected))
            replace_table_if_exists = true;
        else if (ParserKeyword{"WITH REPLACE IF DATABASE EXISTS"}.ignore(pos, expected))
            replace_database_if_exists = true;
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
    query->all_databases = all_databases;
    query->tables = std::move(tables);
    query->dictionaries = std::move(dictionaries);
    query->databases = std::move(databases);
    query->backup_name = std::move(backup_name);
    query->base_backup_name = std::move(base_backup_name);
    query->replace_table_if_exists = replace_table_if_exists;
    query->replace_database_if_exists = replace_database_if_exists;

    return true;
}

}
