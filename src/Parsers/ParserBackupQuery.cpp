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
namespace
{
    using Kind = ASTBackupQuery::Kind;
    using DatabaseInfo = ASTBackupQuery::DatabaseInfo;
    using TableInfo = ASTBackupQuery::TableInfo;

    bool parseDatabaseInfo(IParser::Pos & pos, Expected & expected, Kind kind, DatabaseInfo & out_info)
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

            if ((kind == Kind::RESTORE) && ParserKeyword{"AS"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;

                info.new_database_name = getIdentifierName(ast);
            }

            out_info = std::move(info);
            return true;
        });
    }

    bool parseTableInfo(IParser::Pos & pos, Expected & expected, Kind kind, TableInfo & out_info)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{"TABLE"}.ignore(pos, expected))
                return false;

            TableInfo info;
            if (!parseDatabaseAndTableName(pos, expected, info.database_name, info.table_name))
                return false;

            if ((kind == Kind::RESTORE) && ParserKeyword{"AS"}.ignore(pos, expected))
            {
                if (!parseDatabaseAndTableName(pos, expected, info.new_database_name, info.new_table_name))
                    return false;
            }

            if (ParserKeyword{"PARTITION"}.ignore(pos, expected))
            {
                ASTPtr ast;
                if (!ParserList{std::make_unique<ParserStringLiteral>(), std::make_unique<ParserToken>(TokenType::Comma), false}.parse(pos, ast, expected))
                    return false;

                for (const auto & child : ast->children)
                    info.partitions.emplace_back(child->as<ASTLiteral &>().value.safeGet<String>());
            }

            out_info = std::move(info);
            return true;
        });
    }

    bool parseDatabaseAndTableInfos(
        IParser::Pos & pos,
        Expected & expected,
        Kind kind,
        bool & out_all_databases,
        std::vector<DatabaseInfo> & out_databases,
        std::vector<TableInfo> & out_tables)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            bool all_databases = false;
            if (kind == Kind::BACKUP)
            {
                if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
                    all_databases = true;
            }

            std::vector<DatabaseInfo> databases;
            std::vector<TableInfo> tables;
            if (!all_databases)
            {
                while (true)
                {
                    DatabaseInfo database;
                    TableInfo table;
                    if (parseDatabaseInfo(pos, expected, kind, database))
                        databases.emplace_back(std::move(database));
                    else if (parseTableInfo(pos, expected, kind, table))
                        tables.emplace_back(std::move(table));
                    else if (!databases.empty() || !tables.empty())
                        break;
                    else
                        return false;
                }

                ParserToken{TokenType::Comma}.ignore(pos, expected);
            }

            out_all_databases = all_databases;
            out_databases = std::move(databases);
            out_tables = std::move(tables);
            return true;
        });
    }

    bool parseBackupNameAndDisk(IParser::Pos & pos, Expected & expected, Kind kind, String & out_backup_name, String & out_disk_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{(kind == Kind::BACKUP) ? "TO" : "FROM"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            String backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();

            String disk_name;
            if (ParserKeyword{"ON DISK"}.ignore(pos, expected))
            {
                if (!ParserStringLiteral{}.parse(pos, ast, expected))
                    return false;
                disk_name = ast->as<ASTLiteral &>().value.safeGet<String>();
            }

            out_backup_name = backup_name;
            out_disk_name = disk_name;
            return true;
        });
    }

    bool parseRestoreMode(IParser::Pos & pos, Expected & expected, RestoreMode & out_restore_mode)
    {
        if (ParserKeyword{"NO OLD DATA"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::NO_OLD_DATA;
        else if (ParserKeyword{"KEEP OLD DATA"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::KEEP_OLD_DATA;
        else if (ParserKeyword{"REPLACE OLD DATA"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::REPLACE_OLD_DATA;
        else
            return false;
        return true;
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
    std::vector<DatabaseInfo> databases;
    std::vector<TableInfo> tables;
    if (!parseDatabaseAndTableInfos(pos, expected, kind, all_databases, databases, tables))
    {
        if (kind == Kind::BACKUP)
            return false;
    }

    String backup_name, disk_name;
    if (!parseBackupNameAndDisk(pos, expected, kind, backup_name, disk_name))
        return false;

    RestoreMode restore_mode = RestoreMode::NO_OLD_DATA;
    if (kind == Kind::RESTORE)
        parseRestoreMode(pos, expected, restore_mode);

    auto query = std::make_shared<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->all_databases = all_databases;
    query->databases = std::move(databases);
    query->tables = std::move(tables);
    query->backup_name = std::move(backup_name);
    query->disk_name = std::move(disk_name);
    query->restore_mode = restore_mode;

    return true;
}

}
