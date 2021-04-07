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
            std::vector<DatabaseInfo> databases;
            std::vector<TableInfo> tables;

            auto parse_element = [&]
            {
                if ((kind == Kind::BACKUP) && ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
                {
                    all_databases = true;
                    return true;
                }
                DatabaseInfo database;
                if (parseDatabaseInfo(pos, expected, kind, database))
                {
                    databases.emplace_back(std::move(database));
                    return true;
                }
                TableInfo table;
                if (parseTableInfo(pos, expected, kind, table))
                {
                    tables.emplace_back(std::move(table));
                    return true;
                }
                return false;
            };
            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            if (!all_databases && databases.empty() && tables.empty() && (kind == Kind::BACKUP))
                return false;

            out_all_databases = all_databases;
            out_databases = std::move(databases);
            out_tables = std::move(tables);
            return true;
        });
    }

    bool parseOnDisk(IParser::Pos & pos, Expected & expected, String & out_disk_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            String disk_name;
            if (!ParserKeyword{"ON DISK"}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            if (!ParserStringLiteral{}.parse(pos, ast, expected))
                return false;
            out_disk_name = ast->as<ASTLiteral &>().value.safeGet<String>();
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

            out_backup_name = ast->as<ASTLiteral &>().value.safeGet<String>();
            parseOnDisk(pos, expected, out_disk_name);
            return true;
        });
    }

    bool parseRestoreMode(IParser::Pos & pos, Expected & expected, RestoreMode & out_restore_mode)
    {
        if (ParserKeyword{"FROM SCRATCH"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::FROM_SCRATCH;
        else if (ParserKeyword{"REPLACE OLD DATA"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::REPLACE_OLD_DATA;
        else if (ParserKeyword{"KEEP OLD DATA"}.ignore(pos, expected))
            out_restore_mode = RestoreMode::KEEP_OLD_DATA;
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

    RestoreMode restore_mode = RestoreMode::FROM_SCRATCH;
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
