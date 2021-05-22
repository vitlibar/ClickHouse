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
    using Entry = ASTBackupQuery::Entry;
    using EntryType = ASTBackupQuery::EntryType;

    bool parseEntry(IParser::Pos & pos, Expected & expected, Kind kind, Entry & entry)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            EntryType type;
            if (ParserKeyword{"TABLE"}.ignore(pos, expected))
                type = EntryType::TABLE;
            else if (ParserKeyword{"DICTIONARY"}.ignore(pos, expected))
                type = EntryType::DICTIONARY;
            else if (ParserKeyword{"DATABASE"}.ignore(pos, expected))
                type = EntryType::DATABASE;
            else if (ParserKeyword{"ALL DATABASES"}.ignore(pos, expected))
                type = EntryType::ALL_DATABASES;
            else if (ParserKeyword{"TEMPORARY TABLE"}.ignore(pos, expected))
                type = EntryType::TEMPORARY_TABLE;
            else if (ParserKeyword{"ALL TEMPORARY TABLES"}.ignore(pos, expected))
                type = EntryType::ALL_TEMPORARY_TABLES;
            else if (ParserKeyword{"EVERYTHING"}.ignore(pos, expected))
                type = EntryType::EVERYTHING;
            else
                return false;

            DatabaseAndTableName name, new_name;
            bool need_table_name = (type == EntryType::TABLE) || (type == EntryType::DICTIONARY) || (type == EntryType::TEMPORARY_TABLE);
            bool need_db_name = (type == EntryType::DATABASE);
            bool allow_db_name = need_db_name || (type == EntryType::TABLE) || (type == EntryType::DICTIONARY);

            if (need_table_name && allow_db_name)
            {
                if (!parseDatabaseAndTableName(pos, expected, name.first, name.second))
                    return false;
                if (ParserKeyword{"AS"}.ignore(pos, expected))
                {
                    if (!parseDatabaseAndTableName(pos, expected, new_name.first, new_name.second))
                        return false;
                }
                else
                {
                    new_name = name;
                }
                return true;
            }
            else if (need_table_name || need_db_name)
            {
                assert(!need_table_name || !need_db_name);
                String & name1 = need_db_name ? name.first : name.second;
                String & new_name1 = need_db_name ? new_name.first : new_name.second;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                name1 = getIdentifierName(ast);

                if (ParserKeyword{"AS"}.ignore(pos, expected))
                {
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    new_name1 = getIdentifierName(ast);
                }
                else
                    new_name1 = name1;
            }

            Strings partitions;
            bool allow_partitions = (type == EntryType::TABLE);
            if (allow_partitions && ParserKeyword{"PARTITION"}.ignore(pos, expected))
            {
                auto parse_element = [&]
                {
                    ASTPtr ast;
                    if (!ParserStringLiteral{}.parse(pos, ast, expected))
                        return false;
                    partitions.emplace_back(ast->as<ASTLiteral &>().value.safeGet<String>());
                    return true;
                };
                if (!ParserList::parseUtil(pos, expected, parse_element, false))
                    return false;
            }

            entry.type = type;
            entry.name = std::move(name);
            entry.new_name = std::move(new_name);
            entry.partitions = std::move(partitions);
            return true;
        });
    }

    bool parseEntries(
        IParser::Pos & pos,
        Expected & expected,
        Kind kind,
        std::vector<Entry> & out_entries)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<Entry> entries;

            auto parse_element = [&]
            {
                Entry entry;
                if (parseEntry(pos, expected, kind, entry))
                {
                    entries.emplace_back(std::move(entry));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            out_entries = std::move(entries);
            return true;
        });
    }

    bool parseReplaceMode(IParser::Pos & pos, Expected & expected, RestoreWithReplaceMode & replace_mode)
    {
        RestoreWithReplaceMode rm;
        if (ParserKeyword{"WITH REPLACE IF TABLE EXISTS"}.ignore(pos, expected))
        {
            rm.replace_table_if_exists = true;
        }
        else if (ParserKeyword{"WITH REPLACE IF DATABASE EXISTS"}.ignore(pos, expected))
        {
            rm.replace_database_if_exists = true;
        }
        else if (ParserKeyword{"WITH REPLACE IF TABLE OR DATABASE EXISTS"}.ignore(pos, expected) ||
                 ParserKeyword{"WITH REPLACE IF DATABASE OR TABLE EXISTS"}.ignore(pos, expected))
        {
            rm.replace_table_if_exists = true;
            rm.replace_database_if_exists = true;
        }
        else
            return false;

        replace_mode = std::move(rm);
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

    std::vector<Entry> entries;
    if (!parseEntries(pos, expected, kind, entries))
    {
        if (kind == Kind::BACKUP)
            throw Exception("Objects to backup are not specified", ErrorCodes::SYNTAX_ERROR);
        else
        {
            assert(kind == Kind::RESTORE);
            entries.clear();
            entries.emplace_back().type = EntryType::EVERYTHING;
        }
    }

    String base_backup_name;
    RestoreWithReplaceMode replace_mode;
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
        parseReplaceMode(pos, expected, replace_mode);
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
    query->entries = std::move(entries);
    query->backup_name = std::move(backup_name);
    query->base_backup_name = std::move(base_backup_name);
    query->replace_mode = std::move(replace_mode);

    return true;
}

}
