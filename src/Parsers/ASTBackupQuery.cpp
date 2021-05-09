#include <Parsers/ASTBackupQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using TableInfo = ASTBackupQuery::TableInfo;
    using DictionaryInfo = ASTBackupQuery::DictionaryInfo;
    using DatabaseInfo = ASTBackupQuery::DatabaseInfo;

    void formatTableInfos(const IAST::FormatSettings & format, const std::vector<TableInfo> & infos)
    {
        for (size_t i : ext::range(infos.size()))
        {
            const auto & info = infos[i];
            if (i != 0)
                format.ostr << ",";
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " TABLE " << (format.hilite ? IAST::hilite_none : "");
            if (!info.table_name.first.empty())
                format.ostr << backQuoteIfNeed(info.table_name.first) << ".";
            format.ostr << backQuoteIfNeed(info.table_name.second);
            if (!info.new_table_name.second.empty() && (info.new_table_name != info.table_name))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                if (!info.new_table_name.first.empty())
                    format.ostr << backQuoteIfNeed(info.new_table_name.first) << ".";
                format.ostr << backQuoteIfNeed(info.new_table_name.second);
            }
            if (!info.partitions.empty())
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " PARTITION " << (format.hilite ? IAST::hilite_none : "");
                for (size_t j : ext::range(info.partitions.size()))
                {
                    const auto & partition = info.partitions[j];
                    if (j != 0)
                        format.ostr << ",";
                    format.ostr << " " << quoteString(partition);
                }
            }
        }
    }

    void formatDictionaryInfos(const IAST::FormatSettings & format, const std::vector<DictionaryInfo> & infos)
    {
        for (size_t i : ext::range(infos.size()))
        {
            const auto & info = infos[i];
            if (i != 0)
                format.ostr << ",";
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " DICTIONARY " << (format.hilite ? IAST::hilite_none : "");
            if (!info.dictionary_name.first.empty())
                format.ostr << backQuoteIfNeed(info.dictionary_name.first) << ".";
            format.ostr << backQuoteIfNeed(info.dictionary_name.second);
            if (!info.new_dictionary_name.second.empty() && (info.new_dictionary_name != info.dictionary_name))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                if (!info.new_dictionary_name.first.empty())
                    format.ostr << backQuoteIfNeed(info.new_dictionary_name.first) << ".";
                format.ostr << backQuoteIfNeed(info.new_dictionary_name.second);
            }
        }
    }

    void formatDatabaseInfos(const IAST::FormatSettings & format, const std::vector<DatabaseInfo> & infos)
    {
        for (size_t i : ext::range(infos.size()))
        {
            const auto & info = infos[i];
            if (i != 0)
                format.ostr << ",";
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " DATABASE " << (format.hilite ? IAST::hilite_none : "");
            format.ostr << backQuoteIfNeed(info.database_name);
            if (!info.new_database_name.empty() && (info.new_database_name != info.database_name))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                format.ostr << backQuoteIfNeed(info.new_database_name);
            }
        }
    }

    void formatInfos(
        const IAST::FormatSettings & format,
        const std::vector<TableInfo> & tables,
        const std::vector<DictionaryInfo> & dictionaries,
        const std::vector<DatabaseInfo> & databases)
    {
        bool need_comma = false;
        if (!tables.empty())
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatTableInfos(format, tables);
        }
        if (!dictionaries.empty())
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatDictionaryInfos(format, dictionaries);
        }
        if (!databases.empty())
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatDatabaseInfos(format, databases);
        }
    }
}

String ASTBackupQuery::getID(char) const
{
    return (kind == Kind::BACKUP) ? "BackupQuery" : "RestoreQuery";
}


ASTPtr ASTBackupQuery::clone() const
{
    return std::make_shared<ASTBackupQuery>(*this);
}


void ASTBackupQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? "BACKUP" : "RESTORE")
                << (format.hilite ? hilite_none : "");

    if (all_databases)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ALL DATABASES EXCEPT SYSTEM"
                    << (format.hilite ? IAST::hilite_none : "");
    }
    else
    {
        formatInfos(format, tables, dictionaries, databases);
    }

    if (kind == Kind::BACKUP)
    {
        if (!base_backup_name.empty())
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH BASE " << (format.hilite ? hilite_none : "") << " " << quoteString(base_backup_name);
    }
    else
    {
        assert(kind == Kind::RESTORE);
        if (replace_database_if_exists)
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " WITH REPLACE IF DATABASE EXISTS" << (format.hilite ? IAST::hilite_none : "");
        else if (replace_table_if_exists)
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " WITH REPLACE IF TABLE EXISTS" << (format.hilite ? IAST::hilite_none : "");
    }

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO" : " FROM") << (format.hilite ? hilite_none : "");
    format.ostr << " " << quoteString(backup_name);
}

}
