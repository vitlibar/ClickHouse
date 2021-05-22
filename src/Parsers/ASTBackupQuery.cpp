#include <Parsers/ASTBackupQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Entry = ASTBackupQuery::Entry;
    using EntryType = ASTBackupQuery::EntryType;

    void formatEntry(const Entry & entry, Kind kind, const IAST::FormatSettings & format)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ";
        switch (entry.type)
        {
            case EntryType::TABLE: format.ostr << "TABLE"; break;
            case EntryType::DICTIONARY: format.ostr << "DICTIONARY"; break;
            case EntryType::DATABASE: format.ostr << "DATABASE"; break;
            case EntryType::ALL_DATABASES: format.ostr << "ALL DATABASES"; break;
            case EntryType::TEMPORARY_TABLE: format.ostr << "TEMPORARY TABLE"; break;
            case EntryType::ALL_TEMPORARY_TABLES: format.ostr << "ALL TEMPORARY TABLES"; break;
            case EntryType::EVERYTHING: format.ostr << "EVERYTHING"; break;
        }
        format.ostr << (format.hilite ? IAST::hilite_none : "");

        bool need_table_name = (entry.type == EntryType::TABLE) || (entry.type == EntryType::DICTIONARY) || (entry.type == EntryType::TEMPORARY_TABLE);
        bool need_db_name = (entry.type == EntryType::DATABASE);
        bool allow_db_name = need_db_name || (entry.type == EntryType::TABLE) || (entry.type == EntryType::DICTIONARY);

        if (need_table_name && allow_db_name)
        {
            const auto & name = entry.name;
            const auto & new_name = entry.new_name;
            if (!name.first.empty())
                format.ostr << backQuoteIfNeed(name.first) << ".";
            format.ostr << backQuoteIfNeed(name.second);
            if (!new_name.second.empty() && (new_name != name))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                if (!new_name.first.empty())
                    format.ostr << backQuoteIfNeed(new_name.first) << ".";
                format.ostr << backQuoteIfNeed(new_name.second);
            }
        }
        else if (need_table_name || need_db_name)
        {
            assert(!need_table_name || !need_db_name);
            const auto & name = need_db_name ? entry.name.first : entry.name.second;
            const auto & new_name = need_db_name ? entry.new_name.first : entry.new_name.second;
            format.ostr << backQuoteIfNeed(name);
            if (!new_name.empty() && (new_name != name))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                format.ostr << backQuoteIfNeed(new_name);
            }
        }

        if (!entry.partitions.empty())
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " PARTITION " << (format.hilite ? IAST::hilite_none : "");
            for (size_t j : ext::range(entry.partitions.size()))
            {
                const auto & partition = entry.partitions[j];
                if (j != 0)
                    format.ostr << ",";
                format.ostr << " " << quoteString(partition);
            }
        }
    }

    void formatEntries(
        const std::vector<Entry> & entries,
        Kind kind,
        const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & entry : entries)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatEntry(entry, kind, format);
        }
    }

    void formatReplaceMode(const RestoreWithReplaceMode & replace_mode,
                           const IAST::FormatSettings & format)
    {
        if (replace_mode.replace_table_if_exists && replace_mode.replace_database_if_exists)
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " WITH REPLACE IF TABLE OR DATABASE EXISTS" << (format.hilite ? IAST::hilite_none : "");
        else if (replace_mode.replace_table_if_exists)
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " WITH REPLACE IF TABLE EXISTS" << (format.hilite ? IAST::hilite_none : "");
        else if (replace_mode.replace_database_if_exists)
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " WITH REPLACE IF DATABASE EXISTS" << (format.hilite ? IAST::hilite_none : "");
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

    formatEntries(entries, kind, format);
\
    if (kind == Kind::BACKUP)
    {
        if (!base_backup_name.empty())
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH BASE " << (format.hilite ? hilite_none : "") << " " << quoteString(base_backup_name);
    }
    else
    {
        assert(kind == Kind::RESTORE);
        formatReplaceMode(replace_mode, format);
    }

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO" : " FROM") << (format.hilite ? hilite_none : "");
    format.ostr << " " << quoteString(backup_name);
}

}
