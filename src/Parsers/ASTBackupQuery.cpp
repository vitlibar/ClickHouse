#include <Parsers/ASTBackupQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using DatabaseInfo = ASTBackupQuery::DatabaseInfo;
    using TableInfo = ASTBackupQuery::TableInfo;

    void formatDatabaseInfos(const std::vector<DatabaseInfo> & infos, const IAST::FormatSettings & format)
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

    void formatTableInfos(const std::vector<TableInfo> & infos, const IAST::FormatSettings & format)
    {
        for (size_t i : ext::range(infos.size()))
        {
            const auto & info = infos[i];
            if (i != 0)
                format.ostr << ",";
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " TABLE " << (format.hilite ? IAST::hilite_none : "");
            if (!info.database_name.empty())
                format.ostr << backQuoteIfNeed(info.database_name) << ".";
            format.ostr << backQuoteIfNeed(info.table_name);
            if (!info.new_table_name.empty() && ((info.new_table_name != info.table_name) || (info.new_database_name != info.database_name)))
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                if (!info.new_database_name.empty())
                    format.ostr << backQuoteIfNeed(info.new_database_name) << ".";
                format.ostr << backQuoteIfNeed(info.new_table_name);
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

    void formatRestoreMode(RestoreMode restore_mode, const IAST::FormatSettings & format)
    {
        format.ostr << " " << (format.hilite ? IAST::hilite_keyword : "") << toString(restore_mode)
                    << (format.hilite ? IAST::hilite_none : "");
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

    if ((kind == Kind::BACKUP) && use_incremental_backup)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " DIFFERENCES SINCE" << (format.hilite ? hilite_none : "");
        format.ostr << " " << quoteString(base_backup_name);
        format.ostr << (format.hilite ? hilite_keyword : "") << " IN" << (format.hilite ? hilite_none : "");
    }

    if (all_databases)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " ALL DATABASES" << (format.hilite ? hilite_none : "");
    }
    else
    {
        formatDatabaseInfos(databases, format);

        if (!tables.empty())
        {
            if (!databases.empty())
                format.ostr << ",";
            formatTableInfos(tables, format);
        }
    }

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO" : " FROM") << (format.hilite ? hilite_none : "");
    format.ostr << " " << quoteString(backup_name);

    if ((kind == Kind::RESTORE) && (restore_mode != RestoreMode::FROM_SCRATCH))
        formatRestoreMode(restore_mode, format);
}

}
