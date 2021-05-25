#include <Parsers/ASTBackupQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    void formatName(const DatabaseAndTableName & name, ElementType type, const IAST::FormatSettings & format)
    {
        switch (type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                format.ostr << " ";
                if (!name.first.empty())
                    format.ostr << backQuoteIfNeed(name.first) << ".";
                format.ostr << backQuoteIfNeed(name.second);
                break;
            }
            case ElementType::DATABASE:
            {
                format.ostr << " " << backQuoteIfNeed(name.first);
                break;
            }
            case ElementType::TEMPORARY_TABLE:
            {
                format.ostr << " " << backQuoteIfNeed(name.second);
                break;
            }
            default:
                break;
        }
    }

    void formatPartitions(const std::set<String> & partitions, const IAST::FormatSettings & format)
    {
        if (partitions.empty())
            return;
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " PARTITION " << (format.hilite ? IAST::hilite_none : "");
        bool need_comma = false;
        for (const auto & partition : partitions)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            format.ostr << " " << quoteString(partition);
        }
    }

    void formatElement(const Element & element, const IAST::FormatSettings & format)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ";
        switch (element.type)
        {
            case ElementType::TABLE: format.ostr << "TABLE"; break;
            case ElementType::DICTIONARY: format.ostr << "DICTIONARY"; break;
            case ElementType::DATABASE: format.ostr << "DATABASE"; break;
            case ElementType::ALL_DATABASES: format.ostr << "ALL DATABASES"; break;
            case ElementType::TEMPORARY_TABLE: format.ostr << "TEMPORARY TABLE"; break;
            case ElementType::ALL_TEMPORARY_TABLES: format.ostr << "ALL TEMPORARY TABLES"; break;
            case ElementType::EVERYTHING: format.ostr << "EVERYTHING"; break;
        }
        format.ostr << (format.hilite ? IAST::hilite_none : "");

        const auto & name = element.name;
        formatName(name, element.type, format);

        const auto & name_in_backup = element.name_in_backup;
        bool name_in_backup_empty = (name_in_backup.first.empty() && name_in_backup.second.empty());
        if (!name_in_backup_empty && (name_in_backup != name))
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " USING NAME" << (format.hilite ? IAST::hilite_none : "");
            formatName(name_in_backup, element.type, format);
        }

        formatPartitions(element.partitions, format);
    }

    void formatElements(const std::vector<Element> & elements, const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatElement(element, format);
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

    formatElements(elements, format);

    if ((kind == Kind::BACKUP) && !base_backup_name.empty())
        format.ostr << (format.hilite ? hilite_keyword : "") << " WITH BASE " << (format.hilite ? hilite_none : "") << " " << quoteString(base_backup_name);

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO" : " FROM") << (format.hilite ? hilite_none : "");
    format.ostr << " " << quoteString(backup_name);
}

}
