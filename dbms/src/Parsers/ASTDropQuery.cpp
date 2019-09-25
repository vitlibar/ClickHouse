#include <Parsers/ASTDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + database) + delim + table;
    else if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + database) + delim + table;
    else if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + database) + delim + table;
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = std::make_shared<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    return res;
}

void ASTDropQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (formatDropACLQuery(settings))
        return;

    settings.ostr << (settings.hilite ? hilite_keyword : "");
    if (kind == ASTDropQuery::Kind::Drop)
        settings.ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        settings.ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        settings.ostr << "TRUNCATE ";
    else
        throw Exception("Not supported kind of drop query.", ErrorCodes::SYNTAX_ERROR);

    if (temporary)
        settings.ostr << "TEMPORARY ";

    settings.ostr << ((table.empty() && !database.empty()) ? "DATABASE " : "TABLE ");

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table.empty() && !database.empty())
        settings.ostr << backQuoteIfNeed(database);
    else
        settings.ostr << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

    formatOnCluster(settings);
}


bool ASTDropQuery::formatDropACLQuery(const FormatSettings & settings) const
{
    if (kind != ASTDropQuery::Kind::Drop)
        return false;

    const auto & names = !roles.empty() ? roles : users;
    if (names.empty())
        return false;

    const char * type = !roles.empty() ? "ROLE" : "USER";
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << type
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    for (size_t i = 0; i != names.size(); ++i)
        settings.ostr << (i ? " " : ", ") << backQuoteIfNeed(names[i]);
    return true;
}
}

