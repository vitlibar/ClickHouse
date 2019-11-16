#include <Parsers/ASTShowQuotaQuery.h>
#include <Common/quoteString.h>


namespace DB
{
ASTPtr ASTShowQuotaQuery::clone() const
{
    return std::make_shared<ASTShowQuotaQuery>(*this);
}


void ASTShowQuotaQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SHOW QUOTA"
                  << (settings.hilite ? hilite_none : "");

    if (current)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT" << (settings.hilite ? hilite_none : "");
    }
    else if (all)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ALL" << (settings.hilite ? hilite_none : "");
    }
    else
    {
        if (!name.empty())
            settings.ostr << " " << backQuoteIfNeed(name);

        if (key)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH KEY = " << (settings.hilite ? hilite_none : "")
                          << quoteString(*key);
        }
    }
}
}
