#include <Parsers/ASTAccessEntityReference.h>
#include <Common/quoteString.h>


namespace DB
{
void ASTAccessEntityReference::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (id_mode)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ID" << (settings.hilite ? IAST::hilite_none : "") << "("
                      << quoteString(name) << ")";
    }
    else
    {
        settings.ostr << backQuoteIfNeed(name);
    }
}
}
