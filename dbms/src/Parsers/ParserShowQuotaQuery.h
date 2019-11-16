#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW QUOTA [[name] [WITH KEY = key] | CURRENT | ALL]
  */
class ParserShowQuotaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
