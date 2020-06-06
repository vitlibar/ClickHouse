#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * SHOW CREATE QUOTA [name [, name2 ...] | ALL]
  * SHOW CREATE [ROW] POLICY {name ON [database.]table [, name2 ON database2.table2 ...] | ALL}
  * SHOW CREATE USER [name [, name2 ...] | CURRENT_USER | ALL]
  * SHOW CREATE ROLE {name [, name2 ...] | ALL}
  * SHOW CREATE [SETTINGS] PROFILE {name [, name2 ...] | ALL}
  */
class ParserShowCreateAccessEntityQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
