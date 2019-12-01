#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [{SET {{MAX QUERIES | MAX ERRORS | MAX RESULT ROWS | MAX RESULT BYTES | MAX READ ROWS | MAX READ BYTES | MAX EXECUTION TIME} = {number | ANY} } [,...] |
  *        SET TRACKING
  *       } FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  *
  * ALTER QUOTA [IF EXISTS] name
  *      [RENAME TO new_name]
  *      [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
  *      [{SET {{MAX QUERIES | MAX ERRORS | MAX RESULT ROWS | MAX RESULT BYTES | MAX READ ROWS | MAX READ BYTES | MAX EXECUTION TIME} = {number | ANY} } [,...] |
  *        SET TRACKING |
  *        UNSET TRACKING
  *       } FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY} [,...]]
  *      [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
  */
class ParserCreateQuotaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE QUOTA or ALTER QUOTA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
