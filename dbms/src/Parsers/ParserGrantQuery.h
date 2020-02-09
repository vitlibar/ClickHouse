#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
  * REVOKE [GRANT OPTION FOR] access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | role | CURRENT_USER} [,...]
  *
  * GRANT role [,...] TO {user | role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
  * REVOKE [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | role | CURRENT_USER} [,...]
  */
class ParserGrantQuery : public IParserBase
{
protected:
    const char * getName() const override { return "GRANT or REVOKE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
