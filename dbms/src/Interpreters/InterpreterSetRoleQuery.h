#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTSetRoleQuery;


class InterpreterSetRoleQuery : public IInterpreter
{
public:
    InterpreterSetRoleQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static void setDefaultRoles(const std::vector<UUID> & to_users, const ASTRoleList & roles, bool grant_if_need);

private:
    void setRole(const ASTSetRoleQuery & query);
    void setDefaultRole(const ASTSetRoleQuery & query);

    ASTPtr query_ptr;
    Context & context;
};
}
