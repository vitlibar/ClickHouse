#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>

namespace DB
{


BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    context.getSessionContext().applySettingsChangesChecked(ast.changes, &context);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    context.applySettingsChangesChecked(ast.changes);
}

}
