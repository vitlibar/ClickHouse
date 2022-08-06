#pragma once
#include <Core/QualifiedTableName.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;
class ASTFunctionWithKeyValueArguments;
class ASTStorage;

using TableNamesSet = std::unordered_set<QualifiedTableName>;

TableNamesSet getDependenciesSetFromCreateQuery(const ASTPtr & ast, const ContextPtr & global_context);

/// Visits ASTCreateQuery and extracts names of table (or dictionary) dependencies
/// from column default expressions (joinGet, dictGet, etc)
/// or dictionary source (for dictionaries from local ClickHouse table).
/// Does not validate AST, works a best-effort way.
class DDLDependencyVisitor
{
public:
    struct Data
    {
        ASTPtr create_query;
        QualifiedTableName table_name;
        String default_database;
        ContextPtr global_context;
        TableNamesSet dependencies;
    };

    using Visitor = ConstInDepthNodeVisitor<DDLDependencyVisitor, true>;

    static void visit(const ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);

private:
    static void visit(const ASTFunction & function, Data & data);
    static void visit(const ASTFunctionWithKeyValueArguments & dict_source, Data & data);
    static void visit(const ASTStorage & storage, Data & data);

    static void extractTableNameFromArgument(const ASTFunction & function, Data & data, size_t arg_idx);
};

using TableLoadingDependenciesVisitor = DDLDependencyVisitor::Visitor;

}
