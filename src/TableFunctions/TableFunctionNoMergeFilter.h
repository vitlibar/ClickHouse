#pragma once

#include <TableFunctions/ITableFunction.h>


namespace DB
{

/** `noMergeFilter(X)`
 *
 *  Pragma-style wrapper used in `FROM`. It does NOT produce a storage at runtime:
 *  the analyzer unwraps it to its single argument in `QueryTreeBuilder` (see
 *  `QueryTreeBuilder::tryBuildNoMergeFilterTableExpression`) and tags the resulting
 *  table-expression node with `setNoMergeFilter(true)`. The planner then appends
 *  `OptimizationBarrierStep` on top of the subplan it builds for that node, so
 *  outer filters/expressions are not fused with the inner ones
 *  (`tryMergeExpressions`, `tryMergeFilterIntoJoinCondition`, `tryPushDownFilter`).
 *
 *  Registering this class in `TableFunctionFactory` is a "ghost registration":
 *  it never actually runs. The benefits of registering are:
 *    - `system.table_functions` lists the function with its documentation,
 *    - `TableFunctionFactory::isTableFunctionName("noMergeFilter")` returns true
 *      in the few places that check it (e.g. nested table-function argument paths),
 *    - symmetry with every other name used after `FROM`.
 *
 *  All real methods throw `LOGICAL_ERROR` — if they are ever reached, the interception
 *  in `QueryTreeBuilder` regressed and must be fixed.
 */
class TableFunctionNoMergeFilter : public ITableFunction
{
public:
    static constexpr auto name = "noMergeFilter";

    std::string getName() const override { return name; }

    /// Tell the analyzer not to touch the single argument: it is handled by
    /// `QueryTreeBuilder::tryBuildNoMergeFilterTableExpression` before any
    /// table-function path is taken. Purely defensive — mirrors `view()`.
    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override
    {
        return {0};
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
        ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "NoMergeFilter"; }
};

}
