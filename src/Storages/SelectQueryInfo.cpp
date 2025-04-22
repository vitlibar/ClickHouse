#include <Interpreters/Set.h>
#include <Parsers/ASTSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo()
    : prepared_sets(std::make_shared<PreparedSets>())
{}

bool SelectQueryInfo::isFinal() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->hasFinal();

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}

std::unordered_map<std::string, ColumnWithTypeAndName> SelectQueryInfo::buildNodeNameToInputNodeColumn() const
{
    std::unordered_map<std::string, ColumnWithTypeAndName> node_name_to_input_node_column;
    if (planner_context)
    {
        auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
        const auto & alias_column_expressions = table_expression_data.getAliasColumnExpressions();
        for (const auto & [column_identifier, column_name] : table_expression_data.getColumnIdentifierToColumnName())
        {
            /// ALIAS columns cannot be used in the filter expression without being calculated in ActionsDAG,
            /// so they should not be added to the input nodes.
            if (alias_column_expressions.contains(column_name))
                continue;
            const auto & column = table_expression_data.getColumnOrThrow(column_name);
            node_name_to_input_node_column.emplace(column_identifier, ColumnWithTypeAndName(column.type, column_name));
        }
    }
    return node_name_to_input_node_column;
}

String SelectQueryInfo::toString() const
{
    String str = "SelectQueryInfo(\n";
    if (query)
        str += fmt::format("query = {}\n", query->formatForLogging());
    if (view_query)
        str += fmt::format("view_query = {}\n", view_query->formatForLogging());
    if (query_tree)
        str += fmt::format("query_tree = {}\n", query_tree->dumpTree());
    if (planner_context)
        str += fmt::format("planner_context != 0\n");
    if (table_expression)
        str += fmt::format("table_expression != 0\n");
    if (storage_limits)
        str += fmt::format("storage_limits != 0\n");
    str += ")";
    return str;
}

}
