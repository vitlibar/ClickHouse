#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorHelpers.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns an AST to evaluate the `join_group` column to join the sides of a binary operator on instant vectors.
ASTPtr makeExpressionForJoinGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && group,
    bool metric_name_dropped_from_group,
    bool * metric_name_dropped_from_join_group)
{
    bool dummy;
    if (!metric_name_dropped_from_join_group)
        metric_name_dropped_from_join_group = &dummy;

    /// Group #0 always means a group with no tags.
    if (const auto * literal = group->as<const ASTLiteral>(); literal && literal->value == Field{0u})
    {
        *metric_name_dropped_from_join_group = true;
        return std::move(group);
    }

    if (operator_node->on)
    {
        if (operator_node->labels.empty())
        {
            /// ON() means we ignore all tags.
            *metric_name_dropped_from_join_group = true;
            return make_intrusive<ASTLiteral>(0u);
        }
        else
        {
            /// ON(tags) means we ignore all tags except the specified ones.
            /// If the metric name "__name__" is among the tags in ON(tags) we don't remove it from the join group.

            /// timeSeriesRemoveAllTagsExcept(group, on_tags)
            Strings tags_to_keep = operator_node->labels;
            std::sort(tags_to_keep.begin(), tags_to_keep.end());
            tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

            *metric_name_dropped_from_join_group = !std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName);

            return makeASTFunction(
                "timeSeriesRemoveAllTagsExcept",
                std::move(group),
                make_intrusive<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
        }
    }
    else if (operator_node->ignoring && !operator_node->labels.empty())
    {
        /// IGNORE(tags) means we ignore the specified tags, and also the metric name "__name__".

        /// timeSeriesRemoveTags(group, ignoring_tags + ['__name__'])
        Strings tags_to_remove = operator_node->labels;
        if (!metric_name_dropped_from_group && (std::find(tags_to_remove.begin(), tags_to_remove.end(), kMetricName) == tags_to_remove.end()))
            tags_to_remove.push_back(kMetricName);
        std::sort(tags_to_remove.begin(), tags_to_remove.end());
        tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

        *metric_name_dropped_from_join_group = true;

        return makeASTFunction(
            "timeSeriesRemoveTags", std::move(group), make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
    }
    else
    {
        /// Neither ON() nor IGNORE() keywords are specified, we use all the tags except the metric name "__name__".
        *metric_name_dropped_from_join_group = true;
        if (metric_name_dropped_from_group)
            return std::move(group);
        else
            return makeASTFunction("timeSeriesRemoveTag", std::move(group), make_intrusive<ASTLiteral>(kMetricName));
    }
}

}
