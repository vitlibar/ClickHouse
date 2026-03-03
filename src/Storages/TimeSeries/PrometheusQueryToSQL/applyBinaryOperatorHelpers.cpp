#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorHelpers.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonBinaryOperator.h>


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


namespace
{
    bool isComparisonWithoutBool(const PQT::BinaryOperator * operator_node)
    {
        return isComparisonBinaryOperator(operator_node->operator_name) && !operator_node->bool_modifier;
    }

    /// Returns an AST to evaluate the result group of a binary operator on instant vectors if neither "group_left" nor "group_right" is used.
    /// The function usually just returns `join_group`.
    /// There are two special cases:
    /// 1. If `join_group` contains the metric name "__name__", this function removes it
    ///    because the result of a binary operator shouldn't contain the metric name.
    ///    (`join_group` can contain the metric name if it's specified explicitly in ON(),
    ///    for example "http_errors + on (__name__) http_failures")
    /// 2. If it's a comparisons without the bool modifier the function doesn't remove the metric name, instead it copies it from the left side
    ///    in case the ignoring list doesn't contain the metric name; or neither on() nor ignore() is specified.
    ASTPtr makeExpressionForResultGroup_Default(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && /* right_argument_group */,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool /* metric_name_dropped_from_right */,
        bool metric_name_dropped_from_join_group,
        bool * metric_name_dropped_from_result)
    {
        chassert(!operator_node->group_left && !operator_node->group_right);

        if (isComparisonWithoutBool(operator_node))
        {
            /// For comparison operators without the bool modifier we add the metric name "__name__" to the result group from the left side by default
            /// unless it's explicitly said that it should be ignored.
            bool copy_metric_name;
            if (operator_node->ignoring)
                copy_metric_name = (std::find(operator_node->labels.begin(), operator_node->labels.end(), kMetricName) == operator_node->labels.end());
            else
                copy_metric_name = !operator_node->on;

            copy_metric_name &= !metric_name_dropped_from_left;
            
            if (copy_metric_name)
            {
                /// timeSeriesCopyTag(join_group, left_argument_group, "__name__")
                *metric_name_dropped_from_result = false;
                return makeASTFunction(
                    "timeSeriesCopyTag", std::move(join_group), std::move(left_argument_group), make_intrusive<ASTLiteral>(kMetricName));
            }
            else
            {
                *metric_name_dropped_from_result = metric_name_dropped_from_join_group;
                return std::move(join_group);
            }
        }
       
        /// If it's not a comparison operator or the bool modifier is specified,
        /// then we always remove the metric name "__name__" from the result group.
        *metric_name_dropped_from_result = true;
        if (metric_name_dropped_from_join_group)
            return std::move(join_group);
        else
            return makeASTFunction("timeSeriesRemoveTag", std::move(join_group), make_intrusive<ASTLiteral>(kMetricName));
    }

    /// Returns an AST to evaluate the result group of a binary operator on instant vectors if "group_left" is specified.
    /// The function usually returns
    /// timeSeriesCopyTags(join_group, right_argument_group, extra_tags)
    /// where `extra_tags` are the tags copied from the right side and specified in expression "group_left(extra_tags)".
    /// Notes:
    /// 1. If `join_group` contains the metric name "__name__" then this function removes it
    ///    because the result of a binary operator shouldn't contain the metric name unless it's copied with `extra_tags`.
    ///    (`join_group` can contain the metric name if it's specified explicitly in ON(),
    ///    for example "http_errors + on (__name__) group_left(code) http_failures")
    /// 2. If "group_left" is specified without `extra_tags` then the function just takes the left argument (and removes the metric name from it).
    /// 3. If it's a comparison operator without bool modifier then the function doesn't remove the metric name from the result group.
    ASTPtr makeExpressionForResultGroup_GroupLeft(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && right_argument_group,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool metric_name_dropped_from_right,
        bool metric_name_dropped_from_join_group,
        bool * metric_name_dropped_from_result)
    {
        /// We use this function to implement both group_left() and group_right().
        chassert(operator_node->group_left || operator_node->group_right);

        Strings tags_to_copy = operator_node->extra_labels;

        if (tags_to_copy.empty())
        {
            /// group_left is used with an empty list of tags to copy.
            if (isComparisonWithoutBool(operator_node) || metric_name_dropped_from_left)
            {
                *metric_name_dropped_from_result = metric_name_dropped_from_left;
                return std::move(left_argument_group);
            }
            else
            {
                /// If it's not a comparison operator or the bool modifier is specified,
                /// then we always remove the metric name "__name__" from the result group.
                *metric_name_dropped_from_result = true;
                return makeASTFunction("timeSeriesRemoveTag", std::move(left_argument_group), make_intrusive<ASTLiteral>(kMetricName));
            }
        }

        std::sort(tags_to_copy.begin(), tags_to_copy.end());
        tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());
        bool copy_metric_name = std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName) && !metric_name_dropped_from_right;

        ASTPtr dest_group = join_group;
        bool metric_name_dropped_from_dest_group = metric_name_dropped_from_join_group;

        if (!metric_name_dropped_from_dest_group && !copy_metric_name && !isComparisonWithoutBool(operator_node))
        {
            /// If it's not a comparison operator or the bool modifier is specified,
            /// then we always remove the metric name "__name__" from the result group.
            dest_group = makeASTFunction("timeSeriesRemoveTag", std::move(dest_group), make_intrusive<ASTLiteral>(kMetricName));
            metric_name_dropped_from_dest_group = true;
        }

        *metric_name_dropped_from_result = metric_name_dropped_from_dest_group && !copy_metric_name;

        return makeASTFunction(
            "timeSeriesCopyTags",
            std::move(dest_group),
            std::move(right_argument_group),
            make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));
    }

    ASTPtr makeExpressionForResultGroup_GroupRight(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && right_argument_group,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool metric_name_dropped_from_right,
        bool metric_name_dropped_from_join_group,
        bool * metric_name_dropped_from_result)
    {
        return makeExpressionForResultGroup_GroupLeft(operator_node,
                                                      std::move(right_argument_group), std::move(left_argument_group), std::move(join_group),
                                                      metric_name_dropped_from_right, metric_name_dropped_from_left, metric_name_dropped_from_join_group,
                                                      metric_name_dropped_from_result);
    }
}

ASTPtr makeExpressionForResultGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && left_argument_group,
    ASTPtr && right_argument_group,
    ASTPtr && join_group,
    bool metric_name_dropped_from_left,
    bool metric_name_dropped_from_right,
    bool metric_name_dropped_from_join_group,
    bool * metric_name_dropped_from_result)
{
    bool dummy;
    if (!metric_name_dropped_from_result)
        metric_name_dropped_from_result = &dummy;

    if (operator_node->group_left)
    {
        return makeExpressionForResultGroup_GroupLeft(operator_node,
                                                      std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                      metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                      metric_name_dropped_from_result);
    }
    else if (operator_node->group_right)
    {
        return makeExpressionForResultGroup_GroupRight(operator_node,
                                                       std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                       metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                       metric_name_dropped_from_result);
    }
    else
    {
        return makeExpressionForResultGroup_Default(operator_node,
                                                    std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                    metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                    metric_name_dropped_from_result);
    }
}

}
