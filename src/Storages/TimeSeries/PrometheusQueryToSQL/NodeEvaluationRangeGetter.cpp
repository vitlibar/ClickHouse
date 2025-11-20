#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterConfig.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTimestamp.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

NodeEvaluationRangeGetter::NodeEvaluationRangeGetter(const PrometheusQueryTree & promql_tree, const ConverterConfig & config)
{
    const auto * root = promql_tree.getRoot();
    if (root)
    {
        if (!config.evaluation_time.isNull())
        {
            auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
            auto timestamp = getTimeSeriesTimestamp(config.evaluation_time, timestamp_scale);
            auto lookback_delta = getTimeSeriesInterval(config.lookback_delta, timestamp_scale);
            NodeEvaluationRange range{.start_time = timestamp, .end_time = timestamp, .step = DecimalField<Decimal64>{}, .window = lookback_delta};
            visitNode(root, range, promql_tree, config);
        }
        else if (!config.evaluation_range.isNull())
        {
            auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
            auto start_time = getTimeSeriesTimestamp(config.evaluation_range.start_time, timestamp_scale);
            auto end_time = getTimeSeriesTimestamp(config.evaluation_range.end_time, timestamp_scale);
            auto step = getTimeSeriesInterval(config.evaluation_range.step, timestamp_scale);
            auto lookback_delta = getTimeSeriesInterval(config.lookback_delta, timestamp_scale);
            NodeEvaluationRange range{.start_time = start_time, .end_time = end_time, .step = step, .window = lookback_delta};
            visitNode(root, range, promql_tree, config);
        }
    }
}


void NodeEvaluationRangeGetter::visitNode(
    const PrometheusQueryTree::Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryTree & promql_tree,
    const ConverterConfig & config)
{
    if (node->node_type == PrometheusQueryTree::NodeType::RangeSelector)
    {
        const auto * range_selector_node = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
        auto range_with_corrected_window = range;
        auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
        range_with_corrected_window.window = nodeToInterval(range_selector_node->getRange(), timestamp_scale);
        map[node] = range_with_corrected_window;
        visitChildren(node, range_with_corrected_window, promql_tree, config);
    }
    else
    {
        map[node] = range;
        visitChildren(node, range, promql_tree, config);
    }
}


void NodeEvaluationRangeGetter::visitChildren(
    const PrometheusQueryTree::Node * node,
    const NodeEvaluationRange & range,
    const PrometheusQueryTree & promql_tree,
    const ConverterConfig & config)
{
    switch (node->node_type)
    {
        case PrometheusQueryTree::NodeType::At:
        {
            const auto * at_node = static_cast<const PrometheusQueryTree::At *>(node);
            const auto * expression = at_node->getExpression();
            NodeEvaluationRange expression_range = range;
            if (const auto * at = at_node->getAt())
            {
                auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
                auto timestamp = nodeToTimestamp(at, timestamp_scale);
                if (const auto * offset = at_node->getOffset())
                {
                    auto offset_value = nodeToInterval(offset, timestamp_scale);
                    timestamp = subtractTimeSeriesInterval(timestamp, offset_value);
                }
                expression_range.start_time = timestamp;
                expression_range.end_time = timestamp;
            }
            else if (const auto * offset = at_node->getOffset())
            {
                auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
                auto offset_value = nodeToInterval(offset, timestamp_scale);
                expression_range.start_time = subtractTimeSeriesInterval(expression_range.start_time, offset_value);
                expression_range.end_time = subtractTimeSeriesInterval(expression_range.end_time, offset_value);
            }
            visitNode(expression, expression_range, promql_tree, config);
            break;
        }

        case PrometheusQueryTree::NodeType::Subquery:
        {
            const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
            auto timestamp_scale = getTimeSeriesTimestampScale(config.timestamp_type);
            auto subquery_range = nodeToInterval(subquery_node->getRange(), timestamp_scale);

            DecimalField<Decimal64> subquery_step;
            if (const auto * resolution_node = subquery_node->getResolution())
                subquery_step = nodeToInterval(resolution_node, timestamp_scale);
            else
                subquery_step = getTimeSeriesInterval(config.default_resolution, timestamp_scale);

            const auto * expression = subquery_node->getExpression();
            NodeEvaluationRange expression_range = range;

            expression_range.end_time = roundDownTimeSeriesTimestamp(range.end_time, subquery_step);

            auto unaligned_start_time = subtractTimeSeriesInterval(range.start_time, subquery_range);
            expression_range.start_time = roundUpTimeSeriesTimestamp(unaligned_start_time, subquery_step);
            if (expression_range.start_time == unaligned_start_time)
                expression_range.start_time = addTimeSeriesInterval(unaligned_start_time, subquery_step);

            expression_range.step = subquery_step;

            visitNode(expression, expression_range, promql_tree, config);
            break;
        }

        default:
        {
            for (const auto * child : node->children)
                visitNode(child, range, promql_tree, config);
        }
    }
}


const NodeEvaluationRange & NodeEvaluationRangeGetter::get(const PrometheusQueryTree::Node * promql_node) const
{
    auto it = map.find(promql_node);
    if (it == map.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found node {} in NodeEvaluationRangeGetter", promql_node->node_type);
    return it->second;
}

}
