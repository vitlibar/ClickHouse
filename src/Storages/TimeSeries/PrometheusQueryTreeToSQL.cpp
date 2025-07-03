#include <Storages/TimeSeries/PrometheusQueryTreeToSQL.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    class PromQLToSQLConverter
    {
    public:
        ASTPtr nodeToSQL(const PrometheusQueryTree::Node * node)
        {
            auto node_type = node->node_type;
            switch (node_type)
            {
                case NodeType::InstantSelector:
                    return instantSelectorToSQL(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));
            
                case NodeType::RangeSelector:
                case NodeType::At:
                case NodeType::Subquery:
                    break;
            }
        }

    private:
        /// Converts an instant or range selector to AST query.
        /// http_requests{job="prometheus"} ->
        ///     SELECT timeSeriesIdToGroup(id) AS group, timeSeriesGridLast()(timestamp, value) FROM timeSeriesSelector(...) GROUP BY group
        /// http_requests{job="prometheus"}[10m] ->
        ///     SELECT timeSeriesIdToGroup(id) AS group, timestamp, value FROM timeSeriesSelector(...)
        ASTPtr instantSelectorToSQL(const PrometheusQueryTree::InstantSelector * instant_selector)
        {
            std::optional<TimestampType> time;
            IntervalType time_offset = 0;
            InternalType window = lookback_delta;
            IntervalType range = 0;
            IntervalType step = 0;

            for (const auto * parent = instant_selector->parent; parent; parent = parent->parent)
            {
                if (parent->node_type == NodeType::RangeSelector)
                {
                    /// instant_selector[range]
                    const auto * range_selector = typeid_cast<const PrometheusQueryTree::RangeSelector *>(parent);
                    window = range_selector->range;
                }
                else if (parent->node_type == NodeType::At)
                {
                    /// expression @time offset +-<delta>
                    const auto * at_node = typeid_cast<const PrometheusQueryTree::At *>(parent);
                    if (!time)
                    {
                        if (at_node->at)
                            time = *at_node->at;
                        time_offset -= at_node->offset;
                    }
                }
                else if (parent->node_type == NodeType::Subquery)
                {
                    const auto * subquery = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                    range += subquery->range;
                    if (!step)
                    {
                        if (subquery->resolution)
                            step = subquery->resolution;
                        else
                            step = default_resolution;
                    }
                }
            }

            if (!time)
                time = evaluation_time;
            time += time_offset;
        }

        PrometheusQueryTree promql;
        StorageID time_series_table_id;
        TimestampType evaluation_time;
        OffsetType lookback_delta;
        OffsetType default_resolution;

        ASTPtr makeSelector
    };
}

ASTPtr prometheusQueryToSQL(const PrometheusQueryTree & promql,
                            const StorageID & time_series_table_id,
                            const PrometheusQueryTree::Timestamp & evaluation_time,
                            const PrometheusQueryTree::OffsetType & lookback_delta)
{
    if (promql.empty())
        throw Exception();

    const auto * node = promql.getRoot();
    return PromQLToSQLConverter{promql, time_series_table_id, lookback_delta}.nodeToSQL(promql.getRoot(), evaluation_time);

}

}
