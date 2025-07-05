#include <Storages/TimeSeries/PrometheusQueryToSQLConverter.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class PrometheusQueryToSQLConverter::ASTBuilder
{
public:
    ASTBuilder(const PrometheusQueryToSQLConverter & )

    

private:
    ASTPtr convertSelector(const PrometheusQueryTree::InstantSelector * instant_selector)
    {

    }
};


PrometheusQueryToSQLConverter::PrometheusQueryToSQLConverter(
    const PrometheusQueryTree & promql_,
    const EvaluationTimeType & evaluation_time_,
    const TimeSeriesTableInfo & time_series_table_info_,
    const IntervalType & lookback_delta_,
    const IntervalType & default_resolution_)
    : promql(promql_)
    , evaluation_time(evaluation_time_)
    , time_series_table_info(time_series_table_info_)
    , lookback_delta(lookback_delta_)
    , default_resolution(default_resolution_)
{
}

ASTPtr PrometheusQueryToSQLConverter::getSQL() const
{

}

ColumnsWithTypesAndNames PrometheusQueryToSQLConverter::getResultColumns() const
{

}




namespace
{



    template <typename TimestampType, typename IntervalType>
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
            }
        }

    private:
        /// Converts a selector 
        ASTPtr instantSelectorToSQL(const PrometheusQueryTree::InstantSelector * instant_selector)
        {
            std::optional<TimestampType> evaluation_time;
            IntervalType evaluation_time_offset = 0;
            TimestampType range = lookback_delta;

            for (const auto * parent = instant_selector->parent; parent; parent = parent->parent)
            {
                if (parent->node_type == NodeType::RangeSelector)
                {
                    const auto * range_selector = typeid_cast<const PrometheusQueryTree::RangeSelector *>(parent);
                    range = range_selector->range;
                }
                else if (parent->node_type == NodeType::At)
                {
                    const auto * at_parent = typeid_cast<const PrometheusQueryTree::At *>(parent);
                    if (at_parent->at && !evaluation_time)
                        evaluation_time = *at_parent->at;
                    if ()
                }
                else if (parent->node_type == NodeType::Subquery)
                {
                    const auto * subquery_parent = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                    range = subquery_parent->
                }
            }
        }

        PrometheusQueryTree promql;
        StorageID time_series_table_id;
        TimestampType evaluation_time;
        OffsetType lookback_delta;

        ASTPtr makeSelector
    };
}

template <typename TimestampType, typename IntervalType>
ASTPtr prometheusQueryToSQL(const PrometheusQueryTree & promql,
                            const StorageID & time_series_table_id,
                            const TimestampType & evaluation_time,
                            const IntervalType & lookback_delta,
                            const IntervalType & default_resolution);

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
