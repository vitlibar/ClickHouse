#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTimestamp.h>

#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    DecimalField<Decimal64> extractFromNode(const PrometheusQueryTree::Node * node, UInt32 default_scale, bool extract_interval)
    {
        switch (node->node_type)
        {
            case PrometheusQueryTree::NodeType::IntervalLiteral:
            {
                return static_cast<const PrometheusQueryTree::IntervalLiteral *>(node)->interval;
            }
            case PrometheusQueryTree::NodeType::ScalarLiteral:
            {
                auto scalar = static_cast<const PrometheusQueryTree::ScalarLiteral *>(node)->scalar;
                auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                return DecimalField<Decimal64>{static_cast<Int64>(scalar * scale_multiplier + 0.5), default_scale};
            }
            default:
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Cannot parse node of type {} as a {}",
                                node->node_type,
                                extract_interval ? "time interval" : "timestamp");
            }
        }
    }
}

DecimalField<DateTime64> nodeToTimestamp(const PrometheusQueryTree::Node * scalar_or_interval_node, UInt32 default_scale)
{
    auto res = extractFromNode(scalar_or_interval_node, default_scale, /* extract_interval = */ false);
    return DecimalField<DateTime64>{res.getValue(), res.getScale()};
}

DecimalField<Decimal64> nodeToInterval(const PrometheusQueryTree::Node * scalar_or_interval_node, UInt32 default_scale)
{
    return extractFromNode(scalar_or_interval_node, default_scale, /* extract_interval = */ true);
}

}
