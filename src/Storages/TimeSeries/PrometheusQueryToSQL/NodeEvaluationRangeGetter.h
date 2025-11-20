#pragma once

#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>


namespace DB::PrometheusQueryToSQL
{
struct ConverterConfig;

/// Calculates and keeps evaluation ranges for each node in a PrometheusQueryTree.
class NodeEvaluationRangeGetter
{
public:
    NodeEvaluationRangeGetter(const PrometheusQueryTree & promql_tree, const ConverterConfig & config);

    /// Returns the evaluation range for a specific node in a PrometheusQueryTree.
    const NodeEvaluationRange & get(const PrometheusQueryTree::Node * promql_node) const;

private:
    void visitNode(
        const PrometheusQueryTree::Node * node,
        const NodeEvaluationRange & range,
        const PrometheusQueryTree & promql_tree,
        const ConverterConfig & config);

    void visitChildren(
        const PrometheusQueryTree::Node * node,
        const NodeEvaluationRange & range,
        const PrometheusQueryTree & promql_tree,
        const ConverterConfig & config);

    std::unordered_map<const PrometheusQueryTree::Node *, NodeEvaluationRange> map;
};

}
