#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class QueryPlan;
class PrometheusQueryTree;
struct StorageID;

/// Builds a query plan to execute this query.
class PrometheusQueryPlanBuilder
{
public:
    PrometheusQueryPlanBuilder(std::shared_ptr<const PrometheusQueryTree> promql_query_, const StorageID & time_series_storage_id_);
    ~PrometheusQueryPlanBuilder();

    void buildQueryPlan(
        QueryPlan & query_plan,
        const ContextPtr & context,
        size_t max_block_size,
        size_t num_streams);

private:
    const std::shared_ptr<const PrometheusQueryTree> promql_query;
    const StorageID time_series_storage_id;
};

}
