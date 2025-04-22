#include <Storages/StoragePrometheusQuery.h>

#include <Storages/TimeSeries/ParsedPrometheusQuery.h>
#include <Storages/TimeSeries/getPrometheusQueryOutputColumnsDesc.h>
#include <Storages/TimeSeries/buildQueryPlanForPrometheusQuery.h>


namespace DB
{

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    std::shared_ptr<const ParsedPrometheusQuery> parsed_promql_query_,
    const StorageID & time_series_storage_id_,
    const ContextPtr & local_context)
    : IStorage(table_id_)
    , parsed_promql_query(parsed_promql_query_)
    , time_series_storage_id(time_series_storage_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getPrometheusQueryOutputColumnsDesc(*parsed_promql_query, time_series_storage_id, local_context));
    setInMemoryMetadata(storage_metadata);
}


void StoragePrometheusQuery::read(
    QueryPlan & query_plan,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    buildQueryPlanForPrometheusQuery(query_plan, *parsed_promql_query, time_series_storage_id, context, max_block_size, num_streams);
}

}
