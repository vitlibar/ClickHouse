#include <Storages/StoragePrometheusQuery.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryPlanBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryResultColumnsDesc.h>


namespace DB
{

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    std::shared_ptr<const PrometheusQueryTree> promql_query_,
    const StorageID & time_series_storage_id_,
    const ContextPtr & local_context)
    : IStorage(table_id_)
    , promql_query(promql_query_)
    , time_series_storage_id(time_series_storage_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getPrometheusQueryResultColumnsDesc(*promql_query, time_series_storage_id, local_context));
    setInMemoryMetadata(storage_metadata);
}


void StoragePrometheusQuery::read(
    QueryPlan & /*query_plan*/,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
#if 0
    buildQueryPlanForPrometheusQuery(query_plan, *parsed_promql_query, time_series_storage_id, context, max_block_size, num_streams);
#endif
}

}
