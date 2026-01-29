#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB
{
    struct StorageInMemoryMetadata;
    using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
}


namespace DB::PrometheusQueryToSQL
{

/// Returns description of the columns returned by the query built by function finalizeSQL().
ResultType getResultType(const PrometheusQueryTree & promql_tree, const PrometheusQueryEvaluationSettings & settings);

}
