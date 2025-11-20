#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>


namespace DB
{
    class ColumnsDescription;
}

namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;
using ResultType = PrometheusQueryResultType;

/// Returns description of the columns returned by the query built by function finalizeSQL().
ColumnsDescription getResultColumns(ResultType result_type, const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type);
ColumnsDescription getResultColumns(ResultType result_type, const ConverterContext & context);

}
