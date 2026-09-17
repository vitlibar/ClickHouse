#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Returns the scale of the timestamps in the results of PromQL evaluation.
/// It's the scale of the timestamp column of the TimeSeries table, but not less than 3 (milliseconds).
/// The same scale is used for all timestamps and durations while parsing and evaluating the query
/// (see `PrometheusQueryEvaluationSettings::time_scale`).
UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type);

/// Returns the data type of the timestamps in the results of PromQL evaluation: DateTime64 with the scale getPromQLResultTimestampScale()
/// and with the same time zone as the timestamp column of the TimeSeries table (if it has one).
DataTypePtr getPromQLResultTimestampType(const DataTypePtr & table_timestamp_type);

}
