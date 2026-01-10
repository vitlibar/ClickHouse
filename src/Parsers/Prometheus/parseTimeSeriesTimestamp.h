#pragma once

#include <base/Decimal.h>


namespace DB
{
class Field;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Extracts a timestamp from a Field.
DateTime64 parseTimeSeriesTimestamp(const Field & field, UInt32 timestamp_scale);
DateTime64 parseTimeSeriesTimestamp(const Field & field, const DataTypePtr & field_data_type, UInt32 timestamp_scale);

/// Extracts a duration from a Field.
Decimal64 parseTimeSeriesDuration(const Field & field, UInt32 duration_scale);
Decimal64 parseTimeSeriesDuration(const Field & field, const DataTypePtr & field_data_type, UInt32 duration_scale);

}
