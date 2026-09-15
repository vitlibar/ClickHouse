#include <Storages/TimeSeries/getPromQLResultTimestampType.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>


namespace DB
{

namespace
{
    /// Prometheus uses millisecond timestamps, so the results of PromQL evaluation never use a scale less than 3.
    constexpr UInt32 MIN_RESULT_TIMESTAMP_SCALE = 3;

    /// Returns the time zone of the timestamp column of the TimeSeries table,
    /// or an empty string if the column has no time zone (for example, if its type is UInt32).
    String getTimeZone(const DataTypePtr & table_timestamp_type)
    {
        /// getDateTimeTimezone() throws for types other than DateTime and DateTime64.
        if (!WhichDataType{table_timestamp_type}.isDateTimeOrDateTime64())
            return {};
        return getDateTimeTimezone(*table_timestamp_type);
    }
}


UInt32 getPromQLResultTimestampScale(const DataTypePtr & table_timestamp_type)
{
    UInt32 table_timestamp_scale = tryGetDecimalScale(*table_timestamp_type).value_or(0);
    return std::max(table_timestamp_scale, MIN_RESULT_TIMESTAMP_SCALE);
}


DataTypePtr getPromQLResultTimestampType(const DataTypePtr & table_timestamp_type)
{
    return std::make_shared<DataTypeDateTime64>(getPromQLResultTimestampScale(table_timestamp_type), getTimeZone(table_timestamp_type));
}

}
