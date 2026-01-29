#include <Storages/TimeSeries/PrometheusQueryToSQL/alignTimestamp.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::PrometheusQueryToSQL
{

TimestampType alignTimestampUp(TimestampType timestamp, DurationType step)
{
    if (step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = timestamp % step;
    if (!x)
        return timestamp;
    return timestamp + step - x;
}


TimestampType alignTimestampDown(TimestampType timestamp, DurationType step)
{
    if (step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = timestamp % step;
    if (!x)
        return timestamp;
    return timestamp - x;
}

}
