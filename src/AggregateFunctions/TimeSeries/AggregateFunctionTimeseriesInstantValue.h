#pragma once

#include <cstddef>
#include <cstring>
#include <optional>
#include <type_traits>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionLast2Samples.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesInstantValueTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_rate_ ? "timeSeriesInstantRateToGrid" : "timeSeriesInstantDeltaToGrid";
    }

    using Bucket = typename AggregateFunctionLast2Samples<TimestampType, ValueType>::Data;

    /// The bucket already keeps the last two samples and serves as the aggregation data.
    using AggregationData = Bucket;
};


/// Aggregate function to calculate instant values (irate and idelta) of timeseries on the specified grid
template <typename Traits>
class AggregateFunctionTimeseriesInstantValue final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue<Traits>, Traits>
{
public:
    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;
    using AggregationData = typename Traits::AggregationData;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue<Traits>, Traits>;
    using Base::Base;

    std::optional<ValueType> finalizeAggregation(const AggregationData & aggregate, TimestampType /*grid_timestamp*/) const
    {
        if (aggregate.filled < 2)
            return std::nullopt;

        const TimestampType timestamp = aggregate.timestamps[0];
        const ValueType value = aggregate.values[0];
        const TimestampType previous_timestamp = aggregate.timestamps[1];
        const ValueType previous_value = aggregate.values[1];

        const ValueType time_difference = static_cast<ValueType>(timestamp - previous_timestamp);
        if (time_difference == 0)
            return std::nullopt;

        /// Resets are taken into account for `irate` (counter) but not for `idelta` (gauge).
        ValueType value_difference = (is_rate && value < previous_value) ? value : (value - previous_value);
        ValueType result = value_difference;
        if constexpr (is_rate)
        {
            using TimestampScaleMultiplierType = std::conditional_t<std::is_floating_point_v<ValueType>, ValueType, TimestampType>;
            result = result * static_cast<TimestampScaleMultiplierType>(Base::timestamp_scale_multiplier) / time_difference;
        }
        return result;
    }

    /// `merge` replays up to two samples through the branchy `Last2Samples::add`, so it is relatively costly
    /// and the two-stack queue wins even for narrow windows.
    static constexpr size_t TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD = 10;

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
