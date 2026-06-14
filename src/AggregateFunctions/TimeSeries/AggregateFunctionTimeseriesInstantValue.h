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

    /// Sliding aggregator: `irate`/`idelta` use only the two most recent samples in the window, which is exactly
    /// what `Last2Samples::Data` keeps. Buckets are added in time order, so merging each new bucket keeps the
    /// window's newest two samples; a single `Data` is enough. `removeBucket` drops samples that fell out of the
    /// window (once the newest is out, the whole window is empty).
    struct Aggregator
    {
        Bucket latest;
        TimestampType timestamp_scale_multiplier = 1;

        void addBucket(const Bucket & bucket)
        {
            latest.merge(bucket);
        }

        void removeBucket(TimestampType cut_off)
        {
            /// `timestamps[0]` is the newest sample, `timestamps[1]` the previous one.
            if (latest.filled >= 1 && latest.timestamps[0] <= cut_off)
                latest.filled = 0;
            else if (latest.filled == 2 && latest.timestamps[1] <= cut_off)
                latest.filled = 1;
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            if (latest.filled < 2)
                return std::nullopt;

            const TimestampType timestamp = latest.timestamps[0];
            const ValueType value = latest.values[0];
            const TimestampType previous_timestamp = latest.timestamps[1];
            const ValueType previous_value = latest.values[1];

            const ValueType time_difference = static_cast<ValueType>(timestamp - previous_timestamp);
            if (time_difference == 0)
                return std::nullopt;

            /// Resets are taken into account for `irate` (counter) but not for `idelta` (gauge).
            ValueType value_difference = (is_rate && value < previous_value) ? value : (value - previous_value);
            ValueType result = value_difference;
            if constexpr (is_rate)
            {
                using TimestampScaleMultiplierType = std::conditional_t<std::is_floating_point_v<ValueType>, ValueType, TimestampType>;
                result = result * static_cast<TimestampScaleMultiplierType>(timestamp_scale_multiplier) / time_difference;
            }
            return result;
        }
    };
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

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesInstantValue<Traits>, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator() const
    {
        typename Traits::Aggregator aggregator;
        aggregator.timestamp_scale_multiplier = Base::timestamp_scale_multiplier;
        return aggregator;
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
