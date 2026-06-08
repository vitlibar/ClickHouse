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

    /// Per-bucket aggregation data. The bucket already keeps the last two samples,
    /// so the aggregation data just inherits it and adds the result computation.
    struct AggregationData : Bucket
    {
        std::optional<ValueType> getResult(TimestampType timestamp_scale_multiplier) const
        {
            if (this->filled < 2)
                return std::nullopt;

            const TimestampType timestamp = this->timestamps[0];
            const ValueType value = this->values[0];
            const TimestampType previous_timestamp = this->timestamps[1];
            const ValueType previous_value = this->values[1];

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

    /// The bucket already keeps the last two samples, so it is merged directly (no aggregation pass).
    using Aggregator = void;
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
        return aggregate.getResult(Base::timestamp_scale_multiplier);
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
