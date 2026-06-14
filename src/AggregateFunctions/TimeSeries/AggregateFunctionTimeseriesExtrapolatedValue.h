#pragma once

#include <cstddef>
#include <cstring>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/DequeWithMemoryTracking.h>

#include <optional>

namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesExtrapolatedValueTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_rate ? "timeSeriesRateToGrid" : "timeSeriesDeltaToGrid";
    }

    using Bucket = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Sliding aggregator for rate/delta. Each bucket is preaggregated to its first/last sample, count and
    /// internal reset adjustment (one `BucketSummary`), kept in a time-ordered deque. The window's total count and
    /// reset adjustment (which includes the cross-bucket resets between adjacent buckets) are maintained
    /// incrementally so `getResult` is O(1): it reads the window's first sample (deque front) and last sample
    /// (deque back).
    struct Aggregator
    {
        struct BucketSummary
        {
            TimestampType first_timestamp = 0;
            ValueType first_value = 0;
            TimestampType last_timestamp = 0;
            ValueType last_value = 0;
            UInt64 count = 0;
            Float64 resets = 0;     /// reset adjustment within this bucket (rate only)
        };

        DequeWithMemoryTracking<BucketSummary> deque;
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> temp_buffer;  /// reused sort buffer
        UInt64 total_count = 0;
        Float64 total_resets = 0;
        IntervalType window = 0;
        TimestampType timestamp_scale_multiplier = 1;

        /// The reset adjustment is the sum of pre-decrease values: `rate` expects a counter that only increases,
        /// so a decrease between consecutive samples means a reset. `delta` (a gauge) does not count resets.
        void addBucket(const Bucket & bucket)
        {
            BucketSummary summary{};
            bucket.forEachSampleSorted([&summary](TimestampType timestamp, ValueType value)
            {
                if (summary.count == 0)
                {
                    summary.first_timestamp = timestamp;
                    summary.first_value = value;
                }
                else if constexpr (is_rate)
                {
                    if (summary.last_value > value)
                        summary.resets += static_cast<Float64>(summary.last_value);
                }
                summary.last_timestamp = timestamp;
                summary.last_value = value;
                ++summary.count;
            }, temp_buffer);

            if (summary.count == 0)
                return;

            if constexpr (is_rate)
            {
                if (!deque.empty() && deque.back().last_value > summary.first_value)
                    total_resets += static_cast<Float64>(deque.back().last_value);  /// reset across the bucket boundary
            }
            total_resets += summary.resets;
            total_count += summary.count;
            deque.push_back(summary);
        }

        void removeBucket(TimestampType cut_off)
        {
            while (!deque.empty() && deque.front().last_timestamp <= cut_off)
            {
                const BucketSummary front = deque.front();
                total_resets -= front.resets;
                total_count -= front.count;
                deque.pop_front();
                if constexpr (is_rate)
                {
                    if (!deque.empty() && front.last_value > deque.front().first_value)
                        total_resets -= static_cast<Float64>(front.last_value);  /// drop the cross-boundary reset
                }
            }
        }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
        std::optional<ValueType> getResult(TimestampType grid_timestamp) const
        {
            /// Need at least two samples to calculate the rate or delta.
            if (total_count < 2)
                return std::nullopt;

            const TimestampType first_timestamp = deque.front().first_timestamp;
            const ValueType first_value = deque.front().first_value;
            const TimestampType last_timestamp = deque.back().last_timestamp;
            const ValueType last_value = deque.back().last_value;

            /// The extrapolation logic is copied from Prometheus' rate calculation
            /// (https://github.com/prometheus/prometheus/blob/5e124cf4f2b9467e4ae1c679840005e727efd599/promql/functions.go#L127),
            /// licensed under the Apache License 2.0.
            const TimestampType time_difference = last_timestamp - first_timestamp;
            if (time_difference == 0)
                return std::nullopt;

            Float64 value_difference = last_value - first_value + total_resets;

            // Duration between first/last samples and boundary of range. Subtract in `Int128` first to avoid
            // both signed overflow on `grid_timestamp - window` and `Float64` precision loss when timestamps
            // are large (e.g. `DateTime64(9)` near present-day epoch ~1.7e18).
            Float64 duration_to_start = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(first_timestamp))
                - static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                + static_cast<Int128>(static_cast<Int64>(window)));
            Float64 duration_to_end = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp))
                - static_cast<Int128>(static_cast<Int64>(last_timestamp)));

            const auto sampled_interval = time_difference;
            const Float64 average_duration_between_samples = static_cast<Float64>(sampled_interval) / static_cast<Float64>(total_count - 1);

            // If samples are close enough to the (lower or upper) boundary of the range, we extrapolate the
            // rate all the way to the boundary in question. "Close enough" is up to 10% more than the average
            // duration between samples within the range; otherwise we extrapolate by only half of the average
            // duration between samples (our guess for where the series actually starts or ends).
            const auto extrapolation_threshold = average_duration_between_samples * 1.1;
            Float64 extrapolate_to_interval = static_cast<Float64>(sampled_interval);

            if (duration_to_start >= extrapolation_threshold)
                duration_to_start = average_duration_between_samples / 2;

            if (is_rate && value_difference > 0 && first_value >= 0)
            {
                // Counters cannot be negative. If we have any slope at all we can extrapolate the zero point
                // of the counter; if that is closer than duration_to_start, take it as the start, avoiding
                // extrapolation to negative counter values.
                Float64 duration_to_zero = static_cast<Float64>(sampled_interval) * (first_value / value_difference);
                duration_to_start = std::min(duration_to_zero, duration_to_start);
            }

            extrapolate_to_interval += duration_to_start;

            if (duration_to_end >= extrapolation_threshold)
                duration_to_end = average_duration_between_samples / 2;
            extrapolate_to_interval += duration_to_end;

            Float64 factor = extrapolate_to_interval / static_cast<Float64>(sampled_interval);

            if constexpr (is_rate)
                factor = factor * static_cast<Float64>(timestamp_scale_multiplier) / static_cast<Float64>(window);

            value_difference *= factor;

            return static_cast<ValueType>(value_difference);
        }
#pragma clang diagnostic pop
    };
};


/// Aggregate function to calculate extrapolated values (rate and delta) of timeseries on the specified grid
template <typename Traits>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>
{
public:
    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator() const
    {
        typename Traits::Aggregator aggregator;
        aggregator.window = Base::window;
        aggregator.timestamp_scale_multiplier = Base::timestamp_scale_multiplier;
        return aggregator;
    }

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
