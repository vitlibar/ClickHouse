#pragma once

#include <cstddef>
#include <cstring>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>

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

    /// Per-bucket aggregation data for rate/delta.
    struct AggregationData
    {
        TimestampType first_timestamp = 0;
        ValueType first_value = 0;
        TimestampType last_timestamp = 0;
        ValueType last_value = 0;
        UInt64 count = 0;       /// number of samples
        Float64 resets = 0;     /// accumulated reset adjustment (rate only): sum of pre-reset values on each decrease

        void add(TimestampType timestamp, ValueType value)
        {
            if (count == 0)
            {
                first_timestamp = timestamp;
                first_value = value;
            }
            else if constexpr (is_rate)
            {
                /// Resets are taken into account for `rate` (counter that only increases); a decrease means a reset.
                if (last_value > value)
                    resets += static_cast<Float64>(last_value);
            }
            last_timestamp = timestamp;
            last_value = value;
            ++count;
        }

        /// `later` holds the samples that come right after this aggregate's samples in time.
        void merge(const AggregationData & later)
        {
            if (later.count == 0)
                return;
            if (count == 0)
            {
                *this = later;
                return;
            }
            if constexpr (is_rate)
            {
                if (last_value > later.first_value)
                    resets += static_cast<Float64>(last_value);
            }
            resets += later.resets;
            last_timestamp = later.last_timestamp;
            last_value = later.last_value;
            count += later.count;
        }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
        std::optional<ValueType> getResult(TimestampType grid_timestamp, IntervalType window, TimestampType timestamp_scale_multiplier) const
        {
            /// Need at least two samples to calculate the rate or delta.
            if (count < 2)
                return std::nullopt;

            /// The extrapolation logic is copied from Prometheus' rate calculation
            /// (https://github.com/prometheus/prometheus/blob/5e124cf4f2b9467e4ae1c679840005e727efd599/promql/functions.go#L127),
            /// licensed under the Apache License 2.0.
            const TimestampType time_difference = last_timestamp - first_timestamp;
            if (time_difference == 0)
                return std::nullopt;

            Float64 value_difference = last_value - first_value + resets;

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
            const Float64 average_duration_between_samples = static_cast<Float64>(sampled_interval) / static_cast<Float64>(count - 1);

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

    /// First/last boundary samples and reset accounting need the samples in ascending timestamp order.
    using BucketAggregator = typename Bucket::SortedAggregator;
};

/// Aggregate function to calculate extrapolated values (rate and delta) of timeseries on the specified grid
template <typename Traits>
class AggregateFunctionTimeseriesExtrapolatedValue final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_rate = Traits::is_rate;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesExtrapolatedValue<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;
    using AggregationData = typename Traits::AggregationData;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.samples.size(), buf);
        for (const auto & sample : bucket.samples)
        {
            writeBinaryLittleEndian(sample.first, buf);
            writeBinaryLittleEndian(sample.second, buf);
        }
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count,buf);
        bucket.samples.reserve(sample_count);

        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            Base::checkTimestampInRange(timestamp, bucket_index);

            ValueType value;
            readBinaryLittleEndian(value, buf);

            bucket.add(timestamp, value);
        }
    }

    std::optional<ValueType> finalizeAggregation(const AggregationData & aggregate, TimestampType grid_timestamp) const
    {
        return aggregate.getResult(grid_timestamp, Base::window, Base::timestamp_scale_multiplier);
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
