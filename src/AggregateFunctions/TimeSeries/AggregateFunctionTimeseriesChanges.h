#pragma once

#include <cstddef>
#include <cstring>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <optional>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/DequeWithMemoryTracking.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_resets_>
struct AggregateFunctionTimeseriesChangesTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_resets = is_resets_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_resets ? "timeSeriesResetsToGrid" : "timeSeriesChangesToGrid";
    }

    using Bucket = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Sliding aggregator for changes/resets. Each bucket is preaggregated to its first/last value, count and
    /// internal transition count (one `BucketSummary`), kept in a time-ordered deque. The window's total count and
    /// transition count (including the transition across each adjacent bucket boundary) are maintained
    /// incrementally so `getResult` is O(1).
    struct Aggregator
    {
        struct BucketSummary
        {
            ValueType first_value = 0;
            ValueType last_value = 0;
            TimestampType last_timestamp = 0;
            UInt64 count = 0;
            UInt64 changes = 0;     /// counted transitions within this bucket
        };

        DequeWithMemoryTracking<BucketSummary> deque;
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> temp_buffer;  /// reused sort buffer
        UInt64 total_count = 0;
        UInt64 total_changes = 0;

        /// Whether the transition prev -> curr is counted: a decrease for resets, any change otherwise.
        static bool isCounted(ValueType prev, ValueType curr)
        {
            if constexpr (is_resets)
                return curr < prev;
            else
                return curr != prev;
        }

        void addBucket(const Bucket & bucket)
        {
            BucketSummary summary{};
            bucket.forEachSampleSorted([&summary](TimestampType timestamp, ValueType value)
            {
                if (summary.count == 0)
                    summary.first_value = value;
                else if (isCounted(summary.last_value, value))
                    ++summary.changes;
                summary.last_value = value;
                summary.last_timestamp = timestamp;
                ++summary.count;
            }, temp_buffer);

            if (summary.count == 0)
                return;

            if (!deque.empty() && isCounted(deque.back().last_value, summary.first_value))
                ++total_changes;        /// transition across the bucket boundary
            total_changes += summary.changes;
            total_count += summary.count;
            deque.push_back(summary);
        }

        void removeBucket(TimestampType cut_off)
        {
            while (!deque.empty() && deque.front().last_timestamp <= cut_off)
            {
                const BucketSummary front = deque.front();
                total_changes -= front.changes;
                total_count -= front.count;
                deque.pop_front();
                if (!deque.empty() && isCounted(front.last_value, deque.front().first_value))
                    --total_changes;    /// drop the transition across the boundary
            }
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/) const
        {
            if (total_count == 0)
                return std::nullopt;
            return static_cast<ValueType>(total_changes);
        }
    };
};


template <typename Traits>
class AggregateFunctionTimeseriesChanges final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>
{
public:
    static constexpr bool is_resets = Traits::is_resets;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>;
    using Base::Base;

    typename Traits::Aggregator createAggregator() const
    {
        return {};
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;
};

}
