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

    /// Per-bucket aggregation data for changes/resets.
    struct AggregationData
    {
        ValueType first_value = 0;
        ValueType last_value = 0;
        UInt64 count = 0;       /// number of samples
        UInt64 changes = 0;     /// number of counted transitions between consecutive samples so far

        /// Whether the transition prev -> curr is counted: a decrease for resets, any change otherwise.
        static bool isCounted(ValueType prev, ValueType curr)
        {
            if constexpr (is_resets)
                return curr < prev;
            else
                return curr != prev;
        }

        /// Samples are always added in ascending timestamp order.
        void add(TimestampType /* timestamp */, ValueType value)
        {
            if (count == 0)
                first_value = value;
            else if (isCounted(last_value, value))
                ++changes;
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
            if (isCounted(last_value, later.first_value))
                ++changes;
            changes += later.changes;
            last_value = later.last_value;
            count += later.count;
        }

        /// Number of changes/resets in the window, or nullopt when there are no samples.
        std::optional<ValueType> getResult() const
        {
            if (count == 0)
                return std::nullopt;
            return static_cast<ValueType>(changes);
        }
    };

    /// Counting transitions between consecutive samples needs them in ascending timestamp order.
    using Aggregator = typename Bucket::SortedAggregator;
};


template <typename Traits>
class AggregateFunctionTimeseriesChanges final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>
{
public:
    static constexpr bool is_resets = Traits::is_resets;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesChanges<Traits>, Traits>;
    using Base::Base;

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;
};

}
