#pragma once

#include <cstddef>
#include <cstring>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_rate_>
struct AggregateFunctionTimeseriesToGridSparseTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_rate = is_rate_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return "timeSeriesResampleToGridWithStaleness";
    }

    struct Bucket
    {
        TimestampType first = 0;
        ValueType second = 0;

        void add(TimestampType timestamp, ValueType value)
        {
            if (timestamp > first || (timestamp == first && value > second))
            {
                first = timestamp;
                second = value;
            }
        }

        void merge(const Bucket & other)
        {
            add(other.first, other.second);
        }
    };
};


/// Aggregate function to convert timeseries to the specified grid with staleness
/// Missing values are filled with NULLs
template <typename Traits>
class AggregateFunctionTimeseriesToGridSparse final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static_assert(Traits::is_rate == false, "AggregateFunctionTimeseriesToGridSparse does not have rate version");

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;

    static void serializeBucket(const Bucket & bucket, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(bucket.first, buf);
        writeBinaryLittleEndian(bucket.second, buf);
    }

    void deserializeBucket(Bucket & bucket, ReadBuffer & buf, const size_t bucket_index) const
    {
        TimestampType timestamp;
        readBinaryLittleEndian(timestamp, buf);
        Base::checkTimestampInRange(timestamp, bucket_index);

        ValueType value;
        readBinaryLittleEndian(value, buf);

        bucket = {timestamp, value};
    }

    /// Insert the result into the column
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.empty() ? Base::grid_size : offsets_to.back() + Base::grid_size);

        if (!Base::grid_size)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<typename Base::ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + Base::grid_size);
        nulls_to.resize(old_size + Base::grid_size);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = Base::data(place)->buckets;

        bool has_previous_value = false;
        ValueType previous_value = {};
        TimestampType previous_timestamp = {};

        for (size_t i = 0; i < Base::grid_size; ++i)
        {
            /// Compute `grid_timestamp` via `Base::timestampAtIndex` rather than with a
            /// loop-carried `grid_timestamp += Base::step`. The accumulator form performed
            /// one final, unused `+=` on the last iteration which signed-overflowed
            /// `TimestampType` (e.g. `Decimal<Int64>::operator+=`) when `start_timestamp` was
            /// near `INT64_MIN` and `step` was near `INT64_MAX`, triggering UBSAN.
            const TimestampType grid_timestamp = Base::timestampAtIndex(i);

            /// Update the most recent sample from the buckets owned by this grid point. They are in
            /// ascending time order, so the last non-empty one holds the most recent sample.
            for (size_t bucket_index : Base::bucketRangeForGridPoint(i))
            {
                auto bucket_it = buckets.find(bucket_index);
                if (bucket_it != buckets.end())
                {
                    has_previous_value = true;
                    previous_value = bucket_it->second.second;
                    previous_timestamp = bucket_it->second.first;
                }
            }

            /// The most recent sample may be within the staleness window or not.
            if (has_previous_value && !Base::isSampleOutOfWindow(previous_timestamp, grid_timestamp))
            {
                values[i] = previous_value;
                nulls[i] = 0;
            }
            else
            {
                values[i] = ValueType{};
                nulls[i] = 1;
            }
        }
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
