#pragma once

#include <cstddef>
#include <cstring>
#include <optional>


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
        bool has_value = false;

        void add(TimestampType timestamp, ValueType value)
        {
            if (!has_value || timestamp > first || (timestamp == first && value > second))
            {
                first = timestamp;
                second = value;
                has_value = true;
            }
        }

        void merge(const Bucket & other)
        {
            if (other.has_value)
                add(other.first, other.second);
        }
    };

    /// Per-bucket aggregation data. The bucket already keeps the most recent sample,
    /// so the aggregation data just inherits it and adds the result computation.
    struct AggregationData : Bucket
    {
        std::optional<ValueType> getResult() const
        {
            if (!this->has_value)
                return std::nullopt;
            return this->second;
        }
    };

    /// The bucket already keeps the latest sample, so it is merged directly (no aggregation pass).
    using Aggregator = void;
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
    using AggregationData = typename Traits::AggregationData;

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

        bucket.add(timestamp, value);
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
};

}
