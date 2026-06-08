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

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

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

        void serialize(WriteBuffer & buf) const
        {
            writeBinary(has_value, buf);
            writeBinaryLittleEndian(first, buf);
            writeBinaryLittleEndian(second, buf);
        }

        void deserialize(ReadBuffer & buf)
        {
            readBinary(has_value, buf);
            readBinaryLittleEndian(first, buf);
            readBinaryLittleEndian(second, buf);
        }

        void checkTimestamps(TimestampType start_time, TimestampType end_time) const
        {
            if (has_value && (first <= start_time || first > end_time))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range ({}, {}]",
                    static_cast<Int64>(first), static_cast<Int64>(start_time), static_cast<Int64>(end_time));
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
    static_assert(Traits::is_rate == false, "AggregateFunctionTimeseriesToGridSparse does not have rate version");

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesToGridSparse<Traits>, Traits>;
    using Base::Base;

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
