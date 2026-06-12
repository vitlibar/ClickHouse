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

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & range) const
        {
            if (has_value && !range.contains(first))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(first));
        }

        std::optional<ValueType> getResult() const
        {
            if (!has_value)
                return std::nullopt;
            return second;
        }
    };

    /// The bucket already keeps the most recent sample and serves as the aggregation data.
    using AggregationData = Bucket;
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

    /// `merge` only keeps the latest sample (a compare and assign), quite fast,
    /// so the two-stack queue pays off only for fairly wide windows.
    static constexpr size_t TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD = 38;

    static constexpr UInt16 FORMAT_VERSION = 3;
    static constexpr bool DateTime64Supported = true;
};

}
