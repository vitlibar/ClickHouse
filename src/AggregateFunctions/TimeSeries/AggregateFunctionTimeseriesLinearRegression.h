#pragma once

#include <cmath>
#include <cstddef>
#include <cstring>
#include <optional>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>


namespace DB
{

template <bool array_arguments_, typename TimestampType_, typename IntervalType_, typename ValueType_, bool is_predict_>
struct AggregateFunctionTimeseriesLinearRegressionTraits
{
    static constexpr bool array_arguments = array_arguments_;
    static constexpr bool is_predict = is_predict_;

    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;

    static String getName()
    {
        return is_predict ? "timeSeriesPredictLinearToGrid" : "timeSeriesDerivToGrid";
    }

    using Bucket = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// Per-bucket aggregation data for linear regression.
    struct AggregationData
    {
        Float64 sum_x = 0;      /// sum of x
        Float64 sum_y = 0;      /// sum of y
        Float64 sum_xy = 0;     /// sum of x*y
        Float64 sum_xx = 0;     /// sum of x*x
        Float64 comp_x = 0;     /// Neumaier compensation companions for the sums above
        Float64 comp_y = 0;
        Float64 comp_xy = 0;
        Float64 comp_xx = 0;
        UInt64 count = 0;       /// number of samples

        /// Adds `inc` to the running `sum` keeping a Neumaier compensation term in `comp`.
        static void kahanAdd(Float64 inc, Float64 & sum, Float64 & comp)
        {
            const Float64 new_sum = sum + inc;
            /// Using Neumaier improvement, swap if next term larger than sum.
            if (std::abs(sum) >= std::abs(inc))
                comp += (sum - new_sum) + inc;
            else
                comp += (inc - new_sum) + sum;
            sum = new_sum;
        }

        void add(TimestampType timestamp, ValueType value)
        {
            const Float64 x = static_cast<Float64>(timestamp);
            const Float64 y = static_cast<Float64>(value);
            kahanAdd(x, sum_x, comp_x);
            kahanAdd(y, sum_y, comp_y);
            kahanAdd(x * y, sum_xy, comp_xy);
            kahanAdd(x * x, sum_xx, comp_xx);
            ++count;
        }

        void merge(const AggregationData & other)
        {
            kahanAdd(other.sum_x + other.comp_x, sum_x, comp_x);
            kahanAdd(other.sum_y + other.comp_y, sum_y, comp_y);
            kahanAdd(other.sum_xy + other.comp_xy, sum_xy, comp_xy);
            kahanAdd(other.sum_xx + other.comp_xx, sum_xx, comp_xx);
            count += other.count;
        }

        std::optional<ValueType> getResult(TimestampType grid_timestamp, Float64 predict_offset) const
        {
            if (count < 2)
                return std::nullopt;

            /// Fold the compensation terms into the sums.
            const Float64 total_x = sum_x + comp_x;
            const Float64 total_y = sum_y + comp_y;
            const Float64 total_xy = sum_xy + comp_xy;
            const Float64 total_xx = sum_xx + comp_xx;

            const Float64 n = static_cast<Float64>(count);
            const Float64 cov_xy = total_xy - total_x * total_y / n;
            const Float64 var_x = total_xx - total_x * total_x / n;
            if (var_x == 0)
                return std::nullopt;

            const Float64 slope = cov_xy / var_x;
            if (!is_predict)
                return static_cast<ValueType>(slope);

            /// Line y = slope * x + intercept with x = Float64(timestamp); extrapolate to grid_timestamp + predict_offset.
            const Float64 intercept = total_y / n - slope * total_x / n;
            const Float64 predicted = slope * (static_cast<Float64>(grid_timestamp) + predict_offset) + intercept;
            return static_cast<ValueType>(predicted);
        }
    };

    /// The least-squares sums are order-independent, so the samples can be added in any order.
    using BucketAggregator = typename Bucket::Aggregator;
};

template <typename Traits>
class AggregateFunctionTimeseriesLinearRegression final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>
{
public:
    static constexpr bool DateTime64Supported = true;

    static constexpr bool is_predict = Traits::is_predict;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>;

    using Base::Base;

    using Bucket = typename Base::Bucket;
    using AggregationData = typename Traits::AggregationData;

    /// Constructor for timeSeriesPredictLinearToGrid (is_predict = true).
    /// For timeSeriesDerivToGrid (is_predict = false) it reaches the base constructor via `using Base::Base` above.
    /// The base constructor takes the same arguments except predict_offset_.
    explicit AggregateFunctionTimeseriesLinearRegression(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_, Float64 predict_offset_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , predict_offset(predict_offset_)
    {
    }

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
        return aggregate.getResult(grid_timestamp, predict_offset);
    }

    static constexpr UInt16 FORMAT_VERSION = 1;

protected:
    const Float64 predict_offset{};    /// Predict offset used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

}
