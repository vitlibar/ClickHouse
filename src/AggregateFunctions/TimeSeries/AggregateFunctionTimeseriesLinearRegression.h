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

    /// Per-bucket aggregation data for linear regression, kept as numerically stable centered moments
    /// (Welford's algorithm with Chan's parallel merge). `mean_x`/`mean_y` are the running means;
    /// `m2_x = sum of (x - mean_x)^2` and `c_xy = sum of (x - mean_x)(y - mean_y)` are the
    /// centered (co)moments. Because the moments accumulate deviations from the running mean, they stay small
    /// (~`window^2`) and precise regardless of how far the window sits from `base`.
    struct AggregationData
    {
        Float64 mean_x = 0;     /// running mean of x
        Float64 mean_y = 0;     /// running mean of y
        Float64 m2_x = 0;       /// sum of (x - mean_x)^2
        Float64 c_xy = 0;       /// sum of (x - mean_x)(y - mean_y)
        Float64 count = 0;      /// number of samples

        /// The samples' timestamps are centered on a common base (the grid start) before accumulating,
        /// so x stays small. The centering is necessary because otherwise a raw `DateTime64(9)` timestamp (~1.7e18)
        /// would exceed the Float64 mantissa, so distinct timestamps collapse to the same x.
        void add(TimestampType timestamp, ValueType value, TimestampType base)
        {
            const Float64 x = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(timestamp)) - static_cast<Int128>(static_cast<Int64>(base)));
            const Float64 y = static_cast<Float64>(value);

            ++count;
            const Float64 dx = x - mean_x;
            mean_x += dx / count;
            mean_y += (y - mean_y) / count;
            /// `dx` uses the old `mean_x`; the trailing factors use the just-updated means (Welford).
            m2_x += dx * (x - mean_x);
            c_xy += dx * (y - mean_y);
        }

        /// Chan's parallel merge of two centered-moment aggregates.
        void merge(const AggregationData & other)
        {
            if (other.count == 0)
                return;

            const Float64 na = count;
            const Float64 nb = other.count;
            const Float64 total = na + nb;
            const Float64 dx = other.mean_x - mean_x;
            const Float64 dy = other.mean_y - mean_y;

            mean_x += dx * nb / total;
            mean_y += dy * nb / total;
            m2_x += other.m2_x + dx * dx * na * nb / total;
            c_xy += other.c_xy + dx * dy * na * nb / total;
            count += other.count;
        }

        std::optional<ValueType> getResult(TimestampType grid_timestamp, TimestampType base, Float64 predict_offset) const
        {
            if (count < 2 || m2_x == 0)
                return std::nullopt;

            const Float64 slope = c_xy / m2_x;
            if (!is_predict)
                return static_cast<ValueType>(slope);

            /// Line y = slope * x + intercept with x centered on `base`; extrapolate to `grid_timestamp +
            /// predict_offset`, expressed in the same centered coordinates (subtract `base` in `Int128`).
            const Float64 intercept = mean_y - slope * mean_x;
            const Float64 predict_x = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp)) - static_cast<Int128>(static_cast<Int64>(base)))
                + predict_offset;
            const Float64 predicted = slope * predict_x + intercept;
            return static_cast<ValueType>(predicted);
        }
    };

    /// Builds the regression moments for a bucket, centering each timestamp on `base`.
    /// The moments are order-independent, so samples are added directly without sorting.
    struct Aggregator
    {
        TimestampType base = 0;

        void aggregate(const Bucket & bucket, AggregationData & data) const
        {
            for (const auto & [timestamp, value] : bucket.samples)
                data.add(timestamp, value, base);
        }
    };
};


template <typename Traits>
class AggregateFunctionTimeseriesLinearRegression final :
    public AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>
{
public:
    static constexpr bool is_predict = Traits::is_predict;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;
    using AggregationData = typename Traits::AggregationData;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesLinearRegression<Traits>, Traits>;
    using Base::Base;

    /// Constructor for timeSeriesPredictLinearToGrid (is_predict = true).
    /// For timeSeriesDerivToGrid (is_predict = false) it reaches the base constructor via `using Base::Base` above.
    /// The base constructor takes the same arguments except predict_offset_.
    explicit AggregateFunctionTimeseriesLinearRegression(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_, Float64 predict_offset_)
        : Base(argument_types_, parameters_, start_timestamp_, end_timestamp_, step_, window_, timestamp_scale_)
        , predict_offset(predict_offset_)
    {
    }

    Aggregator createAggregator() const
    {
        return Aggregator{Base::start_timestamp};
    }

    std::optional<ValueType> finalizeAggregation(const AggregationData & aggregate, TimestampType grid_timestamp) const
    {
        return aggregate.getResult(grid_timestamp, Base::start_timestamp, predict_offset);
    }

    /// `merge` is Chan's parallel merge of centered moments — several multiplications and divisions,
    /// quite expensive, so the O(buckets_per_window) recompute path is overtaken at a fairly narrow window.
    static constexpr size_t TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD = 14;

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;

protected:
    const Float64 predict_offset{};    /// Predict offset used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

}
