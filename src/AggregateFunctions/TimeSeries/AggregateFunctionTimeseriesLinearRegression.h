#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <optional>


#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/VectorWithMemoryTracking.h>


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

    /// Per-bucket regression data, kept as numerically stable centered moments (Welford's algorithm with Chan's
    /// parallel merge). `mean_x`/`mean_y` are the running means; `m2_x = sum of (x - mean_x)^2` and
    /// `c_xy = sum of (x - mean_x)(y - mean_y)` are the centered (co)moments. Because the moments accumulate
    /// deviations from the running mean, they stay small (~`window^2`) and precise regardless of how far the
    /// window sits from `base`. The merge is order-independent, so buckets can be combined in any order.
    struct Moments
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
        void merge(const Moments & other)
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
    };

    /// Sliding aggregator for linear regression. The moment merge is associative but not invertible (a bucket
    /// leaving the window cannot be subtracted), so the window is maintained as a two-stack monoid queue (the
    /// "Two-Stacks" sliding-window algorithm): buckets entering at the right edge are pushed onto the back stack,
    /// buckets leaving at the left edge are popped from the front stack (the back stack reversed, rebuilt when it
    /// runs empty). Each stack entry caches the running merge up to itself, so `getResult` combines just the two
    /// stack tops in O(1) amortized regardless of how many buckets the window spans.
    struct Aggregator
    {
        struct StackEntry
        {
            TimestampType last_timestamp;   /// the bucket's latest sample, used to decide when it leaves the window
            Moments single;                 /// this bucket's moments alone
            Moments combined;               /// running merge over this stack up to this entry
        };

        VectorWithMemoryTracking<StackEntry> back_stack;   /// newer buckets; pushed here
        VectorWithMemoryTracking<StackEntry> front_stack;  /// older buckets; popped here
        TimestampType base = 0;
        Float64 predict_offset = 0;

        void addBucket(const Bucket & bucket)
        {
            Moments moments;
            TimestampType last_timestamp = std::numeric_limits<TimestampType>::min();
            const TimestampType base_timestamp = base;
            bucket.forEachSample([&moments, &last_timestamp, base_timestamp](TimestampType timestamp, ValueType value)
            {
                moments.add(timestamp, value, base_timestamp);
                last_timestamp = std::max(last_timestamp, timestamp);
            });
            if (moments.count == 0)
                return;

            Moments combined = moments;
            if (!back_stack.empty())
                combined.merge(back_stack.back().combined);
            back_stack.push_back({last_timestamp, moments, std::move(combined)});
        }

        void removeBucket(TimestampType cut_off)
        {
            while (true)
            {
                if (front_stack.empty())
                {
                    /// Flush the back stack into the front stack, reversing it so the oldest bucket ends up on top.
                    while (!back_stack.empty())
                    {
                        Moments combined = back_stack.back().single;
                        if (!front_stack.empty())
                            combined.merge(front_stack.back().combined);
                        front_stack.push_back({back_stack.back().last_timestamp, back_stack.back().single, std::move(combined)});
                        back_stack.pop_back();
                    }
                }

                if (front_stack.empty() || front_stack.back().last_timestamp > cut_off)
                    break;
                front_stack.pop_back();
            }
        }

        std::optional<ValueType> getResult(TimestampType grid_timestamp) const
        {
            Moments window;
            if (!front_stack.empty())
                window.merge(front_stack.back().combined);
            if (!back_stack.empty())
                window.merge(back_stack.back().combined);

            if (window.count < 2 || window.m2_x == 0)
                return std::nullopt;

            const Float64 slope = window.c_xy / window.m2_x;
            if (!is_predict)
                return static_cast<ValueType>(slope);

            /// Line y = slope * x + intercept with x centered on `base`; extrapolate to `grid_timestamp +
            /// predict_offset`, expressed in the same centered coordinates (subtract `base` in `Int128`).
            const Float64 intercept = window.mean_y - slope * window.mean_x;
            const Float64 predict_x = static_cast<Float64>(
                static_cast<Int128>(static_cast<Int64>(grid_timestamp)) - static_cast<Int128>(static_cast<Int64>(base)))
                + predict_offset;
            const Float64 predicted = slope * predict_x + intercept;
            return static_cast<ValueType>(predicted);
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
        Aggregator aggregator;
        aggregator.base = Base::start_timestamp;
        aggregator.predict_offset = predict_offset;
        return aggregator;
    }

    static constexpr UInt16 FORMAT_VERSION = 2;
    static constexpr bool DateTime64Supported = true;

protected:
    const Float64 predict_offset{};    /// Predict offset used by timeSeriesPredictLinearToGrid function, used to calculate the timestamp of the predicted value
};

}
