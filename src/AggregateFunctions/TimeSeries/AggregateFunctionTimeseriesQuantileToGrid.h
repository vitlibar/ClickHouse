#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <deque>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesBase.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>


namespace DB
{

/// R-7 (quantileExactInclusive) quantile of `values`.
template <typename ValueType>
std::optional<ValueType> computeTimeseriesQuantile(VectorWithMemoryTracking<ValueType> && values, Float64 phi)
{
    if (values.empty())
        return std::nullopt;

    const size_t n = values.size();
    if (n == 1)
        return values[0];

    /// NaN samples are kept and ordered before every real value, like Prometheus' `vectorByValueHeap.Less`.
    /// Spelled as a strict weak ordering (all NaNs equivalent), which plain `<` on floats is not.
    std::sort(values.begin(), values.end(), [](ValueType lhs, ValueType rhs)
    {
        if (isNaN(lhs))
            return !isNaN(rhs);
        return !isNaN(rhs) && lhs < rhs;
    });

    /// rank = phi * (n - 1), interpolated. Callers wrap the output for out-of-range/NaN phi.
    Float64 rank = phi * static_cast<Float64>(n - 1);
    if (std::isnan(rank))
        return static_cast<ValueType>(std::numeric_limits<Float64>::quiet_NaN());
    if (rank < 0.0)
        rank = 0.0;
    else if (rank > static_cast<Float64>(n - 1))
        rank = static_cast<Float64>(n - 1);

    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));

    if (lower == upper)
        return values[lower];

    const Float64 fraction = rank - static_cast<Float64>(lower);
    const Float64 result = static_cast<Float64>(values[lower])
        + fraction * (static_cast<Float64>(values[upper]) - static_cast<Float64>(values[lower]));
    return static_cast<ValueType>(result);
}


template <typename TimestampType_, typename IntervalType_, typename ValueType_>
struct AggregateFunctionTimeseriesQuantileToGridTraits
{
    using TimestampType = TimestampType_;
    using IntervalType = IntervalType_;
    using ValueType = ValueType_;
    using ResultType = ValueType_;

    static String getName()
    {
        return "timeSeriesQuantileToGrid";
    }

    /// The quantile level `phi` is one more argument after the samples: one value for the whole grid or one value per
    /// grid point (see `AggregateFunctionTimeseriesBase::has_grid_argument`).
    static constexpr bool has_grid_argument = true;
    static constexpr std::string_view grid_argument_name = "phi";

    using Samples = AggregateFunctionTimeseriesSamples<TimestampType, ValueType>;

    /// The bucket stores raw samples: a quantile has no summary to preaggregate.
    using Bucket = Samples;

    /// Sliding aggregator: keeps the buckets in the window and computes the phi-quantile (R-7, inclusive) of all their
    /// values for every grid point. The buckets live in the state's map, which does not change during the finalization,
    /// so they are referenced, not copied.
    struct Aggregator
    {
        std::deque<std::pair<TimestampType, const Samples *>> buckets_in_window;

        void add(const Samples & samples, TimestampType bucket_end_timestamp)
        {
            buckets_in_window.emplace_back(bucket_end_timestamp, &samples);
        }

        void removeBefore(TimestampType cut_off)
        {
            while (!buckets_in_window.empty() && buckets_in_window.front().first <= cut_off)
                buckets_in_window.pop_front();
        }

        std::optional<ValueType> getResult(TimestampType /*grid_timestamp*/, Float64 phi) const
        {
            VectorWithMemoryTracking<ValueType> values;
            for (const auto & [_, samples] : buckets_in_window)
            {
                samples->forEachSample([&values](TimestampType /*timestamp*/, ValueType value)
                {
                    values.push_back(value);
                });
            }
            return computeTimeseriesQuantile(std::move(values), phi);
        }
    };

    static constexpr UInt16 FORMAT_VERSION = 1;
};


/// Aggregate function that computes the phi-quantile of time series values on a regular time grid.
/// Returns the R-7 (inclusive) quantile of all sample values within each grid point's window. The quantile level is the
/// argument after the samples: a number, or an array with one level per grid point.
template <typename TimestampType_, typename IntervalType_, typename ValueType_>
class AggregateFunctionTimeseriesQuantileToGrid final :
    public AggregateFunctionTimeseriesBase<
        AggregateFunctionTimeseriesQuantileToGrid<TimestampType_, IntervalType_, ValueType_>,
        AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, IntervalType_, ValueType_>>
{
public:
    using Traits = AggregateFunctionTimeseriesQuantileToGridTraits<TimestampType_, IntervalType_, ValueType_>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;
    using Aggregator = typename Traits::Aggregator;

    using Base = AggregateFunctionTimeseriesBase<AggregateFunctionTimeseriesQuantileToGrid, Traits>;
    using Base::Base;

    Aggregator createAggregator(size_t /* stack_size_for_two_stacks */) const
    {
        return {};
    }
};

}
