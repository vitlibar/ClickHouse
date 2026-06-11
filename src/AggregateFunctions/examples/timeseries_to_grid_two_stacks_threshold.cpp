/// Measures the two-stack-queue threshold for the `timeSeries*ToGrid` aggregate functions, i.e. the value each
/// one should use for `TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD`.
///
/// `fillGridResults` switches from the recompute path to the sliding two-stack queue once a window spans at
/// least `TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD` buckets. The threshold is the smallest `buckets_per_window`
/// at which the two-stack queue's near-constant per-grid-point cost beats recompute's O(buckets_per_window)
/// cost; it depends on how expensive that function's `AggregationData::merge` is, hence the per-class constant.
///
/// This benchmark drives the *real* `fillGridResultsByRecompute` / `fillGridResultsByTwoStacks` on a real
/// function instance with synthetic, fully populated buckets, so it cannot drift from production. It sweeps
/// `buckets_per_window` and prints, for each function, the threshold on a dense input (one populated bucket per
/// step) — the recompute path's worst case, and therefore the safe value for the constant. A sparser input
/// (spacing > 1) only makes recompute cheaper, shown by the density sweep across the table columns. A second
/// table then shows the worst-case slowdown the shipped (dense-tuned) threshold causes on sparser data.
///
/// To find the threshold for a NEW timeSeries*ToGrid function, add one `runFunction(...)` / `runSlowdownRow(...)`
/// line for it.
///
/// Build: it is part of the `clickhouse-examples` multi-call binary.
/// Run:   `clickhouse-examples timeseries_to_grid_two_stacks_threshold`

#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesChanges.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesExtrapolatedValue.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesInstantValue.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesLinearRegression.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesToGridSparse.h>

#include <Core/Field.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Stopwatch.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Examples/clickhouse_examples.h>

#include <fmt/format.h>

#include <type_traits>

namespace
{

using namespace DB;

/// Grid length used for every measurement; long enough to amortise the one-off stack reservation.
constexpr size_t GRID_SIZE = 8000;
constexpr int REPEATS = 250;  /// timing iterations per measurement; the minimum over them is used

/// buckets_per_window values swept when locating the threshold and the worst-case slowdown.
constexpr size_t WINDOWS[]
    = {2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 36, 40, 44, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024};

/// A few samples per bucket so each `AggregationData` is non-empty (`merge` early-returns on empty input).
template <typename Bucket>
Bucket makeBucket(size_t i)
{
    Bucket bucket;
    for (size_t j = 0; j < 4; ++j)
        bucket.add(static_cast<int>(i * 4 + j), static_cast<Float64>((i * 7 + j * 3) % 101));
    return bucket;
}

/// Turns the populated per-bucket entries into the `(bucket index, data)` pointer vector the fill methods
/// consume, placing populated buckets `spacing` index positions apart.
template <typename AggregationDataType>
VectorWithMemoryTracking<std::pair<size_t, const AggregationDataType *>>
buildSortedBuckets(const VectorWithMemoryTracking<AggregationDataType> & storage, size_t spacing)
{
    VectorWithMemoryTracking<std::pair<size_t, const AggregationDataType *>> sorted_buckets;
    sorted_buckets.reserve(storage.size());
    for (size_t i = 0; i < storage.size(); ++i)
        sorted_buckets.emplace_back(i * spacing, &storage[i]);
    return sorted_buckets;
}

/// Returns {best recompute ns/point, best two-stacks ns/point} for `func` at the given bucket spacing.
template <typename Func>
std::pair<Float64, Float64> measureNanoseconds(const Func & func, size_t buckets_per_window, size_t spacing)
{
    using Bucket = typename Func::Bucket;
    using AggregationData = typename Func::AggregationData;
    using ValueType = typename Func::ValueType;

    /// Enough populated buckets that a window of `buckets_per_window` is always full as it slides over the grid.
    const size_t num_buckets = (GRID_SIZE + buckets_per_window) / spacing + 1;
    VectorWithMemoryTracking<AggregationData> storage(num_buckets);
    /// When the bucket already is the aggregation data (`Bucket == AggregationData`) there is no aggregator
    /// pass; otherwise build the per-bucket aggregation data through the aggregator.
    if constexpr (std::is_same_v<Bucket, AggregationData>)
    {
        for (size_t i = 0; i < num_buckets; ++i)
            storage[i] = makeBucket<Bucket>(i);
    }
    else
    {
        auto aggregator = func.createAggregator();
        for (size_t i = 0; i < num_buckets; ++i)
            aggregator.aggregate(makeBucket<Bucket>(i), storage[i]);
    }

    const auto sorted_buckets = buildSortedBuckets(storage, spacing);
    VectorWithMemoryTracking<ValueType> values(GRID_SIZE);
    VectorWithMemoryTracking<UInt8> nulls(GRID_SIZE);

    Float64 best_recompute = 1e30;
    Float64 best_two_stacks = 1e30;
    for (int r = 0; r < REPEATS; ++r)
    {
        Stopwatch sw;
        func.fillGridResultsByRecompute(sorted_buckets, values.data(), nulls.data());
        const Float64 recompute_ns = static_cast<Float64>(sw.elapsedNanoseconds()) / GRID_SIZE;

        sw.restart();
        func.fillGridResultsByTwoStacks(sorted_buckets, values.data(), nulls.data());
        const Float64 two_stacks_ns = static_cast<Float64>(sw.elapsedNanoseconds()) / GRID_SIZE;

        best_recompute = std::min(best_recompute, recompute_ns);
        best_two_stacks = std::min(best_two_stacks, two_stacks_ns);
    }
    return {best_recompute, best_two_stacks};
}

/// Returned when the two-stack queue never beats recompute within the swept windows (its threshold is higher
/// than anything measured). As a `size_t` this is the maximum value, so it reads as "never switch".
constexpr size_t HIGH_THRESHOLD = -1;

/// The threshold: smallest `buckets_per_window` at which the two-stack queue beats recompute, for the given
/// spacing, or `HIGH_THRESHOLD` if it never wins within the swept windows.
template <typename Factory>
size_t measureThreshold(Factory make, size_t spacing)
{
    for (size_t window : WINDOWS)
    {
        auto func = make(window);
        auto [recompute_ns, two_stacks_ns] = measureNanoseconds(*func, window, spacing);
        if (two_stacks_ns < recompute_ns)
            return window;
    }
    return HIGH_THRESHOLD;
}

/// `make(window)` builds the function instance whose `buckets_per_window == window` (step 1, window % step == 0).
template <typename Factory>
void runFunction(const char * name, Factory make)
{
    fmt::print("{:46}", name);
    for (size_t spacing : {1, 2, 4, 8, 16})
    {
        size_t c = measureThreshold(make, spacing);
        if (c == HIGH_THRESHOLD)
            fmt::print(" {:>9}", ">1024");
        else
            fmt::print(" {:9}", c);
    }
    fmt::println("");
}

/// Worst-case slowdown over all windows at which the shipped `threshold` forces the two-stack path
/// (`buckets_per_window >= threshold`), relative to the path that is actually faster at the given spacing.
/// 0% means the two-stack path is never slower than recompute there; a positive value is how much slower the
/// fixed threshold makes the slowest such window compared to the density-optimal choice.
template <typename Factory>
Float64 worstSlowdownPercent(Factory make, size_t threshold, size_t spacing)
{
    Float64 worst = 0.0;
    for (size_t window : WINDOWS)
    {
        if (window < threshold)
            continue;  /// below the threshold we use recompute, which is the optimal choice on dense data
        auto func = make(window);
        auto [recompute_ns, two_stacks_ns] = measureNanoseconds(*func, window, spacing);
        worst = std::max(worst, two_stacks_ns / recompute_ns - 1.0);
    }
    return worst * 100.0;
}

/// Prints, for one function, the worst-case slowdown the shipped threshold causes on sparser data.
template <typename Factory>
void runSlowdownRow(const char * name, Factory make)
{
    using Func = std::remove_reference_t<decltype(*make(size_t{0}))>;
    const size_t threshold = Func::TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD;
    fmt::print("{:38} {:9}", name, threshold);
    for (size_t spacing : {1, 2, 4, 8, 16})
        fmt::print(" {:8.0f}%", worstSlowdownPercent(make, threshold, spacing));
    fmt::println("");
}

}

int mainEntryExampleTimeSeriesToGridTwoStacksThreshold(int, char **)
{
    using namespace DB;

    /// step == 1 over [0, GRID_SIZE-1] gives grid_size == GRID_SIZE and buckets_per_window == window (window % step == 0).
    const DataTypes argument_types{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeFloat64>()};
    const Array parameters{};

    auto scalar = [&]<typename Function>(size_t window)
    {
        return std::make_shared<Function>(argument_types, parameters,
            /* start */ UInt32(0), /* end */ UInt32(GRID_SIZE - 1), /* step */ Int32(1), Int32(window), /* scale */ UInt32(0));
    };

    using RateTraits = AggregateFunctionTimeseriesExtrapolatedValueTraits<false, UInt32, Int32, Float64, true>;
    using ChangesTraits = AggregateFunctionTimeseriesChangesTraits<false, UInt32, Int32, Float64, false>;
    using InstantTraits = AggregateFunctionTimeseriesInstantValueTraits<false, UInt32, Int32, Float64, true>;
    using SparseTraits = AggregateFunctionTimeseriesToGridSparseTraits<false, UInt32, Int32, Float64, false>;
    using LinRegTraits = AggregateFunctionTimeseriesLinearRegressionTraits<false, UInt32, Int32, Float64, false>;

    auto make_resample = [&](size_t w) { return scalar.template operator()<AggregateFunctionTimeseriesToGridSparse<SparseTraits>>(w); };
    auto make_instant = [&](size_t w) { return scalar.template operator()<AggregateFunctionTimeseriesInstantValue<InstantTraits>>(w); };
    auto make_changes = [&](size_t w) { return scalar.template operator()<AggregateFunctionTimeseriesChanges<ChangesTraits>>(w); };
    auto make_rate = [&](size_t w) { return scalar.template operator()<AggregateFunctionTimeseriesExtrapolatedValue<RateTraits>>(w); };
    auto make_deriv = [&](size_t w)
    {
        return std::make_shared<AggregateFunctionTimeseriesLinearRegression<LinRegTraits>>(
            argument_types, parameters, UInt32(0), UInt32(GRID_SIZE - 1), Int32(1), Int32(w), UInt32(0), /* predict_offset */ 0.0);
    };

    fmt::println("Two-stacks threshold buckets_per_window (two-stacks wins at this value or above).");
    fmt::println("D is the gap between populated buckets: D=1 is dense data (every step's bucket has samples), larger");
    fmt::println("D is sparser data (only every D-th step has samples). D=1 is the dense worst case for recompute:");
    fmt::println("set TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD to the D=1 value.\n");
    fmt::println("{:46} {:>9} {:>9} {:>9} {:>9} {:>9}", "function", "D=1", "D=2", "D=4", "D=8", "D=16");
    runFunction("timeSeriesResampleToGridWithStaleness", make_resample);
    runFunction("timeSeriesInstantRateToGrid", make_instant);
    runFunction("timeSeriesChangesToGrid", make_changes);
    runFunction("timeSeriesRateToGrid", make_rate);
    runFunction("timeSeriesDerivToGrid", make_deriv);

    /// The threshold is tuned for dense data (D=1). On sparser data its true threshold is higher, so for windows
    /// just above the shipped threshold we use the two-stack queue where recompute would still be faster. This
    /// shows the worst-case price of that, per spacing (0% = two-stacks is never slower for any window >= T).
    fmt::println("\nWorst-case slowdown from the shipped threshold T vs the density-optimal path.");
    fmt::println("T is each function's current TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD constant (ideally the D=1 value above).\n");
    fmt::println("{:38} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}", "function", "T", "D=1", "D=2", "D=4", "D=8", "D=16");
    runSlowdownRow("timeSeriesResampleToGridWithStaleness", make_resample);
    runSlowdownRow("timeSeriesInstantRateToGrid", make_instant);
    runSlowdownRow("timeSeriesChangesToGrid", make_changes);
    runSlowdownRow("timeSeriesRateToGrid", make_rate);
    runSlowdownRow("timeSeriesDerivToGrid", make_deriv);

    return 0;
}
