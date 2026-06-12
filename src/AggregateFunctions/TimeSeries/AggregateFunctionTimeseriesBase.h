#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>


#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// Base class for time series aggregate functions that map values to a grid specified by start timestamp, end timestamp, step and window.
/// It implements the common logic for handling input data as either scalar timestamps and values or vectors of timestamps and values of
/// equal sizes and adding the data to the grid buckets. The actual aggregation logic within buckets is implemented in derived classes.
template <class FunctionImpl, class Traits>
class AggregateFunctionTimeseriesBase :
    public IAggregateFunctionHelper<AggregateFunctionTimeseriesBase<FunctionImpl, Traits>>
{
public:
    static constexpr bool DateTime64Supported = true;

    using Base = IAggregateFunctionHelper<AggregateFunctionTimeseriesBase<FunctionImpl, Traits>>;

    using TimestampType = typename Traits::TimestampType;
    using IntervalType = typename Traits::IntervalType;
    using ValueType = typename Traits::ValueType;

    using ColVecType = ColumnVectorOrDecimal<TimestampType>;
    using ColVecResultType = ColumnVectorOrDecimal<ValueType>;

    String getName() const override
    {
        return Traits::getName();
    }

    /// Timeseries parameters may carry DecimalField (from toDateTime64(...) casts), whose
    /// default printed form collides with String literals — so we print parameters with ::Type.
    bool shouldPrintParametersWithTypes() const override { return true; }

    using Bucket = typename Traits::Bucket;
    using AggregationData = typename Traits::AggregationData;

    struct State
    {
        /// Maps bucket index to the set of all timestamps and values
        UnorderedMapWithMemoryTracking<size_t, Bucket> buckets;
    };

    explicit AggregateFunctionTimeseriesBase(const DataTypes & argument_types_, const Array & parameters_,
        TimestampType start_timestamp_, TimestampType end_timestamp_, IntervalType step_, IntervalType window_, UInt32 timestamp_scale_)
        : Base(
            argument_types_,
            parameters_,
            createResultType())
        , grid_size(gridSize(start_timestamp_, end_timestamp_, step_))
        , window_remainder(windowRemainder(step_, window_))
        , buckets_per_step(bucketsPerStep(window_remainder))
        , buckets_per_window(bucketsPerWindow(step_, window_, window_remainder))
        , bucket_count(bucketCount(grid_size, buckets_per_window, buckets_per_step))
        , start_timestamp(start_timestamp_)
        , end_timestamp(alignedEndTimestamp(start_timestamp_, grid_size, step_))
        , step(step_)
        , window(window_)
        , timestamp_scale_multiplier(static_cast<TimestampType>(DecimalUtils::scaleMultiplier<Int64>(timestamp_scale_)))
    {
        if (window < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window should be non-negative");
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNumber<ValueType>>()));
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<State>;
    }

    size_t alignOfData() const override
    {
        return alignof(State);
    }

    size_t sizeOfData() const override
    {
        return sizeof(State);
    }

    /// Upper bound on the number of grid points (the output array length) for a single grid.
    /// This prevents absurdly large grids (e.g. from adversarial input that passes extreme
    /// timestamps and a tiny step) from allocating huge amounts of memory or triggering
    /// undefined behaviour in downstream arithmetic. 16M is consistent with the
    /// `MAX_ARRAY_SIZE` used by other aggregate functions (`AggregateFunctionGroupArray`,
    /// `AggregateFunctionIntervalLengthSum`, etc.).
    static constexpr size_t MAX_GRID_SIZE = 0xFFFFFF;

    /// When a window spans at least this many buckets, `fillGridResults` switches from recomputing each
    /// window's aggregate to the sliding two-stack queue, whose per-grid-point cost does not grow with the
    /// window span. Below it the simpler recompute is cheaper thanks to its lower constant.
    ///
    /// It depends on how expensive a single `AggregationData::merge` is, so derived classes redefine it. The
    /// values are measured by the `timeseries_to_grid_two_stacks_threshold` example on dense input (recompute's
    /// worst case), so a sparser input only makes recompute relatively cheaper.
    /// No base default: each derived class must define its own.
    /// static constexpr size_t TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD = 16;

    /// Calculates number of grid points: (end - start) / step + 1.
    static size_t gridSize(TimestampType start_timestamp, TimestampType end_timestamp, IntervalType step)
    {
        if (end_timestamp < start_timestamp)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");

        if (end_timestamp == start_timestamp)
            return 1;

        if (step <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

        /// Compute the grid size using unsigned 64-bit arithmetic to avoid signed overflow
        /// when `start_timestamp` is very negative (e.g. `DateTime64` near `INT64_MIN`
        /// produced by an adversarial fuzzer-generated query). Since we already verified
        /// `end_timestamp >= start_timestamp`, the unsigned difference is the correct
        /// mathematical value for any representable input.
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 end_bits = static_cast<UInt64>(static_cast<Int64>(end_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));

        const UInt64 diff = end_bits - start_bits;
        const UInt64 quotient = diff / step_bits;

        /// Since `MAX_GRID_SIZE` is well below `UINT64_MAX`, checking `quotient >=
        /// MAX_GRID_SIZE` is equivalent to `quotient + 1 > MAX_GRID_SIZE` in the safe
        /// range, but remains correct at the `UInt64` overflow boundary.
        if (quotient >= MAX_GRID_SIZE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Number of grid points in the timeseries grid exceeds maximum ({}). "
                "Consider narrowing the [start, end] range or increasing the step.",
                MAX_GRID_SIZE);

        return static_cast<size_t>(quotient + 1);
    }

    /// Calculates the grid's end timestamp: `start_timestamp + (grid_size - 1) * step`, aligned down by step.
    static TimestampType alignedEndTimestamp(TimestampType start_timestamp, size_t grid_size, IntervalType step)
    {
        /// Compute using unsigned 64-bit arithmetic to avoid signed overflow for extreme inputs
        /// (e.g. `start = INT64_MIN`, `step = INT64_MAX`).
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(grid_size - 1) * step_bits;
        return static_cast<TimestampType>(static_cast<Int64>(result_bits));
    }

    /// Calculates number of buckets: leading buckets related to the window of grid point #0
    /// plus 1 or 2 buckets per each step.
    static size_t bucketCount(size_t grid_size, size_t buckets_per_window, size_t buckets_per_step)
    {
        chassert(grid_size >= 1);
        /// Cannot overflow `size_t`: `grid_size <= MAX_GRID_SIZE` (16M, enforced by `gridSize`),
        /// `buckets_per_step` is 1 or 2.
        return buckets_per_window + (grid_size - 1) * buckets_per_step;
    }

    /// Calculates remainder `window % step` which determines a split point for window-aligned buckets.
    /// Returns 0 if no split is needed: when `window <= step`, or when the window is a whole multiple of the step.
    static IntervalType windowRemainder(IntervalType step, IntervalType window)
    {
        if (step <= 0 || window <= step)
            return 0;
        return static_cast<IntervalType>(window % step);
    }

    /// Calculates number of buckets that tile a window.
    static size_t bucketsPerStep(IntervalType window_remainder)
    {
        return window_remainder != 0 ? 2 : 1;
    }

    /// Calculates number of buckets that tile a window.
    static size_t bucketsPerWindow(IntervalType step, IntervalType window, IntervalType window_remainder)
    {
        if (step <= 0 || window <= 0)
            return 1;

        const size_t whole_steps = static_cast<size_t>(window / step);
        if (window_remainder != 0)
        {
            /// Cannot overflow `size_t`: `window_remainder != 0` implies `step >= 2`.
            return 2 * whole_steps + 1;
        }
        return whole_steps == 0 ? 1 : whole_steps;
    }

    static constexpr size_t NO_BUCKET = -1;

    /// Returns the index of the bucket a sample at `timestamp` contributes to.
    /// The function returns NO_BUCKET if the specified timestamp can't contribute to any buckets
    /// because it's too early, or too late, or already out of window.
    size_t bucketIndexForTimestamp(const TimestampType timestamp) const
    {
        if (timestamp > end_timestamp)
            return NO_BUCKET;

        /// unclamped_grid_index = ceil((timestamp - start) / step), the grid point at the upper edge
        /// of the sample's step.
        /// It's unclamped: for timestamps at or before grid point #0 `unclamped_grid_index <= 0`.
        /// Everything is computed in Int128 to stay overflow-safe when `start_timestamp` is
        /// near INT64_MIN and `step` near INT64_MAX.
        const Int128 offset = static_cast<Int128>(static_cast<Int64>(timestamp)) - static_cast<Int128>(static_cast<Int64>(start_timestamp));
        const Int128 step_128 = static_cast<Int128>(static_cast<Int64>(step));

        /// `step <= 0` is possible only when `start == end` (a single grid point).
        Int128 unclamped_grid_index = 0;
        if (step > 0)
        {
            unclamped_grid_index = offset / step_128;
            if ((offset % step_128) > 0)
                ++unclamped_grid_index;
        }

        /// The related grid point's index is always non-negative.
        const size_t grid_index = (unclamped_grid_index > 0) ? static_cast<size_t>(unclamped_grid_index) : 0;

        /// Skip a sample that is already out of window.
        if (isSampleOutOfWindow(timestamp, timestampAtIndex(grid_index)))
            return NO_BUCKET;

        const Int128 leading_buckets = static_cast<Int128>(buckets_per_window);
        Int128 bucket_index;
        if (window_remainder == 0)
        {
            /// One bucket per step.
            bucket_index = unclamped_grid_index + leading_buckets - 1;
        }
        else
        {
            /// Each step is split at (grid timestamp - window_remainder).
            const Int128 remainder = static_cast<Int128>(static_cast<Int64>(window_remainder));

            /// `before_split_point` means timestamp <= grid timestamp - window_remainder,
            /// i.e. offset + window_remainder <= unclamped_grid_index * step.
            const bool before_split_point = (offset + remainder) <= (unclamped_grid_index * step_128);
            bucket_index = 2 * unclamped_grid_index + leading_buckets - 1 - (before_split_point ? 1 : 0);
        }

        chassert(bucket_index >= 0 && bucket_index < static_cast<Int128>(bucket_count));
        return static_cast<size_t>(bucket_index);
    }

    /// Returns a half-open range [first, last) of bucket indices that fall in a grid point's window.
    /// The function always returns a range of size `buckets_per_window`.
    std::pair<size_t, size_t> bucketRangeInWindow(size_t grid_point) const
    {
        const size_t window_begin = grid_point * buckets_per_step;
        return {window_begin, window_begin + buckets_per_window};
    }

    /// Compute the grid timestamp for a given bucket index, i.e. `start_timestamp + index * step`.
    /// Uses unsigned 64-bit arithmetic internally to avoid signed overflow on extreme inputs
    /// (`start_timestamp` near `INT64_MIN` together with a `step` near `INT64_MAX`). The final
    /// cast back to `TimestampType` preserves the same bit pattern that the signed accumulator
    /// `grid_timestamp += step` would produce for normal inputs, but does not trigger UBSAN
    /// on the adversarial boundary values generated by the AST fuzzer.
    TimestampType timestampAtIndex(size_t index) const
    {
        const UInt64 start_bits = static_cast<UInt64>(static_cast<Int64>(start_timestamp));
        const UInt64 step_bits = static_cast<UInt64>(static_cast<Int64>(step));
        const UInt64 result_bits = start_bits + static_cast<UInt64>(index) * step_bits;
        return static_cast<TimestampType>(static_cast<Int64>(result_bits));
    }

    /// Half-open timestamp range `(start_time, end_time]` of a bucket.
    struct BucketTimeRange
    {
        std::optional<TimestampType> start_time;  /// The lower bound is optional
        TimestampType end_time;

        bool contains(const TimestampType & timestamp) const
        {
            return (!start_time || timestamp > *start_time) && timestamp <= end_time;
        }
    };

    /// Returns the timestamp range of bucket `bucket_index`.
    BucketTimeRange bucketTimeRange(size_t bucket_index) const
    {
        /// Computed in `Int128` to avoid overflow.
        const Int128 buckets_per_step_128 = static_cast<Int128>(buckets_per_step);
        const Int128 offset = static_cast<Int128>(bucket_index) - (static_cast<Int128>(buckets_per_window) - 1);
        const Int128 offset_remainder = offset % buckets_per_step_128;

        /// Round `offset` up to the right grid point. A negative `offset` corresponding to the leading buckets is
        /// already rounded up by truncation toward zero, so here we check only for a positive offset_remainder.
        Int128 grid_index = offset / buckets_per_step_128;
        if (offset_remainder > 0)
            ++grid_index;

        const Int128 grid_timestamp = static_cast<Int128>(static_cast<Int64>(start_timestamp))
            + grid_index * static_cast<Int128>(static_cast<Int64>(step));

        Int128 start_time_128 = grid_timestamp;
        Int128 end_time_128 = grid_timestamp;
        if (buckets_per_step == 1)
        {
            start_time_128 -= static_cast<Int64>((step <= 0 || window < step) ? window : step);
        }
        else if (offset_remainder != 0)
        {
            /// before split
            start_time_128 -= static_cast<Int64>(step);
            end_time_128 -= static_cast<Int64>(window_remainder);
        }
        else
        {
            /// after split
            start_time_128 -= static_cast<Int64>(window_remainder);
        }

        /// `DateTime` can't be negative, so we should check here for underflow.
        TimestampType start_time = static_cast<TimestampType>(static_cast<Int64>(start_time_128));
        TimestampType end_time = static_cast<TimestampType>(static_cast<Int64>(end_time_128));
        if (static_cast<Int128>(static_cast<Int64>(end_time)) != end_time_128)
            return {0, 0};  /// Empty range
        if (static_cast<Int128>(static_cast<Int64>(start_time)) != start_time_128)
            return {std::nullopt, end_time};  /// No lower bound.
        return {start_time, end_time};
    }

    /// Returns whether a sample at `timestamp` is past the sliding-window cutoff for grid point `grid_timestamp`.
    bool isSampleOutOfWindow(const TimestampType timestamp, const TimestampType grid_timestamp) const
    {
        /// Compare as Int128 to avoid signed-overflow `TimestampType` when `window` is set near `INT64_MAX`.
        const Int128 staleness_cutoff =
            static_cast<Int128>(static_cast<Int64>(timestamp)) +
            static_cast<Int128>(static_cast<Int64>(window));
        return staleness_cutoff <= static_cast<Int128>(static_cast<Int64>(grid_timestamp));
    }

    static const State * data(ConstAggregateDataPtr __restrict place)
    {
        return reinterpret_cast<const State *>(place);
    }

    static State * data(AggregateDataPtr __restrict place)
    {
        return reinterpret_cast<State *>(place);
    }

    void create(AggregateDataPtr __restrict place) const override  /// NOLINT
    {
        new (place) State{};
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place)->~State();
    }

    void ALWAYS_INLINE add(AggregateDataPtr __restrict place, TimestampType timestamp, ValueType value) const
    {
        const size_t index = bucketIndexForTimestamp(timestamp);
        if (index == NO_BUCKET)
            return;  /// The sample can't contribute to any bucket.

        auto & bucket = data(place)->buckets[index];
        bucket.add(timestamp, value);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (Traits::array_arguments)
        {
            addBatchSinglePlace(row_num, row_num + 1, place, columns, arena, -1);
        }
        else
        {
            const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
            add(place, timestamp_column.getData()[row_num], value_column.getData()[row_num]);
        }
    }

    void addMany(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyNotNull(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict null_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (!null_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addManyConditional(AggregateDataPtr __restrict place, const TimestampType * __restrict timestamp_ptr, const ValueType * __restrict value_ptr, const UInt8 * __restrict condition_map, size_t start, size_t end) const
    {
        for (size_t i = start; i < end; ++i)
            if (condition_map[i])
                add(place, timestamp_ptr[i], value_ptr[i]);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const UInt8 * include_flags_data = nullptr;
        if (if_argument_pos >= 0)
        {
            const auto & flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            if (row_end > flags.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "row_end {} is greater than flags column size {}", row_end, flags.size());

            include_flags_data = flags.data();
        }

        addBatchSinglePlaceWithFlags<true>(row_begin, row_end, place, columns, include_flags_data);
    }

    /// `flag_value_to_include` parameter determines which rows are included into result.
    /// E.g. if we pass null_map as flags_data and then we want to include rows where null flag is false
    /// or we can pass boolean condition column and include rows where the flag is true
    template <bool flag_value_to_include>
    void addBatchSinglePlaceWithFlags(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * flags_data) const
    {
        if (Traits::array_arguments)
        {
            const auto & timestamp_column = typeid_cast<const ColumnArray &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColumnArray &>(*columns[1]);
            const auto & timestamp_offsets = timestamp_column.getOffsets();
            const auto & value_offsets = value_column.getOffsets();
            const TimestampType * timestamp_data = typeid_cast<const ColVecType *>(timestamp_column.getDataPtr().get())->getData().data();
            const ValueType * value_data = typeid_cast<const ColVecResultType *>(value_column.getDataPtr().get())->getData().data();

            if (flags_data)
            {
                size_t previous_timestamp_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                size_t previous_value_offset = (row_begin == 0 ? 0 : value_offsets[row_begin - 1]);
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    const auto timestamp_array_size = timestamp_offsets[i] - previous_timestamp_offset;
                    const auto value_array_size = value_offsets[i] - previous_value_offset;

                    if (flags_data[i] == flag_value_to_include)
                    {
                        /// Check that timestamp and value arrays have the same size for the selected rows
                        if (timestamp_array_size != value_array_size)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                                i, timestamp_array_size, value_array_size);

                        /// A flag is per row, and each row is a pair of arrays
                        addMany(place, timestamp_data + previous_timestamp_offset, value_data + previous_value_offset, 0, timestamp_array_size);
                    }

                    previous_timestamp_offset = timestamp_offsets[i];
                    previous_value_offset = value_offsets[i];
                }
            }
            else
            {
                {
                    /// Check that timestamp and value arrays have the same size for each row
                    size_t previous_offset = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                    for (size_t i = row_begin; i < row_end; ++i)
                    {
                        const auto timestamp_array_size = timestamp_offsets[i] - previous_offset;
                        const auto value_array_size = value_offsets[i] - previous_offset;

                        if (timestamp_array_size != value_array_size)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Timestamp and value arrays have different sizes at row {} : {} and {}",
                                i, timestamp_array_size, value_array_size);

                        previous_offset = timestamp_offsets[i];
                    }
                }

                const size_t data_row_begin = (row_begin == 0 ? 0 : timestamp_offsets[row_begin - 1]);
                const size_t data_row_end = (row_end == 0 ? 0 : timestamp_offsets[row_end - 1]);

                addMany(place, timestamp_data, value_data, data_row_begin, data_row_end);
            }
        }
        else
        {
            const auto & timestamp_column = typeid_cast<const ColVecType &>(*columns[0]);
            const auto & value_column = typeid_cast<const ColVecResultType &>(*columns[1]);
            const TimestampType * timestamp_data = timestamp_column.getData().data();
            const ValueType * value_data = value_column.getData().data();

            if (flags_data)
            {
                if constexpr (flag_value_to_include)
                    addManyConditional(place, timestamp_data, value_data, flags_data, row_begin, row_end);
                else
                    addManyNotNull(place, timestamp_data, value_data, flags_data, row_begin, row_end);
            }
            else
            {
                addMany(place, timestamp_data, value_data, row_begin, row_end);
            }
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos)
        const override
    {
        const UInt8 * exclude_flags_data = null_map;    /// By default exclude using null_map
        std::unique_ptr<UInt8[]> combined_exclude_flags;

        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = typeid_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            combined_exclude_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                combined_exclude_flags[i] = (!!null_map[i]) | !if_flags[i]; /// Exclude if NULL or if condition is false
            exclude_flags_data = combined_exclude_flags.get();
        }

        addBatchSinglePlaceWithFlags<false>(row_begin, row_end, place, columns, exclude_flags_data);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & buckets = data(place)->buckets;
        const auto & rhs_buckets = data(rhs)->buckets;
        buckets.reserve(rhs_buckets.size());
        for (const auto & rhs_bucket : rhs_buckets)
        {
            auto & bucket = buckets[rhs_bucket.first];
            bucket.merge(rhs_bucket.second);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(bucket_count, buf);

        writeBinaryLittleEndian(data(place)->buckets.size(), buf);

        for (const auto & bucket : data(place)->buckets)
        {
            writeBinaryLittleEndian(bucket.first, buf);
            bucket.second.serialize(buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);

        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

        size_t size = 0;
        readBinaryLittleEndian(size, buf);

        if (size != bucket_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different bucket count");

        size_t buckets_size = 0;
        readBinaryLittleEndian(buckets_size, buf);

        if (buckets_size > bucket_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with more buckets than expected");

        data(place)->buckets.reserve(buckets_size);

        for (size_t i = 0; i < buckets_size; ++i)
        {
            size_t index = 0;
            readBinaryLittleEndian(index, buf);

            if (index >= bucket_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with index {} greater than bucket count {}", index, bucket_count);

            auto & bucket = data(place)->buckets[index];
            bucket.deserialize(buf);

            /// Validate that each deserialized sample falls into this bucket's timestamp range.
            bucket.checkTimestampsInRange(bucketTimeRange(index));
        }
    }

    const FunctionImpl & derived() const
    {
        return static_cast<const FunctionImpl &>(*this);
    }

    /// Creates the per-bucket aggregator used to build the aggregation data from a bucket.
    /// Used only when `Bucket` differs from `AggregationData`; derived classes may redefine it.
    /// The `auto` return type keeps `Traits::Aggregator` unrequired for functions where the bucket already
    /// is the aggregation data (and so have no `Aggregator`).
    static auto createAggregator()
    {
        return typename Traits::Aggregator{};
    }

    /// Turns a window's `AggregationData` into the result value (or nullopt for NULL).
    /// Derived classes may redefine it.
    std::optional<ValueType> finalizeAggregation(const AggregationData & aggregate, TimestampType /*grid_timestamp*/) const
    {
        return aggregate.getResult();
    }

    /// Constructs a result array.
    /// When `Bucket` differs from `AggregationData`, `Traits::Aggregator` turns each bucket into the
    /// per-bucket `AggregationData`; otherwise the bucket already is the aggregation data and is merged
    /// directly. Each derived class turns per-window `AggregationData` into a value via `finalizeAggregation`.
    void doInsertResultInto(AggregateDataPtr __restrict place, IColumn & to) const
    {
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.empty() ? grid_size : offsets_to.back() + grid_size);

        if (!grid_size)
            return;

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        const size_t old_size = data_to.size();
        chassert(old_size == nulls_to.size(), "Sizes of nested column and null map of Nullable column are not equal");

        data_to.resize(old_size + grid_size);
        nulls_to.resize(old_size + grid_size);

        ValueType * values = data_to.data() + old_size;
        UInt8 * nulls = nulls_to.data() + old_size;

        const auto & buckets = data(place)->buckets;

        /// Aggregate each populated bucket into a `(bucket index, aggregation data)` pair, sorted by index, in
        /// one contiguous vector for the window slide below.
        VectorWithMemoryTracking<std::pair<size_t, AggregationData>> sorted_buckets;
        sorted_buckets.reserve(buckets.size());
        if constexpr (std::is_same_v<Bucket, AggregationData>)
        {
            /// The bucket already is the aggregation data.
            for (const auto & [bucket_index, bucket] : buckets)
                sorted_buckets.emplace_back(bucket_index, bucket);
        }
        else
        {
            auto aggregator = derived().createAggregator();
            for (const auto & [bucket_index, bucket] : buckets)
            {
                AggregationData aggregate;
                aggregator.aggregate(bucket, aggregate);
                sorted_buckets.emplace_back(bucket_index, std::move(aggregate));
            }
        }
        std::sort(sorted_buckets.begin(), sorted_buckets.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

        /// Recomputing a window costs O(buckets_per_window) per grid point; the sliding two-stack queue costs
        /// ~O(1) per grid point but with a higher constant. Pick by buckets_per_window.
        if (buckets_per_window >= FunctionImpl::TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD)
            fillGridResultsByTwoStacks(sorted_buckets, values, nulls);
        else
            fillGridResultsByRecompute(sorted_buckets, values, nulls);
    }

    /// Recompute path: re-merge the in-window buckets at each grid point. The set of populated buckets in the
    /// window often stays the same across consecutive grid points (e.g. when the window is large compared to the
    /// data extent), so the merged aggregate is recomputed only when the `[window_first, window_last)` range
    /// actually changes.
    void fillGridResultsByRecompute(
        const VectorWithMemoryTracking<std::pair<size_t, AggregationData>> & sorted_buckets,
        ValueType * values, UInt8 * nulls) const
    {
        const size_t num_buckets = sorted_buckets.size();
        size_t window_first = 0;    /// index into sorted_buckets of the first populated bucket in the window
        size_t window_last = 0;     /// one past the last populated bucket in the window

        /// Initial values (first=1 > last=0) never equal a real range, forcing the first recompute.
        size_t prev_window_first = 1;
        size_t prev_window_last = 0;
        AggregationData window_aggregate;

        for (size_t i = 0; i < grid_size; ++i)
        {
            const auto [window_begin, window_end] = bucketRangeInWindow(i);

            /// Both window edges move forward as `i` grows, so the cursors only ever advance.
            while (window_last < num_buckets && sorted_buckets[window_last].first < window_end)
                ++window_last;
            while (window_first < window_last && sorted_buckets[window_first].first < window_begin)
                ++window_first;

            if (window_first != prev_window_first || window_last != prev_window_last)
            {
                window_aggregate = AggregationData{};
                for (size_t b = window_first; b < window_last; ++b)
                    window_aggregate.merge(sorted_buckets[b].second);
                prev_window_first = window_first;
                prev_window_last = window_last;
            }

            storeGridResult(i, window_aggregate, values, nulls);
        }
    }

    /// Sliding path: a two-stack FIFO monoid queue (the "Two-Stacks" sliding-window aggregation algorithm).
    /// The populated buckets currently inside the window form a queue ordered by time; buckets entering at the
    /// right edge are pushed onto the back stack, buckets leaving at the left edge are popped from the front
    /// stack (the back stack reversed, rebuilt when it runs empty).
    void fillGridResultsByTwoStacks(
        const VectorWithMemoryTracking<std::pair<size_t, AggregationData>> & sorted_buckets,
        ValueType * values, UInt8 * nulls) const
    {
        struct StackEntry
        {
            const AggregationData * value;      /// the bucket's data
            AggregationData aggregate;          /// running merge over this stack, in time order, up to this entry
        };

        const size_t num_buckets = sorted_buckets.size();
        const size_t max_window_buckets = std::min(num_buckets, buckets_per_window);

        VectorWithMemoryTracking<StackEntry> back_stack;   /// newer buckets; pushed here
        VectorWithMemoryTracking<StackEntry> front_stack;  /// older buckets; popped here
        back_stack.reserve(max_window_buckets);
        front_stack.reserve(max_window_buckets);

        size_t window_first = 0;    /// index into sorted_buckets of the queue's front (oldest) bucket
        size_t window_last = 0;     /// one past the queue's back (newest) bucket

        /// The merged aggregate is recomputed only when the `[window_first, window_last)` range
        /// actually changes. Initial values (first=1 > last=0) never equal a real range, forcing the first recompute.
        size_t prev_window_first = 1;
        size_t prev_window_last = 0;
        AggregationData window_aggregate;

        for (size_t i = 0; i < grid_size; ++i)
        {
            const auto [window_begin, window_end] = bucketRangeInWindow(i);

            /// Push buckets entering at the right edge onto the back stack.
            while (window_last < num_buckets && sorted_buckets[window_last].first < window_end)
            {
                const AggregationData * value = &sorted_buckets[window_last].second;
                AggregationData aggregate;
                if (!back_stack.empty())
                    aggregate = back_stack.back().aggregate;    /// older part of the back stack ...
                aggregate.merge(*value);                        /// ... then this (newer) bucket
                back_stack.push_back({value, std::move(aggregate)});
                ++window_last;
            }

            /// Pop buckets leaving at the left edge from the front stack.
            while (window_first < window_last && sorted_buckets[window_first].first < window_begin)
            {
                if (front_stack.empty())
                {
                    /// Flush the back stack into the front stack, reversing it so the oldest bucket ends up on
                    /// top, accumulating each entry's running merge in time order (oldest .. this entry).
                    while (!back_stack.empty())
                    {
                        const AggregationData * value = back_stack.back().value;
                        AggregationData aggregate;
                        aggregate.merge(*value);                            /// this (older) bucket ...
                        if (!front_stack.empty())
                            aggregate.merge(front_stack.back().aggregate);  /// ... then the newer ones
                        front_stack.push_back({value, std::move(aggregate)});
                        back_stack.pop_back();
                    }
                }
                front_stack.pop_back();
                ++window_first;
            }

            /// The window's aggregate is the front stack (older) merged with the back stack (newer).
            if (window_first != prev_window_first || window_last != prev_window_last)
            {
                window_aggregate = AggregationData{};
                if (!front_stack.empty())
                    window_aggregate.merge(front_stack.back().aggregate);
                if (!back_stack.empty())
                    window_aggregate.merge(back_stack.back().aggregate);
                prev_window_first = window_first;
                prev_window_last = window_last;
            }

            storeGridResult(i, window_aggregate, values, nulls);
        }
    }

    /// Finalizes a window's aggregate into the result value (or NULL) at grid point `i`.
    void storeGridResult(size_t i, const AggregationData & window_aggregate, ValueType * values, UInt8 * nulls) const
    {
        if (auto result = derived().finalizeAggregation(window_aggregate, timestampAtIndex(i)))
        {
            values[i] = *result;
            nulls[i] = 0;
        }
        else
        {
            values[i] = ValueType{};
            nulls[i] = 1;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        derived().doInsertResultInto(place, to);
    }

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        IColumn & to,
        Arena *) const override
    {
        size_t batch_index = row_begin;
        const size_t batch_size = row_end - row_begin;

        /// Reserve offsets and values in column to
        ColumnArray & arr_to = typeid_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        ColumnNullable & result_to = typeid_cast<ColumnNullable &>(arr_to.getData());
        auto & data_to = typeid_cast<ColVecResultType &>(result_to.getNestedColumn()).getData();
        auto & nulls_to = result_to.getNullMapData();

        offsets_to.reserve(offsets_to.size() + batch_size);
        data_to.reserve(data_to.size() + batch_size * grid_size);
        nulls_to.reserve(nulls_to.size() + batch_size * grid_size);

        try
        {
            for (; batch_index < row_end; ++batch_index)
            {
                derived().doInsertResultInto(places[batch_index] + place_offset, to);
                /// For State AggregateFunction ownership of aggregate place is passed to result column after insert,
                /// so we need to destroy all states up to state of -State combinator.
                Base::destroyUpToState(places[batch_index] + place_offset);
            }
        }
        catch (...)
        {
            for (size_t destroy_index = batch_index; destroy_index < row_end; ++destroy_index)
                destroy(places[destroy_index] + place_offset);

            throw;
        }
    }

protected:
    static constexpr UInt16 FORMAT_VERSION = FunctionImpl::FORMAT_VERSION;

    const size_t grid_size{};               /// Number of grid points: (end - start) / step + 1
    const IntervalType window_remainder{};  /// (window % step) if (window > step)
    const size_t buckets_per_step{};        /// 2 when window_remainder != 0 (each step is split), else 1
    const size_t buckets_per_window{};      /// Number of buckets tiling each grid point's window
    const size_t bucket_count{};            /// Number of buckets
    const TimestampType start_timestamp{};  /// First timestamp in the grid
    const TimestampType end_timestamp{};    /// Last timestamp in the grid. NOTE: It is aligned down by step relative to start_timestamp
    const IntervalType step{};              /// Grid step (IntervalType represent time difference between timestamps)
    const IntervalType window{};            /// Window size used by derived functions (e.g. for rate and delta calculations)
    const TimestampType timestamp_scale_multiplier{};   /// When timestamps are in DateTime64 (which is Decimal with some scale)
                                                        /// this multiplier is used for calculation rate per second (i.e. it is 1000 for
                                                        /// milliseconds or 1e6 for microseconds)
};

}
