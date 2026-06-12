#pragma once

#include <algorithm>
#include <array>
#include <utility>

#include <absl/container/flat_hash_map.h>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Per-bucket storage of timeseries samples keyed by timestamp.
/// When two samples share a timestamp the larger value is kept.
///
/// A bucket is a sub-interval of a grid step and in the common case holds only a few samples (often a single
/// one per step). Allocating an `absl::flat_hash_map` for every such bucket is wasteful, so the first
/// `SMALL_BUFFER_SIZE` distinct timestamps are kept in a small sorted array with no heap allocation
/// (the small-buffer optimization); a bucket that grows beyond that switches all its samples to the hash map
/// and uses it from then on. `isSmall()` (i.e. an empty `buffer`) selects which storage is live.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    void add(TimestampType timestamp, ValueType value)
    {
        if (!isSmall())
        {
            addToBuffer(timestamp, value);
            return;
        }

        /// Small-buffer path: keep `small_buffer[0, small_buffer_count)` sorted by timestamp, deduplicating by
        /// timestamp (keeping the larger value), matching the hash-map semantics.
        size_t pos = 0;
        while (pos < small_buffer_count && small_buffer[pos].first < timestamp)
            ++pos;

        if (pos < small_buffer_count && small_buffer[pos].first == timestamp)
        {
            small_buffer[pos].second = std::max(small_buffer[pos].second, value);
            return;
        }

        if (small_buffer_count < SMALL_BUFFER_SIZE)
        {
            for (size_t i = small_buffer_count; i > pos; --i)
                small_buffer[i] = small_buffer[i - 1];
            small_buffer[pos] = {timestamp, value};
            ++small_buffer_count;
            return;
        }

        /// The small buffer is full and the timestamp is new: switch everything to the hash map.
        switchToBuffer(SMALL_BUFFER_SIZE + 1);
        addToBuffer(timestamp, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        size_t reserve_size = size() + other.size();
        if (reserve_size <= SMALL_BUFFER_SIZE)
        {
            other.forEachSample([this](TimestampType timestamp, ValueType value) { add(timestamp, value); });
        }
        else
        {
            /// If the merged result won't fit in the small buffer (or this bucket already uses the hash map), add
            /// the other bucket's samples straight to the hash map.
            switchToBuffer(reserve_size);
            other.forEachSample([this](TimestampType timestamp, ValueType value) { addToBuffer(timestamp, value); });
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(size(), buf);
        forEachSample([&buf](TimestampType timestamp, ValueType value)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        });
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces the bucket's contents, so drop any existing state first.
        small_buffer_count = 0;
        buffer.clear();

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        /// Go straight to the hash map when the count won't fit in the small buffer, avoiding a fill-then-switch.
        if (sample_count > SMALL_BUFFER_SIZE)
            buffer.reserve(sample_count);
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            if (sample_count > SMALL_BUFFER_SIZE)
                addToBuffer(timestamp, value);
            else
                add(timestamp, value);
        }
    }

    /// Throws if any sample's timestamp is outside the range.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        forEachSample([&range](TimestampType timestamp, ValueType)
        {
            if (!range.contains(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(timestamp));
        });
    }

    /// Invokes `f(timestamp, value)` for every sample, from whichever storage is live (`small_buffer` in
    /// timestamp order, `buffer` samples in arbitrary order). Used by the aggregator policies below and by the
    /// per-function aggregators defined in the derived classes (e.g. linear regression).
    template <typename F>
    void forEachSample(F && f) const
    {
        if (!isSmall())
        {
            for (const auto & [timestamp, value] : buffer)
                f(timestamp, value);
        }
        else
        {
            for (size_t i = 0; i < small_buffer_count; ++i)
                f(small_buffer[i].first, small_buffer[i].second);
        }
    }

    /// `Aggregator` policy: builds the aggregation data from the bucket's samples, in arbitrary order
    /// (for order-independent aggregates).
    struct Aggregator
    {
        template <typename AggregationData>
        void aggregate(const AggregateFunctionTimeseriesSamples & bucket, AggregationData & data)
        {
            bucket.forEachSample([&data](TimestampType timestamp, ValueType value) { data.add(timestamp, value); });
        }
    };

    /// `Aggregator` policy: builds the aggregation data from the bucket's samples sorted by timestamp
    /// (for order-dependent aggregates, e.g. counting transitions or rate reset accounting). The
    /// `small_buffer` is already sorted, so the common small bucket is fed directly; only a bucket that has
    /// switched to the hash map needs the sort buffer (reused across buckets).
    struct SortedAggregator
    {
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> sorted_samples;

        template <typename AggregationData>
        void aggregate(const AggregateFunctionTimeseriesSamples & bucket, AggregationData & data)
        {
            if (bucket.isSmall())
            {
                for (size_t i = 0; i < bucket.small_buffer_count; ++i)
                    data.add(bucket.small_buffer[i].first, bucket.small_buffer[i].second);
                return;
            }

            sorted_samples.clear();
            sorted_samples.reserve(bucket.buffer.size());
            for (const auto & [timestamp, value] : bucket.buffer)
                sorted_samples.emplace_back(timestamp, value);
            std::sort(sorted_samples.begin(), sorted_samples.end());
            for (const auto & [timestamp, value] : sorted_samples)
                data.add(timestamp, value);
        }
    };

private:
    using Sample = std::pair<TimestampType, ValueType>;

    /// Small-buffer capacity. Bigger covers denser buckets without a hash map at the cost of a larger bucket
    /// struct; it is a storage-layout choice, not a performance threshold (the hash map is always correct, just
    /// slower).
    static constexpr size_t SMALL_BUFFER_SIZE = 4;

    std::array<Sample, SMALL_BUFFER_SIZE> small_buffer{};   /// kept sorted by timestamp, used while the bucket is small
    size_t small_buffer_count = 0;                          /// number of samples in `small_buffer` (0 once switched to the hash map)
    absl::flat_hash_map<TimestampType, ValueType> buffer;   /// non-empty once the bucket switches to the hash map

    bool isSmall() const { return buffer.empty(); }
    size_t size() const { return isSmall() ? small_buffer_count : buffer.size(); }

    void addToBuffer(TimestampType timestamp, ValueType value)
    {
        auto [it, inserted] = buffer.emplace(timestamp, value);
        if (!inserted)
            it->second = std::max(it->second, value);
    }

    /// Moves the small-buffer samples into the hash map and uses it for all subsequent samples.
    void switchToBuffer(size_t reserve_hint)
    {
        buffer.reserve(reserve_hint);
        for (size_t i = 0; i < small_buffer_count; ++i)
            buffer.emplace(small_buffer[i].first, small_buffer[i].second);
        small_buffer_count = 0;
    }
};

}
