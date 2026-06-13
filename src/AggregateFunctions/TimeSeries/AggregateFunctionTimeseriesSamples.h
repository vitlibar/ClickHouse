#pragma once

#include <algorithm>
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
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    void add(TimestampType timestamp, ValueType value)
    {
        auto [it, inserted] = buffer.emplace(timestamp, value);
        if (!inserted)
            it->second = std::max(it->second, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        buffer.reserve(buffer.size() + other.buffer.size());
        for (const auto & [timestamp, value] : other.buffer)
            add(timestamp, value);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(buffer.size(), buf);
        for (const auto & [timestamp, value] : buffer)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces the bucket's contents, so drop any existing state first.
        buffer.clear();

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        buffer.reserve(sample_count);
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
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

    /// Invokes `f(timestamp, value)` for every sample, in arbitrary order. Used by the aggregator policies below
    /// and by the per-function aggregators defined in the derived classes (e.g. linear regression).
    template <typename F>
    void forEachSample(F && f) const
    {
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
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
    /// (for order-dependent aggregates, e.g. counting transitions or rate reset accounting). The sort buffer is
    /// reused across buckets.
    struct SortedAggregator
    {
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> sorted_samples;

        template <typename AggregationData>
        void aggregate(const AggregateFunctionTimeseriesSamples & bucket, AggregationData & data)
        {
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
    absl::flat_hash_map<TimestampType, ValueType> buffer;   /// samples keyed by timestamp
};

}
