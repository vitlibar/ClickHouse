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
struct AggregateFunctionTimeseriesSamples
{
    absl::flat_hash_map<TimestampType, ValueType> samples;

    void add(TimestampType timestamp, ValueType value)
    {
        auto [it, inserted] = samples.emplace(timestamp, value);
        if (!inserted)
            it->second = std::max(it->second, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        samples.reserve(samples.size() + other.samples.size());

        for (const auto & [timestamp, value] : other.samples)
            add(timestamp, value);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(samples.size(), buf);
        for (const auto & [timestamp, value] : samples)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        samples.reserve(sample_count);
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
        for (const auto & [timestamp, _] : samples)
            if (!range.contains(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(timestamp));
    }

    /// `Aggregator` policy: builds the aggregation data from the bucket's samples, in arbitrary order
    /// (for order-independent aggregates).
    struct Aggregator
    {
        template <typename AggregationData>
        void aggregate(const AggregateFunctionTimeseriesSamples & bucket, AggregationData & data)
        {
            for (const auto & [timestamp, value] : bucket.samples)
                data.add(timestamp, value);
        }
    };

    /// `Aggregator` policy: builds the aggregation data from the bucket's samples sorted by timestamp
    /// (for order-dependent aggregates, e.g. counting transitions or rate reset accounting). The sort buffer
    /// is reused across buckets.
    struct SortedAggregator
    {
        VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> sorted_samples;

        template <typename AggregationData>
        void aggregate(const AggregateFunctionTimeseriesSamples & bucket, AggregationData & data)
        {
            sorted_samples.clear();
            sorted_samples.reserve(bucket.samples.size());
            for (const auto & [timestamp, value] : bucket.samples)
                sorted_samples.emplace_back(timestamp, value);
            std::sort(sorted_samples.begin(), sorted_samples.end());
            for (const auto & [timestamp, value] : sorted_samples)
                data.add(timestamp, value);
        }
    };
};

}
