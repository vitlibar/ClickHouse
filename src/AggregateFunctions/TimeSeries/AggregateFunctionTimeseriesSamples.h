#pragma once

#include <algorithm>
#include <utility>

#include <absl/container/flat_hash_map.h>

#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// Per-bucket storage of timeseries samples keyed by timestamp.
/// When two samples share a timestamp the larger value is kept.
template <typename TimestampType, typename ValueType>
struct AggregateFunctionTimeseriesSamples
{
    absl::flat_hash_map<TimestampType, ValueType> samples;

    void add(TimestampType timestamp, ValueType value)
    {
        auto it = samples.find(timestamp);
        if (it != samples.end())
            it->second = std::max(it->second, value);
        else
            samples[timestamp] = value;
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        samples.reserve(samples.size() + other.samples.size());

        for (const auto & [timestamp, value] : other.samples)
            add(timestamp, value);
    }

    /// `BucketAggregator` policy: builds the aggregation data from the bucket's samples, in arbitrary order
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

    /// `BucketAggregator` policy: builds the aggregation data from the bucket's samples sorted by timestamp
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
