#pragma once

#include <Core/Field.h>
#include <Core/TypeId.h>

#include "config.h"

#if USE_PROMETHEUS_PROTOBUFS
#include <prompb/remote.pb.h>
#endif


namespace DB
{
enum class TimeSeriesIDAlgorithm;

/// Calculates a time series identifier associated with a metric name and tags.
class ITimeSeriesIDCalculator
{
public:
    virtual ~ITimeSeriesIDCalculator() = default;
    
#if USE_PROMETHEUS_PROTOBUFS
    virtual Field calculateID(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels, TypeIndex result_type) = 0;
#endif
};

using TimeSeriesIDCalculatorPtr = std::shared_ptr<ITimeSeriesIDCalculator>;

/// Implementation of ITimeSeriesIDCalculator for a specific algorithms.
template <TimeSeriesIDAlgorithm algorithm> 
class TimeSeriesIDCalculatorImpl;

/// Factory for ITimeSeriesIDCalculator.
class TimeSeriesIDCalculatorFactory
{
public:
    static TimeSeriesIDCalculatorFactory & instance();
    TimeSeriesIDCalculatorPtr create(TimeSeriesIDAlgorithm algorithm) const;
};

}
