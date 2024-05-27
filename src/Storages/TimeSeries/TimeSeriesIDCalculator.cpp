#include <Storages/TimeSeries/TimeSeriesIDCalculator.h>

#include <Common/SipHash.h>
#include <Storages/TimeSeries/TimeSeriesIDAlgorithm.h>
#include <Storages/TimeSeries/TimeSeriesLabelNames.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_TYPE;
}

template <>
class TimeSeriesIDCalculatorImpl<TimeSeriesIDAlgorithm::SipHash> : public ITimeSeriesIDCalculator
{
public:
    static const constexpr auto algorithm = TimeSeriesIDAlgorithm::SipHash;

#if USE_PROMETHEUS_PROTOBUFS
    Field calculateID(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels, TypeIndex result_type) override
    {
        SipHash sip_hash;
        for (const auto & label : labels)
        {
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            sip_hash.update(label_name.data(), label_name.length());
            sip_hash.update(label_value.data(), label_value.length());
        }

        switch (result_type)
        {
            case TypeIndex::UInt128:
            {
                return sip_hash.get128();
            }
            case TypeIndex::UInt64:
            {
                return sip_hash.get64();
            }
            default:
            {
                throw Exception(
                    ErrorCodes::UNEXPECTED_DATA_TYPE,
                    "Cannot calculate time series ID: algorithm {} is incompatible with result type {}", algorithm, result_type);
            }
        }
    }
#endif
};


template <>
class TimeSeriesIDCalculatorImpl<TimeSeriesIDAlgorithm::SipHash_MetricNameLow64_And_TagsHigh64> : public ITimeSeriesIDCalculator
{
public:
    static const constexpr auto algorithm = TimeSeriesIDAlgorithm::SipHash_MetricNameLow64_And_TagsHigh64;

#if USE_PROMETHEUS_PROTOBUFS
    Field calculateID(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels, TypeIndex result_type) override
    {
        SipHash metric_name_hash;
        SipHash tags_hash;
        for (const auto & label : labels)
        {
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            if (label_name == TimeSeriesLabelNames::kMetricName)
            {
                metric_name_hash.update(label_value.data(), label_value.length());
            }
            else
            {
                tags_hash.update(label_name.data(), label_name.length());
                tags_hash.update(label_value.data(), label_value.length());
            }
        }

        if (result_type != TypeIndex::UInt128)
        {
            return metric_name_hash.get64() | (static_cast<UInt128>(tags_hash.get64()) << 64);
        }
        else
        {
            throw Exception(
                ErrorCodes::UNEXPECTED_DATA_TYPE,
                    "Cannot calculate time series ID: algorithm {} is incompatible with result type {}", algorithm, result_type);
        }
    }
#endif
};


TimeSeriesIDCalculatorFactory & TimeSeriesIDCalculatorFactory::instance()
{
    static TimeSeriesIDCalculatorFactory factory;
    return factory;
}

TimeSeriesIDCalculatorPtr TimeSeriesIDCalculatorFactory::create(TimeSeriesIDAlgorithm algorithm) const
{
    switch (algorithm)
    {
        case TimeSeriesIDAlgorithm::SipHash:
            return std::make_shared<TimeSeriesIDCalculatorImpl<TimeSeriesIDAlgorithm::SipHash>>();

        case TimeSeriesIDAlgorithm::SipHash_MetricNameLow64_And_TagsHigh64:
            return std::make_shared<TimeSeriesIDCalculatorImpl<TimeSeriesIDAlgorithm::SipHash_MetricNameLow64_And_TagsHigh64>>();
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected algorithm for calculating ID: {}", algorithm);
}

}
