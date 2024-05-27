#include <Storages/TimeSeries/TimeSeriesIDCalculator.h>

#include <Common/SipHash.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_DATA_TYPE;
}


#if USE_PROMETHEUS_PROTOBUFS
Field TimeSeriesIDCalculatorSipHash::calculateID(
    const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels,
    TypeIndex result_type)
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
                "Cannot calculate time series ID: algorithm SipHash is incompatible with result type {}", result_type);
        }
    }
}
#endif


TimeSeriesIDCalculatorFactory & TimeSeriesIDCalculatorFactory::instance()
{
    static TimeSeriesIDCalculatorFactory factory;
    return factory;
}

TimeSeriesIDCalculatorPtr TimeSeriesIDCalculatorFactory::create(TimeSeriesIDAlgorithm algorithm) const
{
    switch (algorithm)
    {
        case TimeSeriesIDAlgorithm::SipHash: return std::make_shared<TimeSeriesIDCalculatorSipHash>();
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected algorithm for calculating ID: {}", algorithm);
}

}
