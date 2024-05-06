#include <Storages/TimeSeries/TimeSeriesIDAlgorithm.h>

#include <boost/range/adaptor/map.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


IMPLEMENT_SETTING_ENUM(TimeSeriesIDAlgorithm, ErrorCodes::BAD_ARGUMENTS,
{
    {"SipHash", TimeSeriesIDAlgorithm::SipHash},
    {"SipHash_MetricNameLow64_And_TagsHigh64", TimeSeriesIDAlgorithm::SipHash_MetricNameLow64_And_TagsHigh64},
})

}
