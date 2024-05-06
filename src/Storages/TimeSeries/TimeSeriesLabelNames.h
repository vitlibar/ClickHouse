#pragma once


namespace DB
{

/// Label names with special meaning.
struct TimeSeriesLabelNames
{
    static constexpr const char * kMetricName = "__name__";
};

}
