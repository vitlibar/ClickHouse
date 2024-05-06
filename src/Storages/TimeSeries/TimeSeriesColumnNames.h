#pragma once


namespace DB
{

struct TimeSeriesColumnNames
{
    /// The "data" table.
    static constexpr const char * kID = "id";
    static constexpr const char * kTimestamp = "timestamp";
    static constexpr const char * kValue = "value";

    /// The "tags" table.
    //static constexpr const char * kID = "id"; /// The tags table also has the "id" column.
    static constexpr const char * kMetricName = "metric_name";
    static constexpr const char * kTags = "tags";

    /// The "metrics" table.
    static constexpr const char * kMetricFamilyName = "metric_family_name";
    static constexpr const char * kType = "type";
    static constexpr const char * kUnit = "unit";
    static constexpr const char * kHelp = "help";

    /// Generated columns of the main table.
    //static constexpr const char * kID = "id";
    //static constexpr const char * kMetricName = "metric_name";
    //static constexpr const char * kMetricFamilyName = "metric_family_name";
    //static constexpr const char * kType = "type";
    //static constexpr const char * kUnit = "unit";
    //static constexpr const char * kTags = "tags";
    static constexpr const char * kTimeSeries = "time_series";
    //static constexpr const char * kHelp = "help";
};

}
