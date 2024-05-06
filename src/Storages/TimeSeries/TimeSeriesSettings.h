#pragma once

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>


namespace DB
{
class ASTStorage;

/// Algorithm used to calculate id from the metric name and tag names and values.
enum class TimeSeriesIDAlgorithm
{
    SipHash,
};

DECLARE_SETTING_ENUM(TimeSeriesIDAlgorithm)

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(String, id_type, "UInt128", "Data type of the id columns in the data and metrics tables", 0) \
    M(TimeSeriesIDAlgorithm, id_algorithm, TimeSeriesIDAlgorithm::SipHash, "Algorithm to calculate id from metric name and tag names and values", 0) \
    M(String, id_codec, "", "Compression codec to compress the id column in the data table", 0) \
    M(String, timestamp_type, "DateTime64(3)", "Data type of the timestamp column in the data table", 0) \
    M(String, timestamp_codec, "", "Compression codec to compress the timestamp column in the data table", 0) \
    M(String, value_type, "Float64", "Data type of the value column in the data table", 0) \
    M(String, value_codec, "", "Compression codec to compress the value column in the data table", 0) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the metrics table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0)

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS)


/// Settings for the TimeSeries table engine.
/// Could be loaded from a CREATE TABLE query (SETTINGS clause). For example:
/// CREATE TABLE mytable ENGINE = TimeSeries() SETTINGS id_type = 'UInt128', id_codec='ZSTD(3)' DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
struct TimeSeriesSettings : public BaseSettings<TimeSeriesSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

}
