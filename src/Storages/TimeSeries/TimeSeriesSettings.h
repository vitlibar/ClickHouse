#pragma once

#include <Core/BaseSettings.h>
#include <Storages/TimeSeries/TimeSeriesIDAlgorithm.h>


namespace DB
{
class ASTStorage;

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(TimeSeriesIDAlgorithm, id_algorithm, TimeSeriesIDAlgorithm::SipHash, "Algorithm used to calculate id from the metric name and tag names and values", 0) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the metrics table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    M(Map, extra_columns, Map{}, "Map specifying extra columns with no special meaning for the table engine in each inner table. Syntax: {'column1': 'data', 'column2': 'tags', 'column3': 'metrics'}", 0)

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
