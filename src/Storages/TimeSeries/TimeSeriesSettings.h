#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
class ASTStorage;

#define LIST_OF_TIME_SERIES_SETTINGS(M, ALIAS) \
    M(Map, tags_to_columns, Map{}, "Map specifying which tags should be put to separate columns of the 'tags' table. Syntax: {'tag1': 'column1', 'tag2' : column2, ...}", 0) \
    M(Bool, use_column_tags_for_other_tags, true, "If set to true then all tags unspecified to the 'tags_to_column' setting will be stored in a column named 'tags'. If set to false then only tags specified in the 'tags_to_column' setting will be allowed", 0) \
    M(Bool, enable_column_all_tags, true, "If set to true then the table will contain a column named 'all_tags', containing all tags, including both those ones which are specified in the 'tags_to_columns' setting and those ones which are stored in the 'tags' column. The 'all_tags' column is not stored anywhere, it's generated on the fly", 0) \
    M(Bool, copy_id_default_to_tags_table, true, "When creating an inner target 'tags' table, this flag enables setting the default expression for the 'id' column", 0) \
    M(Bool, create_ephemeral_all_tags_in_tags_table, true, "When creating an inner target 'tags' table, this flag enables creating an ephemeral column named 'all_tags'", 0)

DECLARE_SETTINGS_TRAITS(TimeSeriesSettingsTraits, LIST_OF_TIME_SERIES_SETTINGS)

/// Settings for the TimeSeries table engine.
/// Could be loaded from a CREATE TABLE query (SETTINGS clause). For example:
/// CREATE TABLE mytable ENGINE = TimeSeries() SETTINGS tags_to_columns = {'job':'job', 'instance':'instance'} DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
struct TimeSeriesSettings : public BaseSettings<TimeSeriesSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

}
