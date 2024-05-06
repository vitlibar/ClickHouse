#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTCreateQuery;
class ColumnsDescription;
struct ColumnDescription;
struct ColumnWithTypeAndName;
struct TimeSeriesSettings;

/// Validates the types of columns for a TimeSeries table.
class TimeSeriesDefinitionNormalizer
{
public:
    using TargetKind = ViewTarget::Kind;

    TimeSeriesDefinitionNormalizer(const StorageID & storage_id_) : storage_id(storage_id_) {}

    /// Adds missing columns and reorders the columns in the proper way.
    /// Also the function validates the types of columns, and throws an exception if some columns have illegal types.
    void addMissingColumnsAndValidate(ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const;

    /// Validates columns of a target table that a TimeSeries table is going to use.
    /// Throws an exception if some of the required columns don't exist or have illegal types.
    void validateTargetColumns(TargetKind target_kind, const ColumnsDescription & target_columns, const TimeSeriesSettings & time_series_settings) const;

    /// Each of the following functions validates a specific column type.
    void validateColumnForID(const ColumnDescription & column, bool check_default = true) const;
    void validateColumnForIDAndAddDefault(const ColumnDescription & column, std::optional<ColumnDescription> & out_column_with_default) const;
    void validateColumnForTimestamp(const ColumnDescription & column) const;
    void validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const;
    void validateColumnForValue(const ColumnDescription & column) const;

    void validateColumnForMetricName(const ColumnDescription & column) const;
    void validateColumnForTagValue(const ColumnDescription & column) const;
    void validateColumnForTagsMap(const ColumnDescription & column) const;

    void validateColumnForMetricFamilyName(const ColumnDescription & column) const;
    void validateColumnForType(const ColumnDescription & column) const;
    void validateColumnForUnit(const ColumnDescription & column) const;
    void validateColumnForHelp(const ColumnDescription & column) const;

    /// Sets the engines of target tables if they're inner
    void setInnerTablesEngines(ASTCreateQuery & create_query, const ContextPtr & context) const;

private:
    /// Returns true if the columns are complete and ordered in the proper way. Returns false if some columns are missing or the need to be reordered.
    /// Throws an exception if some columns can't be used by a TimeSeries table.
    bool areColumnsValid(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const;

    /// Validates the types of columns of a TimeSeries table.
    /// Throws an exception if some columns have illegal types, also adds missing columns and reorders the columns in the proper way.
    ColumnsDescription validateColumns(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const;

    /// Generates a formulae for calculating the identifier of a time series from the metric name and all the tags.
    ASTPtr chooseIDAlgorithm(const ColumnDescription & id_description) const;

    /// Sets the engine of an inner table by default.
    void setInnerTableDefaultEngine(ASTStorage & inner_storage_def, TargetKind inner_table_kind) const;

    StorageID storage_id;
};

}
