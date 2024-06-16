#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int THERE_IS_NO_COLUMN;
}


void TimeSeriesColumnsValidator::validateColumns(ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    if (!areColumnsValid(columns, time_series_settings))
        columns = doValidateColumns(columns, time_series_settings);
}

bool TimeSeriesColumnsValidator::areColumnsValid(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    auto it = columns.begin();
    if (it->name == TimeSeriesColumnNames::ID)
    {
        std::optional<ColumnDescription> new_column;
        validateColumnForID(*it, /* check_default= */ true, time_series_settings, new_column);
        if (new_column.has_value())
            return false;
    }
    else
    {
        return false;
    }

    ++it;
    if (it->name == TimeSeriesColumnNames::Timestamp)
        validateColumnForTimestamp(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::Value)
        validateColumnForValue(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::MetricName)
        validateColumnForMetricName(*it);
    else
        return false;

    const Map & tags_to_columns = time_series_settings.tags_to_columns;
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        ++it;
        if (it->name == column_name)
            validateColumnForTagValue(*it);
        else
            return false;
    }

    if (time_series_settings.use_column_tags_for_other_tags)
    {
        ++it;
        if (it->name == TimeSeriesColumnNames::Tags)
            validateColumnForTagsMap(*it);
        else
            return false;
    }

    if (time_series_settings.enable_column_all_tags)
    {
        ++it;
        if (it->name == TimeSeriesColumnNames::AllTags)
            validateColumnForTagsMap(*it);
        else
            return false;
    }

    ++it;
    if (it->name == TimeSeriesColumnNames::MetricFamilyName)
        validateColumnForMetricFamilyName(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::Type)
        validateColumnForType(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::Unit)
        validateColumnForUnit(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::Help)
        validateColumnForHelp(*it);
    else
        return false;

    ++it;
    return it == columns.end();
}

/// Adds missing columns and reorders the columns.
ColumnsDescription TimeSeriesColumnsValidator::doValidateColumns(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    ColumnsDescription res;

    /// Column `id`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::ID))
        {
            std::optional<ColumnDescription> new_column;
            validateColumnForID(*column, /* check_default= */ true, time_series_settings, new_column);
            if (new_column)
                res.add(std::move(*new_column));
            else
                res.add(*column);
        }
        else
        {
            ColumnDescription new_column{TimeSeriesColumnNames::ID, std::make_shared<DataTypeUUID>()};
            new_column.default_desc.expression = chooseIDAlgorithm(new_column, time_series_settings);
            res.add(std::move(new_column));
        }
    }

    /// Column `timestamp`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Timestamp))
        {
            validateColumnForTimestamp(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::Timestamp, std::make_shared<DataTypeDateTime64>(3)});
        }
    }

    /// Column `value`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Value))
        {
            validateColumnForValue(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::Value, std::make_shared<DataTypeFloat64>()});
        }
    }

    /// Column `metric_name`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::MetricName))
        {
            validateColumnForMetricName(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeString>()});
        }
    }

    /// Columns corresponding to the `tags_to_columns` setting.
    const Map & tags_to_columns = time_series_settings.tags_to_columns;
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (const auto * column = columns.tryGet(column_name))
        {
            validateColumnForTagValue(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{column_name, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
        }
    }

    /// Column `tags`.
    if (time_series_settings.use_column_tags_for_other_tags)
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Tags))
        {
            validateColumnForTagsMap(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{
                TimeSeriesColumnNames::Tags,
                std::make_shared<DataTypeMap>(
                    std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>())});
        }
    }

    /// Column `all_tags`.
    if (time_series_settings.enable_column_all_tags)
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::AllTags))
        {
            validateColumnForTagsMap(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{
                TimeSeriesColumnNames::AllTags,
                std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())});
        }
    }

    /// Column `metric_family_name`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::MetricFamilyName))
        {
            validateColumnForMetricFamilyName(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::MetricFamilyName, std::make_shared<DataTypeString>()});
        }
    }

    /// Column `type`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Type))
        {
            validateColumnForType(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::Type, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
        }
    }

    /// Column `unit`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Unit))
        {
            validateColumnForUnit(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::Unit, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
        }
    }

    /// Column `help`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::Help))
        {
            validateColumnForHelp(*column);
            res.add(*column);
        }
        else
        {
            res.add(ColumnDescription{TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>()});
        }
    }

    /// Each of the passed columns must present in `res`.
    for (const auto & column : columns)
    {
        if (!res.has(column.name))
        {
            throw Exception(
                ErrorCodes::INCOMPATIBLE_COLUMNS,
                "{}: Column {} can't be used in this table. "
                "The TimeSeries table engine supports only a limited set of columns (id, timestamp, value, metric_name, tags, metric_family_name, type, unit, help). "
                "Extra columns representing tags must be specified in the 'tags_to_columns' setting.",
                storage_id.getNameForLogs(), column.name);
        }
    }

    chassert(areColumnsValid(res, time_series_settings));
    return res;
}

/// Generates a formulae for calculating the identifier of a time series from the metric name and all the tags.
ASTPtr TimeSeriesColumnsValidator::chooseIDAlgorithm(const ColumnDescription & id_description, const TimeSeriesSettings & time_series_settings) const
{
    ASTs arguments_for_hash_function;
    arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

    if (time_series_settings.enable_column_all_tags)
    {
        arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::AllTags));
    }
    else
    {
        const Map & tags_to_columns = time_series_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(column_name));
        }
        if (time_series_settings.use_column_tags_for_other_tags)
            arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));
    }

    auto make_hash_function = [&](const String & function_name)
    {
        auto function = std::make_shared<ASTFunction>();
        function->name = function_name;
        auto arguments_list = std::make_shared<ASTExpressionList>();
        arguments_list->children = std::move(arguments_for_hash_function);
        function->arguments = arguments_list;
        return function;
    };

    const auto & id_type = *id_description.type;
    WhichDataType id_type_which(id_type);

    if (id_type_which.isUInt64())
    {
        return make_hash_function("sipHash64");
    }
    else if (id_type_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(id_type).getN() == 16)
    {
        return make_hash_function("sipHash128");
    }
    else if (id_type_which.isUUID())
    {
        return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));
    }
    else if (id_type_which.isUInt128())
    {
        return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));
    }
    else
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: The DEFAULT expression for column {} must contain an expression "
                        "which will be used to calculate the identifier of each time series: {} {} DEFAULT ... "
                        "If the DEFAULT expression is not specified then it can be chosen implicitly but only if the column type is one of these: UInt64, UInt128, UUID. "
                        "For type {} the DEFAULT expression can't be chosen automatically, so please specify it explicitly",
                        storage_id.getNameForLogs(), id_description.name, id_description.name, id_type.getName(), id_type.getName());
    }
}

void TimeSeriesColumnsValidator::validateTargetColumns(TargetKind target_kind, const ColumnsDescription & target_columns, const TimeSeriesSettings & time_series_settings) const
{
    auto get_column_description = [&](const String & column_name) -> const ColumnDescription &
    {
        const auto * column = target_columns.tryGet(column_name);
        if (!column)
        {
            throw Exception(
                ErrorCodes::THERE_IS_NO_COLUMN,
                "{}: Column {} is required for the TimeSeries table engine",
                storage_id.getNameForLogs(), column_name);
        }
        return *column;
    };

    switch (target_kind)
    {
        case TargetKind::Data:
        {
            /// Here "check_default = false" because it's ok for the "id" column in the target table not to contain
            /// an expression for calculating the identifier of a time series.
            validateColumnForID(get_column_description(TimeSeriesColumnNames::ID), /* check_default= */ false);
            validateColumnForTimestamp(get_column_description(TimeSeriesColumnNames::Timestamp));
            validateColumnForValue(get_column_description(TimeSeriesColumnNames::Value));
            break;
        }

        case TargetKind::Tags:
        {
            validateColumnForMetricName(get_column_description(TimeSeriesColumnNames::MetricName));

            const Map & tags_to_columns = time_series_settings.tags_to_columns;
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                validateColumnForTagValue(get_column_description(column_name));
            }

            validateColumnForTagsMap(get_column_description(TimeSeriesColumnNames::Tags));
            break;
        }

        case TargetKind::Metrics:
        {
            validateColumnForMetricFamilyName(get_column_description(TimeSeriesColumnNames::MetricFamilyName));
            validateColumnForType(get_column_description(TimeSeriesColumnNames::Type));
            validateColumnForUnit(get_column_description(TimeSeriesColumnNames::Unit));
            validateColumnForHelp(get_column_description(TimeSeriesColumnNames::Help));
            break;
        }

        default:
            UNREACHABLE();
    }
}

void TimeSeriesColumnsValidator::validateColumnForID(const ColumnDescription & column, bool check_default) const
{
    if (!check_default || column.default_desc.expression)
        return;

    throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: The DEFAULT expression for column {} must contain an expression "
                    "which will be used to calculate the identifier of each time series: {} {} DEFAULT ...",
                    storage_id.getNameForLogs(), column.name, column.name, column.type->getName());
}

void TimeSeriesColumnsValidator::validateColumnForID(const ColumnDescription & column, bool check_default, const TimeSeriesSettings & time_series_settings, std::optional<ColumnDescription> & out_corrected_version) const
{
    if (!check_default || column.default_desc.expression)
        return;

    out_corrected_version = column;
    out_corrected_version->default_desc.expression = chooseIDAlgorithm(column, time_series_settings);
}

void TimeSeriesColumnsValidator::validateColumnForTimestamp(const ColumnDescription & column) const
{
    if (!isDateTime64(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected DateTime64(s)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const
{
    auto maybe_datetime64_type = removeLowCardinalityAndNullable(column.type);
    if (!isDateTime64(maybe_datetime64_type))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected DateTime64(s)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
    const auto & datetime64_type = typeid_cast<const DataTypeDateTime64 &>(*maybe_datetime64_type);
    out_scale = datetime64_type.getScale();
}

void TimeSeriesColumnsValidator::validateColumnForValue(const ColumnDescription & column) const
{
    if (!isFloat(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected Float32 or Float64",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForMetricName(const ColumnDescription & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesColumnsValidator::validateColumnForMetricName(const ColumnWithTypeAndName & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesColumnsValidator::validateColumnForTagValue(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagValue(const ColumnWithTypeAndName & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagsMap(const ColumnDescription & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinalityAndNullable(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinalityAndNullable(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForTagsMap(const ColumnWithTypeAndName & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinalityAndNullable(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinalityAndNullable(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForMetricFamilyName(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesColumnsValidator::validateColumnForType(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesColumnsValidator::validateColumnForUnit(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesColumnsValidator::validateColumnForHelp(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

}
