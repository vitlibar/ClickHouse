#include <Storages/TimeSeries/TimeSeriesDefinitionNormalizer.h>

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
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
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
    extern const int INCORRECT_QUERY;
    extern const int THERE_IS_NO_COLUMN;
}


void TimeSeriesDefinitionNormalizer::addMissingColumnsAndValidate(ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    if (!areColumnsValid(columns, time_series_settings))
        columns = validateColumns(columns, time_series_settings);
}

bool TimeSeriesDefinitionNormalizer::areColumnsValid(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    auto it = columns.begin();
    if (it->name == TimeSeriesColumnNames::ID)
    {
        std::optional<ColumnDescription> new_column;
        validateColumnForIDAndAddDefault(*it, new_column);
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

    ++it;
    if (it->name == TimeSeriesColumnNames::Tags)
        validateColumnForTagsMap(*it);
    else
        return false;

    ++it;
    if (it->name == TimeSeriesColumnNames::AllTags)
        validateColumnForTagsMap(*it);
    else
        return false;

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
ColumnsDescription TimeSeriesDefinitionNormalizer::validateColumns(const ColumnsDescription & columns, const TimeSeriesSettings & time_series_settings) const
{
    ColumnsDescription res;

    /// Column `id`.
    {
        if (const auto * column = columns.tryGet(TimeSeriesColumnNames::ID))
        {
            std::optional<ColumnDescription> new_column;
            validateColumnForIDAndAddDefault(*column, new_column);
            if (new_column)
                res.add(std::move(*new_column));
            else
                res.add(*column);
        }
        else
        {
            ColumnDescription new_column{TimeSeriesColumnNames::ID, std::make_shared<DataTypeUUID>()};
            new_column.default_desc.expression = chooseIDAlgorithm(new_column);
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
            res.add(ColumnDescription{TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
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
                std::make_shared<DataTypeMap>(
                    std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>())});
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
ASTPtr TimeSeriesDefinitionNormalizer::chooseIDAlgorithm(const ColumnDescription & id_description) const
{
    ASTs arguments_for_hash_function;
    arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
    arguments_for_hash_function.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::AllTags));

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

void TimeSeriesDefinitionNormalizer::validateTargetColumns(TargetKind target_kind, const ColumnsDescription & target_columns, const TimeSeriesSettings & time_series_settings) const
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

void TimeSeriesDefinitionNormalizer::validateColumnForID(const ColumnDescription & column, bool check_default) const
{
    if (!check_default || column.default_desc.expression)
        return;

    throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: The DEFAULT expression for column {} must contain an expression "
                    "which will be used to calculate the identifier of each time series: {} {} DEFAULT ...",
                    storage_id.getNameForLogs(), column.name, column.name, column.type->getName());
}

void TimeSeriesDefinitionNormalizer::validateColumnForIDAndAddDefault(const ColumnDescription & column, std::optional<ColumnDescription> & out_column_with_default) const
{
    if (column.default_desc.expression)
        return;

    out_column_with_default = column;
    out_column_with_default->default_desc.expression = chooseIDAlgorithm(column);
}

void TimeSeriesDefinitionNormalizer::validateColumnForTimestamp(const ColumnDescription & column) const
{
    if (!isDateTime64(removeNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected DateTime64",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForTimestamp(const ColumnDescription & column, UInt32 & out_scale) const
{
    auto maybe_datetime64_type = removeNullable(column.type);
    if (!isDateTime64(maybe_datetime64_type))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected DateTime64",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
    const auto & datetime64_type = typeid_cast<const DataTypeDateTime64 &>(*maybe_datetime64_type);
    out_scale = datetime64_type.getScale();
}

void TimeSeriesDefinitionNormalizer::validateColumnForValue(const ColumnDescription & column) const
{
    if (!isFloat(removeNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected Float32 or Float64",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForMetricName(const ColumnDescription & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesDefinitionNormalizer::validateColumnForMetricName(const ColumnWithTypeAndName & column) const
{
    validateColumnForTagValue(column);
}

void TimeSeriesDefinitionNormalizer::validateColumnForTagValue(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForTagValue(const ColumnWithTypeAndName & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForTagsMap(const ColumnDescription & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForTagsMap(const ColumnWithTypeAndName & column) const
{
    if (!isMap(column.type)
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getKeyType()))
        || !isString(removeLowCardinality(typeid_cast<const DataTypeMap &>(*column.type).getValueType())))
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: Column {} has illegal data type {}, expected Map(String, String) or Map(LowCardinality(String), String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForMetricFamilyName(const ColumnDescription & column) const
{
    if (!isString(removeLowCardinalityAndNullable(column.type)))
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "{}: Column {} has illegal data type {}, expected String or LowCardinality(String)",
                        storage_id.getNameForLogs(), column.name, column.type->getName());
    }
}

void TimeSeriesDefinitionNormalizer::validateColumnForType(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesDefinitionNormalizer::validateColumnForUnit(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}

void TimeSeriesDefinitionNormalizer::validateColumnForHelp(const ColumnDescription & column) const
{
    validateColumnForMetricFamilyName(column);
}


void TimeSeriesDefinitionNormalizer::setInnerTablesEngines(ASTCreateQuery & create_query, const ContextPtr & context) const
{
    /// Check if the engines of inner tables are already set.
    bool all_is_set = true;

    for (auto kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
    {
        if (create_query.hasTargetTableID(kind))
            continue; /// An external target doesn't need a table engine.

        if (auto target_engine = create_query.getTargetTableEngine(kind))
        {
            if (!target_engine->engine)
            {
                /// Some part of storage definition (such as PARTITION BY) is specified, but ENGINE is not: just set default one.
                setInnerTableDefaultEngine(*create_query.getTargetTableEngine(kind), kind);
            }
            continue;
        }

        /// The engine of the inner table is not set yet, the rest of this function will take case about it.
        all_is_set = false;
    }

    if (all_is_set)
        return;

    /// We'll try to extract storage definitions from clause `AS`:
    ///     CREATE TABLE time_series_table_name AS other_time_series_table_name
    if (!create_query.as_table.empty())
    {
        const String & as_table_name = create_query.as_table;
        String as_database_name = context->resolveDatabase(create_query.as_database);
        ASTPtr as_create_ptr = DatabaseCatalog::instance().getDatabase(as_database_name)->getCreateTableQuery(as_table_name, context);
        const auto & as_create = as_create_ptr->as<ASTCreateQuery &>();

        for (auto kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
        {
            if (as_create.hasTargetTableID(kind))
            {
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Cannot CREATE a table AS {}.{} because it is a TimeSeries with external tables",
                    backQuoteIfNeed(as_database_name), backQuoteIfNeed(as_table_name));
            }
            create_query.setTargetTableEngine(kind, as_create.getTargetTableEngine(kind));
        }
    }

    /// Set ENGINEs by default.
    for (auto kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
    {
        if (!create_query.getTargetTableEngine(kind))
        {
            auto inner_storage_def = std::make_shared<ASTStorage>();
            setInnerTableDefaultEngine(*inner_storage_def, kind);
            create_query.setTargetTableEngine(kind, inner_storage_def);
        }
    }
}

void TimeSeriesDefinitionNormalizer::setInnerTableDefaultEngine(ASTStorage & inner_storage_def, TargetKind inner_table_kind) const
{
    switch (inner_table_kind)
    {
        case TargetKind::Data:
        {
            inner_storage_def.set(inner_storage_def.engine, makeASTFunction("MergeTree"));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.order_by,
                                      makeASTFunction("tuple",
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID),
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
            }
            break;
        }

        case TargetKind::Tags:
        {
            inner_storage_def.set(inner_storage_def.engine, makeASTFunction("ReplacingMergeTree"));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.primary_key,
                                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                ASTs order_by_list;
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));

                auto order_by_tuple = std::make_shared<ASTFunction>();
                order_by_tuple->name = "tuple";
                auto arguments_list = std::make_shared<ASTExpressionList>();
                arguments_list->children = std::move(order_by_list);
                order_by_tuple->arguments = arguments_list;
                inner_storage_def.set(inner_storage_def.order_by, order_by_tuple);
            }
            break;
        }

        case TargetKind::Metrics:
        {
            inner_storage_def.set(inner_storage_def.engine, makeASTFunction("ReplacingMergeTree"));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.order_by, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
            }
            break;
        }

        default:
            UNREACHABLE();
    }
}

}
