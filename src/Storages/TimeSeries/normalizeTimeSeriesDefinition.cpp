#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/StorageID.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsDataType scalar_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsDataType timestamp_type;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int UNKNOWN_TABLE;
}


namespace
{
    /// Extracts the ID generator function from a column's default expression.
    /// Returns `nullptr` and logs a warning if the expression is not a function.
    boost::intrusive_ptr<ASTFunction> extractIDGeneratorFromDefaultExpression(ASTPtr default_expression, const StorageID & table_id)
    {
        if (!default_expression)
            return nullptr;

        if (!typeid_cast<ASTFunction *>(default_expression.get()))
        {
            /// If the expression to generate ID is not a function then something is wrong.
            LOG_WARNING(
                getLogger("TimeSeries"),
                "{}: Expression {} specified for generating ID (fingerprint) won't work because it's not a function. "
                "The expression will be replaced with the default one",
                table_id.getNameForLogs(), default_expression->formatForLogging());
        }

        return boost::static_pointer_cast<ASTFunction>(default_expression->clone());
    }

    /// Extracts types and generators from the columns if they are not specified in the settings.
    void extractSettingsFromColumns(TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        if (settings[TimeSeriesSetting::timestamp_type] && settings[TimeSeriesSetting::scalar_type] && settings[TimeSeriesSetting::id_type]
            && settings[TimeSeriesSetting::id_generator])
            return; /// Already got these settings

        if (!create_query.columns_list || !create_query.columns_list->columns)
            return; /// Can't get these settings

        for (const auto & column : create_query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (column_declaration->name == TimeSeriesColumnNames::Timestamp)
            {
                if (!settings[TimeSeriesSetting::timestamp_type] && column_declaration->getType())
                    settings[TimeSeriesSetting::timestamp_type] = DataTypeFactory::instance().get(column_declaration->getType());
            }
            else if (column_declaration->name == TimeSeriesColumnNames::Value)
            {
                if (!settings[TimeSeriesSetting::scalar_type] && column_declaration->getType())
                    settings[TimeSeriesSetting::scalar_type] = DataTypeFactory::instance().get(column_declaration->getType());
            }
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
            {
                if (!settings[TimeSeriesSetting::id_type] && column_declaration->getType())
                    settings[TimeSeriesSetting::id_type] = DataTypeFactory::instance().get(column_declaration->getType());

                if (!settings[TimeSeriesSetting::id_generator] && column_declaration->getDefaultExpression())
                    settings[TimeSeriesSetting::id_generator] = extractIDGeneratorFromDefaultExpression(
                        column_declaration->getDefaultExpression(), StorageID{create_query.getDatabase(), create_query.getTable()});
            }
        }
    }

    /// Extracts types and generators from an external target table if they are not specified in the settings.
    void extractSettingsFromTargetTable(TimeSeriesSettings & settings, const ASTCreateQuery & create_query, ViewTarget::Kind kind, const ContextPtr & context)
    {
        if ((kind == ViewTarget::Samples) && settings[TimeSeriesSetting::timestamp_type] && settings[TimeSeriesSetting::scalar_type])
            return; /// Already got these settings
        if ((kind == ViewTarget::Tags) && settings[TimeSeriesSetting::id_type] && settings[TimeSeriesSetting::id_generator])
            return; /// Already got these settings

        if (!create_query.targets)
            return; /// No external target table, can't get these settings

        auto target_table_id = create_query.targets->getTableID(kind);
        if (!target_table_id)
            return; /// No external target table, can't get these settings

        auto target_table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(target_table_id), context);
        if (!target_table)
        {
            /// External target table is specified and must exist.
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", target_table_id.getNameForLogs());
        }

        auto metadata = target_table->getInMemoryMetadataPtr();

        for (const auto & column : metadata->columns)
        {
            if (column.name == TimeSeriesColumnNames::Timestamp)
            {
                if (!settings[TimeSeriesSetting::timestamp_type])
                    settings[TimeSeriesSetting::timestamp_type] = column.type;
            }
            else if (column.name == TimeSeriesColumnNames::Value)
            {
                if (!settings[TimeSeriesSetting::scalar_type])
                    settings[TimeSeriesSetting::scalar_type] = column.type;
            }
            else if (column.name == TimeSeriesColumnNames::ID)
            {
                if (!settings[TimeSeriesSetting::id_type])
                    settings[TimeSeriesSetting::id_type] = column.type;

                /// The default expression for the "id" column is used to calculate it on insertion new time series,
                /// so we need it.
                if (!settings[TimeSeriesSetting::id_generator] && column.default_desc.expression)
                    settings[TimeSeriesSetting::id_generator]
                        = extractIDGeneratorFromDefaultExpression(column.default_desc.expression, target_table_id);
            }
        }
    }

    /// Sets types of timestamps, scalars and identifiers by default if they are not set yet.
    void setTypesByDefault(TimeSeriesSettings & settings)
    {
        if (!settings[TimeSeriesSetting::timestamp_type])
            settings[TimeSeriesSetting::timestamp_type] = DataTypeFactory::instance().get(makeASTDataType("DateTime64", make_intrusive<ASTLiteral>(3u)));

        if (!settings[TimeSeriesSetting::scalar_type])
            settings[TimeSeriesSetting::scalar_type] = DataTypeFactory::instance().get(makeASTDataType("Float64"));

        if (!settings[TimeSeriesSetting::id_type])
            settings[TimeSeriesSetting::id_type] = DataTypeFactory::instance().get(makeASTDataType("UUID"));
    }

    /// Generates a formulae for calculating the identifier of a time series from the metric name and all the tags.
    void setIDGeneratorByDefault(TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        if (settings[TimeSeriesSetting::id_generator])
            return;

        /// Build a list of arguments for a hash function.
        /// All hash functions below allow multiple arguments, so we use two arguments: metric_name, all_tags.
        ASTs arguments_for_hash_function;
        arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

        if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
        {
            arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::AllTags));
        }
        else
        {
            const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(column_name));
            }
            arguments_for_hash_function.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        }

        auto make_hash_function = [&](const String & function_name) -> boost::intrusive_ptr<ASTFunction>
        {
            auto function = make_intrusive<ASTFunction>();
            function->name = function_name;
            auto arguments_list = make_intrusive<ASTExpressionList>();
            arguments_list->children = std::move(arguments_for_hash_function);
            function->arguments = arguments_list;
            return function;
        };

        /// The type of the hash function depends on the type of the 'id' column.
        DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
        WhichDataType id_which(*id_type);

        if (id_which.isUInt64())
        {
            settings[TimeSeriesSetting::id_generator] = make_hash_function("sipHash64");
            return;
        }

        if (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
        {
            settings[TimeSeriesSetting::id_generator] = make_hash_function("sipHash128");
            return;
        }

        if (id_which.isUUID())
        {
            settings[TimeSeriesSetting::id_generator] = makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));
            return;
        }

        if (id_which.isUInt128())
        {
            settings[TimeSeriesSetting::id_generator] = makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));
            return;
        }

        StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
    }

    /// Checks that the settings are correct.
    void validateSettings(const TimeSeriesSettings & settings, const ASTCreateQuery & create_query)
    {
        DataTypePtr timestamp_type = settings[TimeSeriesSetting::timestamp_type];
        WhichDataType timestamp_which{*timestamp_type};
        bool timestamp_ok = timestamp_which.isDateTime64() || timestamp_which.isDateTime() || timestamp_which.isUInt32();
        if (!timestamp_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), timestamp_type->getName(), TimeSeriesColumnNames::Timestamp);
        }

        DataTypePtr scalar_type = settings[TimeSeriesSetting::scalar_type];
        WhichDataType scalar_which{*scalar_type};
        bool scalar_ok = scalar_which.isFloat64() || scalar_which.isFloat32();
        if (!scalar_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), scalar_type->getName(), TimeSeriesColumnNames::Value);
        }

        DataTypePtr id_type = settings[TimeSeriesSetting::id_type];
        WhichDataType id_which{*id_type};
        bool id_ok = id_which.isUInt64() || (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
            || id_which.isUUID() || id_which.isUInt128();
        if (!id_ok)
        {
            StorageID time_series_table_id{create_query.getDatabase(), create_query.getTable()};
            throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column", time_series_table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }
    }

    ColumnsDescription getColumnList(const ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode)
    {
        if (!create_query.columns_list || !create_query.columns_list->columns)
            return {};
        return InterpreterCreateQuery::getColumnsDescription(*create_query.columns_list->columns, context, mode);
    }

    /// Recreates the column list in a `CREATE TABLE` query from a `ColumnsDescription`.
    void setColumnList(ASTCreateQuery & create_query, const ColumnsDescription & columns)
    {
        if (!create_query.columns_list)
            create_query.set(create_query.columns_list, make_intrusive<ASTColumns>());

        create_query.columns_list->setOrReplace(create_query.columns_list->columns,
            InterpreterCreateQuery::formatColumns(columns));
    }

    /// Returns the default inner engine definition for a given target table kind.
    boost::intrusive_ptr<ASTStorage> getInnerEngineByDefault(const TimeSeriesSettings & settings, ViewTarget::Kind target_kind)
    {
        auto storage = make_intrusive<ASTStorage>();

        if (target_kind == ViewTarget::Samples)
        {
            auto engine = makeASTFunction("MergeTree");
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);
            storage->set(storage->order_by,
                makeASTOperator("tuple",
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID),
                    make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
        }
        else if (target_kind == ViewTarget::Tags)
        {
            std::string_view engine_name = settings[TimeSeriesSetting::aggregate_min_time_and_max_time]
                ? "AggregatingMergeTree"
                : "ReplacingMergeTree";
            auto engine = makeASTFunction(engine_name);
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);

            storage->set(storage->primary_key, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

            ASTs order_by_list;
            order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
            order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
            if (settings[TimeSeriesSetting::store_min_time_and_max_time] && !settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
            {
                order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                order_by_list.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));
            }
            auto order_by_tuple = make_intrusive<ASTFunction>();
            order_by_tuple->name = "tuple";
            auto arguments_list = make_intrusive<ASTExpressionList>();
            arguments_list->children = std::move(order_by_list);
            order_by_tuple->arguments = arguments_list;
            storage->set(storage->order_by, order_by_tuple);
        }
        else if (target_kind == ViewTarget::Metrics)
        {
            auto engine = makeASTFunction("ReplacingMergeTree");
            engine->setNoEmptyArgs(false);
            storage->set(storage->engine, engine);
            storage->set(storage->order_by, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        }

        return storage;
    }
}


bool normalizeTimeSeriesSettings(TimeSeriesSettings & settings, const ASTCreateQuery & create_query, const ContextPtr & context)
{
    bool changed = false;

    if (!settings[TimeSeriesSetting::timestamp_type] || !settings[TimeSeriesSetting::scalar_type] || !settings[TimeSeriesSetting::id_type]
        || !settings[TimeSeriesSetting::id_generator])
    {
        if (!create_query.as_table.empty())
        {
            /// The other table specified in AS <other_table> must exist.
            auto other_database = context->resolveDatabase(create_query.as_database);
            auto other_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
                DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));
            if (other_create_query->storage)
                settings.loadFromQuery(*other_create_query->storage);
            extractSettingsFromColumns(settings, *other_create_query);
        }
        else
        {
            if (create_query.storage)
                settings.loadFromQuery(*create_query.storage);
            extractSettingsFromColumns(settings, create_query);
        }

        extractSettingsFromTargetTable(settings, create_query, ViewTarget::Samples, context);
        extractSettingsFromTargetTable(settings, create_query, ViewTarget::Tags, context);
        setTypesByDefault(settings);
        changed = true;
    }

    if (!settings[TimeSeriesSetting::id_generator])
    {
        setIDGeneratorByDefault(settings, create_query);
        changed = true;
    }

    validateSettings(settings, create_query);
    return changed;
}


bool normalizeTimeSeriesColumns(ColumnsDescription & columns, const TimeSeriesSettings & settings)
{
    /// Get the list of names of all original columns.
    std::unordered_set<String> original_column_names;
    for (const auto & column : columns)
        original_column_names.insert(column.name);

    ColumnsDescription new_columns;
    bool changed = false;

    /// Lambda to add a new or replaced column, marking that a change has occurred.
    auto add_column = [&](ColumnDescription && col)
    {
        new_columns.add(std::move(col));
        changed = true;
    };

    /// Lambda to move an original column to the destination list.
    auto move_original_column = [&](const String & name) -> bool
    {
        if (!original_column_names.erase(name))
            return false;
        new_columns.add(columns.get(name));
        return true;
    };

    /// Lambda to move an original column to the destination list only if its type matches the expected one.
    /// If the column exists but its type doesn't match, it is still erased from the original list
    /// (so it won't appear among the remaining columns at the end).
    auto move_original_column_if_type = [&](const String & name, const DataTypePtr & expected_type) -> bool
    {
        if (!original_column_names.erase(name))
            return false;
        const auto & original_column = columns.get(name);
        if (!expected_type->equals(*original_column.type))
            return false;
        new_columns.add(original_column);
        return true;
    };

    /// We recreate the "id" column if its type doesn't match the settings.
    if (!move_original_column_if_type(TimeSeriesColumnNames::ID, settings[TimeSeriesSetting::id_type]))
        add_column(ColumnDescription{TimeSeriesColumnNames::ID, settings[TimeSeriesSetting::id_type]});

    auto timestamp_type = settings[TimeSeriesSetting::timestamp_type];

    /// We recreate the "timestamp" column if its type doesn't match the settings.
    if (!move_original_column_if_type(TimeSeriesColumnNames::Timestamp, timestamp_type))
        add_column(ColumnDescription{TimeSeriesColumnNames::Timestamp, timestamp_type});

    /// We recreate the "value" column if its type doesn't match the settings.
    if (!move_original_column_if_type(TimeSeriesColumnNames::Value, settings[TimeSeriesSetting::scalar_type]))
        add_column(ColumnDescription{TimeSeriesColumnNames::Value, settings[TimeSeriesSetting::scalar_type]});

    /// We try to keep other columns, and create them only if they're missing.
    if (!move_original_column(TimeSeriesColumnNames::MetricName))
        add_column(ColumnDescription{TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});

    const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (!move_original_column(column_name))
            add_column(ColumnDescription{column_name, std::make_shared<DataTypeString>()});
    }

    /// We use 'Map(LowCardinality(String), String)' as the default type of the `tags` column:
    /// it looks like a correct optimization because there are shouldn't be too many different tag names.
    if (!move_original_column(TimeSeriesColumnNames::Tags))
        add_column(ColumnDescription{TimeSeriesColumnNames::Tags,
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>())});

    /// The `all_tags` column is virtual (it's calculated on the fly and never stored anywhere)
    /// so here we don't need to use the LowCardinality optimization as for the `tags` column.
    if (!move_original_column(TimeSeriesColumnNames::AllTags))
        add_column(ColumnDescription{TimeSeriesColumnNames::AllTags,
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())});

    if (settings[TimeSeriesSetting::store_min_time_and_max_time])
    {
        /// We use Nullable(DateTime64(3)) as the default type of the `min_time` and `max_time` columns.
        /// It's nullable because it allows the aggregation (see aggregate_min_time_and_max_time) work correctly even
        /// for rows in the "tags" table which doesn't have `min_time` and `max_time` (because they have no matching rows in the "samples" table).
        auto nullable_timestamp_type = makeNullable(timestamp_type);
        if (!move_original_column_if_type(TimeSeriesColumnNames::MinTime, nullable_timestamp_type))
            add_column(ColumnDescription{TimeSeriesColumnNames::MinTime, nullable_timestamp_type});
        if (!move_original_column_if_type(TimeSeriesColumnNames::MaxTime, nullable_timestamp_type))
            add_column(ColumnDescription{TimeSeriesColumnNames::MaxTime, nullable_timestamp_type});
    }

    if (!move_original_column(TimeSeriesColumnNames::MetricFamilyName))
        add_column(ColumnDescription{TimeSeriesColumnNames::MetricFamilyName, std::make_shared<DataTypeString>()});
    if (!move_original_column(TimeSeriesColumnNames::Type))
        add_column(ColumnDescription{TimeSeriesColumnNames::Type, std::make_shared<DataTypeString>()});
    if (!move_original_column(TimeSeriesColumnNames::Unit))
        add_column(ColumnDescription{TimeSeriesColumnNames::Unit, std::make_shared<DataTypeString>()});
    if (!move_original_column(TimeSeriesColumnNames::Help))
        add_column(ColumnDescription{TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>()});

    /// Add any remaining original columns that weren't explicitly handled above.
    for (const auto & name : original_column_names)
        new_columns.add(columns.get(name));

    columns = std::move(new_columns);
    return changed;
}


boost::intrusive_ptr<ASTStorage> getTimeSeriesInnerEngine(ViewTarget::Kind target_kind, const ASTCreateQuery & create_query, const TimeSeriesSettings & settings, const ContextPtr & context)
{
    if (create_query.hasTargetTableID(target_kind))
        return nullptr;

    auto * inner_target = create_query.getTargetInnerEngine(target_kind);
    if (inner_target)
        return inner_target;

    if (!create_query.as_table.empty())
    {
        /// The other table specified in AS <other_table> must exist.
        auto other_database = context->resolveDatabase(create_query.as_database);
        auto other_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));

        if (other_create_query->hasTargetTableID(target_kind))
        {
            /// It's unlikely correct to use "CREATE table AS other_table" when "other_table" has external tables like this:
            /// CREATE TABLE other_table ENGINE=TimeSeries data mydata
            /// (because `table` would use the same table "mydata").
            /// Thus we just prohibit that.
            StorageID other_table_id{other_create_query->getDatabase(), other_create_query->getTable()};
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a table AS {} because it has external tables",
                other_table_id.getNameForLogs());
        }

        auto * other_inner_target = other_create_query->getTargetInnerEngine(target_kind);
        if (other_inner_target)
            return other_inner_target;
    }

    return getInnerEngineByDefault(settings, target_kind);
}


void normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context)
{
    if (!create_query.storage)
        create_query.set(create_query.storage, make_intrusive<ASTStorage>());

    TimeSeriesSettings settings;
    settings.loadFromQuery(*create_query.storage);
    if (normalizeTimeSeriesSettings(settings, create_query, context))
        settings.copyToQuery(*create_query.storage);

    /// Use SECONDARY_CREATE mode to skip validation of default expressions during the intermediate
    /// column list parsing step. The default expression of the `id` column may reference TimeSeries
    /// columns like `metric_name` and `all_tags` that don't exist in the raw user-specified column
    /// list yet.
    ColumnsDescription columns = getColumnList(create_query, context, LoadingStrictnessLevel::SECONDARY_CREATE);
    if (normalizeTimeSeriesColumns(columns, settings))
        setColumnList(create_query, columns);

    for (auto kind : {ViewTarget::Samples, ViewTarget::Tags, ViewTarget::Metrics})
    {
        if (!create_query.hasTargetTableID(kind) && !create_query.getTargetInnerEngine(kind))
        {
            create_query.setTargetInnerEngine(kind, getTimeSeriesInnerEngine(kind, create_query, settings, context));
        }
    }
}

}
