#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/StorageID.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/dataTypeToAST.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_TABLE;
}


namespace
{
    constexpr std::array<ViewTarget::Kind, 3> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::Tags, ViewTarget::Metrics};
    }

    /// Cross-source conflict-checking setter for `DataTypePtr`. The first non-null value wins;
    /// any subsequent non-null value must equal it or normalization fails.
    void setOrCheckType(
        DataTypePtr & target, String & target_source,
        const DataTypePtr & value, const String & value_source,
        std::string_view what, const StorageID & table_id)
    {
        if (!value)
            return;
        if (target)
        {
            if (!target->equals(*value))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Conflicting {} type: {} declares {} but {} declares {}",
                    table_id.getNameForLogs(), what,
                    target_source, target->getName(),
                    value_source, value->getName());
            return;
        }
        target = value;
        target_source = value_source;
    }

    /// Cross-source conflict-checking setter for `ASTPtr` (used for the id-generator DEFAULT expression).
    /// Two expressions are considered equal if their formatted form matches.
    void setOrCheckExpression(
        ASTPtr & target, String & target_source,
        const ASTPtr & value, const String & value_source,
        const StorageID & table_id)
    {
        if (!value)
            return;
        if (target)
        {
            if (target->formatWithSecretsOneLine() != value->formatWithSecretsOneLine())
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Conflicting id_generator expression: {} declares `{}` but {} declares `{}`",
                    table_id.getNameForLogs(),
                    target_source, target->formatForLogging(),
                    value_source, value->formatForLogging());
            return;
        }
        target = value;
        target_source = value_source;
    }

    /// Reads the outer `time_series` column declaration of a TimeSeries-engine table.
    /// If the column is declared with type `Array(Tuple(ts, val))`, extracts ts/val into the resolver.
    void readTypesFromOuterColumns(
        const ASTCreateQuery & query, const String & query_descr,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        const StorageID & table_id)
    {
        if (!query.columns_list || !query.columns_list->columns)
            return;

        for (const auto & column : query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (column_declaration->name == TimeSeriesColumnNames::TimeSeries && column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
                const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
                if (tuple_type && tuple_type->getElements().size() >= 2)
                {
                    const auto & elems = tuple_type->getElements();
                    String source = query_descr + " outer `time_series` column";
                    setOrCheckType(timestamp_type, timestamp_src, elems[0], source, "timestamp", table_id);
                    setOrCheckType(scalar_type, scalar_src, elems[1], source, "scalar", table_id);
                }
            }
        }
    }

    /// Reads samples INNER COLUMNS declarations and extracts timestamp / value / id types.
    void readTypesFromInnerSamples(
        const ASTCreateQuery & query, const String & query_descr,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        const auto * inner_columns = query.getTargetInnerColumns(ViewTarget::Samples);
        if (!inner_columns || !inner_columns->columns)
            return;

        for (const auto & column : inner_columns->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (!column_declaration->getType())
                continue;
            auto column_type = DataTypeFactory::instance().get(column_declaration->getType());

            if (column_declaration->name == TimeSeriesColumnNames::Timestamp)
                setOrCheckType(timestamp_type, timestamp_src, column_type, query_descr + " samples INNER COLUMNS `timestamp`", "timestamp", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::Value)
                setOrCheckType(scalar_type, scalar_src, column_type, query_descr + " samples INNER COLUMNS `value`", "scalar", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
                setOrCheckType(id_type, id_src, column_type, query_descr + " samples INNER COLUMNS `id`", "id", table_id);
        }
    }

    /// Reads tags INNER COLUMNS declarations and extracts id type and id-generator default expression.
    void readTypesFromInnerTags(
        const ASTCreateQuery & query, const String & query_descr,
        DataTypePtr & id_type, String & id_src,
        ASTPtr & id_generator, String & id_gen_src,
        const StorageID & table_id)
    {
        const auto * inner_columns = query.getTargetInnerColumns(ViewTarget::Tags);
        if (!inner_columns || !inner_columns->columns)
            return;

        for (const auto & column : inner_columns->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            if (column_declaration->name != TimeSeriesColumnNames::ID)
                continue;

            if (column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                setOrCheckType(id_type, id_src, column_type, query_descr + " tags INNER COLUMNS `id`", "id", table_id);
            }
            if (auto expr = column_declaration->getDefaultExpression())
                setOrCheckExpression(id_generator, id_gen_src, expr->clone(), query_descr + " tags INNER COLUMNS `id` DEFAULT", table_id);
        }
    }

    /// Reads timestamp / value / id types from an external samples target table.
    void readTypesFromExternalSamples(
        const StorageID & external_table_id, const ColumnsDescription & external_columns, const String & query_descr,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name == TimeSeriesColumnNames::Timestamp)
                setOrCheckType(timestamp_type, timestamp_src, column.type,
                    fmt::format("{} external samples table {} `timestamp`", query_descr, external_table_id.getNameForLogs()),
                    "timestamp", table_id);
            else if (column.name == TimeSeriesColumnNames::Value)
                setOrCheckType(scalar_type, scalar_src, column.type,
                    fmt::format("{} external samples table {} `value`", query_descr, external_table_id.getNameForLogs()),
                    "scalar", table_id);
            else if (column.name == TimeSeriesColumnNames::ID)
                setOrCheckType(id_type, id_src, column.type,
                    fmt::format("{} external samples table {} `id`", query_descr, external_table_id.getNameForLogs()),
                    "id", table_id);
        }
    }

    /// Reads the id type from an external tags target table.
    /// Note: the id-generator DEFAULT on the external table is intentionally NOT read here — the
    /// `id_generator` setting is allowed to override it at runtime, and the write path (sink /
    /// remote-write protocol) resolves the effective expression there. Reading the external
    /// table's DEFAULT here and enforcing must-agree would prevent that override.
    void readTypesFromExternalTags(
        const StorageID & external_table_id, const ColumnsDescription & external_columns, const String & query_descr,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name != TimeSeriesColumnNames::ID)
                continue;

            setOrCheckType(id_type, id_src, column.type,
                fmt::format("{} external tags table {} `id`", query_descr, external_table_id.getNameForLogs()),
                "id", table_id);
        }
    }

    /// Walks all user-explicit sources of one `create_query` (outer columns, samples/tags INNER COLUMNS, external samples/tags tables)
    /// and aggregates the resolved types via the cross-source conflict checks.
    void readTypesFromOneQuery(
        const ASTCreateQuery & query, const String & query_descr, const ContextPtr & context,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        ASTPtr & id_generator, String & id_gen_src,
        const StorageID & table_id)
    {
        readTypesFromOuterColumns(query, query_descr,
            timestamp_type, timestamp_src, scalar_type, scalar_src, table_id);

        readTypesFromInnerSamples(query, query_descr,
            timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src, table_id);

        readTypesFromInnerTags(query, query_descr,
            id_type, id_src, id_generator, id_gen_src, table_id);

        for (auto kind : {ViewTarget::Samples, ViewTarget::Tags})
        {
            auto external_table_id = query.getTargetTableID(kind);
            if (!external_table_id)
                continue;
            auto external_table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(external_table_id), context);
            if (!external_table)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", external_table_id.getNameForLogs());
            auto metadata = external_table->getInMemoryMetadataPtr(context, false);
            if (kind == ViewTarget::Samples)
                readTypesFromExternalSamples(external_table_id, metadata->columns, query_descr,
                    timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src, table_id);
            else
                readTypesFromExternalTags(external_table_id, metadata->columns, query_descr,
                    id_type, id_src, table_id);
        }
    }

/// Internal aggregate for the four type-related values needed during normalization:
    /// timestamp, scalar, id type, and the DEFAULT expression that computes the id from tag columns.
    /// At runtime these are read directly from target table metadata; only the normalizer assembles
    /// them in one place to verify cross-source consistency and apply defaults.
    struct ResolvedTimeSeriesTypes
    {
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
        ASTPtr id_generator;
    };

    /// Resolves the four type-related values that used to be `TimeSeriesSettings` fields.
    /// Walks `create_query` and, if present, `as_create_query` and verifies all user-explicit sources agree.
    /// Falls back to hardcoded defaults (`DateTime64(3)`, `Float64`, `UUID`, derived id-generator).
    /// Also performs type-shape validation that used to live in `validateSettings`.
    ResolvedTimeSeriesTypes resolveTimeSeriesTypes(
        const ASTCreateQuery & create_query,
        const ASTCreateQuery * as_create_query,
        const TimeSeriesSettings & settings,
        const ContextPtr & context)
    {
        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
        ASTPtr id_generator;
        String timestamp_src;
        String scalar_src;
        String id_src;
        String id_gen_src;

        readTypesFromOneQuery(create_query, "create_query", context,
            timestamp_type, timestamp_src,
            scalar_type, scalar_src,
            id_type, id_src,
            id_generator, id_gen_src,
            table_id);

        /// The `id_generator` setting participates as a must-agree source. For inner tags it must equal the
        /// `TAGS INNER COLUMNS (id ... DEFAULT ...)` expression (if both are specified). For external tags it's
        /// the only way to customize the id-generator at the TimeSeries-CREATE level — the external table's own
        /// DEFAULT is read at runtime by the write path and is overridden by this setting if it's set.
        if (ASTPtr from_setting = settings[TimeSeriesSetting::id_generator].value)
            setOrCheckExpression(id_generator, id_gen_src, from_setting->clone(),
                "create_query SETTINGS id_generator", table_id);

        /// AS-source provides fallback defaults. Anything explicitly declared in `create_query` overrides
        /// the AS-source declaration silently — we do not run the cross-source agreement check across the
        /// AS-source boundary, only within each query.
        if (as_create_query)
        {
            DataTypePtr as_ts, as_scalar, as_id;
            ASTPtr as_id_gen;
            String as_ts_src, as_scalar_src, as_id_src, as_id_gen_src;
            readTypesFromOneQuery(*as_create_query, "AS-source", context,
                as_ts, as_ts_src,
                as_scalar, as_scalar_src,
                as_id, as_id_src,
                as_id_gen, as_id_gen_src,
                table_id);
            if (!timestamp_type) { timestamp_type = std::move(as_ts); timestamp_src = std::move(as_ts_src); }
            if (!scalar_type) { scalar_type = std::move(as_scalar); scalar_src = std::move(as_scalar_src); }
            if (!id_type) { id_type = std::move(as_id); id_src = std::move(as_id_src); }
            if (!id_generator) { id_generator = std::move(as_id_gen); id_gen_src = std::move(as_id_gen_src); }
        }

        /// Apply defaults for anything still unset.
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!scalar_type)
            scalar_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeUUID>();
        if (!id_generator)
            id_generator = makeASTForTimeSeriesIDGenerator(id_type, settings, table_id);

        /// Validate type shapes (the checks that used to live in `validateSettings`).
        {
            WhichDataType ts_which{*timestamp_type};
            if (!(ts_which.isDateTime64() || ts_which.isDateTime() || ts_which.isUInt32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), timestamp_type->getName(), TimeSeriesColumnNames::Timestamp);
        }
        {
            WhichDataType sc_which{*scalar_type};
            if (!(sc_which.isFloat64() || sc_which.isFloat32()))
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), scalar_type->getName(), TimeSeriesColumnNames::Value);
        }
        {
            WhichDataType id_which{*id_type};
            bool id_ok = id_which.isUInt64()
                || (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
                || id_which.isUUID()
                || id_which.isUInt128();
            if (!id_ok)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
                    table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }

        return ResolvedTimeSeriesTypes{
            .timestamp_type = std::move(timestamp_type),
            .scalar_type = std::move(scalar_type),
            .id_type = std::move(id_type),
            .id_generator = std::move(id_generator),
        };
    }

    /// Adds missing required columns to an inner table's column list, building them in canonical order.
    /// Existing columns are taken from `inner_table_columns`; time_series_columns_map time series columns are copied when available.
    /// Returns true if the column list was modified.
    bool normalizeInnerTableColumns(
        ASTColumns & inner_table_columns,
        ViewTarget::Kind inner_table_kind,
        const ASTColumns * time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved)
    {
        /// Build a map of the existing inner columns by name.
        std::map<String, ASTPtr> original;
        if (inner_table_columns.columns)
        {
            for (auto & child : inner_table_columns.columns->children)
                original[child->as<ASTColumnDeclaration &>().name] = child;
        }

        /// Build a lookup map for the time_series_columns_map time series columns.
        std::map<String, ASTPtr> time_series_columns_map;
        if (time_series_columns && time_series_columns->columns)
        {
            for (const auto & child : time_series_columns->columns->children)
                time_series_columns_map[child->as<ASTColumnDeclaration>()->name] = child;
        }

        auto new_list = make_intrusive<ASTExpressionList>();
        bool changed = false;

        /// If `name` exists in `original`, move it to new_list (erasing from map) and return false.
        /// Otherwise copy from `time_series_columns_map` (if present) or create a new column with `type_ast`,
        /// mark `changed`, and return true.
        auto add_column_if_missing = [&](const String & name, ASTPtr type_ast) -> bool
        {
            if (auto it = original.find(name); it != original.end())
            {
                new_list->children.push_back(it->second);
                original.erase(it);
                return false;
            }
            if (auto it = time_series_columns_map.find(name); it != time_series_columns_map.end())
                new_list->children.push_back(it->second->clone());
            else
            {
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            }
            changed = true;
            return true;
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id" - no default expression in the samples table.
                /// Reset any default expression if the column was copied from the time series columns -
                /// the identifier of the samples table is computed in the "tags" inner table,
                /// because it depends on columns like "metric_name" or "all_tags" which don't exist in the samples table.
                if (add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved.id_type)))
                {
                    auto & column = new_list->children.back();
                    auto & decl = column->as<ASTColumnDeclaration &>();
                    if (decl.getDefaultExpression() || decl.ephemeral_default || decl.default_specifier != ColumnDefaultSpecifier::Empty)
                    {
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                        new_decl.ephemeral_default = false;
                        new_decl.resetDefaultExpression();
                        changed = true;
                    }
                }

                add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved.timestamp_type));
                add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(resolved.scalar_type));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with the id_generator expression that computes the identifier from "metric_name" and tags.
                /// The DEFAULT is auto-added if the user-provided column declaration didn't include one,
                /// so a user can write e.g. `TAGS INNER COLUMNS (id UUID CODEC(ZSTD))` and still get the id_generator.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved.id_type));
                {
                    auto & column = new_list->children.back();
                    if (!column->as<ASTColumnDeclaration &>().getDefaultExpression())
                    {
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                        new_decl.ephemeral_default = false;
                        new_decl.setDefaultExpression(resolved.id_generator->clone());
                        changed = true;
                    }
                }

                add_column_if_missing(TimeSeriesColumnNames::MetricName,
                    makeASTDataType("LowCardinality", makeASTDataType("String")));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    add_column_if_missing(column_name, makeASTDataType("String"));
                }

                add_column_if_missing(TimeSeriesColumnNames::Tags,
                    makeASTDataType("Map", makeASTDataType("LowCardinality", makeASTDataType("String")), makeASTDataType("String")));

                /// Column "all_tags" is ephemeral - only used to calculate the "id" column.
                if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                {
                    if (add_column_if_missing(TimeSeriesColumnNames::AllTags,
                        makeASTDataType("Map", makeASTDataType("String"), makeASTDataType("String"))))
                    {
                        auto & column = new_list->children.back();
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Ephemeral;
                        new_decl.ephemeral_default = true;
                        changed = true;
                    }
                }

                /// Columns "min_time" and "max_time".
                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                    {
                        /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                        auto make_agg_type = [&](const String & func_name) -> ASTPtr
                        {
                            DataTypePtr ts_type = makeNullable(resolved.timestamp_type);
                            AggregateFunctionProperties properties;
                            auto func = AggregateFunctionFactory::instance().get(func_name, NullsAction::EMPTY, {ts_type}, {}, properties);
                            auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(func, DataTypes{ts_type}, Array{});
                            auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                            return dataTypeToAST(type);
                        };

                        add_column_if_missing(TimeSeriesColumnNames::MinTime, make_agg_type("min"));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime, make_agg_type("max"));
                    }
                    else
                    {
                        add_column_if_missing(TimeSeriesColumnNames::MinTime,
                            dataTypeToAST(makeNullable(resolved.timestamp_type)));
                        add_column_if_missing(TimeSeriesColumnNames::MaxTime,
                            dataTypeToAST(makeNullable(resolved.timestamp_type)));
                    }
                }

                break;
            }

            case ViewTarget::Metrics:
            {
                add_column_if_missing(TimeSeriesColumnNames::MetricFamilyName, makeASTDataType("String"));
                add_column_if_missing(TimeSeriesColumnNames::Type, makeASTDataType("LowCardinality", makeASTDataType("String")));
                add_column_if_missing(TimeSeriesColumnNames::Unit, makeASTDataType("LowCardinality", makeASTDataType("String")));
                add_column_if_missing(TimeSeriesColumnNames::Help, makeASTDataType("String"));
                break;
            }

            default:
                UNREACHABLE();
        }

        /// Copy all remaining original columns at the end (user-defined extra columns).
        for (auto & [name, col] : original)
            new_list->children.push_back(col);

        if (!changed)
            return false;

        inner_table_columns.setOrReplace(inner_table_columns.columns, new_list);
        return true;
    }

    /// Generates the column list for an inner table from scratch, used for upgrading from old format
    /// where inner columns weren't stored in the query. Time series columns are copied when available.
    boost::intrusive_ptr<ASTColumns> generateInnerColumnsForOldVersion(
        ViewTarget::Kind inner_table_kind,
        const ASTColumns * time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved)
    {
        /// Build a lookup map for the time series columns.
        std::map<String, ASTPtr> time_series_columns_map;
        if (time_series_columns && time_series_columns->columns)
            for (const auto & child : time_series_columns->columns->children)
                time_series_columns_map[child->as<ASTColumnDeclaration>()->name] = child;

        auto new_list = make_intrusive<ASTExpressionList>();

        /// Copy from `time_series_columns_map` (if present) or create a new column with `type_ast`.
        auto add_column = [&](const String & name, ASTPtr type_ast)
        {
            if (auto it = time_series_columns_map.find(name); it != time_series_columns_map.end())
                new_list->children.push_back(it->second->clone());
            else
            {
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            }
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id" - no default expression in the samples table.
                /// Reset any default expression if the column was copied from the time series columns -
                /// the identifier of the samples table is computed in the "tags" inner table,
                /// because it depends on columns like "metric_name" or "all_tags" which don't exist in the samples table.
                add_column(TimeSeriesColumnNames::ID, dataTypeToAST(resolved.id_type));

                {
                    auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                    new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                    new_decl.ephemeral_default = false;
                    new_decl.resetDefaultExpression();
                }

                add_column(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved.timestamp_type));
                add_column(TimeSeriesColumnNames::Value, dataTypeToAST(resolved.scalar_type));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with the id_generator expression that computes the identifier from "metric_name" and tags.
                add_column(TimeSeriesColumnNames::ID, dataTypeToAST(resolved.id_type));

                {
                    auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                    new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                    new_decl.ephemeral_default = false;
                    new_decl.setDefaultExpression(resolved.id_generator->clone());
                }

                add_column(TimeSeriesColumnNames::MetricName,
                    makeASTDataType("LowCardinality", makeASTDataType("String")));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    add_column(column_name, makeASTDataType("String"));
                }

                add_column(TimeSeriesColumnNames::Tags,
                    makeASTDataType("Map", makeASTDataType("LowCardinality", makeASTDataType("String")), makeASTDataType("String")));

                /// Column "all_tags" is ephemeral - only used to calculate the "id" column.
                if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                {
                    add_column(TimeSeriesColumnNames::AllTags,
                        makeASTDataType("Map", makeASTDataType("String"), makeASTDataType("String")));

                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Ephemeral;
                        new_decl.ephemeral_default = true;
                    }
                }

                /// Columns "min_time" and "max_time".
                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                    {
                        /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                        auto make_agg_type = [&](const String & func_name) -> ASTPtr
                        {
                            DataTypePtr ts_type = makeNullable(resolved.timestamp_type);
                            AggregateFunctionProperties properties;
                            auto func = AggregateFunctionFactory::instance().get(func_name, NullsAction::EMPTY, {ts_type}, {}, properties);
                            auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(func, DataTypes{ts_type}, Array{});
                            auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                            return dataTypeToAST(type);
                        };

                        add_column(TimeSeriesColumnNames::MinTime, make_agg_type("min"));
                        add_column(TimeSeriesColumnNames::MaxTime, make_agg_type("max"));
                    }
                    else
                    {
                        add_column(TimeSeriesColumnNames::MinTime,
                            dataTypeToAST(makeNullable(resolved.timestamp_type)));
                        add_column(TimeSeriesColumnNames::MaxTime,
                            dataTypeToAST(makeNullable(resolved.timestamp_type)));
                    }
                }

                break;
            }

            case ViewTarget::Metrics:
            {
                add_column(TimeSeriesColumnNames::MetricFamilyName, makeASTDataType("String"));
                add_column(TimeSeriesColumnNames::Type, makeASTDataType("String"));
                add_column(TimeSeriesColumnNames::Unit, makeASTDataType("String"));
                add_column(TimeSeriesColumnNames::Help, makeASTDataType("String"));
                break;
            }

            default:
                UNREACHABLE();
        }

        auto result = make_intrusive<ASTColumns>();
        result->columns = new_list.get();
        result->children.push_back(std::move(new_list));
        return result;
    }


    /// Generates the engine definition for an inner table.
    /// Inherits it from `as_create_query` (AS <other_table>) if provided, or falls back to a default engine based on the target kind and settings.
    /// Returns `nullptr` if the target has an explicit external table ID (no inner table to generate for).
    boost::intrusive_ptr<ASTStorage> generateInnerEngine(ViewTarget::Kind target_kind, const ASTCreateQuery & create_query, const ASTCreateQuery * as_create_query, const TimeSeriesSettings & settings)
    {
        /// This function is only for inner tables (those without an explicit target table ID).
        if (create_query.hasTargetTableID(target_kind))
            return nullptr;

        /// If the engine is already specified in the query, use it as-is.
        auto * inner_target = create_query.getTargetInnerEngine(target_kind);
        if (inner_target)
            return inner_target;

        /// If the table is created AS <other_table>, try to inherit the engine from there.
        if (as_create_query)
        {
            if (as_create_query->hasTargetTableID(target_kind))
            {
                /// It's unlikely correct to use "CREATE table AS other_table" when "other_table" has external tables like this:
                /// CREATE TABLE other_table ENGINE=TimeSeries data mydata
                /// (because `table` would use the same table "mydata").
                /// Thus we just prohibit that.
                StorageID other_table_id{as_create_query->getDatabase(), as_create_query->getTable()};
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "Cannot CREATE a table AS {} because it has external tables",
                    other_table_id.getNameForLogs());
            }

            auto * other_inner_target = as_create_query->getTargetInnerEngine(target_kind);
            if (other_inner_target)
                return other_inner_target;
        }

        /// Neither the query nor the AS-source specified an engine — build a sensible default.
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

    /// Checks that an external target table has all the columns required by the TimeSeries table engine,
    /// and that those columns match the resolved types.
    void checkTargetTable(
        const StorageID & target_table_id,
        const ColumnsDescription & target_table_columns,
        ViewTarget::Kind target_kind,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved)
    {
        auto check_column = [&](std::string_view column_name)
        {
            if (!target_table_columns.tryGet(String(column_name)))
                throw Exception(
                    ErrorCodes::THERE_IS_NO_COLUMN,
                    "{}: Column {} is required for the {} table used by TimeSeries table engine",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind);
        };

        auto check_column_type = [&](std::string_view column_name, const DataTypePtr & expected_type)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!col->type->equals(*expected_type))
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected {}",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName(),
                    expected_type->getName());
        };

        auto check_column_is_string = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!isString(removeLowCardinalityAndNullable(col->type)))
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected String or LowCardinality(String)",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        auto check_column_is_string_map = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            WhichDataType which{*col->type};
            bool ok = false;
            if (which.isMap())
            {
                const auto & map_type = typeid_cast<const DataTypeMap &>(*col->type);
                ok = isString(removeLowCardinality(map_type.getKeyType()))
                    && isString(removeLowCardinality(map_type.getValueType()));
            }
            if (!ok)
                throw Exception(
                    ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Column {} in the {} table has type {}, but expected Map with String or LowCardinality(String) keys and values",
                    target_table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        switch (target_kind)
        {
            case ViewTarget::Samples:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved.id_type);
                check_column_type(TimeSeriesColumnNames::Timestamp, resolved.timestamp_type);
                check_column_type(TimeSeriesColumnNames::Value, resolved.scalar_type);
                break;
            }

            case ViewTarget::Tags:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved.id_type);
                check_column_is_string(TimeSeriesColumnNames::MetricName);

                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    check_column_is_string(column_name);
                }

                check_column_is_string_map(TimeSeriesColumnNames::Tags);
                break;
            }

            case ViewTarget::Metrics:
            {
                check_column_is_string(TimeSeriesColumnNames::MetricFamilyName);
                check_column_is_string(TimeSeriesColumnNames::Type);
                check_column_is_string(TimeSeriesColumnNames::Unit);
                check_column_is_string(TimeSeriesColumnNames::Help);
                break;
            }

            default:
                UNREACHABLE();
        }
    }

}


ASTPtr makeASTForTimeSeriesIDGenerator(
    const DataTypePtr & id_type, const TimeSeriesSettings & settings, const StorageID & for_error)
{
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

    WhichDataType id_which(*id_type);

    if (id_which.isUInt64())
        return make_hash_function("sipHash64");

    if (id_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
        return make_hash_function("sipHash128");

    if (id_which.isUUID())
        return makeASTFunction("reinterpretAsUUID", make_hash_function("sipHash128"));

    if (id_which.isUInt128())
        return makeASTFunction("reinterpretAsUInt128", make_hash_function("sipHash128"));

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "{}: Unexpected type {} of the {} column",
        for_error.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
}


/// Generates the canonical column list for the TimeSeries table from the given resolved types.
static ColumnsDescription generateTimeSeriesColumns(const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type)
{
    ColumnsDescription result;

    auto add_column = [&](const String & name, DataTypePtr type)
    {
        result.add(ColumnDescription{name, std::move(type)});
    };

    add_column(TimeSeriesColumnNames::MetricName, std::make_shared<DataTypeString>());

    add_column(TimeSeriesColumnNames::Tags,
               std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()));

    add_column(TimeSeriesColumnNames::TimeSeries,
        std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type})));

    add_column(TimeSeriesColumnNames::MetricFamily, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Type, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Unit, std::make_shared<DataTypeString>());
    add_column(TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>());

    return result;
}


bool normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    /// Whether we're creating a new table.
    /// `is_new_table` is false if we're restoring from a backup.
    bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;

    /// Resolve AS <other_table> once, so we don't repeat the lookup in multiple places.
    boost::intrusive_ptr<const ASTCreateQuery> as_create_query;
    if (!create_query.as_table.empty())
    {
        auto other_database = context->resolveDatabase(create_query.as_database);
        as_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));
    }

    bool changed = false;

    /// Load settings: inherit from AS-source first (if any), then overlay create_query's own SETTINGS clause.
    TimeSeriesSettings settings;
    if (as_create_query && as_create_query->storage)
    {
        settings.loadFromQuery(*as_create_query->storage);
        changed = true;
    }
    if (create_query.storage)
        settings.loadFromQuery(*create_query.storage);

    if (changed)
    {
        if (!create_query.storage)
            create_query.set(create_query.storage, make_intrusive<ASTStorage>());
        settings.copyToQuery(*create_query.storage);
    }

    /// Resolve the four type-related values (timestamp, scalar, id, id_generator) from the various sources,
    /// verifying that all explicit declarations agree, and applying hardcoded defaults for anything not declared.
    ResolvedTimeSeriesTypes resolved = resolveTimeSeriesTypes(create_query, as_create_query.get(), settings, context);

    /// For each target kind: check external tables or normalize inner table definitions.
    for (auto kind : getTargetKinds())
    {
        if (create_query.hasTargetTableID(kind))
        {
            /// An external target table is specified.
            /// If it's a new table, check that the specified target table has all the required columns.
            if (is_new_table)
            {
                auto target_table_id = create_query.getTargetTableID(kind);
                auto target_table = DatabaseCatalog::instance().getTable(target_table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr(context, false);
                checkTargetTable(target_table_id, target_metadata->columns, kind, settings, resolved);
            }
        }
        else
        {
            /// An inner target table should be used.
            /// Normalize its column definitions and assign a table engine if not specified.
            boost::intrusive_ptr<ASTColumns> inner_columns;
            bool inner_columns_changed = false;
            if (create_query.getTargetInnerColumns(kind))
                inner_columns = boost::static_pointer_cast<ASTColumns>(create_query.getTargetInnerColumns(kind)->clone());

            if (is_new_table)
            {
                if (!inner_columns)
                    inner_columns = make_intrusive<ASTColumns>();
                inner_columns_changed |= normalizeInnerTableColumns(*inner_columns, kind, create_query.columns_list, settings, resolved);
            }
            else if (!inner_columns)
            {
                /// Older versions didn't store inner table column definitions in the `CREATE` query, so reconstruct them now.
                inner_columns = generateInnerColumnsForOldVersion(kind, create_query.columns_list, settings, resolved);
                inner_columns_changed = true;
            }

            if (inner_columns_changed)
            {
                create_query.setTargetInnerColumns(kind, inner_columns);
                changed = true;
            }

            if (!create_query.getTargetInnerEngine(kind))
            {
                create_query.setTargetInnerEngine(kind, generateInnerEngine(kind, create_query, as_create_query.get(), settings));
                changed = true;
            }
        }
    }

    /// Regenerate the columns of TimeSeries table from the resolved types.
    /// We can change the columns of TimeSeries table because these columns are designed to work
    /// as IO interface. They store no data, in fact the data is stored in target or inner columns.
    {
        auto new_columns_ast = make_intrusive<ASTColumns>();
        new_columns_ast->set(new_columns_ast->columns,
            InterpreterCreateQuery::formatColumns(generateTimeSeriesColumns(resolved.timestamp_type, resolved.scalar_type)));
        const auto * old_columns = create_query.columns_list;
        if (!old_columns
            || !old_columns->columns
            || old_columns->formatWithSecretsOneLine() != new_columns_ast->formatWithSecretsOneLine())
        {
            create_query.set(create_query.columns_list, new_columns_ast);
            changed = true;
        }
    }

    return changed;
}

}
