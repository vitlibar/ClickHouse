#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeAggregateFunction.h>
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
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesIDGenerator.h>
#include <base/EnumReflection.h>
#include <unordered_set>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsUInt64 recent_samples_index_granularity;
    extern const TimeSeriesSettingsASTFunction recent_samples_partition_by;
    extern const TimeSeriesSettingsUInt64 recent_samples_ttl_seconds;
    extern const TimeSeriesSettingsUInt64 samples_index_granularity;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsUInt64 tags_index_granularity;
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace Setting
{
    extern const SettingsDefaultTableEngine default_table_engine;
}

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int INCORRECT_QUERY;
    extern const int INVALID_SETTING_VALUE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_TABLE;
}


namespace
{
    /// All target kinds of a TimeSeries table.
    /// The RecentSamples target is optional: it's enabled by the `recent_samples_ttl_seconds` setting.
    constexpr std::array<ViewTarget::Kind, 4> getTargetKinds()
    {
        return {ViewTarget::Samples, ViewTarget::RecentSamples, ViewTarget::Tags, ViewTarget::Metrics};
    }

    /// Whether the create query defines inner columns for the specified target.
    bool hasInnerColumns(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerColumns(kind) != nullptr;
    }

    /// Whether the create query defines an inner engine for the specified target.
    bool hasInnerEngine(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerEngine(kind) != nullptr;
    }

    /// Whether the create query specifies an external table for the specified target.
    bool hasTargetTableID(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.hasTargetTableID(kind);
    }

    /// Whether the create query has an inner UUID for the specified target.
    bool hasInnerUUID(const ASTCreateQuery & create_query, ViewTarget::Kind kind)
    {
        return create_query.getTargetInnerUUID(kind) != UUIDHelpers::Nil;
    }

    /// Conflict-checking setter for `DataTypePtr`.
    /// Keeps the first non-null value, any subsequent non-null values must equal it.
    void setOrCheckDataType(
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

    /// Reads the declaration of the outer columns.
    /// If the `time_series` column is found and it is declared with type `Array(Tuple(timestamp_type, scalar_type))`,
    /// the function extracts `timestamp_type` and `scalar_type`.
    void readTypesFromOuterColumns(
        const ASTCreateQuery & query,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        const StorageID & table_id)
    {
        if (!query.columns_list || !query.columns_list->columns)
            return;

        for (const auto & column : query.columns_list->columns->children)
        {
            auto column_declaration = boost::static_pointer_cast<ASTColumnDeclaration>(column);
            const auto & name = column_declaration->name;

            if (name == TimeSeriesColumnNames::TimeSeries && column_declaration->getType())
            {
                auto column_type = DataTypeFactory::instance().get(column_declaration->getType());
                const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
                const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
                if (!tuple_type || (tuple_type->getElements().size() != 2))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                        "{}: Column `{}` must have type Array(Tuple(timestamp, value)), got {}",
                        table_id.getNameForLogs(), TimeSeriesColumnNames::TimeSeries, column_type->getName());

                const auto & elems = tuple_type->getElements();
                String source = "outer column `time_series`";
                setOrCheckDataType(timestamp_type, timestamp_src, elems[0], source, "timestamp", table_id);
                setOrCheckDataType(scalar_type, scalar_src, elems[1], source, "scalar", table_id);
            }

            /// Columns `id`, `timestamp`, `value` belong to the prealpha version and must not be here.
            if (name == TimeSeriesColumnNames::Timestamp
                || name == TimeSeriesColumnNames::Value
                || name == TimeSeriesColumnNames::ID)
            {
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "{}: Column `{}` is not allowed in the column list of a TimeSeries table; "
                    "use INNER COLUMNS to specify inner table column types",
                    table_id.getNameForLogs(), name);
            }
        }
    }

    /// Reads SAMPLES INNER COLUMNS declarations and extracts types
    /// `timestamp_type`, `scalar_type`, `id_type`.
    void readTypesFromInnerSamples(
        const ASTCreateQuery & query,
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
                setOrCheckDataType(timestamp_type, timestamp_src, column_type, "samples inner column `timestamp`", "timestamp", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::Value)
                setOrCheckDataType(scalar_type, scalar_src, column_type, "samples inner column `value`", "scalar", table_id);
            else if (column_declaration->name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column_type, "samples inner column `id`", "id", table_id);
        }
    }

    /// Reads TAGS INNER COLUMNS declarations and extracts type `id_type`.
    void readTypesFromInnerTags(
        const ASTCreateQuery & query,
        DataTypePtr & id_type, String & id_src,
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
                setOrCheckDataType(id_type, id_src, column_type, "tags inner column `id`", "id", table_id);
            }
        }
    }

    /// Reads the declaration of the external samples target table and
    /// extract types `timestamp_type`, `scalar_type`, id_type`.
    void readTypesFromExternalSamples(
        std::string_view table_kind_name,
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name == TimeSeriesColumnNames::Timestamp)
                setOrCheckDataType(timestamp_type, timestamp_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "timestamp", table_id);
            else if (column.name == TimeSeriesColumnNames::Value)
                setOrCheckDataType(scalar_type, scalar_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "scalar", table_id);
            else if (column.name == TimeSeriesColumnNames::ID)
                setOrCheckDataType(id_type, id_src, column.type,
                    fmt::format("column `{}` of the external `{}` table {}", column.name, table_kind_name, external_table_id.getNameForLogs()),
                    "id", table_id);
        }
    }

    /// Reads the declaration of the external tags target table and
    /// extract type `id_type`.
    void readTypesFromExternalTags(
        const StorageID & external_table_id, const ColumnsDescription & external_columns,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        for (const auto & column : external_columns)
        {
            if (column.name != TimeSeriesColumnNames::ID)
                continue;

            setOrCheckDataType(id_type, id_src, column.type,
                fmt::format("column `{}` of the external `tags` table {}", column.name, external_table_id.getNameForLogs()),
                "id", table_id);
        }
    }

    /// Reads types from columns of the external target tables referenced in a CREATE query.
    void readTypesFromExternalTargets(
        const ASTCreateQuery & query, const ContextPtr & context,
        DataTypePtr & timestamp_type, String & timestamp_src,
        DataTypePtr & scalar_type, String & scalar_src,
        DataTypePtr & id_type, String & id_src,
        const StorageID & table_id)
    {
        auto resolve_external = [&](ViewTarget::Kind kind) -> std::pair<StorageID, ColumnsDescription>
        {
            auto external_table_id = query.getTargetTableID(kind);
            if (!external_table_id)
                return {StorageID::createEmpty(), {}};
            auto external_table = DatabaseCatalog::instance().tryGetTable(context->tryResolveStorageID(external_table_id), context);
            if (!external_table)
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "TimeSeries: Target table {} doesn't exist", external_table_id.getNameForLogs());
            auto external_metadata = external_table->getInMemoryMetadataPtr(context, false);
            return {external_table_id, external_metadata->columns};
        };

        auto [samples_id, samples_columns] = resolve_external(ViewTarget::Samples);
        if (!samples_id.empty())
            readTypesFromExternalSamples("samples", samples_id, samples_columns,
                                         timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src,
                                         table_id);

        /// An external recent-samples table has the same layout as an external samples table,
        /// and it can be the only declared source of the column types.
        auto [recent_samples_id, recent_samples_columns] = resolve_external(ViewTarget::RecentSamples);
        if (!recent_samples_id.empty())
            readTypesFromExternalSamples("recent samples", recent_samples_id, recent_samples_columns,
                                         timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src,
                                         table_id);

        auto [tags_id, tags_columns] = resolve_external(ViewTarget::Tags);
        if (!tags_id.empty())
            readTypesFromExternalTags(tags_id, tags_columns, id_type, id_src, table_id);
    }

    /// Resolved column types needed during normalization.
    struct ResolvedTimeSeriesTypes
    {
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
    };

    /// Resolves types `timestamp_type`, `scalar_type`, `id_type`; sets by defaults the types
    /// which are not set explicitly.
    /// `check_external_targets` is set when external target tables are expected to exist (CREATE time);
    /// on ATTACH they are allowed not to be loaded yet.
    ResolvedTimeSeriesTypes resolveTimeSeriesTypes(
        const ASTCreateQuery & create_query,
        const ContextPtr & context,
        bool check_external_targets)
    {
        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;
        DataTypePtr id_type;
        String timestamp_src;
        String scalar_src;
        String id_src;

        readTypesFromOuterColumns(create_query,
            timestamp_type, timestamp_src, scalar_type, scalar_src, table_id);

        readTypesFromInnerSamples(create_query,
            timestamp_type, timestamp_src, scalar_type, scalar_src, id_type, id_src, table_id);

        readTypesFromInnerTags(create_query,
            id_type, id_src, table_id);

        if (check_external_targets)
        {
            readTypesFromExternalTargets(create_query, context,
                timestamp_type, timestamp_src,
                scalar_type, scalar_src,
                id_type, id_src,
                table_id);
        }

        /// Apply defaults for unset types.
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!scalar_type)
            scalar_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUUID>())});

        /// Validate types.
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
            /// Identifiers can be of any comparable type: the id column is used in the sorting keys of the inner tables
            /// and in JOINs between them.
            bool id_ok = id_type->isComparable() && !id_type->isNullable() && !id_type->isLowCardinalityNullable()
                && !isNothing(*id_type) && !isVariant(*id_type) && !id_type->hasDynamicSubcolumns();
            if (!id_ok)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                    "{}: Unexpected type {} of the {} column, it must be a comparable non-Nullable type",
                    table_id.getNameForLogs(), id_type->getName(), TimeSeriesColumnNames::ID);
        }

        return ResolvedTimeSeriesTypes{
            .timestamp_type = std::move(timestamp_type),
            .scalar_type = std::move(scalar_type),
            .id_type = std::move(id_type),
        };
    }

    /// Adds missing required columns to an inner table's column list, building them in canonical order.
    /// Existing columns are taken from `inner_table_columns`; missing columns are created with the given type.
    /// Returns true if the column list was modified.
    bool normalizeInnerColumns(
        ASTColumns & inner_table_columns,
        ViewTarget::Kind inner_table_kind,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id)
    {
        /// Build a map of the existing inner columns by name.
        std::map<String, ASTPtr> original;
        if (inner_table_columns.columns)
        {
            for (auto & child : inner_table_columns.columns->children)
                original[child->as<ASTColumnDeclaration &>().name] = child;
        }

        auto new_list = make_intrusive<ASTExpressionList>();
        bool changed = false;

        /// If `name` exists in `original`, move it to new_list (erasing from map) and return nullptr.
        /// Otherwise create a new column with `type_ast`, mark `changed`, and return the new declaration.
        auto add_column_if_missing = [&](const String & name, ASTPtr type_ast) -> ASTColumnDeclaration *
        {
            if (auto it = original.find(name); it != original.end())
            {
                new_list->children.push_back(it->second);
                original.erase(it);
                return nullptr;
            }
            auto decl = make_intrusive<ASTColumnDeclaration>();
            decl->name = name;
            decl->setType(std::move(type_ast));
            new_list->children.push_back(decl);
            changed = true;
            return decl.get();
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// Column "id" - no DEFAULT in the samples table: the identifier is computed in the "tags"
                /// inner table because it depends on columns like "metric_name" or "tags" which don't
                /// exist in samples.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));

                /// Auto-created "timestamp" and "value" columns get compression codecs: under generic LZ4
                /// near-monotonic millisecond timestamps barely compress and dominate the table size
                /// (>90% of on-disk bytes on a scrape-like corpus). All types accepted by the validation
                /// above are compatible with DoubleDelta (DateTime64/DateTime/UInt32). The "value" column
                /// gets plain ZSTD(3): specialized floating-point codecs such as Gorilla proved unreliable
                /// in practice. Explicitly declared columns keep whatever the user wrote.
                if (auto * timestamp_decl = add_column_if_missing(TimeSeriesColumnNames::Timestamp, dataTypeToAST(resolved_types.timestamp_type)))
                    timestamp_decl->setCodec(makeASTFunction(
                        "CODEC", make_intrusive<ASTIdentifier>("DoubleDelta"), makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{1}))));
                if (auto * value_decl = add_column_if_missing(TimeSeriesColumnNames::Value, dataTypeToAST(resolved_types.scalar_type)))
                    value_decl->setCodec(makeASTFunction("CODEC", makeASTFunction("ZSTD", make_intrusive<ASTLiteral>(UInt64{3}))));

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id" - with a DEFAULT expression that computes the identifier from "metric_name" and tags.
                /// The `id_generator` setting says how the identifier is computed, so it replaces the DEFAULT
                /// expression, which can also come from another table through `CREATE ... AS`. Without that
                /// setting the DEFAULT is kept, and derived from the id type when there is none.
                add_column_if_missing(TimeSeriesColumnNames::ID, dataTypeToAST(resolved_types.id_type));
                {
                    auto & column = new_list->children.back();
                    auto default_expression = column->as<ASTColumnDeclaration &>().getDefaultExpression();

                    ASTPtr new_default_expression;
                    if (const auto & id_generator = time_series_settings[TimeSeriesSetting::id_generator].value)
                        new_default_expression = id_generator->clone();
                    else if (!default_expression)
                        new_default_expression = TimeSeriesIDGenerator::getDefault(resolved_types.id_type, table_id);

                    if (new_default_expression
                        && (!default_expression
                            || (default_expression->formatWithSecretsOneLine()
                                != new_default_expression->formatWithSecretsOneLine())))
                    {
                        column = column->clone();
                        auto & new_decl = column->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                        new_decl.ephemeral_default = false;
                        new_decl.setDefaultExpression(std::move(new_default_expression));
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

                /// Columns "min_time" and "max_time". Their type is determined by the settings, so a column
                /// whose type doesn't match is replaced - it can also come from another table through
                /// `CREATE ... AS`. Both columns are dropped when they are not stored at all.
                const bool aggregate_min_time_and_max_time
                    = time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time];

                /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                auto make_min_max_time_type = [&](const String & func_name) -> ASTPtr
                {
                    DataTypePtr ts_type = makeNullable(resolved_types.timestamp_type);
                    if (!aggregate_min_time_and_max_time)
                        return dataTypeToAST(ts_type);
                    AggregateFunctionProperties properties;
                    auto func = AggregateFunctionFactory::instance().get(func_name, NullsAction::EMPTY, {ts_type}, {}, properties);
                    auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(func, DataTypes{ts_type}, Array{});
                    auto type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                    return dataTypeToAST(type);
                };

                /// Adds the column, or replaces the type of an existing one when it doesn't match the settings.
                auto add_or_retype_min_max_time_column = [&](const String & name, const String & func_name)
                {
                    auto type_ast = make_min_max_time_type(func_name);
                    if (add_column_if_missing(name, type_ast->clone()))
                        return;

                    auto & column = new_list->children.back();
                    auto declared_type = column->as<ASTColumnDeclaration &>().getType();
                    if (declared_type && (declared_type->formatWithSecretsOneLine() == type_ast->formatWithSecretsOneLine()))
                        return;

                    column = column->clone();
                    column->as<ASTColumnDeclaration &>().setType(std::move(type_ast));
                    changed = true;
                };

                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    add_or_retype_min_max_time_column(TimeSeriesColumnNames::MinTime, "min");
                    add_or_retype_min_max_time_column(TimeSeriesColumnNames::MaxTime, "max");
                }
                else
                {
                    /// Nothing reads these columns, so they are dropped even if the other table had them.
                    if (original.erase(TimeSeriesColumnNames::MinTime))
                        changed = true;
                    if (original.erase(TimeSeriesColumnNames::MaxTime))
                        changed = true;
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

    /// Whether the SETTINGS clause of an inner table's engine declaration contains the specified setting.
    bool hasEngineSetting(const ASTStorage & storage, std::string_view name)
    {
        return storage.settings && storage.settings->changes.tryGet(name);
    }

    /// Sets a setting in the SETTINGS clause of an inner table's engine declaration,
    /// overwriting the existing value if present.
    /// Returns false if the setting already had this value.
    bool setEngineSettings(ASTStorage & storage, std::string_view name, const Field & value)
    {
        if (storage.settings)
        {
            if (const auto * current = storage.settings->changes.tryGet(name); current && (*current == value))
                return false;
        }
        else
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage.set(storage.settings, settings_ast);
        }
        storage.settings->changes.setSetting(name, value);
        return true;
    }

    /// Replaces a clause of an inner table's engine declaration. Returns false if it was already the same.
    template <typename T>
    bool setEngineClause(ASTStorage & storage, T *& clause, ASTPtr new_clause)
    {
        if (clause && (clause->formatWithSecretsOneLine() == new_clause->formatWithSecretsOneLine()))
            return false;
        storage.setOrReplace(clause, new_clause);
        return true;
    }

    /// Detects prealpha version by outer columns: prealpha had outer columns `id`, `timestamp`, `value`,
    /// and now we don't have them.
    bool isPrealpha(const ASTCreateQuery & create_query)
    {
        if (!create_query.columns_list || !create_query.columns_list->columns)
            return false;
        for (const auto & column : create_query.columns_list->columns->children)
        {
            const auto & decl = column->as<ASTColumnDeclaration &>();
            if (decl.name == TimeSeriesColumnNames::Timestamp
                || decl.name == TimeSeriesColumnNames::Value
                || decl.name == TimeSeriesColumnNames::ID)
                return true;
        }
        return false;
    }

    /// Migrates a prealpha CREATE query: generates `INNER COLUMNS` for inner targets and
    /// replaces outer columns with a single `time_series` column carrying the resolved types.
    /// Function normalizeTimeSeriesDefinition() will rebuild the full list of the outer columns afterwards.
    void upgradeFromPrealpha(ASTCreateQuery & create_query)
    {
        TimeSeriesSettings time_series_settings;
        if (create_query.storage)
            time_series_settings.loadFromQuery(*create_query.storage);

        StorageID table_id{create_query.getDatabase(), create_query.getTable()};

        /// Map of the original outer columns.
        std::map<String, ASTPtr> outer_columns_by_name;
        if (create_query.columns_list && create_query.columns_list->columns)
        {
            for (const auto & child : create_query.columns_list->columns->children)
                outer_columns_by_name[child->as<ASTColumnDeclaration &>().name] = child;
        }

        auto type_from_outer = [&](const String & name) -> DataTypePtr
        {
            if (auto it = outer_columns_by_name.find(name); it != outer_columns_by_name.end())
            {
                const auto & decl = it->second->as<ASTColumnDeclaration &>();
                if (decl.getType())
                    return DataTypeFactory::instance().get(decl.getType());
            }
            return nullptr;
        };

        /// Columns `id`, `timestamp`, `value` were outer columns in the prealpha version.
        DataTypePtr timestamp_type = type_from_outer(TimeSeriesColumnNames::Timestamp);
        DataTypePtr scalar_type = type_from_outer(TimeSeriesColumnNames::Value);
        DataTypePtr id_type = type_from_outer(TimeSeriesColumnNames::ID);
        chassert(timestamp_type || scalar_type || id_type);
        if (!timestamp_type)
            timestamp_type = std::make_shared<DataTypeDateTime64>(3);
        if (!scalar_type)
            scalar_type = std::make_shared<DataTypeFloat64>();
        if (!id_type)
            id_type = std::make_shared<DataTypeUUID>();

        for (auto inner_table_kind : getTargetKinds())
        {
            /// Prealpha tables predate the recent samples table, so there is nothing to upgrade for it,
            /// and no RECENT SAMPLES target should be added to an old table's definition.
            if (inner_table_kind == ViewTarget::RecentSamples)
                continue;
            if (hasTargetTableID(create_query, inner_table_kind))
                continue;
            if (hasInnerColumns(create_query, inner_table_kind))
                continue;

            auto new_list = make_intrusive<ASTExpressionList>();

            auto add_column = [&](const String & name, ASTPtr type_ast)
            {
                if (auto it = outer_columns_by_name.find(name); it != outer_columns_by_name.end())
                {
                    new_list->children.push_back(it->second->clone());
                    return;
                }
                auto decl = make_intrusive<ASTColumnDeclaration>();
                decl->name = name;
                decl->setType(std::move(type_ast));
                new_list->children.push_back(decl);
            };

            switch (inner_table_kind)
            {
                case ViewTarget::Samples:
                {
                    add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));
                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                        new_decl.ephemeral_default = false;
                        new_decl.resetDefaultExpression();
                    }
                    add_column(TimeSeriesColumnNames::Timestamp, dataTypeToAST(timestamp_type));
                    add_column(TimeSeriesColumnNames::Value, dataTypeToAST(scalar_type));
                    break;
                }

                case ViewTarget::Tags:
                {
                    /// Column "id".
                    add_column(TimeSeriesColumnNames::ID, dataTypeToAST(id_type));
                    {
                        auto & new_decl = new_list->children.back()->as<ASTColumnDeclaration &>();
                        new_decl.ephemeral_default = false;
                        if (!time_series_settings[TimeSeriesSetting::id_generator].value)
                        {
                            /// Function getDefault has changed since the prealpha version,
                            /// so it can generate different identifiers now.
                            new_decl.default_specifier = ColumnDefaultSpecifier::Default;
                            new_decl.setDefaultExpression(TimeSeriesIDGenerator::getDefault(id_type, table_id));
                        }
                        else
                        {
                            new_decl.default_specifier = ColumnDefaultSpecifier::Empty;
                            new_decl.resetDefaultExpression();
                        }
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

                    /// Columns "min_time" and "max_time".
                    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                    {
                        if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                        {
                            /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type.
                            auto make_agg_type = [&](const String & func_name) -> ASTPtr
                            {
                                DataTypePtr ts_type = makeNullable(timestamp_type);
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
                                dataTypeToAST(makeNullable(timestamp_type)));
                            add_column(TimeSeriesColumnNames::MaxTime,
                                dataTypeToAST(makeNullable(timestamp_type)));
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
            create_query.setTargetInnerColumns(inner_table_kind, result);
        }

        /// Replace the prealpha flat outer columns with a single `time_series` column.
        auto time_series_decl = make_intrusive<ASTColumnDeclaration>();
        time_series_decl->name = TimeSeriesColumnNames::TimeSeries;
        time_series_decl->setType(dataTypeToAST(std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type}))));

        auto new_outer_list = make_intrusive<ASTExpressionList>();
        new_outer_list->children.push_back(std::move(time_series_decl));

        auto new_outer_columns = make_intrusive<ASTColumns>();
        new_outer_columns->set(new_outer_columns->columns, new_outer_list);
        create_query.set(create_query.columns_list, new_outer_columns);
    }

    /// Whether the create query was made by a version before the recent samples table existed,
    /// i.e. it doesn't record the `recent_samples_ttl_seconds` setting in its SETTINGS clause.
    bool isVersionWithNoRecentSamplesTTL(const ASTCreateQuery & create_query)
    {
        return create_query.storage && !hasExplicitTimeSeriesSettingRecentSamplesTTL(create_query);
    }

    /// Upgrades a create query made by a version before the `recent_samples_ttl_seconds` setting existed:
    /// records the setting explicitly in the query's SETTINGS clause, so that its value always matches the table.
    void upgradeFromVersionWithNoRecentSamplesTTL(ASTCreateQuery & create_query)
    {
        /// Normally the setting is pinned to zero: the table was initially created without the recent
        /// samples table, while the absent setting would read as its non-zero default. However a query
        /// carrying a RECENT SAMPLES target in any form was authored with the recent samples table
        /// enabled and gets the default TTL instead. Such a query can come from an old-format ON CLUSTER
        /// DDL entry (the query text is shipped un-normalized, only the inner UUID is set by the
        /// initiator) or from a hand-written ATTACH query.
        bool authored_with_recent_samples
            = hasInnerColumns(create_query, ViewTarget::RecentSamples) || hasInnerEngine(create_query, ViewTarget::RecentSamples)
            || hasTargetTableID(create_query, ViewTarget::RecentSamples) || hasInnerUUID(create_query, ViewTarget::RecentSamples);
        UInt64 ttl_to_pin = authored_with_recent_samples ? static_cast<UInt64>(TimeSeriesSettings{}[TimeSeriesSetting::recent_samples_ttl_seconds]) : 0;
        setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds", Field(ttl_to_pin));
    }

    /// Returns the prefix ("", "Replicated" or "Shared") for the names of generated inner table engines,
    /// based on the `default_table_engine` setting. Only the family can be taken from the setting because
    /// each inner table needs its own engine kind (e.g. AggregatingMergeTree for the tags table), so only
    /// the plain MergeTree, ReplicatedMergeTree and SharedMergeTree default engines are supported;
    /// with any other default engine the inner table's engine must be specified explicitly.
    std::string_view getInnerEngineFamilyPrefix(ViewTarget::Kind target_kind, const ContextPtr & context)
    {
        auto default_table_engine = context->getSettingsRef()[Setting::default_table_engine].value;
        switch (default_table_engine)
        {
            case DefaultTableEngine::MergeTree:
                return "";
            case DefaultTableEngine::ReplicatedMergeTree:
                return "Replicated";
            case DefaultTableEngine::SharedMergeTree:
                return "Shared";
            case DefaultTableEngine::None:
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The inner {} table of a TimeSeries table requires an explicit engine "
                    "because the `default_table_engine` setting is 'None'", target_kind);
            default:
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The `default_table_engine` setting value '{}' cannot be used to choose the engine of the inner {} table "
                    "of a TimeSeries table (supported values are MergeTree, ReplicatedMergeTree and SharedMergeTree); "
                    "specify the inner table's engine explicitly", magic_enum::enum_name(default_table_engine), target_kind);
        }
    }

    /// Brings the engine of an inner table into line with the TimeSeries settings: derives an engine if
    /// there is none, corrects a declared one which contradicts the settings, and sets the properties the
    /// settings define. The counterpart of `normalizeInnerColumns` for the engine.
    /// Returns true if the engine was modified.
    bool normalizeInnerEngine(
        ASTStorage & inner_engine,
        ViewTarget::Kind inner_table_kind,
        const TimeSeriesSettings & time_series_settings,
        const StorageID & table_id,
        const ContextPtr & context)
    {
        bool changed = false;

        auto column = [](std::string_view name) -> ASTPtr { return make_intrusive<ASTIdentifier>(String{name}); };

        /// The engine family (plain, Replicated or Shared) follows the `default_table_engine` setting.
        auto set_engine = [&](std::string_view engine_kind)
        {
            auto engine = makeASTFunction(
                fmt::format("{}{}", getInnerEngineFamilyPrefix(inner_table_kind, context), engine_kind));
            engine->setNoEmptyArgs(false);
            inner_engine.setOrReplace(inner_engine.engine, engine);
            changed = true;
        };

        auto set_sorting_key = [&](ASTs key_columns)
        {
            auto sorting_key = makeASTFunction("tuple");
            sorting_key->arguments->children = std::move(key_columns);
            changed |= setEngineClause(inner_engine, inner_engine.order_by, sorting_key);
        };

        auto set_setting = [&](std::string_view name, UInt64 value)
        {
            changed |= setEngineSettings(inner_engine, name, Field(value));
        };

        auto is_merge_tree = [&] { return inner_engine.engine && inner_engine.engine->name.ends_with("MergeTree"); };

        /// Whether `ast` uses the identifier `name`.
        auto uses_identifier = [](this auto && self, const IAST & ast, std::string_view name) -> bool
        {
            if (const auto * identifier = ast.as<ASTIdentifier>(); identifier && (identifier->name() == name))
                return true;
            for (const auto & child : ast.children)
            {
                if (self(*child, name))
                    return true;
            }
            return false;
        };

        auto sorting_key_uses = [&](std::string_view name)
        {
            return inner_engine.order_by && uses_identifier(*inner_engine.order_by, name);
        };

        /// Whether the engine collapses the rows sharing a sorting key when it merges parts.
        auto collapses_rows = [&]
        {
            const String & engine_name = inner_engine.engine->name;
            return engine_name.contains("Aggregating") || engine_name.contains("Replacing")
                || engine_name.contains("Collapsing") || engine_name.contains("Summing");
        };

        /// The reader prunes time series by `min_time` and `max_time` (see the `filter_by_min_time_and_max_time`
        /// setting), so the inner `tags` table has to keep them as the bounds over all the samples of a series.
        /// A merge collapses the rows of a series into one, and then the bounds survive only if the engine merges
        /// them - which is what `aggregate_min_time_and_max_time` asks for - or if they belong to the sorting key,
        /// so that the rows of different insertions are never collapsed together. `normalizeInnerColumns` types
        /// the columns after the same setting, and an aggregated type cannot be a part of a key, so which of the
        /// two layouts is required follows from the setting alone.
        auto keeps_min_time_and_max_time = [&]
        {
            if (!time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                return true;

            if (!inner_engine.engine || !collapses_rows())
                return true;

            if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                return inner_engine.engine->name.contains("Aggregating");

            return sorting_key_uses(TimeSeriesColumnNames::MinTime) && sorting_key_uses(TimeSeriesColumnNames::MaxTime);
        };

        /// The `*_index_granularity` settings set `index_granularity` of the inner MergeTree tables, overriding the engine declaration.
        auto apply_index_granularity = [&](const SettingFieldUInt64 & index_granularity)
        {
            if (is_merge_tree() && (index_granularity.isChanged() || !hasEngineSetting(inner_engine, "index_granularity")))
                set_setting("index_granularity", index_granularity.value);
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                /// The recent samples table gets the same engine as the samples table; it becomes partitioned
                /// and TTL'd below.
                if (!inner_engine.engine)
                {
                    set_engine("MergeTree");
                    set_sorting_key({column(TimeSeriesColumnNames::ID), column(TimeSeriesColumnNames::Timestamp)});
                }

                apply_index_granularity(time_series_settings[(inner_table_kind == ViewTarget::Samples)
                    ? TimeSeriesSetting::samples_index_granularity
                    : TimeSeriesSetting::recent_samples_index_granularity]);

                if (inner_table_kind != ViewTarget::RecentSamples)
                    break;

                /// The table is partitioned by time, so `ttl_only_drop_parts` lets the TTL drop whole expired parts instead of rewriting them.
                if (is_merge_tree() && !hasEngineSetting(inner_engine, "ttl_only_drop_parts"))
                    set_setting("ttl_only_drop_parts", 1);

                if (is_merge_tree())
                {
                    if (const auto & partition_by = time_series_settings[TimeSeriesSetting::recent_samples_partition_by].value)
                    {
                        /// An explicitly set `recent_samples_partition_by` overrides the partition key from the engine declaration.
                        changed |= setEngineClause(inner_engine, inner_engine.partition_by, partition_by->clone());
                    }
                    else if (!inner_engine.partition_by)
                    {
                        /// Otherwise a declared partition key is kept; if there is none, the default one (5-hour buckets) is used.
                        /// `toDateTime` makes the default partition key work for any timestamp type (e.g. a raw `UInt32`),
                        /// same as the TTL expression.
                        inner_engine.set(inner_engine.partition_by,
                            makeASTFunction("toStartOfInterval",
                                makeASTFunction("toDateTime", column(TimeSeriesColumnNames::Timestamp)),
                                makeASTFunction("toIntervalHour", make_intrusive<ASTLiteral>(static_cast<UInt64>(5)))));
                        changed = true;
                    }
                }

                /// `recent_samples_ttl_seconds` is a correctness contract for the reader: the TTL always comes from it; non-TTL engines are rejected.
                if (!is_merge_tree())
                    throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                        "{}: The inner recent samples table requires a MergeTree-family engine to apply the TTL "
                        "defined by the `recent_samples_ttl_seconds` setting", table_id.getNameForLogs());

                auto ttl_element = make_intrusive<ASTTTLElement>(TTLMode::DELETE, DataDestinationType::DELETE, "", /*if_exists=*/ false);
                ttl_element->setTTL(makeASTOperator("plus",
                    makeASTFunction("toDateTime", column(TimeSeriesColumnNames::Timestamp)),
                    makeASTFunction("toIntervalSecond",
                        make_intrusive<ASTLiteral>(time_series_settings[TimeSeriesSetting::recent_samples_ttl_seconds].value))));
                auto ttl_list = make_intrusive<ASTExpressionList>();
                ttl_list->children.push_back(std::move(ttl_element));
                changed |= setEngineClause(inner_engine, inner_engine.ttl_table, ttl_list);
                break;
            }

            case ViewTarget::Tags:
            {
                const bool aggregate_min_time_and_max_time
                    = time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time];

                /// The settings decide how `min_time` and `max_time` are kept, so a declared engine which
                /// contradicts them is corrected - it can also come from another table through `CREATE ... AS`,
                /// where it was derived from different settings. Only the engine and the keys are corrected;
                /// the rest of the declaration is kept, so that e.g. a declared `index_granularity` still
                /// loses only to an explicit `tags_index_granularity`.
                if (!inner_engine.engine || !keeps_min_time_and_max_time())
                {
                    set_engine(aggregate_min_time_and_max_time ? "AggregatingMergeTree" : "ReplacingMergeTree");
                    changed |= setEngineClause(
                        inner_engine, inner_engine.primary_key, column(TimeSeriesColumnNames::MetricName));

                    ASTs key_columns = {column(TimeSeriesColumnNames::MetricName), column(TimeSeriesColumnNames::ID)};
                    if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time] && !aggregate_min_time_and_max_time)
                    {
                        /// Without aggregating them, the bounds survive a merge only in the sorting key, so that
                        /// the rows of different insertions are never collapsed together.
                        key_columns.push_back(column(TimeSeriesColumnNames::MinTime));
                        key_columns.push_back(column(TimeSeriesColumnNames::MaxTime));
                    }
                    set_sorting_key(std::move(key_columns));
                }

                apply_index_granularity(time_series_settings[TimeSeriesSetting::tags_index_granularity]);

                /// The TimeSeries `tags` inner table keeps the tag columns (and the `tags` Map) outside
                /// the sorting key, but they are functionally dependent on `id`, which is part of it: every group of
                /// rows that a background merge collapses together shares the same `id`, hence the same values of
                /// those columns, so this off-key layout is safe here. `AggregatingMergeTree` rejects such a layout
                /// by default (see the `allow_dimensions_outside_sorting_key` setting and
                /// https://github.com/ClickHouse/ClickHouse/issues/751), so enable that setting on the inner tags
                /// engine - both when we derive it and when the user specifies an aggregating engine explicitly.
                if (inner_engine.engine && inner_engine.engine->name.contains("Aggregating")
                    && !hasEngineSetting(inner_engine, "allow_dimensions_outside_sorting_key"))
                {
                    set_setting("allow_dimensions_outside_sorting_key", 1);
                }

                /// `min_time` and `max_time` are nullable, so a sorting key containing them needs `allow_nullable_key` -
                /// both for the engine we derive and for an engine which puts those columns into its sorting key itself.
                if (is_merge_tree()
                    && sorting_key_uses(TimeSeriesColumnNames::MinTime)
                    && sorting_key_uses(TimeSeriesColumnNames::MaxTime)
                    && !hasEngineSetting(inner_engine, "allow_nullable_key"))
                {
                    set_setting("allow_nullable_key", 1);
                }
                break;
            }

            case ViewTarget::Metrics:
            {
                if (!inner_engine.engine)
                {
                    set_engine("ReplacingMergeTree");
                    changed |= setEngineClause(
                        inner_engine, inner_engine.order_by, column(TimeSeriesColumnNames::MetricFamilyName));
                }
                break;
            }

            default:
                UNREACHABLE();
        }

        return changed;
    }

    /// Checks that two inner tables have the same replication type (replicated, shared, or non-replicated),
    /// otherwise their contents would diverge between replicas.
    void checkInnerEngineReplicationMatches(
        ViewTarget::Kind kind, const ASTStorage & inner_engine,
        ViewTarget::Kind prev_kind, const ASTStorage * prev_inner_engine)
    {
        if (!prev_inner_engine || !prev_inner_engine->engine || !inner_engine.engine)
            return;

        const String & engine_name = inner_engine.engine->name;
        const String & prev_engine_name = prev_inner_engine->engine->name;

        auto is_replicated = [](const String & name) { return name.starts_with("Replicated"); };
        auto is_shared = [](const String & name) { return name.starts_with("Shared"); };

        if (is_shared(prev_engine_name) != is_shared(engine_name))
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "The inner {} table {} shared ({}) while the inner {} table {} shared ({})",
                magic_enum::enum_name(prev_kind), is_shared(prev_engine_name) ? "is" : "is not", prev_engine_name,
                magic_enum::enum_name(kind), is_shared(engine_name) ? "is" : "is not", engine_name);
        }

        if (is_replicated(prev_engine_name) != is_replicated(engine_name))
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "The inner {} table {} replicated ({}) while the inner {} table {} replicated ({})",
                magic_enum::enum_name(prev_kind), is_replicated(prev_engine_name) ? "is" : "is not", prev_engine_name,
                magic_enum::enum_name(kind), is_replicated(engine_name) ? "is" : "is not", engine_name);
        }
    }

    /// Checks that a target table or an inner-columns list has all the columns required by the
    /// TimeSeries table engine, and that those columns match the resolved types.
    void checkTargetTable(
        const ColumnsDescription & target_table_columns,
        ViewTarget::Kind target_kind,
        const TimeSeriesSettings & time_series_settings,
        const ResolvedTimeSeriesTypes & resolved_types,
        const StorageID & table_id)
    {
        auto check_column = [&](std::string_view column_name)
        {
            if (!target_table_columns.tryGet(String(column_name)))
                throw Exception(
                    ErrorCodes::THERE_IS_NO_COLUMN,
                    "{}: Column {} is required for the {} table used by TimeSeries table engine",
                    table_id.getNameForLogs(),
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
                    table_id.getNameForLogs(),
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
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        auto check_column_is_string_map = [&](std::string_view column_name, bool if_exists = false)
        {
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (!col)
            {
                if (!if_exists)
                    check_column(column_name);
                return;
            }
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
                    table_id.getNameForLogs(),
                    column_name,
                    target_kind,
                    col->type->getName());
        };

        /// Accepts `Nullable(timestamp_type)` or any aggregate function wrapper.
        auto check_column_min_max_time = [&](std::string_view column_name)
        {
            check_column(column_name);
            const auto * col = target_table_columns.tryGet(String(column_name));
            if (removeNullable(col->type)->equals(*resolved_types.timestamp_type))
                return;
            if (typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(col->type->getCustomName()))
                return;
            if (typeid_cast<const DataTypeAggregateFunction *>(col->type.get()))
                return;
            throw Exception(
                ErrorCodes::BAD_TYPE_OF_FIELD,
                "{}: Column {} in the {} table has type {}, but expected {} (optionally Nullable) or an aggregate-function wrapper",
                table_id.getNameForLogs(),
                column_name,
                target_kind,
                col->type->getName(),
                resolved_types.timestamp_type->getName());
        };

        switch (target_kind)
        {
            case ViewTarget::Samples:
            case ViewTarget::RecentSamples:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);
                check_column_type(TimeSeriesColumnNames::Timestamp, resolved_types.timestamp_type);
                check_column_type(TimeSeriesColumnNames::Value, resolved_types.scalar_type);
                break;
            }

            case ViewTarget::Tags:
            {
                check_column_type(TimeSeriesColumnNames::ID, resolved_types.id_type);
                check_column_is_string(TimeSeriesColumnNames::MetricName);

                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    check_column_is_string(column_name);
                }

                check_column_is_string_map(TimeSeriesColumnNames::Tags);
                check_column_is_string_map(TimeSeriesColumnNames::AllTags, /*if_exists=*/ true);

                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    check_column_min_max_time(TimeSeriesColumnNames::MinTime);
                    check_column_min_max_time(TimeSeriesColumnNames::MaxTime);
                }
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

    /// If `create_query` has clause `AS <other_table>`,
    /// the function reads the CREATE query of the <other_table> and applies outer columns, inner columns, inner engines,
    /// and the `SETTINGS` clause to the current `create_query`.
    void applyASClause(ASTCreateQuery & create_query, const ContextPtr & context)
    {
        chassert (!create_query.as_table.empty());
        auto other_database = context->resolveDatabase(create_query.as_database);
        auto as_create_query = boost::static_pointer_cast<const ASTCreateQuery>(
            DatabaseCatalog::instance().getDatabase(other_database)->getCreateTableQuery(create_query.as_table, context));

        /// The stored metadata of the other table can be written by an older version,
        /// so copy from its normalized form.
        if (as_create_query->is_time_series_table)
        {
            auto normalized = boost::static_pointer_cast<ASTCreateQuery>(as_create_query->clone());
            normalizeTimeSeriesDefinition(*normalized, context, LoadingStrictnessLevel::ATTACH, /* is_restore_from_backup = */ false);
            as_create_query = normalized;
        }

        /// Copy settings from the other table. Settings are merged by name: a setting written in this query wins.
        if (as_create_query->storage && as_create_query->storage->settings)
        {
            if (!create_query.storage)
                create_query.set(create_query.storage, make_intrusive<ASTStorage>());

            auto merged_settings = boost::static_pointer_cast<ASTSetQuery>(as_create_query->storage->settings->clone());
            if (create_query.storage->settings)
                merged_settings->changes.setSettings(create_query.storage->settings->changes);
            create_query.storage->set(create_query.storage->settings, merged_settings);
        }

        /// Copy outer column from the other table.
        if (!create_query.columns_list && as_create_query->columns_list)
        {
            create_query.set(create_query.columns_list,
                boost::static_pointer_cast<ASTColumns>(as_create_query->columns_list->clone()));
        }

        /// Copy inner columns and inner engines from the other table.
        for (auto kind : getTargetKinds())
        {
            if (!hasInnerColumns(create_query, kind))
            {
                if (auto * as_inner_cols = as_create_query->getTargetInnerColumns(kind))
                    create_query.setTargetInnerColumns(kind, boost::static_pointer_cast<ASTColumns>(as_inner_cols->clone()));
            }

            if (!hasTargetTableID(create_query, kind) && !hasInnerEngine(create_query, kind))
            {
                if (hasTargetTableID(*as_create_query, kind))
                {
                    StorageID other_table_id{as_create_query->getDatabase(), as_create_query->getTable()};
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Cannot CREATE a table AS {} because it has external tables", other_table_id.getNameForLogs());
                }
                if (auto * other_inner_engine = as_create_query->getTargetInnerEngine(kind))
                    create_query.setTargetInnerEngine(kind, other_inner_engine->clone());
            }
        }
    }

    /// Generates the canonical column list for the TimeSeries table from the resolved types.
    ColumnsDescription generateTimeSeriesColumns(const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type)
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

}


void normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup)
{
    chassert(create_query.is_time_series_table);

    /// Whether we're creating a new table.
    /// `is_new_table` is false if we're restoring from a backup.
    bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;

    /// Whether the create query may come from an older version, so it can be upgraded to the current form.
    /// The initial CREATE query is excluded: it must be written in the current form already.
    bool can_upgrade = (mode != LoadingStrictnessLevel::CREATE) || is_restore_from_backup;

    /// Upgrade the create_query if it was created by the old versions.
    /// (A new query written in the prealpha form must be rejected, see readTypesFromOuterColumns.)
    if (can_upgrade && isPrealpha(create_query))
    {
        upgradeFromPrealpha(create_query);
        chassert(!isPrealpha(create_query));
    }

    /// Upgrade the create_query if it was created before the recent samples table existed.
    if (can_upgrade && isVersionWithNoRecentSamplesTTL(create_query))
    {
        upgradeFromVersionWithNoRecentSamplesTTL(create_query);
        chassert(!isVersionWithNoRecentSamplesTTL(create_query));
    }

    /// Whether the query itself declares a RECENT SAMPLES target. This is checked before applyASClause,
    /// so the flag doesn't count a target copied from the `AS <other_table>` clause. An inner UUID doesn't
    /// count either: it's not written by users, it's stamped by UUID generation - which can legitimately
    /// happen before normalization (e.g. for an ON CLUSTER query using an old DDL entry format).
    bool has_recent_samples_definition
        = hasInnerColumns(create_query, ViewTarget::RecentSamples) || hasInnerEngine(create_query, ViewTarget::RecentSamples)
        || hasTargetTableID(create_query, ViewTarget::RecentSamples);

    /// Apply the clause `AS <other_table>` if any.
    if (!create_query.as_table.empty())
        applyASClause(create_query, context);

    /// Resolve types timestamp_type, scalar_type, id_type.
    /// External targets are checked only at CREATE time; on ATTACH they may not be loaded yet.
    ResolvedTimeSeriesTypes resolved_types = resolveTimeSeriesTypes(create_query, context, /*check_external_targets=*/ is_new_table);

    /// For new tables: per-kind, check external tables or normalize the inner table's columns and assign its engine.
    if (is_new_table)
    {
        TimeSeriesSettings settings;
        if (create_query.storage)
            settings.loadFromQuery(*create_query.storage);
        checkTimeSeriesSettings(settings);

        /// Pin `recent_samples_ttl_seconds`, so that the table keeps its TTL if a future version changes the default.
        if (!settings[TimeSeriesSetting::recent_samples_ttl_seconds].isChanged() && create_query.storage)
        {
            setEngineSettings(*create_query.storage, "recent_samples_ttl_seconds",
                Field(settings[TimeSeriesSetting::recent_samples_ttl_seconds].value));
        }

        const bool recent_samples_enabled = settings[TimeSeriesSetting::recent_samples_ttl_seconds] != 0;

        /// A RECENT SAMPLES declaration can't be used with `recent_samples_ttl_seconds = 0`
        if (!recent_samples_enabled)
        {
            if (has_recent_samples_definition)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "The RECENT SAMPLES target requires the setting `recent_samples_ttl_seconds` to be set to a non-zero value");
            /// A RECENT SAMPLES definition inherited from the `AS <other_table>` clause is just removed
            /// when `recent_samples_ttl_seconds = 0` disables it.
            if (create_query.targets)
                create_query.targets->removeTarget(ViewTarget::RecentSamples);
        }

        /// The previous inner engine to compare with in checkInnerEngineReplicationMatches. It owns the engine,
        /// which is not always stored in `create_query` - `normalizeInnerEngine` can leave it unchanged.
        ViewTarget::Kind prev_inner_kind{};
        boost::intrusive_ptr<ASTStorage> prev_inner_engine;

        for (auto kind : getTargetKinds())
        {
            /// The recent samples target is on by default and disabled by an explicit `recent_samples_ttl_seconds = 0`.
            if ((kind == ViewTarget::RecentSamples) && !recent_samples_enabled)
                continue;

            if (hasTargetTableID(create_query, kind))
            {
                /// An external target table is specified — check it has all the required columns.
                auto target_table_id = create_query.getTargetTableID(kind);
                auto target_table = DatabaseCatalog::instance().getTable(target_table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr(context, false);
                checkTargetTable(target_metadata->columns, kind, settings, resolved_types, target_table_id);
            }
            else
            {
                /// An inner target table should be used. Normalize its column definitions and assign a table engine if not specified.
                StorageID table_id{create_query.getDatabase(), create_query.getTable()};

                auto inner_columns = create_query.getTargetInnerColumns(kind)
                    ? boost::static_pointer_cast<ASTColumns>(create_query.getTargetInnerColumns(kind)->clone())
                    : make_intrusive<ASTColumns>();
                if (normalizeInnerColumns(*inner_columns, kind, settings, resolved_types, table_id))
                    create_query.setTargetInnerColumns(kind, inner_columns);

                /// Validate the user-provided types of the inner columns the same way external targets are validated.
                auto inner_columns_description = InterpreterCreateQuery::getColumnsDescription(
                    *inner_columns->columns, context, mode);
                checkTargetTable(inner_columns_description, kind, settings, resolved_types, table_id);

                auto inner_engine = create_query.getTargetInnerEngine(kind)
                    ? boost::static_pointer_cast<ASTStorage>(create_query.getTargetInnerEngine(kind)->clone())
                    : make_intrusive<ASTStorage>();
                if (normalizeInnerEngine(*inner_engine, kind, settings, table_id, context))
                    create_query.setTargetInnerEngine(kind, inner_engine);

                checkInnerEngineReplicationMatches(kind, *inner_engine, prev_inner_kind, prev_inner_engine.get());
                prev_inner_kind = kind;
                prev_inner_engine = inner_engine;
            }
        }
    }

    /// Regenerate the columns of TimeSeries table from the resolved types.
    /// We can change the columns of TimeSeries table because these columns are designed to work
    /// as IO interface. They store no data, in fact the data is stored in target or inner columns.
    {
        auto new_columns_ast = make_intrusive<ASTColumns>();
        new_columns_ast->set(new_columns_ast->columns,
            InterpreterCreateQuery::formatColumns(generateTimeSeriesColumns(resolved_types.timestamp_type, resolved_types.scalar_type)));
        const auto * old_columns = create_query.columns_list;
        if (!old_columns
            || !old_columns->columns
            || old_columns->formatWithSecretsOneLine() != new_columns_ast->formatWithSecretsOneLine())
        {
            create_query.set(create_query.columns_list, new_columns_ast);
        }
    }
}

}
