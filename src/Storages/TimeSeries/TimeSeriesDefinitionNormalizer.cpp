#include <Storages/TimeSeries/TimeSeriesDefinitionNormalizer.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int INCORRECT_QUERY;
}


namespace
{
    template <typename... Args>
    std::shared_ptr<ASTFunction> makeASTFunctionForDataType(const String & type_name, Args &&... args)
    {
        auto function = std::make_shared<ASTFunction>();
        function->name = type_name;
        function->no_empty_args = true;
        if (sizeof...(args))
        {
            function->arguments = std::make_shared<ASTExpressionList>();
            function->children.push_back(function->arguments);
            function->arguments->children = { std::forward<Args>(args)... };
        }
        return function;
    };

}

void TimeSeriesDefinitionNormalizer::normalize(ASTCreateQuery & create_query,
                                               const TimeSeriesSettings & time_series_settings,
                                               const ASTCreateQuery * as_create_query) const
{
    reorderColumns(create_query, time_series_settings);
    addMissingColumns(create_query, time_series_settings);
    addMissingDefaultForIDColumn(create_query, time_series_settings);

    if (as_create_query)
        addMissingInnerEnginesFromAsTable(create_query, *as_create_query);

    addMissingInnerEngines(create_query, time_series_settings);
}


void TimeSeriesDefinitionNormalizer::reorderColumns(ASTCreateQuery & create, const TimeSeriesSettings & time_series_settings) const
{
    if (!create.columns_list || !create.columns_list->columns)
        return;

    auto & columns = create.columns_list->columns->children;

    std::unordered_map<std::string_view, std::shared_ptr<ASTColumnDeclaration>> columns_by_name;
    for (const auto & column : columns)
    {
        auto column_declaration = typeid_cast<std::shared_ptr<ASTColumnDeclaration>>(column);
        columns_by_name[column_declaration->name] = column_declaration;
    }

    columns.clear();

    auto add_column_in_correct_order = [&](std::string_view column_name)
    {
        auto it = columns_by_name.find(column_name);
        if (it != columns_by_name.end())
        {
            columns.push_back(it->second);
            columns_by_name.erase(it);
        }
    };

    add_column_in_correct_order(TimeSeriesColumnNames::ID);
    add_column_in_correct_order(TimeSeriesColumnNames::Timestamp);
    add_column_in_correct_order(TimeSeriesColumnNames::Value);

    add_column_in_correct_order(TimeSeriesColumnNames::MetricName);

    const Map & tags_to_columns = time_series_settings.tags_to_columns;
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        add_column_in_correct_order(column_name);
    }

    if (time_series_settings.store_other_tags_as_map)
        add_column_in_correct_order(TimeSeriesColumnNames::Tags);

    if (time_series_settings.enable_column_all_tags)
        add_column_in_correct_order(TimeSeriesColumnNames::AllTags);

    if (time_series_settings.store_min_time_and_max_time)
    {
        add_column_in_correct_order(TimeSeriesColumnNames::MinTime);
        add_column_in_correct_order(TimeSeriesColumnNames::MaxTime);
    }

    add_column_in_correct_order(TimeSeriesColumnNames::MetricFamilyName);
    add_column_in_correct_order(TimeSeriesColumnNames::Type);
    add_column_in_correct_order(TimeSeriesColumnNames::Unit);
    add_column_in_correct_order(TimeSeriesColumnNames::Help);

    if (!columns_by_name.empty())
    {
        throw Exception(
            ErrorCodes::INCOMPATIBLE_COLUMNS,
            "{}: Column {} can't be used in this table. "
            "The TimeSeries table engine supports only a limited set of columns (id, timestamp, value, metric_name, tags, metric_family_name, type, unit, help). "
            "Extra columns representing tags must be specified in the 'tags_to_columns' setting.",
            time_series_storage_id.getNameForLogs(), columns_by_name.begin()->first);
    }
}


void TimeSeriesDefinitionNormalizer::addMissingColumns(ASTCreateQuery & create, const TimeSeriesSettings & time_series_settings) const
{
    if (!create.as_table.empty())
    {
        /// Collumns must be extracted from the AS <table>.
        return;
    }

    if (!create.columns_list)
        create.set(create.columns_list, std::make_shared<ASTColumns>());

    if (!create.columns_list->columns)
        create.columns_list->set(create.columns_list->columns, std::make_shared<ASTExpressionList>());
    auto & columns = create.columns_list->columns->children;

    size_t position = 0;

    auto is_next_column_named = [&](std::string_view column_name)
    {
        if (position < columns.size() && (typeid_cast<const ASTColumnDeclaration &>(*columns[position]).name == column_name))
        {
            ++position;
            return true;
        }
        return false;
    };

    auto make_new_column = [&](const String & column_name, ASTPtr type)
    {
        auto new_column = std::make_shared<ASTColumnDeclaration>();
        new_column->name = column_name;
        new_column->type = type;
        columns.insert(columns.begin() + position, new_column);
        ++position;
    };

    auto make_nullable = [&](std::shared_ptr<ASTFunction> type)
    {
        if (type->name == "Nullable")
            return type;
        else
            return makeASTFunctionForDataType("Nullable", type);
    };

    auto get_uuid_type = [] { return makeASTFunctionForDataType("UUID"); };
    auto get_datetime_type = [] { return makeASTFunctionForDataType("DateTime64", std::make_shared<ASTLiteral>(3ul)); };
    auto get_float_type = [] { return makeASTFunctionForDataType("Float64"); };
    auto get_string_type = [] { return makeASTFunctionForDataType("String"); };
    auto get_lc_string_type = [&] { return makeASTFunctionForDataType("LowCardinality", get_string_type()); };
    auto get_string_to_string_map_type = [&] { return makeASTFunctionForDataType("Map", get_string_type(), get_string_type()); };
    auto get_lc_string_to_string_map_type = [&] { return makeASTFunctionForDataType("Map", get_lc_string_type(), get_string_type()); };

    if (!is_next_column_named(TimeSeriesColumnNames::ID))
        make_new_column(TimeSeriesColumnNames::ID, get_uuid_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Timestamp))
        make_new_column(TimeSeriesColumnNames::Timestamp, get_datetime_type());

    auto timestamp_type
        = typeid_cast<std::shared_ptr<ASTFunction>>(typeid_cast<const ASTColumnDeclaration &>(*columns[position - 1]).type->ptr());

    if (!is_next_column_named(TimeSeriesColumnNames::Value))
        make_new_column(TimeSeriesColumnNames::Value, get_float_type());

    if (!is_next_column_named(TimeSeriesColumnNames::MetricName))
        make_new_column(TimeSeriesColumnNames::MetricName, get_lc_string_type());

    const Map & tags_to_columns = time_series_settings.tags_to_columns;
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (!is_next_column_named(column_name))
            make_new_column(column_name, get_lc_string_type());
    }

    if (time_series_settings.store_other_tags_as_map)
    {
        if (!is_next_column_named(TimeSeriesColumnNames::Tags))
            make_new_column(TimeSeriesColumnNames::Tags, get_lc_string_to_string_map_type());
    }

    if (time_series_settings.enable_column_all_tags)
    {
        if (!is_next_column_named(TimeSeriesColumnNames::AllTags))
            make_new_column(TimeSeriesColumnNames::AllTags, get_string_to_string_map_type());
    }

    if (time_series_settings.store_min_time_and_max_time)
    {
        if (!is_next_column_named(TimeSeriesColumnNames::MinTime))
            make_new_column(TimeSeriesColumnNames::MinTime, make_nullable(timestamp_type));
        if (!is_next_column_named(TimeSeriesColumnNames::MaxTime))
            make_new_column(TimeSeriesColumnNames::MaxTime, make_nullable(timestamp_type));
    }

    if (!is_next_column_named(TimeSeriesColumnNames::MetricFamilyName))
        make_new_column(TimeSeriesColumnNames::MetricFamilyName, get_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Type))
        make_new_column(TimeSeriesColumnNames::Type, get_lc_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Unit))
        make_new_column(TimeSeriesColumnNames::Unit, get_lc_string_type());

    if (!is_next_column_named(TimeSeriesColumnNames::Help))
        make_new_column(TimeSeriesColumnNames::Help, get_string_type());
}


void TimeSeriesDefinitionNormalizer::addMissingDefaultForIDColumn(ASTCreateQuery & create, const TimeSeriesSettings & time_series_settings) const
{
    if (!create.columns_list || !create.columns_list->columns)
        return;

    auto & columns = create.columns_list->columns->children;
    auto it = std::find_if(columns.begin(), columns.end(), [](const ASTPtr & column)
    {
        return typeid_cast<const ASTColumnDeclaration &>(*column).name == TimeSeriesColumnNames::ID;
    });

    if (it == columns.end())
        return;

    auto & column_declaration = typeid_cast<ASTColumnDeclaration &>(**it);

    if (column_declaration.default_specifier.empty() && !column_declaration.default_expression)
    {
        column_declaration.default_specifier = "DEFAULT";
        column_declaration.default_expression = chooseIDAlgorithm(column_declaration, time_series_settings);
    }
}


/// Generates a formulae for calculating the identifier of a time series from the metric name and all the tags.
ASTPtr TimeSeriesDefinitionNormalizer::chooseIDAlgorithm(const ASTColumnDeclaration & id_column, const TimeSeriesSettings & time_series_settings) const
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
        if (time_series_settings.store_other_tags_as_map)
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

    auto id_type = DataTypeFactory::instance().get(id_column.type);
    WhichDataType id_type_which(*id_type);

    if (id_type_which.isUInt64())
    {
        return make_hash_function("sipHash64");
    }
    else if (id_type_which.isFixedString() && typeid_cast<const DataTypeFixedString &>(*id_type).getN() == 16)
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
                        time_series_storage_id.getNameForLogs(), id_column.name, id_column.name, id_type->getName(), id_type->getName());
    }
}


void TimeSeriesDefinitionNormalizer::addMissingInnerEnginesFromAsTable(ASTCreateQuery & create, const ASTCreateQuery & as_create) const
{
    for (auto kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
    {
        if (as_create.hasTargetTableID(kind))
        {
            QualifiedTableName as_table{as_create.getDatabase(), as_create.getTable()};
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Cannot CREATE a table AS {}.{} because it has external tables",
                backQuoteIfNeed(as_table.database), backQuoteIfNeed(as_table.table));
        }

        auto inner_storage_def = create.getTargetTableEngine(kind);
        if (inner_storage_def)
            continue;

        auto extracted_storage_def = as_create.getTargetTableEngine(kind);
        if (extracted_storage_def)
            create.setTargetTableEngine(kind, typeid_cast<std::shared_ptr<ASTStorage>>(extracted_storage_def->clone()));
    }
}


void TimeSeriesDefinitionNormalizer::addMissingInnerEngines(ASTCreateQuery & create, const TimeSeriesSettings & time_series_settings) const
{
    for (auto kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
    {
        auto inner_storage_def = create.getTargetTableEngine(kind);
        if (inner_storage_def && inner_storage_def->engine)
            continue;
        if (!inner_storage_def)
        {
            inner_storage_def = std::make_shared<ASTStorage>();
            create.setTargetTableEngine(kind, inner_storage_def);
        }
        setInnerDefaultEngine(kind, *inner_storage_def, time_series_settings);
    }
}


void TimeSeriesDefinitionNormalizer::setInnerDefaultEngine(
    TargetKind kind, ASTStorage & inner_storage_def, const TimeSeriesSettings & time_series_settings) const
{
    switch (kind)
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
            String engine_name;
            if (time_series_settings.aggregate_min_time_and_max_time)
                engine_name = "AggregatingMergeTree";
            else
                engine_name = "ReplacingMergeTree";

            inner_storage_def.set(inner_storage_def.engine, makeASTFunction(engine_name));
            inner_storage_def.engine->no_empty_args = false;

            if (!inner_storage_def.order_by && !inner_storage_def.primary_key && inner_storage_def.engine->name.ends_with("MergeTree"))
            {
                inner_storage_def.set(inner_storage_def.primary_key,
                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                ASTs order_by_list;
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));

                if (time_series_settings.store_min_time_and_max_time && !time_series_settings.aggregate_min_time_and_max_time)
                {
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));
                }

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
