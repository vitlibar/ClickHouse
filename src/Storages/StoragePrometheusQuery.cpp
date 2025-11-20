#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterConfig.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StoragePrometheusQuery::Configuration StoragePrometheusQuery::getConfiguration(ASTs & args, ContextPtr context, bool is_query_range)
{
    std::string_view function_name = is_query_range ? "prometheusQueryRange" : "prometheusQuery";
    size_t min_num_args = 3 + is_query_range * 2;
    size_t max_num_args = 4 + is_query_range * 2;

    if ((args.size() < min_num_args) || (args.size() > max_num_args))
    {
        std::string_view expected_args = is_query_range ? "[database, ] time_series_table, promql_query, start_time, end_time, step"
                                                        : "[database, ] time_series_table, promql_query, evaluation_time";
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires {}..{} arguments: {}([database, ] time_series_table, promql_query, {})",
                        function_name, min_num_args, max_num_args, function_name, expected_args);
    }

    Configuration configuration;
    size_t argument_index = 0;

    if (args.size() == min_num_args)
    {
        /// prometheusQuery( [my_db.]my_time_series_table, ... )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                configuration.time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    for (size_t i = argument_index; i != args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    if (configuration.time_series_storage_id.empty())
    {
        if (args.size() == min_num_args)
        {
            /// prometheusQuery( 'my_time_series_table', ... )
            configuration.time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
        else
        {
            /// prometheusQuery( 'mydb', 'my_time_series_table', ... )
            configuration.time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[argument_index++], "database_name");
            configuration.time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
    }

    configuration.time_series_storage_id = context->resolveStorageID(configuration.time_series_storage_id);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(configuration.time_series_storage_id, context));
    auto data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, context)->getInMemoryMetadataPtr();
    configuration.timestamp_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    configuration.scalar_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

    configuration.promql_query.parse(checkAndGetLiteralArgument<String>(args[argument_index++], "promql_query"));

    if (is_query_range)
    {
        configuration.evaluation_range.start_time = args[argument_index++]->as<const ASTLiteral &>().value;
        configuration.evaluation_range.end_time = args[argument_index++]->as<const ASTLiteral &>().value;
        configuration.evaluation_range.step = args[argument_index++]->as<const ASTLiteral &>().value;
    }
    else
    {
        configuration.evaluation_time = args[argument_index++]->as<const ASTLiteral &>().value;
    }

    chassert(argument_index == args.size());
    return configuration;
}


StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const Configuration & configuration_)
    : IStorage{table_id_}
    , configuration(configuration_)
    , log(getLogger("StoragePrometheusQuery"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

void StoragePrometheusQuery::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    LOG_INFO(log, "Building SQL to evaluate promql query: {}", configuration.promql_query);

    PrometheusQueryToSQL::ConverterConfig converter_config;
    converter_config.time_series_storage_id = configuration.time_series_storage_id;
    converter_config.timestamp_type = configuration.timestamp_type;
    converter_config.scalar_type = configuration.scalar_type;
    converter_config.evaluation_time = configuration.evaluation_time;
    converter_config.evaluation_range = configuration.evaluation_range;

    PrometheusQueryToSQL::Converter converter{configuration.promql_query, converter_config};
    ASTPtr select_query = converter.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());
    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);
    InterpreterSelectQueryAnalyzer interpreter(select_query, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
