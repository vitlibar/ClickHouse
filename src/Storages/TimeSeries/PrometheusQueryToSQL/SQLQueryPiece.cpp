#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

std::string_view getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    return context.promql_tree.getQuery(query_piece.promql_node);
}


void throwStoreMethodIsNotSupported(const SQLQueryPiece & query_piece, const ConverterContext & context)
{
    throw Exception(
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
        "Query {} is of type {} and can't use store method {}",
        getPromQLQuery(query_piece, context), query_piece.type, query_piece.store_method);
}


void checkStartTimeEqualsToEndTime(const SQLQueryPiece & result, const ConverterContext & context)
{
    if (result.start_time != result.end_time)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Query {} is of type {} and can't be executed on range ({}..{}).",
            getPromQLQuery(result, context), Field{result.type}, Field{result.start_time}, Field{result.end_time});
    }
}

}
