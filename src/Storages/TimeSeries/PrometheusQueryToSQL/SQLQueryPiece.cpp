#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

void checkStartTimeEqualsToEndTime(const SQLQueryPiece & result, const PrometheusQueryTree & promql_tree)
{
    if (result.start_time != result.end_time)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
            "Query {} is of type {} and can't be executed on range ({}..{}).",
            promql_tree.getQuery(result.promql_node), Field{result.type}, Field{result.start_time}, Field{result.end_time});
    }
}

void throwStoreMethodIsNotSupported(const SQLQueryPiece & piece, const PrometheusQueryTree & promql_tree)
{
    throw Exception(
        ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
        "Query {} is of type {} and can't use store method {}",
        promql_tree.getQuery(piece.promql_node), piece.type, piece.store_method);
}

}
