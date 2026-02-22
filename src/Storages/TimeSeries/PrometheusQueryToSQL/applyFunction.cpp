#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunction.h>

#include <Common/Exception.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    std::string_view function_name = function_node->function_name;

    if (isFunctionOverRange(function_name))
        return applyFunctionOverRange(function_node, std::move(arguments), context);

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", function_name);
}

}
