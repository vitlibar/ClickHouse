#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Contains helper information to evaluate an argument of a simple function
/// taking arguments of types scalar or instant vector and transforming each value separately.
/// Examples: math functions (e.g. abs(), sin()), date/time functions (e.g. day_of_week()),
/// binary operators when at least one of the operands is scalar.
///
/// Simple functions are evaluated either directly (for example "SELECT sin(value) AS value"
/// or via arrayMap (for example "SELECT arrayMap(x -> sin(x), values) AS values")
struct SimpleFunctionArgumentHelper
{
    size_t argument_index = 0;
    ASTPtr ast;
    ASTPtr array_map_source_array;
    ASTPtr array_map_lambda_arg;
    String table_to_select_from;
    StoreMethod store_method = StoreMethod::EMPTY;
    bool metric_name_dropped = false;
    TimestampType start_time = {};
    TimestampType end_time = {};
    DurationType step = {};

    SimpleFunctionArgumentHelper(size_t argument_index_, SQLQueryPiece && argument, ConverterContext & context);
};


/// Makes AST for evaluating a simple function taking one argument.
ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr)> & transform_ast,
                                              const SimpleFunctionArgumentHelper & argument);

/// Makes AST for evaluating a simple function taking two arguments.
ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr, ASTPtr)> & transform_ast,
                                              const SimpleFunctionArgumentHelper & argument1,
                                              const SimpleFunctionArgumentHelper & argument2);

/// Makes AST for evaluating a simple function taking two arguments.
ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr, ASTPtr, ASTPtr)> & transform_ast,
                                              const SimpleFunctionArgumentHelper & argument1,
                                              const SimpleFunctionArgumentHelper & argument2,
                                              const SimpleFunctionArgumentHelper & argument3);

/// Returns the store method of the result.
StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument);
StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2);
StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2, const SimpleFunctionArgumentHelper & argument3);

String getTableToSelectFrom(const SimpleFunctionArgumentHelper & argument);
String getTableToSelectFrom(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2);
String getTableToSelectFrom(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2, const SimpleFunctionArgumentHelper & argument3);

}
