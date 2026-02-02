#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.metric_name_dropped)
        return std::move(query_piece);

    switch (query_piece.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SCALAR_GRID:
        {
            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// When we remove the metric name `__name__` it's possible that we get the same set of tags (i.e. the same `group`)
            /// on time series which were different before we removed the metric name.
            /// Generally this is not allowed (we can't have multiple time series with the same set of tags).
            /// However if multiple time series have values at different times then we can coalesce them into one time series:
            ///
            ///             tags                           timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2'}        value_a            NULL
            /// metric2{tag1='value1', tag2='value2'}         NULL             value_b
            ///
            ///                                  ||
            ///                                  \/
            ///
            ///             tags                           timestamp1        timestamp2
            /// {tag1='value1', tag2='value2'}               value_a            NULL
            /// {tag1='value1', tag2='value2'}                NULL             value_b
            ///
            ///                                  ||
            ///                                  \/
            ///
            ///             tags                           timestamp1        timestamp2
            /// {tag1='value1', tag2='value2'}               value_a           value_b
            ///
            /// That's why we need the function timeSeriesCoalesceGridValues().

            /// Here we're building the following query:
            /// SELECT timeSeriesRemoveTag(group, '__name__') AS group,
            ///        timeSeriesCoalesceGridValues('throw')(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY group
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeASTFunction(
                "timeSeriesRemoveTag",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTLiteral>(kMetricName)));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            auto coalesce_function = addParametersToAggregateFunction(
                makeASTFunction(
                    "timeSeriesCoalesceGridValues",
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                make_intrusive<ASTLiteral>("throw"));

            builder.select_list.push_back(std::move(coalesce_function));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            query_piece.select_query = builder.getSelectQuery();

            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::RAW_DATA:
        {
            /// dropMetricName() must not be called with StoreMethod::RAW_DATA.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot drop the metric name from the result of expression {} because of its store method {}",
                            getPromQLQuery(query_piece, context), query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
