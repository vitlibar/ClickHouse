#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>
#include <base/types.h>


namespace DB::PrometheusQueryToSQL
{

struct SelectQueryParams
{
    ASTs select_list;
    String from_subquery;
    ASTPtr from_table_function;
    ASTPtr where;
    ASTs group_by;
    ASTs order_by;
    bool descending_order = false;
    std::optional<size_t> limit;
    SQLSubqueries with;
};

/// Builds a SELECT query reading or calculating something as a part of executing a prometheus query.
ASTPtr buildSelectQuery(SelectQueryParams && params);

}
