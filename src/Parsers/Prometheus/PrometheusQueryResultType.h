#pragma once

namespace DB
{

/// A query in prometheus can evaluate to one of the following four types:
enum class PrometheusQueryResultType
{
    SCALAR,
    INTERVAL,
    STRING,
    INSTANT_VECTOR,
    RANGE_VECTOR,
};

}
