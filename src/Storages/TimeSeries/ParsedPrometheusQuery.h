#pragma once

#include <Common/Logger.h>


namespace DB
{
enum class PrometheusQueryResultType;
struct TimeSeriesMatchersWithTimeRanges;

/// Represents a parsed promql query, i.e. a query written in the prometheus query language,
/// for example 'http_requests_total{job="prometheus",group="canary"}'.
class ParsedPrometheusQuery
{
public:
    /// Parses a promql query, can throw an exception if the syntax is wrong.
    explicit ParsedPrometheusQuery(const String & promql_query_);
    ~ParsedPrometheusQuery();

    /// Returns the promql query passed to the constructor.
    const String & getQuery() const { return promql_query; }

    /// Dumps the tree of the parsed promql query to a string for debugging purposes.
    String getQueryTree() const;

    /// Returns the type of the query's result.
    PrometheusQueryResultType getResultType() const { return result_type; }

    /// Finds all matchers for metric names and tags and corresponding time ranges used in the query.
    TimeSeriesMatchersWithTimeRanges findMatchersWithTimeRanges(DecimalField<DateTime64> evaluation_time, UInt64 lookback_delta_ms) const;

private:
    void determineResultType();

    String promql_query;

    class PromQLParserImpl;
    std::unique_ptr<PromQLParserImpl> parser;

    PrometheusQueryResultType result_type;

    LoggerPtr log;
};

}
