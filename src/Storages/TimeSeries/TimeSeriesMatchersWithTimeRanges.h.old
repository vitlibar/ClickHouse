#pragma once

#include <Core/Field.h>


namespace DB
{

/// Represents all matchers with corresponding time ranges extracted from a promql query.
///
/// Examples:
/// 1. PromQL: up
/// Matchers with time ranges:
/// #0: Matcher(__name__ EQ "up"), TimeRange(evaluation_time - lookback, evaluation_time)
///
/// 2. PromQL: up - (up offset 1h)
/// Matchers with time ranges:
/// #0: Matcher(__name__ EQ "up"), TimeRange(evaluation_time - lookback, evaluation_time), TimeRange(evaluation_time - 1h - lookback, evaluation_time - 1h)
///
/// 3. PromQL: http_errors{code="500"} / ignoring(code) http_requests
/// Matchers with time ranges:
/// #0: Matcher(__name__ EQ "http_errors"), Matcher(code EQ "500") TimeRange(evaluation_time - lookback, evaluation_time)
/// #1: Matcher(__name__ EQ "http_requests"), TimeRange(evaluation_time - lookback, evaluation_time)
///
/// 4. PromQL: rate(http_requests_total{job="prometheus}[1m])[1h:1m]
/// Matchers with time ranges:
/// #0: Matcher(__name__ EQ "http_requests_total"), Matcher(job EQ "prometheus"), TimeRange(evaluation_time - 1h - lookback, evaluation_time)

struct TimeSeriesMatchersWithTimeRanges
{
    /// Represents a single matcher for a metric name or tag, for example '__name__="http_requests_total"' or 'group=~"canary|production"'
    struct Matcher
    {
        String tag_name;
        String tag_value;
        enum class Type { EQ /* = */, NE /* != */, RE /* =~ */, NRE /* !~ */};
        Type type;

        String toString() const;
        ASTPtr matcherToAST() const;
    };

    /// A time range [min_time .. max_time].
    struct TimeRange
    {
        DecimalField<DateTime64> min_time;
        DecimalField<DateTime64> max_time;

        String toString() const;
    };

    std::vector<Matcher> matchers;
    std::vector<TimeRange> time_ranges;

    String toString() const;
    ASTPtr matchersToAST() const;
};

String toString(const std::vector<TimeSeriesMatchersWithTimeRanges> & elements);
ASTPtr matchersToAST(const std::vector<TimeSeriesMatchersWithTimeRanges> & elements);


    /// Represents matchers for tags with a time range extracted from the query.
    /// Here the time range is actually a union of all the time ranges used to evaluate the query, for example
    /// for 'rate(http_requests_total{job="prometheus}[1m])[1h:1m]' the time range is [evaluation_time - 1h - lookback, evaluation_time].
    /// Also there are two matchers in that example: '__name="http_requests_total"' and 'job="prometheus"'.
    struct Element
    {
        std::set<Matcher> matchers; /// std::set to keep this list sorted (which we need to make append() work)
        std::vector<TimeRange> time_ranges;

        String toString() const;
        ASTPtr matchersToAST() const;
    };

    /// Matchers for multiple occurences in the promql query.
    std::vector<Element> elements;

    bool empty() const { return elements.empty(); }

    /// Dumps the matchers with time ranges to a string for debug purposes.
    String toString() const;

    /// Appends another list of matchers and time ranges.
    /// This function just appends time ranges to the lists without compacting time ranges.
    void append(const TimeSeriesMatchersWithTimeRanges & other);

    /// Tries to remove overlapped time ranges and join adjacent time ranges together.
    void compactTimeRanges();

    /// Makes a WHERE condition combining all the matchers.
    /// This function generates a condition like
    /// (metric_name = "up" AND match(tags[instance], "^canary.*")) OR (metric_name = "up" AND match(tags[instance], "^testing.*")).
    /// The function returns nullptr if there are no matchers (i.e. if empty() == true).
    ASTPtr matchersToAST() const;
};

}
