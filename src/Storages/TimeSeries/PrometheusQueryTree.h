#pragma once


namespace DB
{

/// This tree represents how we execute a prometheus query.
class PrometheusQueryTree
{
public:
    using Time = DecimalField<DateTime64>;

    struct Node
    {
        virtual ~Node() = default;
    };

    /// Finds all IDs in the tags table and cache them; then reads all the data corresponding to those IDs from the data table.
    /// Produces columns `id`, `time`, `value`.
    struct ReadAllNode : public Node
    {
        std::vector<MatchersAndTimeRanges> all_matchers_and_time_ranges;
    };

    /// Filters the data received from ReadAllData. Leaves only data related a particular part of the query.
    struct FilterNode : public Node
    {
        Node * input;
        MatchersAndTimeRanges matchers_and_time_ranges;
    };

    /// Aggregates the data data over specified time ranges.
    /// This aggregation is used to implement <aggregation>_over_time() functions,
    /// and also the instant selector (it's actually the same as function last_over_time),
    /// and also functions 
    /// and also functions rate(), irate(), increase(), resets() (after SortNode).
    /// Produces one result per each ID and each specified time range.
    /// For each time point in `input` we finds time ranges containing it and then evaluate the aggregation function.
    /// AggregationOverTime actually transforms data in these 5 steps:
    /// 1. Find time ranges for each time point - this will add a new column named `max_time` for each time point
    ///    (we don't need to store `min_time` because all time ranges have the same duration).
    ///    There can be multiple `max_time` for one time point, so this step can produce more rows than it was before.
    /// 2. Sort rows in each chunk by `id`, `max_time` to be able to do some preaggregation.
    /// 3. Do some preaggregation, which means we calculate some aggregation data for the data in each chunk and store them
    ///    in a new column named `aggregation_data`. Columns `timestamp`, `value` are not needed after that, so we can remove it.
    ///    The type of aggregation data depends on the aggregation function we use.
    ///    For example for "avg" it's the number of values and their total sum.
    /// 4. Sort the data in all the preaggregated chunks by `id`, `max_time` to make the aggregated data for each aggregation become adjacent.
    /// 5. Finalize the aggregation - rename the `max_time` column to `time`, add a new column with the result of aggregation named `value`.
    ///    The column `aggregation_data` is not needed any longer, so we can remove it.
    struct TimeAggregationNode : public Node
    {
        Node * input;

        TimeRanges time_ranges;
        std::optional<std::unordered_map<Time, size_t>> explicit_map_of_time_ranges;

        /// What type of aggregation we do here. It can be "instant", "avg", "min", "max"
        String function_name;
    };

    /// Sorts the data in all the chunks by `id`, `time`.
    /// What is actully needed is to ensure that chunks don't have overlapping time ranges for same time series.
    /// This is required to execute functions rate(), resets().
    struct SortNode : public Node
    {
        Node * input;
    };

    /// Transform value with some function, like "abs".
    struct ValueTransformNode : public Node
    {
        Node * input;
    };

};

}
