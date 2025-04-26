#pragma once

#include <Core/Field.h>


namespace DB
{
enum class PrometheusQueryResultType;

/// This tree represents a parsed prometheus query.
class PrometheusQueryTree
{
public:
    using ScalarType = Float64;
    using TimeType = DecimalField<DateTime64>;
    using DurationType = TimeType;
    using ResultType = PrometheusQueryResultType;

    enum class NodeType
    {
        ScalarLiteral,
        StringLiteral,
        Matcher,
        InstantSelector,
        RangeSelector,
        At,
        Subquery,
        Function,
        UnaryOperator,
        BinaryOperator,
        AggregationOperator
    };

    class Node
    {
    public:
        NodeType node_type;
        std::string_view promql; /// Part of the promql query which this node represents with its children.
        ResultType result_type;  /// The data type this node with its children evaluates to.
        std::vector<const Node *> children; /// E.g. arguments for a function, matchers for selectors.
        const Node * parent = nullptr;
        Node(const Node &) = default;
        virtual ~Node() = default;
        virtual String dumpTree(size_t indent) const = 0;
    };

    /// A scalar literal, i.e. a floating-point number.
    /// Example: -2.43
    class ScalarLiteral : public Node
    {
    public:
        ScalarType scalar;
        String dumpTree(size_t indent) const override;
    };

    /// A string literal.
    /// Example: "abc"
    class StringLiteral : public Node
    {
    public:
        String string;
        String dumpTree(size_t indent) const override;
    };
    
    enum class MatcherType { EQ /* = */, NE /* != */, RE /* =~ */, NRE /* !~ */};

    /// A matcher for a label or for the metric name. Matchers are used in instant selectors and range selectors.
    /// Examples: __name__="http_requests"
    ///           job="prometheus"
    ///           release=~"canary|testing"
    class Matcher : public Node
    {
    public:
        String label_name;
        String label_value;
        MatcherType matcher_type;
        String dumpTree(size_t indent) const override;
    };

    /// An instant selector.
    /// Example: http_requests{job="prometheus"}
    class InstantSelector : public Node
    {
    public:
        const std::vector<const Node *> & getMatchers() const { return children; }
        String dumpTree(size_t indent) const override;
    };
    
    /// A range selector.
    /// Example: http_requests{job="prometheus"}[20m]
    class RangeSelector : public Node
    {
    public:
        DurationType range;
        const std::vector<const Node *> & getMatchers() const { return children; }
        String dumpTree(size_t indent) const override;
    };

    /// Specifies a change of the evaluation time for the part of the query.
    /// That includes either `@ <timestamp>` or `offset <duration>` or even both `@ <timestamp> offset <duration>`
    /// Examples: <expression> offset 5m
    ///           <expression> @ 1609746000
    ///           <expression> @ 1609746000 offset 5m
    class At : public Node
    {
    public:
        std::optional<TimeType> timestamp;  /// @ timestamp
        std::optional<DurationType> offset; /// offset <offset>
        const Node * getExpression() const { return children[0]; }
        String dumpTree(size_t indent) const override;
    };

    /// Represents a subquery, i.e. <expression>[<range>:<resolution>]. Here resolution can be omitted, but the colon always presents.
    /// Examples: <expression>[1h:5m]
    ///           <expression>[1h:]
    class Subquery : public Node
    {
    public:
        DurationType range;
        std::optional<DurationType> resolution;
        const Node * getExpression() const { return children[0]; }
        String dumpTree(size_t indent) const override;
    };

    /// A function with parameters in parentheses.
    /// Examples: abs(<argument>)
    ///           rate(<argument>)
    ///           pi()
    class Function : public Node
    {
    public:
        String function_name;
        const std::vector<const Node *> & getArguments() const { return children; }
        String dumpTree(size_t indent) const override;
    };

    /// An unary operator: either +<argument> or -<argument>.
    class UnaryOperator : public Node
    {
    public:
        String operator_name;
        const Node * getArgument() const { return children[0]; }
        String dumpTree(size_t indent) const override;
    };

    /// A binary operator: <left-argument> <operation-name> on(<on-labels>) group_left(<extra-labels>) <right-argument>
    /// Examples: foo + on(color) bar
    ///           foo + on(color) group_left bar
    class BinaryOperator : public Node
    {
    public:
        String operator_name;
        bool bool_modifier = false;
        Strings on_labels;
        Strings ignore_labels;
        bool group_left = false;
        bool group_right = false;
        Strings extra_labels;
        const Node * getLeftArgument() const { return children[0]; }
        const Node * getRightArgument() const { return children[1]; }
        String dumpTree(size_t indent) const override;
    };

    /// An aggregation operator: <operator-name> [by (<by-labels>) | without (<without-labels>)] (<arguments>)
    /// Examples: sum without (instance) (http_requests_total)
    ///           sum by (application, group) (http_requests_total)
    ///           sum(http_requests_total)
    class AggregationOperator : public Node
    {
    public:
        String operator_name;
        Strings by_labels;
        Strings without_labels;
        const std::vector<const Node *> & getArguments() const { return children; }
        String dumpTree(size_t indent) const override;
    };

    PrometheusQueryTree() = default;
    PrometheusQueryTree(const PrometheusQueryTree & src) { *this = src; }
    PrometheusQueryTree(PrometheusQueryTree && src) { *this = std::move(src); }
    PrometheusQueryTree & operator=(const PrometheusQueryTree & src);
    PrometheusQueryTree & operator=(PrometheusQueryTree && src);

    /// Parses a promql query.
    /// This function throws an exception if something is wrong with the syntax.
    static PrometheusQueryTree parse(const String & promql_query_);

    bool tryParse(const String & promql_query_);

    bool empty() const { return nodes.empty(); }
    size_t size() const { return nodes.size(); }

    /// Returns the root node.
    const Node * getRoot() const { return root; }

    /// Outputs the tree to string as a promql query.
    const String & toString() const { return promql_query; }

    /// Returns the type of the query's returning value.
    ResultType getResultType() const;

    /// Dumps the tree to string as a tree for debugging purposes.
    String dumpTree(size_t indent = 0) const;

private:
    class Builder;
    std::vector<std::unique_ptr<Node>> nodes;
    const Node * root = nullptr;
    String promql_query;
};

}
