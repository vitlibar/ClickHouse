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
        size_t start_pos = String::npos; /// Start position of the promql query's part which this node represents with its children.
        size_t length = 0;               /// Length of the promql query's part which this node represents with its children.
        ResultType result_type;          /// The data type this node with its children evaluates to.
        std::vector<const Node *> children;  /// E.g. arguments for a function, matchers for selectors.
        const Node * parent = nullptr;
        Node() = default;
        Node(const Node &) = default;
        virtual ~Node() = default;
        virtual String dumpTree(size_t indent) const = 0;
    };

    /// A scalar literal, i.e. a number or a duration.
    /// Examples: -2.43, 2h30m
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

    /// An instant selector with an optional offset.
    /// Example: http_requests{job="prometheus"} offset 1d
    class InstantSelector : public Node
    {
    public:
        std::optional<TimeType> at;  /// @ timestamp
        DurationType offset;         /// offset <offset>
        const std::vector<const Node *> & getMatchers() const { return children; }
        String dumpTree(size_t indent) const override;
    };
    
    /// A range selector with an optional offset.
    /// Example: http_requests{job="prometheus"}[20m] offset 1d
    class RangeSelector : public Node
    {
    public:
        DurationType range;          /// [20m]
        std::optional<TimeType> at;  /// @ timestamp
        DurationType offset;         /// offset <offset>
        const std::vector<const Node *> & getMatchers() const { return children; }
        String dumpTree(size_t indent) const override;
    };

    /// Represents a subquery, i.e. <expression>[<range>:<resolution>]. Here resolution can be omitted, but the colon always presents.
    /// Also an optional offset can be specified.
    /// Examples: <expression>[1h:5m]
    ///           <expression>[1h:] offset 1d
    ///           <expression>[1h:] @ 1609746000
    class Subquery : public Node
    {
    public:
        DurationType range;                      /// [1h: ...]
        std::optional<DurationType> resolution;  /// [... :5m]
        std::optional<TimeType> at;              /// @ timestamp
        DurationType offset;                     /// offset <offset>
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
        bool on = false;
        bool ignoring = false;
        Strings labels;
        bool group_left = false;
        bool group_right = false;
        Strings extra_labels;
        bool bool_modifier = false;
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
        bool by = false;
        bool without = false;
        Strings labels;
        const std::vector<const Node *> & getArguments() const { return children; }
        String dumpTree(size_t indent) const override;
    };

    PrometheusQueryTree() = default;
    PrometheusQueryTree(const PrometheusQueryTree & src) { *this = src; }
    PrometheusQueryTree(PrometheusQueryTree && src) { *this = std::move(src); }
    PrometheusQueryTree & operator=(const PrometheusQueryTree & src);
    PrometheusQueryTree & operator=(PrometheusQueryTree && src);

    explicit PrometheusQueryTree(const String & promql_query_) { parse(promql_query_); }

    /// Parses a promql query.
    /// This function throws an exception if something is wrong with the syntax.
    void parse(const String & promql_query_);

    /// Tries to parse a promql query. Returns true if successful.
    /// If it isn't successful the function sets `error_pos` and `error_message` and returns false.
    bool tryParse(const String & promql_query_, size_t & error_pos, String & error_message);

    bool empty() const { return nodes.empty(); }
    size_t size() const { return nodes.size(); }

    /// Returns the root node.
    const Node * getRoot() const { return root; }

    /// Returns the promql query which was parsed to build this tree.
    const String & getPromQLQuery() const { return promql_query; }

    /// Returns a part of the promql query corresponding to a specific node of this tree.
    std::string_view getPromQL(const Node * node) const { return std::string_view{getPromQLQuery()}.substr(node->start_pos, node->length); }

    /// Returns the type of the query's returning value.
    ResultType getResultType() const;

    /// Dumps the tree to string as a tree for debugging purposes.
    String dumpTree(size_t indent = 0) const;

private:
    class ErrorListener;
    class Builder;
    std::vector<std::unique_ptr<Node>> nodes;
    const Node * root = nullptr;
    String promql_query;
};

}
