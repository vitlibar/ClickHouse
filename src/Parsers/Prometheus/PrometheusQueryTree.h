#pragma once

#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>


namespace DB
{
enum class PrometheusQueryResultType;

/// Base class for class PrometheusQueryTree.
class PrometheusQueryTreeBase
{
public:
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
        AggregationOperator,
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
        virtual Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const = 0;
        virtual String dumpTree(size_t indent) const = 0;
    };

    /// A scalar literal, i.e. a number or a duration.
    /// Examples: -2.43, 2h30m
    template <typename ValueType>
    class ScalarLiteral : public Node
    {
    public:
        ValueType scalar;
        ScalarLiteral() { node_type = NodeType::ScalarLiteral; result_type = ResultType::SCALAR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A string literal.
    /// Example: "abc"
    class StringLiteral : public Node
    {
    public:
        String string;
        StringLiteral() { node_type = NodeType::StringLiteral; result_type = ResultType::STRING; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
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
        Matcher() { node_type = NodeType::Matcher; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    template <typename TimestampType, typename IntervalType>
    class Selector : public Node
    {
    public:
        std::optional<TimestampType> at;  /// @ timestamp
        IntervalType offset;              /// offset <offset>
        const std::vector<const Node *> & getMatchers() const { return children; }
    };

    /// An instant selector with an optional offset.
    /// Example: http_requests{job="prometheus"} offset 1d
    template <typename TimestampType, typename IntervalType>
    class InstantSelector : public Selector<TimestampType, IntervalType>
    {
    public:
        InstantSelector() { this->node_type = NodeType::InstantSelector; this->result_type = ResultType::INSTANT_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A range selector with an optional offset.
    /// Example: http_requests{job="prometheus"}[20m] offset 1d
    template <typename TimestampType, typename IntervalType>
    class RangeSelector : public Selector<TimestampType, IntervalType>
    {
    public:
        IntervalType range; /// [20m]
        RangeSelector() { this->node_type = NodeType::RangeSelector; this->result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// Represents a subquery, i.e. <expression>[<range>:<resolution>]. Here resolution can be omitted, but the colon always presents.
    /// Also an optional offset can be specified.
    /// Examples: <expression>[1h:5m]
    ///           <expression>[1h:] offset 1d
    ///           <expression>[1h:] @ 1609746000
    template <typename TimestampType, typename IntervalType>
    class Subquery : public Node
    {
    public:
        IntervalType range;                     /// [1h: ...]
        std::optional<IntervalType> resolution; /// [... :5m]
        std::optional<TimestampType> at;        /// @ timestamp
        IntervalType offset;                    /// offset <offset>
        const Node * getExpression() const { return children[0]; }
        Subquery() { this->node_type = NodeType::Subquery; this->result_type = ResultType::RANGE_VECTOR; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
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
        Function() { node_type = NodeType::Function; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    class Operator : public Node
    {
    public:
        String operator_name;
    };

    /// An unary operator: either +<argument> or -<argument>.
    class UnaryOperator : public Operator
    {
    public:
        const Node * getArgument() const { return children[0]; }
        UnaryOperator() { node_type = NodeType::UnaryOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// A binary operator: <left-argument> <operation-name> on(<on-labels>) group_left(<extra-labels>) <right-argument>
    /// Examples: foo + on(color) bar
    ///           foo + on(color) group_left bar
    class BinaryOperator : public Operator
    {
    public:
        bool on = false;
        bool ignoring = false;
        Strings labels;
        bool group_left = false;
        bool group_right = false;
        Strings extra_labels;
        bool bool_modifier = false;
        const Node * getLeftArgument() const { return children[0]; }
        const Node * getRightArgument() const { return children[1]; }
        BinaryOperator() { node_type = NodeType::BinaryOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    /// An aggregation operator: <operator-name> [by (<by-labels>) | without (<without-labels>)] (<arguments>)
    /// Examples: sum without (instance) (http_requests_total)
    ///           sum by (application, group) (http_requests_total)
    ///           sum(http_requests_total)
    class AggregationOperator : public Operator
    {
    public:
        bool by = false;
        bool without = false;
        Strings labels;
        const std::vector<const Node *> & getArguments() const { return children; }
        AggregationOperator() { node_type = NodeType::AggregationOperator; }
        Node * clone(std::vector<std::unique_ptr<Node>> & node_list_) const override;
        String dumpTree(size_t indent) const override;
    };

    PrometheusQueryTreeBase() = default;
    PrometheusQueryTreeBase(const PrometheusQueryTreeBase & src) { *this = src; }
    PrometheusQueryTreeBase(PrometheusQueryTreeBase && src) { *this = std::move(src); }
    PrometheusQueryTreeBase & operator=(const PrometheusQueryTreeBase & src);
    PrometheusQueryTreeBase & operator=(PrometheusQueryTreeBase && src);

    /// Constructs a PrometheusQueryTree from a prepared list of nodes.
    PrometheusQueryTreeBase(const String & promql_query_, const Node * root_, std::vector<std::unique_ptr<Node>> node_list_);

    bool empty() const { return node_list.empty(); }
    size_t size() const { return node_list.size(); }

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
    String promql_query;
    const Node * root = nullptr;
    std::vector<std::unique_ptr<Node>> node_list;
};


/// A tree representing a parsed prometheus query.
template <typename TimestampType, typename ValueType>
class PrometheusQueryTree : public PrometheusQueryTreeBase
{
public:
    using IntervalType = std::conditional_t<TimestampType, DecimalField<DateTime64>, DecimalField<Decimal64>, TimestampType>;

    using ScalarLiteral = PrometheusQueryTreeBase::ScalarLiteral<ValueType>;
    using Selector = PrometheusQueryTreeBase::Selector<TimestampType, IntervalType>;
    using InstantSelector = PrometheusQueryTreeBase::InstantSelector<TimestampType, IntervalType>;
    using RangeSelector = PrometheusQueryTreeBase::RangeSelector<TimestampType, IntervalType>;
    using Subquery = PrometheusQueryTreeBase::Subquery<TimestampType, IntervalType>;
 
    using PrometheusQueryTreeBase::PrometheusQueryTreeBase;

    /// Parses a promql query.
    explicit PrometheusQueryTree(const String & promql_query_) { parse(promql_query_); }

    /// Parses a promql query.
    /// This function throws an exception if something is wrong with the syntax.
    void parse(const String & promql_query_);

    /// Tries to parse a promql query. Returns true if successful.
    /// If it isn't successful the function sets `error_pos` and `error_message` and returns false.
    bool tryParse(const String & promql_query_, String * error_message, size_t * error_pos);
};

}
