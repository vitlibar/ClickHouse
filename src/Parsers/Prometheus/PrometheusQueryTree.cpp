#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace
{
    std::unique_ptr<PrometheusQueryTree::Node> cloneNode(const PrometheusQueryTree::Node & src)
    {
        using PQT = PrometheusQueryTree;
        switch (src.node_type)
        {
            case PQT::NodeType::ScalarLiteral:       return std::make_unique<PQT::ScalarLiteral>      (typeid_cast<const PQT::ScalarLiteral &>(src)     );
            case PQT::NodeType::StringLiteral:       return std::make_unique<PQT::StringLiteral>      (typeid_cast<const PQT::StringLiteral &>(src)     );
            case PQT::NodeType::Matcher:             return std::make_unique<PQT::Matcher>            (typeid_cast<const PQT::Matcher &>(src)           );
            case PQT::NodeType::InstantSelector:     return std::make_unique<PQT::InstantSelector>    (typeid_cast<const PQT::InstantSelector &>(src)   );
            case PQT::NodeType::RangeSelector:       return std::make_unique<PQT::RangeSelector>      (typeid_cast<const PQT::RangeSelector &>(src)     );
            case PQT::NodeType::At:                  return std::make_unique<PQT::At>                 (typeid_cast<const PQT::At &>(src)                );
            case PQT::NodeType::Subquery:            return std::make_unique<PQT::Subquery>           (typeid_cast<const PQT::Subquery &>(src)          );
            case PQT::NodeType::Function:            return std::make_unique<PQT::Function>           (typeid_cast<const PQT::Function &>(src)          );
            case PQT::NodeType::UnaryOperator:       return std::make_unique<PQT::UnaryOperator>      (typeid_cast<const PQT::UnaryOperator &>(src)     );
            case PQT::NodeType::BinaryOperator:      return std::make_unique<PQT::BinaryOperator>     (typeid_cast<const PQT::BinaryOperator &>(src)    );
            case PQT::NodeType::AggregationOperator: return std::make_unique<PQT::AggregationOperator>(typeid_cast<const PQT::AggregationOperator &>(src));
        }
    }
}

PrometheusQueryTree & PrometheusQueryTree::operator=(const PrometheusQueryTree & src)
{
    if (this == &src)
        return *this;

    String new_promql_query = src.promql_query;
    std::vector<std::unique_ptr<Node>> new_nodes;
    new_nodes.reserve(src.nodes.size());

    std::vector<std::pair<const Node *, const Node *>> src_to_new;
    src_to_new.reserve(src.nodes.size());

    for (const auto & src_node : src.nodes)
    {
        auto new_node = cloneNode(*src_node);
        src_to_new.emplace_back(src_node.get(), new_node.get());
        new_nodes.emplace_back(std::move(new_node));
    }

    std::sort(src_to_new.begin(), src_to_new.end());

    auto less = [](std::pair<const Node *, const Node *> left, const Node * right) { return left.first < right; };

    auto find_new_node = [&](const Node * src_node)
    {
        auto it = std::lower_bound(src_to_new.begin(), src_to_new.end(), src_node, less);
        if ((it != src_to_new.end()) && (it->first == src_node))
            return it->second;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistency in prometheus query tree detected");
    };

    for (const auto & new_node : new_nodes)
    {
        if (new_node->parent)
            new_node->parent = find_new_node(new_node->parent);
        for (auto *& child : new_node->children)
            child = find_new_node(child);
    }

    const Node * new_root = src.root ? find_new_node(src.root) : nullptr;

    promql_query = std::move(new_promql_query);
    nodes = std::move(new_nodes);
    root = new_root;

    return *this;
}

PrometheusQueryTree & PrometheusQueryTree::operator=(PrometheusQueryTree && src)
{
    promql_query = std::exchange(src.promql_query, {});
    nodes = std::exchange(src.nodes, {});
    root = std::exchange(src.root, nullptr);
    return *this;
}

PrometheusQueryTree::ResultType PrometheusQueryTree::getResultType() const
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Prometheus query tree shouldn't be empty");
    return root->result_type;
}


namespace
{
    constexpr const size_t NUM_SPACES_PER_INDENT = 2;

    String makeIndent(size_t indent) { return String(indent * NUM_SPACES_PER_INDENT, ' '); }

    String durationToString(PrometheusQueryTree::DurationType duration)
    {
        return fmt::format("{}s", DecimalUtils::convertTo<Float64>(duration.getValue(), duration.getScale()));
    }
}

String PrometheusQueryTree::dumpTree(size_t indent) const
{
    if (root)
        return fmt::format("{}PrometheusQueryTree (result is {})\n{}", makeIndent(indent), root->result_type, root->toString(indent + 1));
    else
        return fmt::format("{}PrometheusQueryTree: empty\n", makeIndent(indent));
}

String PrometheusQueryTree::ScalarLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}ScalarLiteral(\"{}\")\n", makeIndent(indent), scalar);
}

String PrometheusQueryTree::StringLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}StringLiteral(\"{}\")\n", makeIndent(indent), string);
}

String PrometheusQueryTree::Matcher::dumpTree(size_t indent) const
{
    return fmt::format("{}Matcher({} {} \"{}\")\n", makeIndent(indent), label_name, matcher_type, label_value);
}

String PrometheusQueryTree::InstantSelector::dumpTree(size_t indent) const
{
    const auto & matchers = getMatchers();
    String str = fmt::format("{}InstantSelector(), {} matchers{}\n",
                             makeIndent(indent), matchers.size(), (matchers.empty() ? "" : ":"));
    for (const auto * matcher : matchers)
        str += matcher->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::RangeSelector::dumpTree(size_t indent) const
{
    const auto & matchers = getMatchers();
    String str = fmt::format("{}RangeSelector(range = {}), {} matchers{}\n",
                             makeIndent(indent), durationToString(range), matchers.size(), (matchers.empty() ? "" : ":"));
    for (const auto * matcher : matchers)
        str += matcher->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::At::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "At(";
    if (timestamp)
    {
        str += fmt::format("timestamp = {}", ::DB::toString(*timestamp));
        if (offset)
            str += ", ";
    }
    if (offset)
        str += fmt::format("offset = {}", durationToString(*offset));
    str += "), expression:\n";
    str += getExpression()->dumpTree(indent + 1);    
    return str;
}

String PrometheusQueryTree::Subquery::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "Subquery(";
    str += fmt::format("range = {}", durationToString(range));

    if (resolution)
        str += fmt::format(", resolution = {}", durationToString(*resolution));

    str += "), expression:\n";
    str += getExpression()->dumpTree(indent + 1);    
    return str;
}

String PrometheusQueryTree::Function::dumpTree(size_t indent) const
{
    const auto & arguments = getArguments();
    String str = fmt::format("{}Function(name = \"{}\"): {} arguments{}\n",
                             makeIndent(indent), function_name, arguments.size(), (arguments.empty() ? "" : ":"));
    for (const auto * argument : arguments)
        str += argument->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::UnaryOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "UnaryOperator(";
    str += fmt::format("name = \"{}\"", operator_name);
    str += "), 1 argument:\n";
    str += getArgument()->dumpTree(indent + 1);
    return str;
}

String PrometheusQueryTree::BinaryOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "BinaryOperator(";
    str += fmt::format("name = \"{}\"", operator_name);

    if (bool_modifier)
        str += ", bool";

    if (!on_labels.empty())
        str += ", on [\"" + boost::algorithm::join(on_labels, "\", \"") + "\"]";

    if (!ignore_labels.empty())
        str += ", ignore [\"" + boost::algorithm::join(ignore_labels, "\", \"") + "\"]";

    if (group_left)
        str += ", group_left";

    if (group_right)
        str += ", group_right";

    if (!extra_labels.empty())
        str += ", extra_labels = [\"" + boost::algorithm::join(extra_labels, "\", \"") + "\"]";

    str += "), 2 arguments:\n";
    str += getLeftArgument()->dumpTree(indent + 1);
    str += getRightArgument()->dumpTree(indent + 1);

    return str;
}

String PrometheusQueryTree::AggregationOperator::dumpTree(size_t indent) const
{
    String str = makeIndent(indent) + "AggregationOperator(";
    str += fmt::format("name = \"{}\"", operator_name);

    if (!by_labels.empty())
        str += ", by [\"" + boost::algorithm::join(by_labels, "\", \"") + "\"]";

    if (!without_labels.empty())
        str += ", without [\"" + boost::algorithm::join(without_labels, "\", \"") + "\"]";

    const auto & arguments = getArguments();
    str += fmt::format("), {} arguments{}\n", arguments.size(), (arguments.empty() ? "" : ":"));
    for (const auto * argument : arguments)
        str += argument->dumpTree(indent + 1);

    return str;
}


namespace
{
    /// Parses a promql query using ANTLR4.
    class PromQLANTLR4ErrorListener : public antlr4::BaseErrorListener
    {
    public:
        explicit PromQLANTLR4ErrorListener(const String & promql_query_) : promql_query(promql_query_) {}

        void syntaxError(antlr4::Recognizer * /*recognizer*/, antlr4::Token * /*offending_symbol*/,
            size_t line, size_t position_in_line, const std::string & msg, std::exception_ptr /*e*/) override
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                            "Syntax error: {} while parsing PromQL query: {} (line {}, column {})",
                            msg, promql_query, line, position_in_line + 1);
        }

    private:
        String promql_query;
    };
}

class PrometheusQueryTree::Builder : public antlr4_grammars::PromQLParserBaseVisitor
{
public:
    Node * makeNode(antlr4_grammars::PromQLParser::ExpressionContext * expression) { return std::any_cast<Node *>(visit(expression)); }
    std::vector<std::unique_ptr<Node>> extractNodes() { return std::exchange(nodes, {}); }

private:
    std::vector<std::unique_ptr<Node>> nodes;

    Node * makeScalarLiteral(antlr4::ParserRuleContext * ctx)
    {
        auto new_node = std::make_unique<ScalarLiteral>();
        nodes.reserve(nodes.size() + 1);
        new_node->node_type = NodeType::ScalarLiteral;
        new_node->scalar = scalar;
        new_node->result_type = ResultType::SCALAR;
        new_node->hash = CityHash64WithSeed(&new_node->node_type, sizeof(NodeType),
                                 CityHash64(&scalar, sizeof(scalar)));
        return nodes.emplace_back(std::move(new_node)).get();
    }

    Node * makeStringLiteral(const String & string)
    {
        auto new_node = std::make_unique<StringLiteral>();
        nodes.reserve(nodes.size() + 1);
        new_node->node_type = NodeType::StringLiteral;
        new_node->string = string;
        new_node->result_type = ResultType::STRING;
        new_node->hash = CityHash64WithSeed(&new_node->node_type, sizeof(NodeType),
                                 CityHash64(string.data(), string.length()));
        return nodes.emplace_back(std::move(new_node)).get();
    }

    Node * makeUnaryOperation(const String & operation_name, Node * argument)
    {
        auto new_node = std::make_unique<UnaryOperation>();
        nodes.reserve(nodes.size() + 1);
        new_node->children.reserve(1);
        new_node->node_type = NodeType::UnaryOperation;
        new_node->operation_name = operation_name;
        new_node->result_type = argument->result_type;
        new_node->hash = CityHash64WithSeed(&new_node->node_type, sizeof(NodeType),
                         CityHash64WithSeed(operation_name.data(), operation_name.length(),
                                            argument->hash));
        new_node->children.push_back(argument);
        argument->parent = new_node.get();
        return nodes.emplace_back(std::move(new_node)).get();
    }

    Node * makeBinaryOperation(const String & operation_name, Node * left_argument, )
    {

    std::any visitVectorOperation(PromQLParser::VectorOperationContext * ctx) override
    {
        if (auto * unary_op = ctx->unaryOp())
        {
            String operator_name;
            if (unary_op->ADD())
                operator_name = "+";
            else if (unary_op->SUB())
                operator_name = "-";            
            return makeUnaryOperation(operation_name, makeNode(ctx->vectorOperation(0)));
        }
        else if (auto * pow_op = ctx->powOp())
        {
            return makeBinaryOperation("^", makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), pow_op->grouping());
        }
        else if (auto * mult_op = ctx->multOp())
        {
            String operator_name = (mult_op->MULT() ? "*" : (mult_op->DIV() ? "/" : "%"));
            return makeBinaryOperation(
                operator_name, makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), mult_op->grouping());
        }
        else if (auto * add_op = ctx->addOp())
        {
            String operator_name;
            if (add_op->ADD())
                operator_name = "+";
            else if (add_op->SUB())
                operator_name = "-";
            return makeBinaryOperation(
                operator_name, makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), add_op->grouping());
        }
        else if (auto * compare_op = ctx->compareOp())
        {
            String operator_name;
            if (compare_op->DEQ())
                operator_name = "==";
            else if (compare_op->NE())
                operator_name = "!=";
            else if (compare_op->GT())
                operator_name = ">";
            else if (compare_op->LT())
                operator_name = "<";
            else if (compare_op->GE())
                operator_name = ">=";
            else if (compare_op->LE())
                operator_name = "<=";
            bool bool_modifier = (compare_op->BOOL() != nullptr);
            return makeBinaryOperation(operator_name, makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), compare_op->grouping(), bool_modifier);
        }
        else if (auto * and_unless_op = ctx->andUnlessOp())
        {
            String operator_name;
            if (and_unless_op->AND())
                operator_name = "and";
            else if (and_unless_op->UNLESS())
                operator_name = "unless";
            return makeBinaryOperation(operator_name, makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), and_unless_op->grouping());
        }
        else if (auto * or_op = ctx->orOp())
        {
            return makeBinaryOperation("or", makeNode(ctx->vectorOperation(0)), makeNode(ctx->vectorOperation(1)), and_unless_op->grouping());
        }
        else if (auto * subquery_op = ctx->subqueryOp())
        {
            
        }
        return visitChildren(ctx);
    }












        else
        {
            auto new_node = std::make_unique<StringLiteral>();
            new_node->scalar = unquoteString();
            Node * res = new_node.get();
            nodes.emplace_back(new_node);
            return res;
        }
    }

    std::any visitInstantSelector(InstantSelectorContext * ctx) override
    {

    }
    std::any visitMatrixSelector(MatrixSelectorContext *) override { return ResultType::RANGE_VECTOR; }  
};

PrometheusQueryTree::PrometheusQueryTree(const String & promql_query_)
{
    String copied_promql_query = promql_query_;
    antlr4::ANTLRInputStream input_stream{copied_promql_query};
    PromQLANTLR4ErrorListener error_listener{copied_promql_query};

    antlr4_grammars::PromQLLexer promql_lexer{&input_stream);
    promql_lexer.removeErrorListeners();
    promql_lexer.addErrorListener(&error_listener);

    antlr4::CommonTokenStream token_stream{&promql_lexer};

    antlr4_grammars::PromQLParser promql_parser{&token_stream};
    promql_parser.removeErrorListeners();
    promql_parser.addErrorListener(&error_listener);

    auto * expression = promql_parser.expression();
    if (!expression)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Couldn't get an expression while parsing promql query: {}", promql_query_);

    Builder builder;
    Node * new_root = builder.makeNode(expression);
    std::vector<std::unique_ptr<Node>> new_nodes = builder.extractNodes();

    PrometheusQueryTree res;
    res.root = new_root;
    res.nodes = std::move(new_nodes);
    res.promql_query = promql_query_;
    return res;
}

}
