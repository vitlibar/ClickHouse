#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/UTF8Helpers.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <IO/readDecimalText.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryResultType.h>
#include <boost/algorithm/string/join.hpp>

#include "config.h"

#if USE_ANTLR4_GRAMMARS
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wdocumentation-deprecated-sync"
#pragma clang diagnostic ignored "-Wdocumentation-html"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Winconsistent-missing-destructor-override"
#pragma clang diagnostic ignored "-Wshadow-field"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wsuggest-destructor-override"
#include <antlr4_grammars/PromQLLexer.h>
#include <antlr4_grammars/PromQLParser.h>
#include <antlr4_grammars/PromQLParserBaseVisitor.h>
#pragma clang diagnostic pop

#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

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
        return fmt::format("{}PrometheusQueryTree (result is {})\n{}", makeIndent(indent), root->result_type, root->dumpTree(indent + 1));
    else
        return fmt::format("{}PrometheusQueryTree: empty\n", makeIndent(indent));
}

String PrometheusQueryTree::ScalarLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}ScalarLiteral({})\n", makeIndent(indent), scalar);
}

String PrometheusQueryTree::StringLiteral::dumpTree(size_t indent) const
{
    return fmt::format("{}StringLiteral({})\n", makeIndent(indent), quoteString(string));
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
    /// Finds next underscore between two digits (or two hexadecimal digits if `is_hex` is true).
    /// The function returns String::npos if not found,
    size_t findUnderscoreBetweenDigits(std::string_view str, bool is_hex, size_t start_pos)
    {
        chassert(start_pos <= str.length());
        size_t pos = str.find('_', start_pos);
        while (pos != String::npos)
        {
            if ((1 <= pos) && (pos + 2 <= str.length()))
            {
                char before = str[pos - 1];
                char after = str[pos + 1];
                bool between_digits = is_hex ? (std::isxdigit(before) && std::isxdigit(after)) : (std::isdigit(before) && std::isdigit(after));
                if (between_digits)
                    break;
            }
            pos = str.find('_', pos + 1);
        }
        return pos;
    }

    /// Removes all underscores between digits (or two hexadecimal digits if `is_hex` is true).
    /// For example, the function converts "1000_000_000" to "1000000000", "0x23_F_B" to "0x23FB" (with is_hex == true).
    String removeUnderscoresBetweenDigits(std::string_view input, bool is_hex)
    {
        String result;
        result.reserve(input.length());
        size_t pos = 0;
        while (pos != input.length())
        {
            size_t underscore_pos = findUnderscoreBetweenDigits(input, is_hex, pos);
            if (underscore_pos == String::npos)
            {
                result.append(input.substr(pos));
                break;
            }
            result.append(input.substr(pos, underscore_pos - pos));
            pos = underscore_pos + 1;
        }
        return result;
    }

    /// Tries to parse an unsigned scalar in hex format, for example "0x23_F_B".
    /// The function recognizes prefixes "0x" and "0X" and ignores underscores between digits.
    /// If it succeeds the function returns true and sets `result`.
    /// If it fails the function returns false and sets either `allow_other_formats` or `error_pos` & `error_message`.
    template <typename ScalarType>
    bool tryParseUnsignedScalarInHexFormat(std::string_view input, ScalarType & result,
                                           bool & allow_other_formats, size_t & error_pos, String & error_message)
    {
        bool found_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (std::tolower(input[1]) == 'x');
        if (!found_hex_prefix)
        {
            /// No prefix "0x" is in the `input`, but we can still try other scalar formats.
            allow_other_formats = true;
            return false;
        }
        /// Remove prefix "0x" and underscores between digits.
        std::string_view input_without_prefix = input.substr(2);
        String str = removeUnderscoresBetweenDigits(input_without_prefix, /* is_hex = */ true);
        /// Parse hexadecimal number.
        Int64 value;
        if (!tryParseInt</* base = 16 */>(value, str))
        {
            error_message = fmt::format("Couldn't parse a hexadecimal number from {}", quoteString(input_without_prefix));
            error_pos = 2;
            allow_other_formats = false;
            return false;
        }
        if constexpr(std::is_same_v<ScalarType, DecimalField<DateTime64>>)
        {
            DateTime64 duration_ms;
            if (!DecimalUtils::tryRescale(DateTime64{value}, 0, 3, duration_ms))
            {
                error_message = fmt::format("Number {} is too big", input_without_prefix);
                error_pos = 2;
                allow_other_formats = false;
                return false;
            }
            result = DecimalField<DateTime64>{duration_ms, 3};
        }
        else
        {
            result = static_cast<ScalarType>(value);
        }
        return true;
    }

    /// Tries to parse an unsigned scalar in duration format, for example "1y2w5d13h15m30s1ms".
    /// If it succeeds the function returns true and sets `result`.
    /// If it fails the function returns false and sets either `allow_other_formats` or `error_pos` & `error_message`.
    template <typename ScalarType>
    bool tryParseUnsignedScalarInDurationFormat(std::string_view input, ScalarType & result,
                                                bool & allow_other_formats, size_t & error_pos, String & error_message)
    {
        bool found_time_unit = (input.find_first_of("ywdhms") != String::npos);
        if (!found_time_unit)
        {
            /// No time units are in the `input`, but we can still try other scalar formats.
            allow_other_formats = true;
            return false;
        }
        Int64 result_ms = 0;
        /// Iterate through all {number & time unit} pairs.
        size_t pos = 0;
        std::string_view previous_unit;
        Int64 previous_unit_ms = 0;
        while (pos != input.length())
        {
            size_t number_start_pos = pos;
            while (pos != input.length() && std::isdigit(input[pos]))
                ++pos;
            if (pos == number_start_pos)
            {
                error_message = fmt::format("{} is not a digit. Expected a decimal integer number combined with a time unit in duration {}",
                                            quoteString(input.substr(number_start_pos, 1)), quoteString(input));
                error_pos = number_start_pos;
                allow_other_formats = false;
                return false;
            }
            Int64 number = 0;
            std::string_view number_as_str = input.substr(number_start_pos, pos - number_start_pos);
            if(!::DB::tryParse(number, number_as_str))
            {
                error_message = fmt::format("Too big number {} of time units in duration {}", number_as_str, quoteString(input));
                error_pos = number_start_pos;
                allow_other_formats = false;
                return false;
            }
            size_t unit_start_pos = 0;
            while (pos != input.length() && !std::isdigit(input[pos]))
                ++pos;
            std::string_view unit = input.substr(unit_start_pos, pos - unit_start_pos);
            Int64 unit_ms = 0;
            if (unit == "y")
                unit_ms = 365ULL * 24 * 60 * 60 * 1000;  /// 1y equals 365d (ignoring leap days)
            else if (unit == "w")
                unit_ms = 7 * 24 * 60 * 60 * 1000;  /// 1w equals 7d
            else if (unit == "d")
                unit_ms = 24 * 60 * 60 * 1000;  /// 1d equals 24h
            else if (unit == "h")
                unit_ms = 60 * 60 * 1000;  /// 1h equals 60m
            else if (unit == "m")
                unit_ms = 60 * 1000;  /// 1m equals 60s
            else if (unit == "s")
                unit_ms = 1000;  /// 1s equals 1000ms
            else if (unit == "ms")
                unit_ms = 1;  /// milliseconds
            if (!unit_ms)
            {
                error_message = fmt::format("Unknown unit {} in duration {}", quoteString(unit), quoteString(input));
                error_pos = unit_start_pos;
                allow_other_formats = false;
                return false;
            }
            if (!previous_unit.empty() && (previous_unit_ms <= unit_ms))
            {
                error_message = fmt::format("Units must be ordered from the longest to the shortest: '{}' must appear before '{}'. "
                                            "Wrong order of units in duration {}",
                                            unit, previous_unit, quoteString(input));
                error_pos = unit_start_pos;
                allow_other_formats = false;
                return false;
            }
            Int64 add_ms = 0;
            bool overflow = common::mulOverflow(number, unit_ms, add_ms) || common::addOverflow(add_ms, result_ms, result_ms);
            if (overflow)
            {
                error_message = fmt::format("Duration {} is too big", quoteString(input));
                error_pos = number_start_pos;
                allow_other_formats = false;
                return false;
            }
            previous_unit = unit;
        }
        /// There should be at least one number with a time unit.
        if (previous_unit.empty())
        {
            error_message = fmt::format("Expected a decimal integer number combined with a time unit in duration {}", quoteString(input));
            error_pos = pos;
            allow_other_formats = false;
            return false;
        }
        if constexpr(std::is_same_v<ScalarType, DecimalField<DateTime64>>)
            result = DecimalField<DateTime64>{result_ms, 3};
        else
            result = static_cast<ScalarType>(result_ms) / 1000;
        return true;
    }


    /// Parses an unsigned scalar in number format, for example "1000" or "1_000" or "5.67" or "2e10" or "Inf" or "Nan".
    /// Underscores between digits are ignored.
    template <typename ScalarType>
    bool tryParseUnsignedScalarInNumberFormat(std::string_view input, ScalarType & result,
                                              size_t & error_pos, String & error_message)
    {
        /// Remove underscores between digits if necessary.
        String str = removeUnderscoresBetweenDigits(input, /* is_hex = */ false);
        if constexpr(std::is_same_v<ScalarType, DecimalField<DateTime64>>)
        {
            DateTime64 value;
            UInt32 default_precision = std::numeric_limits<Int64>::digits10;
            UInt32 scale;
            if (!tryParseDecimal(str, value, default_precision, scale))
            {
                error_message = fmt::format("Couldn't parse a duration from {} ", quoteString(input));
                error_pos = 0;
                return false;
            }
            DateTime64 value_ms;
            if (!DecimalUtils::tryRescale(value, scale, 3, value_ms))
            {
                error_message = fmt::format("Duration {} is too big", quoteString(input));
                error_pos = 0;
                return false;
            }
            result = DecimalField<DateTime64>{value_ms, 3};
            return true;
        }
        else
        {
            if (!tryParse(result, str))
            {
                error_message = fmt::format("Couldn't parse a scalar from {}", quoteString(input));
                error_pos = 0;
                return false;
            }
            return true;
        }
    }

    /// Parses a scalar which is either a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or a time duration in the promql format (e.g. 1y2w5d13h15m30s1ms).
    /// Underscores (_) can be used in between decimal or hexadecimal digits (they don't mean anything).
    /// ScalarType here is either a floating-point type (Float64), or DecimalField<DateTime64>. 
    template <typename ScalarType>
    bool tryParseScalarLiteral(std::string_view input, ScalarType & result, bool allow_sign, size_t & error_pos, String & error_message)
    {
        /// Parse a sign.
        size_t pos = 0;
        bool negative = false;
        if (!input.empty())
        {
            if (allow_sign)
            {
                if (input[0] == '+')
                {
                    ++pos;
                }
                else if (input[0] == '-')
                {
                    negative = true;
                    ++pos;
                }
            }
            while (pos != input.length() && std::isspace(input[pos]))
                ++pos;
        }
        /// Parse an unsigned number in one of three formats.
        bool allow_other_formats = false;
        bool ok = tryParseUnsignedScalarInHexFormat(input.substr(pos), result, allow_other_formats, error_pos, error_message);

        if (!ok && allow_other_formats)
            ok = tryParseUnsignedScalarInDurationFormat(input.substr(pos), result, allow_other_formats, error_pos, error_message);

        if (!ok)
            ok = tryParseUnsignedScalarInNumberFormat(input.substr(pos), result, error_pos, error_message);

        if (!ok)
        {
            chassert(!error_message.empty());
            error_pos += pos;
            return false;
        }

        if (negative)
            result = -result;
        return true;
    }

    /// Parses a time range as how it's used in a range selector: "[1h30m]".
    bool tryParseTimeRange(std::string_view input, DecimalField<DateTime64> & range, size_t & error_pos, String & error_message)
    {
        /// Check opening and closing brackets.
        if (input.empty() || (input[0] != '['))
        {
            error_message = "Time range should start with an opening bracket [";
            error_pos = 0;
            return false;
        }
        if (input.length() < 2 || (input[input.length() - 1] != ']'))
        {
            error_message = "Time range should end with a closing bracket ]";
            error_pos = input.length() - 1;
            return false;
        }
        /// Skip spaces.
        size_t start_pos = 1;
        while (start_pos != input.length() && std::isspace(start_pos))
        {
            ++start_pos;
        }
        size_t end_pos = input.length() - 1;
        while (end_pos != start_pos && std::isspace(end_pos - 1))
        {
            --end_pos;
        }
        /// Parse a scalar between the brackets.
        std::string_view range_as_str = input.substr(start_pos, end_pos - start_pos);
        if (!tryParseScalarLiteral(range_as_str, range, /* allow_sign = */ false, error_pos, error_message))
        {
            error_pos += start_pos;
            return false;
        }
        return true;
    }

    /// Parses a time range as how it's used in a subquery: "[1h:5m]" or "[1h:]".
    bool tryParseSubqueryRangeImpl(std::string_view input,
                                   std::pair<DecimalField<DateTime64>, std::optional<DecimalField<DateTime64>>> & range_and_resolution,
                                   size_t & error_pos, String & error_message)
    {
        /// Check opening and closing brackets.
        if (input.empty() || (input[0] != '['))
        {
            error_message = "Subquery range should start with an opening bracket [";
            error_pos = 0;
            return false;
        }
        if (input.length() < 2 || (input[input.length() - 1] != ']'))
        {
            error_message = "Subquery range should end with a closing bracket ]";
            error_pos = input.length() - 1;
            return false;
        }
        /// Find a colon between the brackets.
        size_t colon_pos = input.find(':', 1);
        if (colon_pos == String::npos)
        {
            error_message = "Not found colon : in the subquery range";
            error_pos = input.length() - 1;
            return false;
        }
        /// Skip spaces.
        size_t range_start_pos = 1;
        while (range_start_pos != input.length() && std::isspace(range_start_pos))
        {
            ++range_start_pos;
        }
        size_t range_end_pos = colon_pos;
        while (range_end_pos != range_start_pos && std::isspace(range_end_pos - 1))
        {
            --range_end_pos;
        }
        size_t resolution_start_pos = colon_pos + 1;
        while (resolution_start_pos != input.length() && std::isspace(resolution_start_pos))
        {
            ++resolution_start_pos;
        }
        size_t resolution_end_pos = input.length() - 1;
        while (resolution_end_pos != resolution_start_pos && std::isspace(resolution_end_pos - 1))
        {
            --resolution_end_pos;
        }
        /// Parse a scalar between the brackets and the colon.
        std::string_view range_as_str = input.substr(range_start_pos, range_end_pos - range_start_pos);
        std::string_view resolution_as_str = input.substr(resolution_start_pos, resolution_end_pos - resolution_start_pos);
        DecimalField<DateTime64> range;
        std::optional<DecimalField<DateTime64>> resolution;
        if (!tryParseScalarLiteral(range_as_str, range, /* allow_sign = */ false, error_pos, error_message))
        {
            error_pos += range_start_pos;
            return false;
        }
        if (!resolution_as_str.empty() && !tryParseScalarLiteral(resolution_as_str, resolution.emplace(), /* allow_sign = */ false, error_pos, error_message))
        {
            error_pos += resolution_start_pos;
            return false;
        }
        range_and_resolution = std::make_pair(range, resolution);
        return true;
    }

    /// Parses escape sequences in a string literal and replaces them with the characters which they mean.
    bool tryUnescapeStringLiteral(std::string_view input, String & result, size_t & error_pos, String & error_message)
    {
        result.clear();
        result.reserve(input.length());

        for (size_t pos = 0; pos < input.length();)
        {
            size_t next_pos = input.find('\\', pos);
            result.append(input.substr(pos, next_pos - pos));
            pos = next_pos;

            if (pos >= input.length())
                break;

            /// An escape sequences contains at least 2 characters.
            if (pos + 2 > input.length())
            {
                error_message = fmt::format("Invalid escape sequence {}", input.substr(pos));
                error_pos = pos;
                return false;
            }

            /// input[pos] is a backslash
            char c = input[pos + 1];

            switch (c)
            {
                case 'a':  result.push_back(0x07); pos += 2; break;  /// \a  U+0007 alert or bell
                case 'b':  result.push_back(0x08); pos += 2; break;  /// \b  U+0008 backspace
                case 'f':  result.push_back(0x0C); pos += 2; break;  /// \f  U+000C form feed
                case 'n':  result.push_back(0x0A); pos += 2; break;  /// \n  U+000A line feed or newline
                case 'r':  result.push_back(0x0D); pos += 2; break;  /// \r  U+000D carriage return
                case 't':  result.push_back(0x09); pos += 2; break;  /// \t  U+0009 horizontal tab
                case 'v':  result.push_back(0x0B); pos += 2; break;  /// \v  U+000B vertical tab
                case '\\': result.push_back('\''); pos += 2; break;  /// \\  U+005C backslash
                case '\'': result.push_back('\''); pos += 2; break;  /// \'  U+0027 single quote
                case '"':  result.push_back('"');  pos += 2; break;  /// \"  U+0022 double quote
                case 'x': 
                {
                    /// \x followed by exactly two hexadecimal digits represents a single byte.
                    /// Example: \x51 is the 'Q' letter.
                    if (pos + 4 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos));
                        error_pos = pos;
                        return false;
                    }
                    char byte;
                    if (!tryParseInt</* base = */ 16>(byte, input.substr(pos + 2, 2)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos, 4));
                        error_pos = pos;
                        return false;
                    }
                    result.push_back(byte);
                    pos += 4;
                    break;
                }
                case '0': [[fallthrough]];
                case '1': [[fallthrough]];
                case '2': [[fallthrough]];
                case '3': [[fallthrough]];
                case '4': [[fallthrough]];
                case '5': [[fallthrough]];
                case '6': [[fallthrough]];
                case '7':
                {
                    /// \nnn - three digits octal represents a single byte.
                    /// Example: \121 is the 'Q' letter.
                    if (pos + 4 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos));
                        error_pos = pos;
                        return false;
                    }
                    UInt16 byte;
                    if (!tryParseInt</* base = */ 8>(byte, input.substr(pos + 1, 3)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos, 4));
                        error_pos = pos;
                        return false;
                    }
                    if (byte > 0xFF)
                    {
                        error_message = fmt::format("Invalid escape sequence {}: an octal representation \nnn must represent a single byte", input.substr(pos, 4));
                        error_pos = pos;
                        return false;
                    }
                    result.push_back(static_cast<char>(byte));
                    pos += 4;
                    break;
                }
                case 'u':
                {
                    /// \u followed by exactly four hexadecimal digits represents a single Unicode code point.
                    /// Example: \u0051 is the 'Q' letter.
                    if (pos + 6 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos));
                        error_pos = pos;
                        return false;
                    }
                    UInt16 code_point;
                    if (!tryParseInt</* base = */ 16>(code_point, input.substr(pos + 2, 4)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos, 6));
                        error_pos = pos;
                        return false;
                    }
                    char bytes[3];  /// 3 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    result.append(bytes, num_bytes);
                    pos += 6;
                    break;
                }
                case 'U':
                {
                    /// \U followed by exactly eight hexadecimal digits represents a single Unicode code point.
                    /// Example: \U00000051 is the 'Q' letter.
                    if (pos + 10 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos));
                        error_pos = pos;
                        return false;
                    }
                    UInt32 code_point;
                    if (!tryParseInt</* base = */ 16>(code_point, input.substr(pos + 2, 8)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}", input.substr(pos, 10));
                        error_pos = pos;
                        return false;
                    }
                    if (code_point > 0x10FFFF)  /// There should be no Unicode code point beyond 0x10FFFF.
                    {
                        error_message = fmt::format("Invalid escape sequence {}: a Unicode code point can't be greater than 0x10FFFF",
                                                    input.substr(pos, 10));
                        error_pos = pos;
                        return false;
                    }
                    char bytes[4];  /// 4 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    result.append(bytes, num_bytes);
                    pos += 10;
                    break;
                }
                default:
                {
                    error_message = fmt::format("Invalid escape sequence {}", input.substr(pos, 2));
                    error_pos = pos;
                    return false;
                }
            }
        }
        return true;
    }

    /// Converts a quoted string literal to its unquoted version: "abc" -> abc
    /// Accepts an input string in quotes or double quotes or backticks, and also handles escape sequences
    /// according to the promql rules (see https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals).
    bool tryParseStringLiteral(std::string_view input, String & result, size_t & error_pos, String & error_message)
    {
        if (input.empty())
        {
            error_message = "A string literal should open with a quote ', a double quote \" or a backtick `";
            error_pos = 0;
            return false;
        }

        char quote_char = input[0];

        /// A string literal enclosed in backticks: escape sequences are not parsed.
        if (quote_char == '`')
        {
            if ((input.length() < 2) || (input[input.length() - 1] != '`'))
            {
                error_message = "No closing backtick ` found for the string literal";
                error_pos = input.length() - 1;
                return false;
            }
            size_t closing_backtick = input.find('`', 1);
            if (closing_backtick != input.length() - 1)
            {
                error_message = "A string literal in backticks can't contain other backticks";
                error_pos = closing_backtick;
                return false;
            }
            result = input.substr(1, input.length() - 2);
            return true;
        }

        /// A string literal enclosed in quotes or double quotes: escape sequences are parsed.
        if (!((quote_char == '\'') || (quote_char == '\"')))
        {
            error_message = "A string literal should open with a quote ', a double quote \" or a backtick `";
            error_pos = 0;
            return false;
        }

        if ((input.length() < 2) || (input[input.length() - 1] != quote_char))
        {
            error_message = fmt::format("No closing {} {} found for the string literal",
                                        (quote_char == '\'' ? "quote" : "double quote"), quote_char);
            error_pos = input.length() - 1;
            return false;
        }

        std::string_view unquoted = input.substr(1, input.length() - 2);

        /// Parse escape sequences.
        if (!tryUnescapeStringLiteral(unquoted, result, error_pos, error_message))
        {
            ++error_pos; /// Skip a quote at the beginning of the `input`.
            return false;
        }

        return true;
    }
}


#if USE_ANTLR4_GRAMMARS

/// Parses a promql query using ANTLR4.
class PrometheusQueryTree::ErrorListener : public antlr4::BaseErrorListener
{
public:
    explicit ErrorListener(const String & promql_query_) : promql_query(promql_query_) {}

    void setError(size_t error_pos_, const String & error_message_)
    {
        /// Only the first error is interesting.
        if (error_message.empty() && !error_message_.empty())
        {
            error_pos = error_pos_;
            error_message = error_message_;
        }
    }

    size_t getErrorPos() const { return error_pos; }
    const String & getErrorMessage() const { return error_message; }

protected:
    void syntaxError(antlr4::Recognizer * /* recognizer */, antlr4::Token * offending_symbol,
        size_t line, size_t position_in_line, const std::string & msg, std::exception_ptr /* exception */) override
    {
        chassert(!msg.empty());

        size_t pos;
        if (offending_symbol)
            pos = offending_symbol->getStartIndex();
        else  /// `offending_symbol` can be null if `recognizer` is a lexer.
            pos = convertLineAndPositionInLine(line, position_in_line);

        setError(pos, msg);
    }

    /// ANTLR4's lexer returns the position of an error as a line number and a position in that line;
    /// we need to convert them to a char index.
    size_t convertLineAndPositionInLine(size_t line, size_t position_in_line) const
    {
        size_t char_index = 0;
        if (line != 1)
        {
            size_t cur_line = 1;
            while (char_index != promql_query.length())
            {
                char c = promql_query[char_index++];
                /// ANTLR4 considers only '\n' as end-of-line (see LexerATNSimulator::consume()).
                if (c == '\n')
                {
                    if (++cur_line == line)
                        break;
                }
            }
        }
        return std::max(char_index + position_in_line, promql_query.length());
    }

private:
    [[maybe_unused]] const String & promql_query;
    size_t error_pos = String::npos;
    String error_message;
};


class PrometheusQueryTree::Builder : public antlr4_grammars::PromQLParserBaseVisitor
{
public:
    explicit Builder(const String & promql_query_, ErrorListener & error_listener_)
        : promql_query(promql_query_), error_listener(error_listener_) {}

    Node * makeNode(antlr4_grammars::PromQLParser::ExpressionContext * expression)
    {
        std::any any = visit(expression);
        return any.has_value() ? std::any_cast<Node *>(any) : nullptr;
    }

    std::vector<std::unique_ptr<Node>> extractNodes() { return std::exchange(nodes, {}); }

private:
    const String & promql_query;
    ErrorListener & error_listener;
    std::vector<std::unique_ptr<Node>> nodes;

    std::any aggregateResult(std::any aggregate, std::any next_result) override
    {
        if (!aggregate.has_value())
            return next_result;
        if (!next_result.has_value())
            return aggregate;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't aggregate\n {} and\n{}",
                        std::any_cast<Node *>(aggregate)->dumpTree(1),
                        std::any_cast<Node *>(next_result)->dumpTree(1));
    }


    Node * makeScalarLiteral(antlr4::tree::TerminalNode * ctx)
    {
        auto new_node = std::make_unique<ScalarLiteral>();
        nodes.reserve(nodes.size() + 1);
        new_node->node_type = NodeType::ScalarLiteral;
        new_node->result_type = ResultType::SCALAR;
        new_node->start_pos = ctx->getSymbol()->getStartIndex();
        new_node->length = ctx->getSymbol()->getStopIndex() - new_node->start_pos + 1;
        std::string_view scalar_literal = std::string_view{promql_query}.substr(new_node->start_pos, new_node->length);
        size_t error_pos = String::npos;
        String error_message;
        if (!tryParseScalarLiteral(scalar_literal, new_node->scalar, /* allow_sign = */ true, error_pos, error_message))
        {
            error_listener.setError(new_node->start_pos + error_pos, error_message);
            return nullptr;
        }
        return nodes.emplace_back(std::move(new_node)).get();
    }

    Node * makeStringLiteral(antlr4::tree::TerminalNode * ctx)
    {
        auto new_node = std::make_unique<StringLiteral>();
        nodes.reserve(nodes.size() + 1);
        new_node->node_type = NodeType::StringLiteral;
        new_node->result_type = ResultType::STRING;
        new_node->start_pos = ctx->getSymbol()->getStartIndex();
        new_node->length = ctx->getSymbol()->getStopIndex() - new_node->start_pos + 1;
        std::string_view string_literal = std::string_view{promql_query}.substr(new_node->start_pos, new_node->length);
        size_t error_pos = String::npos;
        String error_message;
        if (!tryParseStringLiteral(string_literal, new_node->string, error_pos, error_message))
        {
            error_listener.setError(new_node->start_pos + error_pos, error_message);
            return nullptr;
        }
        return nodes.emplace_back(std::move(new_node)).get();
    }

    Node * makeMatcher(PromQLParser::LabelMatcherContext * ctx)
    {
        auto new_node = std::make_unique<Matcher>();
        nodes.reserve(nodes.size() + 1);
        new_node->node_type = NodeType::Matcher;
        new_node->result_type = ResultType::SCALAR;
        new_node->start_pos = ctx->start->getStartIndex();
        new_node->length = ctx->stop->getStopIndex() - new_node->start_pos + 1;
        new_node->label_name = ctx->labelName()->getText();
        size_t error_pos = String::npos;
        String error_message;
        if (!tryParseStringLiteral(ctx->STRING(), new_node->label_value, error_pos, error_message))
        {
            error_listener.setError(new_node->start_pos + error_pos, error_message);
            return nullptr;
        }
        auto * op = ctx->labelMatcherOperator();
        if (op->EQ())
            new_node->matcher_type = MatcherType::EQ;
        else if (op->NE())
            new_node->matcher_type = MatcherType::NE;
        else if (op->RE())
            new_node->matcher_type = MatcherType::RE;
        else if (op->NRE())
            new_node->matcher_type = MatcherType::NRE;
        return nodes.emplace_back(std::move(new_node)).get();
    }

    std::any visitLiteral(antlr4_grammars::PromQLParser::LiteralContext * ctx) override
    {
        if (auto * scalar = ctx->SCALAR())
            return makeScalarLiteral(scalar);

        if (auto * string = ctx->STRING())
            return makeStringLiteral(string);

        return nullptr;
    }

    std::any visitLabelMatcher(PromQLParser::LabelMatcherContext * ctx) override
    {
        return makeMatcher(ctx);
    }

#if 0
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

    void syntaxError(antlr4::Recognizer * /*recognizer*/, antlr4::Token * /*offending_symbol*/,
        size_t line, size_t position_in_line, const std::string & msg, std::exception_ptr /*e*/) override
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                        "Syntax error: {} while parsing PromQL query: {} (line {}, column {})",
                        msg, promql_query, line, position_in_line + 1);
    }

#endif

};

#endif

bool PrometheusQueryTree::tryParse(const String & promql_query_, size_t & error_pos, String & error_message)
{
#if USE_ANTLR4_GRAMMARS
    ErrorListener error_listener{promql_query_};
    antlr4::ANTLRInputStream input_stream{promql_query_};

    antlr4_grammars::PromQLLexer promql_lexer{&input_stream};
    promql_lexer.removeErrorListeners();
    promql_lexer.addErrorListener(&error_listener);

    antlr4::CommonTokenStream token_stream{&promql_lexer};

    antlr4_grammars::PromQLParser promql_parser{&token_stream};
    promql_parser.removeErrorListeners();
    promql_parser.addErrorListener(&error_listener);

    auto * expression = promql_parser.expression();
    if (!expression)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Couldn't get an expression while parsing promql query: {}", promql_query_);

    Builder builder{promql_query_, error_listener};
    Node * new_root = builder.makeNode(expression);

    error_pos = error_listener.getErrorPos();
    error_message = error_listener.getErrorMessage();

    if (!error_message.empty())
        return false;

    if (!new_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parsing promql query '{}' failed without setting any error message", promql_query_);

    PrometheusQueryTree res;
    res.promql_query = promql_query_;
    res.nodes = builder.extractNodes();
    res.root = new_root;
    *this = std::move(res);
    return true;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ANTLR4 support is disabled");
#endif
}

void PrometheusQueryTree::parse(const String & promql_query_)
{
    size_t error_pos;
    String error_message;
    if (!tryParse(promql_query_, error_pos, error_message))
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                        "{} at position {} while parsing promql query: {}",
                        error_message, error_pos, promql_query_);
    }
}

}
