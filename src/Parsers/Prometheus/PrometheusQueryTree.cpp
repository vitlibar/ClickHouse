#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
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
    /// For example, the function converts 1000_000_000 -> 1000000000; 0x23_F_B -> 0x23FB.
    Strihg removeUnderscoresBetweenDigits(std::string_view input, bool is_hex)
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

    size_t countRemovedUnderscores(std::string_view original_input, std::string_view new_input, size_t new_pos)
    {
        size_t count = 0;
        size_t old_pos = 0;
        while ((old_pos != old_input.length()) && (old_pos - count < new_pos))
        {
            if (original_input[old_pos] == new_input[old_pos - count])
            {
                ++old_pos;
            }
            else
            {
                ++old_pos
                ++count;
            }
        }
        return count;
    }

    /// Tries to parse an unsigned scalar in hex format, for example "0x23_F_B".
    /// Underscores between digits are ignored.
    template <typename ScalarType>
    bool tryParseUnsignedScalarInHexFormat(ScalarType & result, std::string_view input, size_t & error_pos, String & error_message)
    {
        bool starts_with_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (input[1] == 'x' || input[1] == 'X');
        if (!starts_with_hex_prefix)
            return false;
        /// Remove the prefix and underscores.
        String str = removeUnderscoresBetweenDigits(input.substr(2), /* is_hex = */ true);
        const char * begin = str.data();
        const char * end = nullptr;
        errno = 0;
        auto value = std::strtoul(begin, const_cast<char **>(&end), 16);
        if (errno == ERANGE)
        {
            error_message = fmt::format("Number {} is too big", str);
            error_pos = pos + 2;
            return false;
        }
        size_t end_pos = end - begin;
        if (end_pos != str.length())
        {
            error_message = fmt::format("{} is not a hexadecimal digit", quoteString(str[end_pos]);
            error_pos = 2 + end_pos - countRemovedUnderscores(input.substr(2), end_pos, /* is_hex = */ true);
            return false;
        }
        if constexpr(std::is_same_v<ScalarType, DecimalField<DateTime64>)
        {
            result = DecimalField<DateTime64>{value * 1000, 3};
        }
        else
        {
            result = static_cast<ScalarType>(value);
        }
        return true;
    }

    /// Parses an unsigned scalar in number format, for example "1_000" or "5.67" or "2e10" or "Inf" or "Nan".
    /// Underscores between digits are ignored.
    template <typename ScalarType>
    bool tryParseUnsignedScalarInNumberFormat(ScalarType & result, std::string_view input, size_t & error_pos, String & error_message)
    {

    }


    bool isDurationFormat(std::string_view input)
    {
        size_t pos = 0;
        while (pos < input.length() && std::isdigit(input[pos]))
            ++pos;
        if (pos == 0 || pos == input.length())
            return false;
        char c = input[pos];
        return (c == 'y') || (c == 'w') || (c == 'd') || (c == 'h') || (c == 'm') || (c == 's');
    }

    /// Parses a scalar which is either a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or a time duration in the promql format (e.g. 1y2w5d13h15m30s1ms).
    /// Underscores (_) can be used in between decimal or hexadecimal digits (they don't mean anything).
    /// ScalarType here is either a floating-point type (Float64), or DecimalField<DateTime64>. 
    template <typename ScalarType>
    bool tryParseScalarLiteral(ScalarType & result, std::string_view input, bool allow_sign, size_t & error_pos, String & error_message)
    {
        if (input.empty())
        {
            error_message = "A scalar literal is expected";
            error_pos = 0;
            return false;
        }

        size_t pos = 0;

        bool negative = false;
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

        while (pos < input.length() && std::isspace(input[pos]))
            ++pos;

        String temp;

        /// Hexadecimal format (e.g. 0xA7CD).
        if (pos + 2 <= input.length() && input[pos] == '0' && (input.pos[1] == 'x' || input.pos[1] == 'X'))
        {
            std::string_view input2 = removeUnderscoresBetweenDigits(input.substr(pos + 2), temp);
            const char * begin = input2.data();
            const char * end = begin;
            errno = 0;
            auto value = std::strtoul(begin, const_cast<char **>(&end), 16);
            if (errno == ERANGE)
            {
                error_message = fmt::format("Value {} is too big", input2);
                error_pos = pos + 2;
                return false;
            }
            if (end != begin + input2.length())
            {
                error_message = fmt::format("A hexadecimal value is expected", input2);
                error_pos = (end - begin) + pos + 2;
                return false;
            }
            if constexpr(std::is_same_v<ScalarType, DecimalField<DateTime64>)
            {
                result = DecimalField<DateTime64>{value * 1000, 3};
            }
            else
            {
                result = static_cast<ScalarType>(value);
            }
            if (negative)
                result = -result;
            return true;
        }


        if (!::DB::tryParse(result, str))
        {
            error_message = fmt::format("Couldn't parse a scalar from {}", input);
            error_pos = 0;
            return false;
        }
        return true;
    }

    /// Converts a quoted string literal to its unquoted version: "abc" -> abc
    /// Accepts an input string in quotes or double quotes or backticks, and also handles escape sequences
    /// according to the promql rules (see https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals).
    bool tryParseStringLiteral(String & result, std::string_view input, size_t & error_pos, String & error_message)
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
            size_t closing_backtick = input.find('`', 1);
            if (closing_backtick == String::npos)
            {
                error_message = "No closing backtick ` found for the string literal";
                error_pos = input.length();
                return false;
            }
            if (closing_backtick < input.length() - 1)
            {
                error_message = "A string literal in backticks can't contain other backticks";
                error_pos = closing_backtick;
                return false;
            }
            result = input.substr(1, str.length() - 2);
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
            error_pos = input.length();
            return false;
        }

        std::string_view unquoted = input.substr(1, input.length() - 2);
        result.reserve(unquoted.length());

        for (size_t pos = 0; pos < unquoted.length();)
        {
            size_t next_pos = unquoted.find('\\', pos);
            result.append(unquoted.substr(pos, next_pos - pos));
            pos = next_pos;

            if (pos >= unquoted.length())
                break;

            /// Escape sequences contain at least 2 characters.
            if (pos + 2 > unquoted.length())
            {
                error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                error_pos = pos + 1;
                return false;
            }

            /// input[pos] is a backslash
            c = input[pos + 1];

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
                    if (pos + 4 > unquoted.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                        error_pos = pos + 1;
                        return false;
                    }
                    result.push_back(unhex2(&unquoted[pos + 2]));
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
                    if (pos + 4 > unquoted.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                        error_pos = pos + 1;
                        return false;
                    }
                    const char * octal = &unquoted[pos + 1];
                    UInt16 value = 0;
                    for (size_t i = 0; i != 3; ++i)
                    {
                        char c = octal[i];
                        if (c < '0' || c > '7')
                        {
                            error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                            error_pos = pos + 1;
                            return false;
                        }
                        value = value * 8 + static_cast<UInt16>(c - '0');
                    }
                    if (value >= 0xFF)  /// A three digits octal represents a single byte.
                    {
                        error_message = fmt::format("Invalid escape sequence {}: a three digit octal can't be greater than 0xFF",
                                                    unquoted.substr(pos, 4));
                        error_pos = pos + 1;
                        return false;
                    }
                    result.append(value);
                    pos += 4;
                    break;
                }
                case 'u':
                {
                    /// \u followed by exactly four hexadecimal digits represents a single Unicode code point.
                    /// Example: \u0051 is the 'Q' letter.
                    if (pos + 6 > unquoted.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                        error_pos = pos + 1;
                        return false;
                    }
                    auto code_point = unhex4(&unquoted[pos + 2]);
                    char buf[3];  /// 3 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, buf, sizeof(buf));
                    result.append(buf, num_bytes)
                    pos += 6;
                    break;
                }
                case 'U':
                {
                    /// \U followed by exactly eight hexadecimal digits represents a single Unicode code point.
                    /// Example: \U00000051 is the 'Q' letter.
                    if (pos + 10 > unquoted.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos));
                        error_pos = pos + 1;
                        return false;
                    }
                    auto code_point = unhexUInt<UInt32>(&unquoted[pos + 2]);
                    if (code_point > 0x10FFFF)  /// There should be no Unicode code point beyond 0x10FFFF.
                    {
                        error_message = fmt::format("Invalid escape sequence {}: a Unicode code point can't be greater than 0x10FFFF",
                                                    unquoted.substr(pos, 10));
                        error_pos = pos + 1;
                        return false;
                    }
                    char buf[4];  /// 4 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, buf, sizeof(buf));
                    result.append(buf, num_bytes)
                    pos += 10;
                    break;
                }
                default:
                {
                    error_message = fmt::format("Invalid escape sequence {}", unquoted.substr(pos, 2));
                    error_pos = pos + 1;
                    return false;
                }
            }
        }
        return true;
    }
}


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
        if (!tryParseScalarLiteral(scalar_literal, new_node->scalar, error_pos, error_message))
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

    std::any visitLiteral(antlr4_grammars::PromQLParser::LiteralContext * ctx) override
    {
        if (auto * number = ctx->NUMBER())
            return makeScalarLiteral(number);

        if (auto * string = ctx->STRING())
            return makeStringLiteral(string);

        return nullptr;
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
