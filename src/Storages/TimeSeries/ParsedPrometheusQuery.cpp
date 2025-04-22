#include <Storages/TimeSeries/ParsedPrometheusQuery.h>

#include "config.h"
#if USE_ANTLR4_GRAMMARS

#include <Common/logger_useful.h>
#include <Core/DecimalFunctions.h>
#include <IO/ReadHelpers.h>

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


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_PROMQL_QUERY;
}
        
/// Parses a promql query using ANTLR4.
class ParsedPrometheusQuery::PromQLParserImpl : public antlr4::BaseErrorListener
{
public:
    explicit PromQLParserImpl(const String & promql_query_)
        : promql_query(promql_query_)
    {
        input_stream = std::make_unique<antlr4::ANTLRInputStream>(promql_query);

        promql_lexer = std::make_unique<antlr4_grammars::PromQLLexer>(input_stream.get());
        promql_lexer->removeErrorListeners();
        promql_lexer->addErrorListener(this);
    
        token_stream = std::make_unique<antlr4::CommonTokenStream>(promql_lexer.get());
    
        promql_parser = std::make_unique<antlr4_grammars::PromQLParser>(token_stream.get());
        promql_parser->removeErrorListeners();
        promql_parser->addErrorListener(this);

        expression = promql_parser->expression();
        if (!expression)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Couldn't get an expression while parsing promql query: {}", promql_query);
    }

    antlr4_grammars::PromQLParser::ExpressionContext * getExpression() const { return expression; }

    antlr4_grammars::PromQLParser * getANTLR4Parser() const { return promql_parser.get(); }

private:
    String promql_query;
    std::unique_ptr<antlr4::ANTLRInputStream> input_stream;
    std::unique_ptr<antlr4_grammars::PromQLLexer> promql_lexer;
    std::unique_ptr<antlr4::CommonTokenStream> token_stream;
    std::unique_ptr<antlr4_grammars::PromQLParser> promql_parser;
    antlr4_grammars::PromQLParser::ExpressionContext * expression = nullptr;

    void syntaxError(antlr4::Recognizer * /*recognizer*/, antlr4::Token * /*offendingSymbol*/,
        size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr /*e*/) override
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY,
                        "Syntax error: {} while parsing PromQL query: {} (line {}, column {})",
                        msg, promql_query, line, charPositionInLine + 1);
    }
};


ParsedPrometheusQuery::ParsedPrometheusQuery(const String & promql_query_)
    : promql_query(promql_query_)
    , parser(std::make_unique<PromQLParserImpl>(promql_query))
    , log(getLogger("ParsedPrometheusQuery"))
{
    LOG_TRACE(log, "Parsed promql query:\n{}", getQueryTree());
    determineResultType();
}


String ParsedPrometheusQuery::getQueryTree() const
{
    return parser->getExpression()->toStringTree(parser->getANTLR4Parser(), /* pretty = */ true);
}


ParsedPrometheusQuery::~ParsedPrometheusQuery() = default;


namespace
{
    using LiteralContext = antlr4_grammars::PromQLParser::LiteralContext;
    using InstantSelectorContext = antlr4_grammars::PromQLParser::InstantSelectorContext;
    using MatrixSelectorContext = antlr4_grammars::PromQLParser::MatrixSelectorContext;
    using SubqueryOpContext = antlr4_grammars::PromQLParser::SubqueryOpContext;
    using Function_Context = antlr4_grammars::PromQLParser::Function_Context;
    using VectorOperationContext = antlr4_grammars::PromQLParser::VectorOperationContext;
    using OffsetContext = antlr4_grammars::PromQLParser::OffsetContext;
    using OffsetOpContext = antlr4_grammars::PromQLParser::OffsetOpContext;
    using LabelMatcherContext = antlr4_grammars::PromQLParser::LabelMatcherContext;

    /// Determines the result type of a promql query.
    class PromQLResultTypeGetter : public antlr4_grammars::PromQLParserBaseVisitor
    {
    public:
        using ResultType = ParsedPrometheusQuery::ResultType;

        std::optional<ResultType> getResultType(antlr4_grammars::PromQLParser::ExpressionContext * expression)
        {
            std::any result_type = visit(expression);
            if (!result_type.has_value())
                return {};
            return std::any_cast<ResultType>(result_type);
        }

    private:
        std::any aggregateResult(std::any aggregate, std::any next_result) override
        {
            return aggregate.has_value() ? aggregate : next_result;
        }

        std::any visitLiteral(LiteralContext * ctx) override { return ctx->NUMBER() ? ResultType::SCALAR : ResultType::STRING; }
        std::any visitInstantSelector(InstantSelectorContext *) override { return ResultType::INSTANT_VECTOR; }
        std::any visitMatrixSelector(MatrixSelectorContext *) override { return ResultType::RANGE_VECTOR; }

        std::any visitFunction_(Function_Context * ctx) override
        {
            String function_name = ctx->FUNCTION()->getText();
            if (function_name == "scalar")
                return ResultType::SCALAR;
            else
                return ResultType::INSTANT_VECTOR;
        }

        std::any visitVectorOperation(VectorOperationContext * ctx) override
        {
            if (ctx->subqueryOp())
                return ResultType::RANGE_VECTOR;
            else
                return visitChildren(ctx);
        }
    };
}


void ParsedPrometheusQuery::determineResultType()
{
    auto maybe_result_type = PromQLResultTypeGetter{}.getResultType(parser->getExpression());
    if (!maybe_result_type)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Couldn't get the returning type of promql query: {}", getQueryTree());
    result_type = *maybe_result_type;
    LOG_TRACE(log, "Result type: {}", result_type);
}


namespace
{
    /// Parses a duration (for example "1h20m30s") and converts it to DecimalField<DateTime64> using a specified scale.
    DecimalField<DateTime64> parseDuration(std::string_view str, UInt32 scale)
    {
        size_t pos = 0;
        while (pos < str.length() && std::isspace(str[pos]))
            ++pos;
        DecimalField<DateTime64> res{0, scale};
        size_t num_parts = 0;
        bool ok = true;
        while (pos < str.length() && !std::isspace(str[pos]))
        {
            size_t next_pos = pos;
            while (next_pos < str.length() && std::isdigit(str[next_pos]))
                ++next_pos;
            ok &= (next_pos > pos);
            if (!ok)
                break;
            Int64 value;
            ok &= tryParse(value, str.substr(pos, next_pos - pos));
            if (!ok)
                break;
            pos = next_pos;
            while (next_pos < str.length() && !std::isdigit(str[next_pos]))
                ++next_pos;
            ok &= (next_pos > pos);
            if (!ok)
                break;
            auto time_unit = str.substr(pos, next_pos - pos);
            pos = next_pos;

            DecimalField<DateTime64> add;

            if (time_unit == "ms")
                add = DecimalField<DateTime64>{(scale >= 3) ? (value * DecimalUtils::scaleMultiplier<Int64>(scale - 3))
                                                            : (value / DecimalUtils::scaleMultiplier<Int64>(3 - scale)),
                                               scale};
            else if (time_unit == "s")  /// 1s equals 1000ms
                add = DecimalField<DateTime64>{value * res.getScaleMultiplier(), scale};
            else if (time_unit == "m")  /// 1m equals 60s (ignoring leap seconds)
                add = DecimalField<DateTime64>{value * 60 * res.getScaleMultiplier(), scale};
            else if (time_unit == "h")  /// 1h equals 60m
                add = DecimalField<DateTime64>{value * 3600 * res.getScaleMultiplier(), scale};
            else if (time_unit == "d")  /// 1d equals 24h (ignoring so-called daylight saving time)
                add = DecimalField<DateTime64>{value * 86400 * res.getScaleMultiplier(), scale};
            else if (time_unit == "w")  /// 1w equals 7d
                add = DecimalField<DateTime64>{value * 604800 * res.getScaleMultiplier(), scale};
            else if (time_unit == "y")  /// 1y equals 365d (ignoring leap days)
                add = DecimalField<DateTime64>{value * 220752000 * res.getScaleMultiplier(), scale};
            else
                ok = false;

            if (!ok)
                break;

            res += add;
            ++num_parts;
        }

        while (pos < str.length() && std::isspace(str[pos]))
            ++pos;

        ok &= (num_parts > 0) && (pos == str.length());

        if (!ok)
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Could not parse duration from {}", str);

        return res;
    }

    /// Parses a time range in square brackets, for example '[1h15m]'.
    DecimalField<DateTime64> parseTimeRange(std::string_view str, UInt32 scale)
    {
        if (str.starts_with('[') && str.ends_with(']'))
            return parseDuration(str.substr(1, str.length() - 2), scale);
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Could not parse a time range from {}", str);
    }

    /// Parses a subquery range in square brackets, for example '[2d:1h]' or '[2d:]'.
    std::pair<DecimalField<DateTime64>, std::optional<DecimalField<DateTime64>>> parseSubqueryRange(std::string_view str, UInt32 scale)
    {
        size_t colon_pos = str.find(':');
        if (str.starts_with('[') && str.ends_with(']') && (colon_pos != String::npos))
        {
            size_t after_colon_pos = colon_pos + 1;
            while ((after_colon_pos < str.length() - 1) && std::isspace(str[after_colon_pos]))
                ++after_colon_pos;
            auto duration = parseDuration(str.substr(1, colon_pos - 1), scale);
            std::optional<DecimalField<DateTime64>> step;
            if (str[after_colon_pos] != ']')
                step = parseDuration(str.substr(after_colon_pos, str.length() - after_colon_pos - 1), scale);
            return {duration, step};
        }
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Could not parse a subquery range from {}", str);
    }

    /// Parses a unix time (the one which is written after @, for example "1609746000")
    /// and converts it to DecimalField<DateTime64> using a specified scale.
    DecimalField<DateTime64> parseUnixTime(std::string_view timestamp, UInt32 scale)
    {
        Float64 value;
        if (!tryParse(value, timestamp))
            throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Could not parse a unix time from {}", timestamp);
        return DecimalField<DateTime64>{static_cast<Int64>(value * DecimalUtils::scaleMultiplier<Int64>(scale)), scale};
    }

    /// Unquote and unescape a string literal.
    /// String literals can use escaping according to https://go.dev/ref/spec#String_literals
    String unquoteString(std::string_view str)
    {
        if (str.length() >= 2)
        {
            /// TODO: Add support for escaped characters.
            if (str[0] == '\'' && str[str.length() - 1] == '\'')
                return String{str.substr(1, str.length() - 2)};
            else if (str[0] == '\"' && str[str.length() - 1] == '\"')
                return String{str.substr(1, str.length() - 2)};
        }
        throw Exception(ErrorCodes::CANNOT_PARSE_PROMQL_QUERY, "Could not unquote string {}", str);
    }

    /// Finds all matchers and corresponding time ranges used in a promql query.
    class PromQLMatchersWithTimeRangesFinder : public antlr4_grammars::PromQLParserBaseVisitor
    {
    public:
        PromQLMatchersWithTimeRangesFinder(DecimalField<DateTime64> evaluation_time_,
                                          DecimalField<DateTime64> lookback_delta_,
                                          bool calculate_min_time_ = true,
                                          bool calculate_max_time_ = true)
            : evaluation_time(evaluation_time_)
            , lookback_delta(lookback_delta_)
            , scale(evaluation_time.getScale())
            , calculate_min_time(calculate_min_time_)
            , calculate_max_time(calculate_max_time_)
        {
            chassert(evaluation_time_.getScale() == lookback_delta_.getScale());
        }

        using Matcher = TimeSeriesMatchersWithTimeRanges::Matcher;
        using TimeRange = TimeSeriesMatchersWithTimeRanges::TimeRange;
        using Element = TimeSeriesMatchersWithTimeRanges::Element;
        using Elements = TimeSeriesMatchersWithTimeRanges;

        Elements findMatchersAndTimeRanges(antlr4::ParserRuleContext * ctx)
        {
            std::any res = visit(ctx);
            if (!res.has_value())
                return {};
            return std::any_cast<Elements>(res);
        }
    
    private:
        DecimalField<DateTime64> evaluation_time;
        DecimalField<DateTime64> lookback_delta;
        UInt32 scale;
        bool calculate_min_time;
        bool calculate_max_time;

        std::any aggregateResult(std::any aggregate, std::any next_result) override
        {
            if (!aggregate.has_value())
                return next_result;
            if (!next_result.has_value())
                return aggregate;
            Elements res = std::any_cast<Elements>(aggregate);
            Elements more_res = std::any_cast<Elements>(next_result);
            return res.append(more_res);
        }

        std::any visitInstantSelector(InstantSelectorContext * ctx) override
        {
            Element element;

            if (auto * metric_name = ctx->METRIC_NAME())
                element.matchers.emplace_back(Matcher{.tag_name = "__metric_name__", .tag_value = metric_name->getText(), .type = Matcher::Type::EQ});

            if (auto * label_matcher_list = ctx->labelMatcherList())
            {
                LabelMatcherContext* label_matcher = nullptr;
                for (size_t i = 0; (label_matcher = label_matcher_list->labelMatcher(i)) != nullptr; ++i)
                {
                    String tag_name = label_matcher->labelName()->getText();
                    String tag_value = unquoteString(label_matcher->STRING()->getText());
                    auto * op = label_matcher->labelMatcherOperator();
                    Matcher::Type matcher_type;
                    if (op->EQ())
                        matcher_type = Matcher::Type::EQ;
                    else if (op->NE())
                        matcher_type = Matcher::Type::NE;
                    else if (op->RE())
                        matcher_type = Matcher::Type::RE;
                    else if (op->NRE())
                        matcher_type = Matcher::Type::NRE;
                    else
                        UNREACHABLE();
                    element.matchers.emplace_back(Matcher{.tag_name = std::move(tag_name), .tag_value = std::move(tag_value), .type = matcher_type});
                }
            }

            auto & time_range = element.time_ranges.emplace_back();
            if (calculate_min_time)
                time_range.min_time = evaluation_time - lookback_delta;
            if (calculate_max_time)
                time_range.max_time = evaluation_time;

            MatchersAndTimeRanges res;
            res.emplace_back(element);
            return res;
        }

        std::any visitMatrixSelector(MatrixSelectorContext * ctx) override
        {
            auto res = std::any_cast<MatchersAndTimeRanges>(visit(ctx->instantSelector()));
            if (calculate_min_time)
            {
                DecimalField<DateTime64> duration = parseTimeRange(ctx->TIME_RANGE()->getText(), scale);
                chassert(res.size() == 1 && res[0].time_ranges.size() == 1);
                res[0].time_ranges[0].time_min_time = evaluation_time - duration;
            }
            return res;
        }

        std::any visitOffset(OffsetContext * ctx) override
        {
            if (!calculate_min_time && !calculate_max_time)
                return visitChildren(ctx);

            auto new_evaluation_time = evaluation_time;
            if (auto * offset_op = ctx->offsetOp())
                new_evaluation_time = getEvaluationTimeWithOffset(offset_op);

            if (auto * instant_selector = ctx->instantSelector())
                return PromQLMatchersAndTimeRangesFinder{new_evaluation_time, lookback_delta, calculate_min_time, calculate_max_time}.findMatchersAndTimeRanges(instant_selector);
            else if (auto * matrix_selector = ctx->matrixSelector())
                return PromQLMatchersAndTimeRangesFinder{new_evaluation_time, lookback_delta, calculate_min_time, calculate_max_time}.findMatchersAndTimeRanges(matrix_selector);
            else
                UNREACHABLE();
        }

        DecimalField<DateTime64> getEvaluationTimeWithOffset(OffsetOpContext * offset_op) const
        {
            auto new_evaluation_time = evaluation_time;

            if (auto * literal = offset_op->literal())
                new_evaluation_time = parseUnixTime(literal->getText(), scale);

            if (auto * duration = offset_op->DURATION())
            {
                /// A negative offset enables comparisons forward in time.
                if (offset_op->SUB())
                    new_evaluation_time += parseDuration(duration->getText(), scale);
                else
                    new_evaluation_time -= parseDuration(duration->getText(), scale);
            }
            return new_evaluation_time;
        }

        std::any visitVectorOperation(VectorOperationContext * ctx) override
        {
            if (ctx->subqueryOp())
            {
                if (!calculate_min_time && !calculate_max_time)
                    return visitChildren(ctx);

                auto new_evaluation_time = evaluation_time;
                if (auto * offset_op = ctx->subqueryOp()->offsetOp())
                    new_evaluation_time = getEvaluationTimeWithOffset(offset_op);

                MatchersAndTimeRanges res_min, res_max;
                if (calculate_min_time)
                {
                    auto subquery_range = parseSubqueryRange(ctx->subqueryOp()->SUBQUERY_RANGE()->getText(), scale).first;
                    res_min = PromQLMatchersAndTimeRangesFinder{new_evaluation_time - subquery_range, lookback_delta, true, false}.findMatchersAndTimeRanges(ctx->vectorOperation(0));
                }

                if (calculate_max_time)
                    res_max = PromQLMatchersAndTimeRangesFinder{new_evaluation_time, lookback_delta, false, true}.findMatchersAndTimeRanges(ctx->vectorOperation(0));

                MatchersAndTimeRanges res;
                if (res_min.empty())
                {
                    res = std::move(res_max);
                }
                else if (res_max.empty())
                {
                    res = std::move(res_min);
                }
                else
                {
                    chassert(res_min.size() == res_max.size());
                    res = std::move(res_max);
                    for (size_t i = 0; i != res.size(); ++i)
                        for (size_t j = 0; j != res[i].time_ranges.size(); ++j)
                            res[i].min_time = res_min[i].min_time;
                }
                return res;
            }
            else
            {
                return visitChildren(ctx);
            }
        }
    };
}

TimeSeriesMatchersWithTimeRanges ParsedPrometheusQuery::findMatchersWithTimeRanges(DecimalField<DateTime64> evaluation_time, UInt64 lookback_delta_ms) const
{
    UInt32 scale = evaluation_time.getScale();
    DecimalField<DateTime64> lookback_delta{(scale >= 3) ? (lookback_delta_ms * DecimalUtils::scaleMultiplier<Int64>(scale - 3))
                                                         : (lookback_delta_ms / DecimalUtils::scaleMultiplier<Int64>(3 - scale)),
                                            scale};

    auto res = PromQLMatchersAndTimeRangesFinder{evaluation_time, lookback_delta}.findMatchersAndTimeRanges(parser->getExpression());
    res.compactTimeRanges();

    LOG_TRACE(log, "Found matchers and time ranges:\n{}", toString(res));

    return res;
}

}

#endif
