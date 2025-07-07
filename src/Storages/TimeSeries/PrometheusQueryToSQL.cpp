#include <Storages/TimeSeries/PrometheusQueryToSQL.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EMPTY_QUERY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    using ResultType = PrometheusQueryResultType;

    /// Finds an interval data type corresponding to a specified timestamp data type.
    /// We support only DateTime64, DateTime and UInt32 as types to specify time.
    /// For them we use Decimal64 and Int32 to specify intervals.
    DataTypePtr getIntervalDataType(const DataTypePtr & timestamp_data_type)
    {
        switch (WhichDataType{*timestamp_data_type}.idx)
        {
            case TypeIndex::UInt32: // nobreak
            case TypeIndex::DateTime:
                return std::make_shared<DataTypeInt32>();
            case TypeIndex::DateTime64:
                return std::make_shared<DataTypeDecimal64>(getDecimalPrecision(*timestamp_data_type), getDecimalScale(*timestamp_data_type));
            default:
                break;
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find an interval type for timestamp type {}", timestamp_data_type);
    }

    /// Casts a timestamp or an interval to a specific data type.
    Field castToType(const Field & field, const DataTypePtr & target_data_type)
    {
        auto field_type = field.getType();
        switch (WhichDataType{*target_data_type}.idx)
        {
            case TypeIndex::Int32:
            {
                if (field_type == Field::Types::Int64)
                    return field;
                else if (field_type == Field::Types::UInt64)
                    return static_cast<Int64>(field.safeGet<UInt64>());
                else if (field_type == Field::Types::Float64)
                    return static_cast<Int64>(field.safeGet<Float64>());
                else if (field_type == Field::Types::Decimal32)
                    return static_cast<Int64>(static_cast<Float64>(field.safeGet<Decimal32>()));
                else if (field_type == Field::Types::Decimal64)
                    return static_cast<Int64>(static_cast<Float64>(field.safeGet<Decimal64>()));
                break;
            }
            case TypeIndex::UInt32: /// nobreak
            case TypeIndex::DateTime:
            {
                if (field_type == Field::Types::UInt64)
                    return field;
                else if (field_type == Field::Types::Int64)
                    return static_cast<UInt64>(field.safeGet<Int64>());
                else if (field_type == Field::Types::Float64)
                    return static_cast<UInt64>(field.safeGet<Float64>());
                else if (field_type == Field::Types::Decimal32)
                    return static_cast<UInt64>(static_cast<Float64>(field.safeGet<Decimal32>()));
                else if (field_type == Field::Types::Decimal64)
                    return static_cast<UInt64>(static_cast<Float64>(field.safeGet<Decimal64>()));
                break;
            }
            case TypeIndex::DateTime64: /// nobreak
            case TypeIndex::Decimal64:
            {
                UInt32 target_scale = getDecimalScale(*target_data_type);
                if (field_type == Field::Types::UInt64)
                    return DecimalField<Decimal64>{field.safeGet<UInt64>() * DecimalUtils::scaleMultiplier<Decimal64>(target_scale), target_scale};
                else if (field_type == Field::Types::Int64)
                    return DecimalField<Decimal64>{field.safeGet<Int64>() * DecimalUtils::scaleMultiplier<Decimal64>(target_scale), target_scale};
                else if (field_type == Field::Types::Float64)
                    return DecimalField<Decimal64>{static_cast<Int64>(field.safeGet<Float64>() * DecimalUtils::scaleMultiplier<Decimal64>(target_scale)), target_scale};
                else if (field_type == Field::Types::Decimal32)
                {
                    auto x = field.safeGet<Decimal32>();
                    return DecimalField<Decimal64>{DecimalUtils::convertTo<Decimal64>(target_scale, x.getValue(), x.getScale()), target_scale};
                }
                else if (field_type == Field::Types::Decimal64)
                {
                    auto x = field.safeGet<Decimal64>();
                    return DecimalField<Decimal64>{DecimalUtils::convertTo<Decimal64>(target_scale, x.getValue(), x.getScale()), target_scale};
                }
                break;
            }
            default:
                break;
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot cast field of type {} to data type {}", field_type, target_data_type);
    }

    /// Converts a timestamp or an interval to AST.
    ASTPtr fieldToAST(const Field & field, const DataTypePtr & data_type)
    {
        auto data_type_idx = WhichDataType{*data_type}.idx;
        if (data_type_idx == TypeIndex::DateTime64)
            return makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(toString(field)), std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));
        else if (data_type_idx == TypeIndex::Decimal64)
            return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(toString(field)), std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));
        else
            return std::make_shared<ASTLiteral>(field);
    }

    /// Adds a time interval to another time interval or to a timestamp.
    static Field add(const Field & left, const Field & right)
    {
        if (left.getType() == right.getType())
        {
            switch (left.getType())
            {
                case Field::Types::Int64: return left.safeGet<Int64>() + right.safeGet<Int64>();
                case Field::Types::UInt64: return left.safeGet<UInt64>() + right.safeGet<UInt64>();
                case Field::Types::Decimal64: { auto sum = left.safeGet<Decimal64>(); sum += right.safeGet<Decimal64>(); return sum; }
                default: break;
            }
        }
        else if ((left.getType() == Field::Types::UInt64) && (right.getType() == Field::Types::Int64))
        {
            return static_cast<UInt64>(left.safeGet<UInt64>() + right.safeGet<Int64>());
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot add {} and {}", left.getType(), right.getType());
    }

    /// Subtract a time interval from another time interval or from a timestamp.
    static Field subtract(const Field & left, const Field & right)
    {
        if (left.getType() == right.getType())
        {
            switch (left.getType())
            {
                case Field::Types::Int64: return left.safeGet<Int64>() - right.safeGet<Int64>();
                case Field::Types::UInt64: return left.safeGet<UInt64>() - right.safeGet<UInt64>();
                case Field::Types::Decimal64: { auto diff = left.safeGet<Decimal64>(); diff -= right.safeGet<Decimal64>(); return diff; }
                default: break;
            }
        }
        else if ((left.getType() == Field::Types::UInt64) && (right.getType() == Field::Types::Int64))
        {
            return static_cast<UInt64>(left.safeGet<UInt64>() - right.safeGet<Int64>());
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot subtract {} from {}", right.getType(), left.getType());
    }

    /// Helper template for function alignStartTimeAndEndTime().
    template <typename TimestampType, typename IntervalType>
    bool alignStartTimeAndEndTimeTemplate(Field & start_time, Field & end_time, const Field & step)
    {
        TimestampType start_time_value = start_time.safeGet<TimestampType>();
        TimestampType end_time_value = end_time.safeGet<TimestampType>();
        IntervalType step_value = step.safeGet<IntervalType>();

        auto x = start_time_value;
        x %= step_value;
        if (x)
        {
            start_time_value += step_value;
            start_time_value -= x;
        }

        auto y = end_time_value;
        y %= step_value;
        end_time_value -= y;

        if (start_time_value > end_time_value)
            return false;

        start_time = start_time_value;
        end_time = end_time_value;
        return true;
    }

    template <typename TimestampType, typename IntervalType>
    bool alignStartTimeAndEndTimeTemplate2(Field & start_time, Field & end_time, const Field & step)
    {
        TimestampType start_time_value = start_time.safeGet<TimestampType>();
        TimestampType end_time_value = end_time.safeGet<TimestampType>();
        IntervalType step_value = step.safeGet<IntervalType>();

        LOG_INFO(getLogger("!!!"), "start scale = {}", start_time_value.getScale());

        auto x = start_time_value;
        x %= step_value;
        if (x)
        {
            start_time_value += step_value;
            start_time_value -= x;
        }

        LOG_INFO(getLogger("!!!"), "start scale = {}", start_time_value.getScale());

        auto y = end_time_value;
        y %= step_value;
        end_time_value -= y;

        if (start_time_value > end_time_value)
            return false;

        start_time = start_time_value;
        end_time = end_time_value;
        return true;
    }

    /// Increases the start time by some value to make it divisible by `step`.
    /// Decreases the end time by some value to make it divisible by `step`.
    /// If after that still `start_time <= end_time` then the function returns true.
    bool alignStartTimeAndEndTime(Field & start_time, Field & end_time, const Field & step)
    {
        if ((start_time.getType() == end_time.getType()) && (start_time.getType() == step.getType()))
        {
            switch (start_time.getType())
            {
                case Field::Types::Int64: return alignStartTimeAndEndTimeTemplate<Int64, Int64>(start_time, end_time, step);
                case Field::Types::UInt64: return alignStartTimeAndEndTimeTemplate<UInt64, UInt64>(start_time, end_time, step);
                case Field::Types::Decimal64: return alignStartTimeAndEndTimeTemplate2<DecimalField<Decimal64>, DecimalField<Decimal64>>(start_time, end_time, step);
                default: break;
            }
        }
        else if ((start_time.getType() == end_time.getType()) && (start_time.getType() == Field::Types::UInt64) && (step.getType() == Field::Types::Int64))
        {
            return alignStartTimeAndEndTimeTemplate<UInt64, Int64>(start_time, end_time, step);
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot align start time by step because the combination of types is not supported: start_time: {}, end_time: {}, step: {}",
                        start_time.getType(), step.getType(), end_time.getType());
    }
}


/// Builder of an AST query to evaluate a promql query.
class PrometheusQueryToSQLConverter::ASTBuilder
{
public:
    ASTBuilder(const PrometheusQueryToSQLConverter & converter_)
        : converter(converter_)
        , timestamp_data_type(getTimeSeriesTableInfo().timestamp_data_type)
        , interval_data_type(getIntervalDataType(timestamp_data_type))
        , value_data_type(getTimeSeriesTableInfo().value_data_type)
    {
    }

    ASTPtr getSQL()
    {
        auto * root_node = getPromQLTree().getRoot();
        if (!root_node)
            throw Exception(ErrorCodes::EMPTY_QUERY, "Can't evaluate an empty prometheus query.");
        return toAST(finalize(buildPiece(root_node)));
    }

private:
    const PrometheusQueryToSQLConverter & converter;
    DataTypePtr timestamp_data_type;
    DataTypePtr interval_data_type;
    DataTypePtr value_data_type;

    const PrometheusQueryTree & getPromQLTree() const { return converter.promql; }
    std::string_view getPromQLText(const PrometheusQueryTree::Node * node) const { return getPromQLTree().getQuery(node); }
    const TimeSeriesTableInfo & getTimeSeriesTableInfo() const { return converter.time_series_table_info; }
    Field getEvaluationTime() const { return converter.evaluation_time; }
    Field getLookbackDelta() const { return converter.lookback_delta; }
    Field getDefaultResolution() const { return converter.default_resolution; }

    using NodeType = PrometheusQueryTree::NodeType;

    /// Represents a SELECT query built for a node in a prometheus query tree.
    /// [WITH ...] SELECT ... FROM ... [GROUP BY ...] [WHERE ...]
    struct Piece
    {
        /// Result of the query.
        ResultType result_type;

        /// A window is extracted from a range selector. The window is used only by functions accepting range vectors, e.g. rate().
        Field window;

        /// Columns to select (nullptr if there is no such column).
        /// The names of these columns are always TimeSeriesColumnNames::Group, TimeSeriesColumnNames::Tags and so on.
        ASTPtr group_column;
        ASTPtr tags_column;
        ASTPtr timestamp_column;
        ASTPtr value_column;
        ASTPtr time_series_column;
        ASTPtr scalar_column;
        ASTPtr string_column;

        /// Whether the "timestamp" column and the "value" column are columns of arrays.
        bool timestamp_column_is_array = false;
        bool value_column_is_array = false;

        size_t num_columns() const
        {
            return (group_column != nullptr) + (tags_column != nullptr) + (timestamp_column != nullptr)
                + (value_column != nullptr) + (time_series_column != nullptr) + (scalar_column != nullptr) + (string_column != nullptr);
        }

        bool empty () const { return num_columns() == 0; }

        /// The "FROM" expression when it's a table function. or the temporary table name denoting a subquery.
        ASTPtr from_table_function;

        /// The "FROM" expression when it's a temporary table name denoting a subquery.
        String from_subquery;

        /// The "GROUP BY" expression.
        ASTs group_by;

        ASTPtr where;
        std::vector<std::pair<String, ASTPtr>> with;
    };

    /// List of collected subqueries.
    /// At the end of the process when we finalize the prepared SELECT query we add such collected subqueries to the "WITH" clause of it.
    std::vector<std::pair<String, Piece>> subqueries;

    /// Adds a piece to the list of collected subqueries.
    /// Returns a generated temporary table name for the new subquery.
    String addSubquery(Piece && piece)
    {
        String name = fmt::format("prom{}", subqueries.size() + 1);
        subqueries.emplace_back(name, std::move(piece));
        return name;
    }

    /// Converts a Piece to AST.
    static ASTPtr toAST(const Piece & piece)
    {
        chassert(!piece.empty());
        auto select_query = std::make_shared<ASTSelectQuery>();

        auto select_list_exp = std::make_shared<ASTExpressionList>();
        auto & select_list = select_list_exp->children;
        if (piece.group_column)
            select_list.push_back(piece.group_column);
        if (piece.tags_column)
            select_list.push_back(piece.tags_column);
        if (piece.timestamp_column)
            select_list.push_back(piece.timestamp_column);
        if (piece.value_column)
            select_list.push_back(piece.value_column);
        if (piece.time_series_column)
            select_list.push_back(piece.time_series_column);
        if (piece.scalar_column)
            select_list.push_back(piece.scalar_column);
        if (piece.string_column)
            select_list.push_back(piece.string_column);
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

        if (!piece.from_subquery.empty() || piece.from_table_function)
        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            if (!piece.from_subquery.empty())
            {
                table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(piece.from_subquery);
                table_exp->children.emplace_back(table_exp->database_and_table_name);
            }
            else if (piece.from_table_function)
            {
                table_exp->table_function = piece.from_table_function;
                table_exp->children.emplace_back(table_exp->table_function);
            }
            table->table_expression = table_exp;
            tables->children.push_back(table);
            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        if (!piece.group_by.empty())
        {
            auto group_by_list = std::make_shared<ASTExpressionList>();
            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by_list);
            group_by_list->children = piece.group_by;
        }

        if (piece.where)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, piece.where->clone());

        if (!piece.with.empty())
        {
            auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
            for (const auto & [name, ast] : piece.with)
            {
                auto with_element_ast = std::make_shared<ASTWithElement>();
                with_element_ast->name = name;
                with_element_ast->subquery = std::make_shared<ASTSubquery>(ast);
                with_element_ast->children.push_back(with_element_ast->subquery);
                with_expression_list_ast->children.push_back(std::move(with_element_ast));
            }
            select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
        }

        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        auto list_of_selects = std::make_shared<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->list_of_selects = list_of_selects;
        select_with_union_query->children.push_back(list_of_selects);

        return select_with_union_query;
    }

    /// Finalizes a Piece built to evaluate a prometheus query.
    Piece finalize(Piece && piece)
    {
        Piece res;

        /// Finalize depending on the result type.
        switch (piece.result_type)
        {
            case ResultType::STRING: res = finalizeWithStringResult(std::move(piece)); break;
            case ResultType::SCALAR: res = finalizeWithScalarResult(std::move(piece)); break;
            case ResultType::INSTANT_VECTOR: res = finalizeWithInstantVectorResult(std::move(piece)); break;
            case ResultType::RANGE_VECTOR: res = finalizeWithRangeVectorResult(std::move(piece)); break;
        }

        /// Add the collected subqueries to the WITH clause of the final query.
        if (!subqueries.empty())
        {
            res.with.reserve(subqueries.size());
            for (const auto & [name, piece_for_subquery] : subqueries)
                res.with.emplace_back(name, toAST(piece_for_subquery));
        }

        return res;
    }

    /// Finalizes a Piece returning a string.
    Piece finalizeWithStringResult(Piece && piece)
    {
        if (piece.string_column && piece.num_columns() == 1)
            return piece;

        Piece res;
        res.result_type = piece.result_type;
        res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(fmt::format("{} String", TimeSeriesColumnNames::String)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning a scalar.
    Piece finalizeWithScalarResult(Piece && piece)
    {
        if (piece.scalar_column && (piece.num_columns() == 1))
            return piece;

        Piece res;
        res.result_type = piece.result_type;
        res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(fmt::format("{} {}", TimeSeriesColumnNames::Scalar, getTimeSeriesTableInfo().value_data_type)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning an instant vector.
    Piece finalizeWithInstantVectorResult(Piece && piece)
    {
        if (piece.tags_column && piece.timestamp_column && piece.value_column && (piece.num_columns() == 3))
            return piece;

        Piece res;
        res.result_type = piece.result_type;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                            TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            res.tags_column = makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
            res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.timestamp_column_is_array || piece.value_column_is_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} are not expected to be arrays", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value);

        if (piece.timestamp_column && piece.value_column)
        {
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        }
        else if (piece.time_series_column)
        {
            res.where = makeASTFunction("notEmpty", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
            auto array_element = makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                                 std::make_shared<ASTLiteral>(Field{1u}));
            res.timestamp_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{1u}));
            res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
            res.value_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{2u}));
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns ({} and {}) or {} while building an SQL query", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value, TimeSeriesColumnNames::TimeSeries);
        }

        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Finalizes a Piece returning a range vector.
    Piece finalizeWithRangeVectorResult(Piece && piece)
    {
        if (piece.tags_column && piece.time_series_column && (piece.num_columns() == 2))
            return piece;

        Piece res;
        res.result_type = piece.result_type;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::TimeSeries,
                            getTimeSeriesTableInfo().timestamp_data_type, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            res.tags_column = makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
            res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.timestamp_column_is_array || piece.value_column_is_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} are not expected to be arrays", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value);

        if (piece.time_series_column)
        {
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
        }
        else if (piece.timestamp_column && piece.value_column)
        {
            res.time_series_column = makeASTFunction("timeSeriesGroupArraySorted",
                                                     std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                     std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
            res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
            res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns ({} and {}) or {} while building an SQL query", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value, TimeSeriesColumnNames::TimeSeries);
        }

        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Builds a piece to evaluate a node in a prometheus query tree.
    Piece buildPiece(const PrometheusQueryTree::Node * node)
    {
        auto node_type = node->node_type;
        switch (node_type)
        {
            case NodeType::InstantSelector:
                return buildPieceForInstantSelector(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));

            case NodeType::RangeSelector:
                return buildPieceForRangeSelector(typeid_cast<const PrometheusQueryTree::RangeSelector *>(node));

            case NodeType::Subquery:
                return buildPieceForSubquery(typeid_cast<const PrometheusQueryTree::Subquery *>(node));

            case NodeType::Function:
                return buildPieceForFunction(typeid_cast<const PrometheusQueryTree::Function *>(node));

            case NodeType::BinaryOperator:
                return buildPieceForBinaryOperator(typeid_cast<const PrometheusQueryTree::BinaryOperator *>(node));

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query tree node type {} is not implemented", node_type);
        }
    }

    /// Builds an empty piece.
    static Piece getEmptyPiece(ResultType result_type)
    {
        Piece empty;
        empty.result_type = result_type;
        return empty;
    }

    /// Builds a piece to evaluate an instant selector.
    Piece buildPieceForInstantSelector(const PrometheusQueryTree::InstantSelector * instant_selector) const
    {
        Field window = castToIntervalType(getLookbackDelta());

        Field start_time, end_time, step;
        extractRangeAndStep(instant_selector, start_time, end_time, step);

        LOG_INFO(getLogger("!!!"), "start_time = {}, scale = {}", start_time, start_time.safeGet<DateTime64>().getScale());
        LOG_INFO(getLogger("!!!"), "end_time = {}, scale = {}", end_time, end_time.safeGet<DateTime64>().getScale());

        if (start_time == end_time)
            step = getDummyStep();
        else if (!alignStartTimeAndEndTime(start_time, end_time, step))
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        LOG_INFO(getLogger("!!!"), "aligned_start_time = {}, scale = {}", start_time, start_time.safeGet<DateTime64>().getScale());
        LOG_INFO(getLogger("!!!"), "aligned_end_time = {}, scale = {}", end_time, end_time.safeGet<DateTime64>().getScale());

        LOG_INFO(getLogger("!!!"), "window = {}, scale = {}", window, window.safeGet<Decimal64>().getScale());

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            std::make_shared<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(subtract(start_time, window)),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.time_series_column = makeGridFunction("timeSeriesLastToGrid", start_time, end_time, step, window, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.result_type = ResultType::INSTANT_VECTOR;

        return res;
    }

    /// Builds a piece to evaluate a range selector.
    Piece buildPieceForRangeSelector(const PrometheusQueryTree::RangeSelector * range_selector) const
    {
        const auto * instant_selector = range_selector->getInstantSelector();
        Field window = castToIntervalType(nodeToField(range_selector->getRange()));

        Field start_time, end_time, step;
        extractRangeAndStep(range_selector, start_time, end_time, step);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            std::make_shared<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(subtract(start_time, window)),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.result_type = ResultType::RANGE_VECTOR;
        res.window = window;

        return res;
    }

    /// Builds a piece for a subquery.
    Piece buildPieceForSubquery(const PrometheusQueryTree::Subquery * subquery)
    {
        auto piece = buildPiece(subquery->getExpression());

        if (piece.empty())
            return getEmptyPiece(ResultType::RANGE_VECTOR);

        piece.result_type = ResultType::RANGE_VECTOR;
        piece.window = castToIntervalType(nodeToField(subquery->getRange()));
        return piece;
    }

    /// Builds a piece to evaluate a function.
    Piece buildPieceForFunction(const PrometheusQueryTree::Function * func)
    {
        const auto & function_name = func->function_name;
        std::vector<Piece> args = buildPiecesForArguments(func);

        if (function_name == "rate" || function_name == "irate" || function_name == "delta" || function_name == "idelta" || function_name == "last_over_time")
            return buildPieceForRangeFunction(func, std::move(args));

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);
    }

    /// Checks the number of arguments of a promql function.
    static void checkNumberArguments(const PrometheusQueryTree::Function * func, const std::vector<Piece> & arguments, size_t expected)
    {
        if (arguments.size() != expected)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires {} arguments, got {}",
                            func->function_name, expected, arguments.size());
    }

    /// Checks the type of an argument of a promql function.
    static void checkArgumentType(const PrometheusQueryTree::Function * func, const std::vector<Piece> & arguments, size_t index, ResultType expected)
    {
        if (arguments.at(index).result_type != expected)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} must be {}, got {}",
                index + 1, func->function_name, expected, arguments.at(index).result_type);
    }

    /// Builds pieces to evaluate the arguments of a function.
    std::vector<Piece> buildPiecesForArguments(const PrometheusQueryTree::Function * func)
    {
        std::vector<Piece> res;
        res.reserve(func->getArguments().size());
        for (const auto * argument : func->getArguments())
            res.push_back(buildPiece(argument));
        return res;
    }

    /// Builds a piece to evaluate a range function, i.e. a function accepting a range vector and returning an instant vector.
    Piece buildPieceForRangeFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::RANGE_VECTOR);

        auto & argument = arguments[0];

        if (argument.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        std::string_view grid_function_name;
        if (func->function_name == "rate")
            grid_function_name = "timeSeriesRateToGrid";
        else if (func->function_name == "irate")
            grid_function_name = "timeSeriesInstantRateToGrid";
        else if (func->function_name == "delta")
            grid_function_name = "timeSeriesDeltaToGrid";
        else if (func->function_name == "idelta")
            grid_function_name = "timeSeriesInstantDeltaToGrid";
        else if (func->function_name == "last_over_time")
            grid_function_name = "timeSeriesLastToGrid";
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);

        Field window = argument.window;

        Field start_time, end_time, step;
        extractRangeAndStep(func, start_time, end_time, step);

        if (start_time == end_time)
            step = getDummyStep();
        else if (!alignStartTimeAndEndTime(start_time, end_time, step))
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.time_series_column = makeGridFunction(grid_function_name, start_time, end_time, step, window, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoArrays(std::move(argument)));
        return res;
    }

    /// Builds a piece to evaluate a binary operator.
    Piece buildPieceForBinaryOperator(const PrometheusQueryTree::BinaryOperator * binary_operator)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", binary_operator->operator_name);
    }

    /// Builds a piece splitting the "time_series" column into two columns "timestamp" and "values", both of them are arrays.
    Piece splitTimeSeriesColumnToTwoArrays(Piece && piece)
    {
        if (!piece.time_series_column)
            return piece;

        Piece res;
        res.result_type = ResultType::RANGE_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                               std::make_shared<ASTLiteral>(Field{1u}));
        res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                           std::make_shared<ASTLiteral>(Field{2u}));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(std::move(piece));
        res.timestamp_column_is_array = true;
        res.value_column_is_array = true;
        return res;
    }

    /// Builds an AST to call functions generating time series on a grid.
    /// Returns something like timeSeriesGrid(<start_time>, <step>, timeSeries*ToGrid(<start_time>, <end_time>, <step>, <window>)(<timestamp>, <value>)
    ASTPtr makeGridFunction(std::string_view grid_function_name,
                                   const Field & start_time, const Field & end_time, const Field & step, const Field & window,
                                   ASTPtr timestamp_column, ASTPtr value_column) const
    {
        auto aggregate_function = makeASTFunction(grid_function_name, timestamp_column, value_column);
        aggregate_function->parameters = std::make_shared<ASTExpressionList>();
        aggregate_function->parameters->children.push_back(timestampToAST(start_time));
        aggregate_function->parameters->children.push_back(timestampToAST(end_time));
        aggregate_function->parameters->children.push_back(intervalToAST(step));
        aggregate_function->parameters->children.push_back(intervalToAST(window));
        return makeASTFunction("timeSeriesGrid", timestampToAST(start_time), intervalToAST(step), aggregate_function);
    }

    /// Finds all subqueries and @ and offset operations related to a specific node
    /// and determine the total time range and optionally the step used in the most inner subquery.
    /// The function always set `start_time` and `end_time`. If the node isn't used in any subquery the function sets `step` to Null.
    void extractRangeAndStep(const PrometheusQueryTree::Node * node, Field & start_time, Field & end_time, Field & step) const
    {
        start_time = Field{};
        end_time = Field{};
        step = Field{};
        Field end_time_offset;
        Field range;

        for (const auto * parent = node->parent; parent; parent = parent->parent)
        {
            if (parent->node_type == NodeType::At)
            {
                const auto * at_node = typeid_cast<const PrometheusQueryTree::At *>(parent);
                if (end_time.isNull())
                {
                    if (const auto * offset = at_node->getOffset())
                    {
                        auto offset_interval = castToIntervalType(nodeToField(offset));
                        if (end_time_offset.isNull())
                            end_time_offset = offset_interval;
                        else
                            end_time_offset = add(end_time_offset, offset_interval);
                    }
                    if (const auto * at = at_node->getAt())
                        end_time = castToTimestampType(nodeToField(at));
                }
            }
            else if (parent->node_type == NodeType::Subquery)
            {
                const auto * subquery_node = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                if (step.isNull())
                {
                    if (const auto * resolution = subquery_node->getResolution())
                        step = castToIntervalType(nodeToField(resolution));
                    else
                        step = castToIntervalType(getDefaultResolution());
                }
                auto subquery_range = castToIntervalType(nodeToField(subquery_node->getRange()));
                if (range.isNull())
                    range = subquery_range;
                else
                    range = add(range, subquery_range);
            }
        }

        if (end_time.isNull())
            end_time = castToTimestampType(getEvaluationTime());

        if (!end_time_offset.isNull())
            end_time = subtract(end_time, end_time_offset);

        start_time = end_time;
        if (!range.isNull())
            start_time = subtract(start_time, range);
    }

    /// Extracts a value from a scalar literal or an interval literal node.
    Field nodeToField(const PrometheusQueryTree::Node * scalar_or_interval_node) const
    {
        auto node_type = scalar_or_interval_node->node_type;
        if (node_type == NodeType::ScalarLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::ScalarLiteral *>(scalar_or_interval_node)->scalar};
        else if (node_type == NodeType::IntervalLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::IntervalLiteral *>(scalar_or_interval_node)->interval};
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a scalar literal or a interval literal node, got {} ({})", node_type, getPromQLText(scalar_or_interval_node));
    }

    /// Converts a scalar or an interval value to a timestamp compatible with the data types used in the TimeSeries table.
    Field castToTimestampType(const Field & field) const
    {
        return castToType(field, timestamp_data_type);
    }

    /// Converts a scalar or an interval value to an interval compatible with the data types used in the TimeSeries table.
    Field castToIntervalType(const Field & field) const
    {
        return castToType(field, interval_data_type);
    }

    /// Converts a casted timestamp to AST.
    ASTPtr timestampToAST(const Field & field) const
    {
        return fieldToAST(field, timestamp_data_type);
    }

    /// Converts a casted interval to AST.
    ASTPtr intervalToAST(const Field & field) const
    {
        return fieldToAST(field, interval_data_type);
    }

    /// Generates a non-zero step for timeSeries*ToGrid() functions.
    /// (Those functions don't allow zero step even if start_time == end_time.)
    Field getDummyStep() const
    {
        return castToIntervalType(Field{1});
    }
};


PrometheusQueryToSQLConverter::PrometheusQueryToSQLConverter(
    const PrometheusQueryTree & promql_,
    const Field & evaluation_time_,
    const TimeSeriesTableInfo & time_series_table_info_,
    const Field & lookback_delta_,
    const Field & default_resolution_)
    : promql(promql_)
    , evaluation_time(evaluation_time_)
    , time_series_table_info(time_series_table_info_)
    , lookback_delta(lookback_delta_)
    , default_resolution(default_resolution_)
{
}

ASTPtr PrometheusQueryToSQLConverter::getSQL() const
{
    return ASTBuilder{*this}.getSQL();
}

ColumnsDescription PrometheusQueryToSQLConverter::getResultColumns() const
{
    ColumnsDescription columns;

    switch (promql.getResultType())
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Scalar, time_series_table_info.value_data_type});
            break;
        }
        case ResultType::STRING:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::String, std::make_shared<DataTypeString>()});
            break;
        }
        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Timestamp,
                    time_series_table_info.timestamp_data_type});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Value,
                    time_series_table_info.value_data_type});
            break;
        }
        case ResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{time_series_table_info.timestamp_data_type, time_series_table_info.value_data_type}))});
            break;
        }
    }
    return columns;
}

}
