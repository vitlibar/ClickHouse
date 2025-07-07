#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    template <typename TimestampType, typename IntervalType>
    bool alignStartTimeAndEndTimeTemplate(Field & start_time, Field & end_time, const Field & step)
    {
        TimestampType start_time_value = start_time.safeGet<TimestampType>();
        TimestampType end_time_value = end_time.safeGet<TimestampType>();
        IntervalType step_value = step.safeGet<IntervalType>();

        start_time_value = (start_time_value + step_value - 1) / step_value * step_value;
        end_time_value = end_time_value / step_value * step_value;

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
            switch (left.getType())
            {
                case Field::Types::Int64: return alignStartTimeAndEndTimeTemplate<Int64, Int64>(start_time, end_time, step);
                case Field::Types::UInt64: return alignStartTimeAndEndTimeTemplate<UInt64, UInt64>(start_time, end_time, step);
                case Field::Types::Decimal32: return alignStartTimeAndEndTimeTemplate<Decimal32, Decimal32>(start_time, end_time, step);
                case Field::Types::Decimal64: return alignStartTimeAndEndTimeTemplate<Decimal64, Decimal64>(start_time, end_time, step);
                default: break;
            }
        }
        else if ((start_time.getType() == end_time.getType()) && (start_time.getType() == Field::Types::UInt64) && (step.getType() == Field::Types::Int64))
        {
            return alignStartTimeAndEndTimeTemplate<UInt64, Int64>(start_time, end_time, step);
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot align start time of type {} by step of type {} with end time of type {}",
                        start_time.getType(), step.getType(), end_time.getType());
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
                case Field::Types::Float64: return left.safeGet<Float64>() + right.safeGet<Float64>();
                case Field::Types::Decimal32: return left.safeGet<Decimal32>() + right.safeGet<Decimal32>();
                case Field::Types::Decimal64: return left.safeGet<Decimal64>() + right.safeGet<Decimal64>();
                default: break;
            }
        }
        else if ((left.getType() == Field::Types::UInt64) && (right.getType() == Field::Types::Int64))
        {
            return static_cast<UInt64>(left.safeGet<UInt64>() + right.safeGet<Int64>());
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add {} and {}", left.getType(), right.getType());
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
                case Field::Types::Float64: return left.safeGet<Float64>() - right.safeGet<Float64>();
                case Field::Types::Decimal32: return left.safeGet<Decimal32>() - right.safeGet<Decimal32>();
                case Field::Types::Decimal64: return left.safeGet<Decimal64>() - right.safeGet<Decimal64>();
                default: break;
            }
        }
        else if ((left.getType() == Field::Types::UInt64) && (right.getType() == Field::Types::Int64))
        {
            return static_cast<UInt64>(left.safeGet<UInt64>() - right.safeGet<Int64>());
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot subtract {} from {}", right.getType(), left.getType());
    }
}


class PrometheusQueryToSQLConverter::ASTBuilder
{
public:
    ASTBuilder(const PrometheusQueryToSQLConverter & converter_)
        : converter(converter_)
    {
    }

    ASTPtr getSQL()
    {
        auto * root_node = getPromQLTree().getRootNode();
        if (!root_node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't evaluate an empty prometheus query.");
        return toAST(finalize(buildPiece(root_node)));
    }

private:
    const PrometheusQueryToSQLConverter & converter;

    const PrometheusQueryTree & getPromQLTree() const { return converter.promql; }
    const TimeSeriesTableInfo & getTimeSeriesTableInfo() const { return converter.time_series_table_info; }

    using NodeType = PrometheusQueryTree::NodeType;
    using ResultType = PrometheusQueryResultType;

    /// Represents a SELECT query built for a node in a prometheus query tree.
    /// SELECT <tags>, <timestamp>, <value> FROM <from> [GROUP BY <group_by>]
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
    
        size_t num_columns() const
        {
            return (group_column != nullptr) + (tags_column != nullptr) + (timestamp_column != nullptr)
                + (value_column != nullptr) + (time_series_column != nullptr) + (scalar_column != nullptr) + (string_column != nullptr);
        }

        bool empty () const { return num_columns() == 0; }

        /// The FROM expression - either a table function or the CTE name of a subquery.
        ASTPtr from_table_function;
        String from_subquery;

        /// The GROUP BY expression.
        ASTs group_by;

        ASTPtr where;
        std::vector<std::pair<String, ASTPtr>> with;
    };

    /// Collected subqueries.
    std::vector<std::pair<String, Piece>> subqueries;

    String addSubquery(Piece && piece)
    {
        String name = fmt::format("prom{}", subqueries.size() + 1);
        subqueries.emplace_back(name, std::move(piece));
        return name;
    }

    /// Converts a Piece to AST.
    static ASTPtr toAST(const Piece & piece)
    {
        chassert(!piece.empty);
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

        if (piece.from_table || piece.from_table_function)
        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            if (piece.from_subquery)
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
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, where);

        if (!piece.with.empty())
        {
            auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
            with_expression_list_ast->children.push_back(std::move(with_element_ast));
            for (const auto & [name, ast] : piece.with)
            {
                auto with_element_ast = std::make_shared<ASTWithElement>();
                with_element_ast->name = name;
                with_element_ast->subquery = std::make_shared<ASTSubquery>(ast);
                with_element_ast->children.push_back(with_element_ast->subquery);
            }
            select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
        }
            
        return select_query;
    }

    /// Finalizes a Piece built to execute a prometheus query.
    Piece finalize(Piece && piece)
    {
        Piece res;

        /// Finalize depending on the result type.
        switch (piece.result_type)
        {
            case ResultType::STRING: res = finalizeWithStringResult(std::move(piece)); break;
            case ResultType::SCALAR: /// nobreak
            case ResultType::INTERVAL: res = finalizeWithScalarResult(std::move(piece)); break;
            case ResultType::INSTANT_VECTOR: res = finalizeWithInstantVectorResult(std::move(piece)); break;
            case ResultType::RANGE_VECTOR: res = finalizeWithRangeVectorResult(std::move(piece)); break;
        }

        /// Add subqueries as CTEs to the final query.
        if (!subqueries.empty())
        {
            res.with.reserve(subqueries.size());
            for (const auto & [name, piece_for_subquery] : subqueries)
                res.with.push_back(name, toAST(piece_for_subquery));
        }

        return res;
    }

    /// Finalizes a Piece returning a string.
    Piece finalizeWithStringResult(Piece && piece)
    {
        if (piece.string_column && piece.num_columns() == 1)
            return piece;

        Piece res;
        res.type = piece.type;
        res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", fmt::format("{} String", TimeSeriesColumnNames::String));
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
        res.type = piece.type;
        res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", fmt::format("{} {}", TimeSeriesColumnNames::Scalar, getTimeSeriesTableInfo().value_data_type));
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
        res.type = piece.type;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
            res.from_table_function = makeASTFunction("null",
                fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                            TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type));
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
            res.group_by.push_back(std::make_shared<ASTIdentifier>(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.timestamp_column && piece.value_column)
        {
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        }
        else if (piece.time_series_column)
        {
            res.where = makeASTFunction("notEmpty", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
            auto array_element = makeASTFunction("arrayLast", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
            res.timestamp_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{1}));
            res.value_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{2}));
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
        res.type = piece.type;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
            res.from_table_function = makeASTFunction("null",
                fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::TimeSeries,
                            getTimeSeriesTableInfo().timestamp_data_type, getTimeSeriesTableInfo().value_data_type));
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
            res.group_by.push_back(std::make_shared<ASTIdentifier>(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.time_series_column)
        {
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
        }
        else if (piece.timestamp_column && piece.value_column)
        {
            auto group_array_function = makeASTFunction("timeSeriesGroupArraySorted",
                                                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
            group_array_function->setAlias(TimeSeriesColumnNames::TimeSeries);
            res.time_series_column = group_array_function;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns ({} and {}) or {} while building an SQL query", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value, TimeSeriesColumnNames::TimeSeries);
        }

        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Builds a query piece to execute a node in a prometheus query tree.
    Piece buildPiece(const PrometheusQueryTree::Node * node)
    {
        auto node_type = node->node_type;
        switch (node_type)
        {
            case NodeType::InstantSelector:
                return buildPieceForInstantSelector(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));

            case NodeType::RangeSelector:
                return buildPieceForRangeSelector(typeid_cast<const PrometheusQueryTree::RangeSelector *>(node));

            case NodeType::Function:
                return buildPieceForFunction(typeid_cast<const PrometheusQueryTree::Function *>(node));

            case NodeType::BinaryOperator:
                return buildPieceForBinaryOperator(typeid_cast<const PrometheusQueryTree::BinaryOperator *>(node));

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query tree node type {} is not implemented", node_type);
        }
    }

    /// Builds a query piece to execute an instant selector.
    Piece buildPieceForInstantSelector(const PrometheusQueryTree::InstantSelector * instant_selector)
    {
        Field window = castToIntervalDataType(getLookbackDelta());

        Field min_time, max_time, step;
        extractRangeAndStep(instant_selector, min_time, max_time, step);

        if (min_time == max_time)
        {
            step = getDummyStep();
        }
        else if (!alignStartTimeAndEndTime(min_time, max_time, step))
        {
            /// Couldn't align by `step`.
            Piece empty;
            empty.result_type = ResultType::INSTANT_VECTOR;
            return empty;
        }

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector", getPromQLText(instant_selector), std::make_shared<ASTLiteral>(subtract(min_time, window)), std::make_shared<ASTLiteral>(max_time));
        res.group_column = makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.time_series_column = makeGridFunction("timeSeriesLastToGrid", min_time, max_time, step, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.result_type = ResultType::INSTANT_VECTOR;

        return res;
    }

    /// Builds a query piece to execute a range selector.
    Piece buildPieceForRangeSelector(const PrometheusQueryTree::RangeSelector * range_selector)
    {
        const auto * instant_selector = range_selector->getInstantSelector();
        Field window = range_selector->range;

        Field min_time, max_time, step;
        extractRangeAndStep(range_selector, min_time, max_time, step);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector", getPromQLText(instant_selector), std::make_shared<ASTLiteral>(subtract(min_time, window)), std::make_shared<ASTLiteral>(max_time));
        res.group_column = makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.timestamp = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.result_type = ResultType::RANGE_VECTOR;
        res.window = window;

        return res;
    }

    Piece buildPieceForFunction(const PrometheusQueryTree::Function * func)
    {
        std::vector<Piece> args = buildPiecesForArguments(func);
        if (func->function_name == "rate")
            return buildPieceForRangeFunction(func, std::move(args));
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);
    }

    Piece buildPieceForRangeFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func->function_name, args, 1);
        checkArgumentType(func->function_name, args[0], ResultType::RANGE_VECTOR);

        const auto & arg = args[0];

        std::string_view to_grid_function_name;
        if (func)

        Field window = arg.window;

        Field min_time, max_time, step;
        extractRangeAndStep(func, min_time, max_time, step);

        if (min_time == max_time)
        {
            step = getDummyStep();
        }
        else if (!alignStartTimeAndEndTime(min_time, max_time, step))
        {
            /// Couldn't align by `step`.
            Piece empty;
            empty.result_type = ResultType::INSTANT_VECTOR;
            return empty;
        }

        Piece intermedite;
        if (arg.timestamp_column && arg.value_column)
        {
            intermediate = arg;
        }
        else
        {
            intermedite.result_type = ResultType::RANGE_VECTOR;
            intermedite.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
            intermedite.timestamp_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeriees),
                                                std::make_shared<ASTLiteral>(Field{1}));
            intermedite.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
            intermedite.value_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                                std::make_shared<ASTLiteral>(Field{2}));
            intermedite.value_column->setAlias(TimeSeriesColumnNames::Value);
            intermediate.from_subquery = addSubquery(arg);
        }

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.time_series_column = makeGridFunction(to_grid_function_name, start_time, max_time, step, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp), std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.from_subquery = addSubquery(intermediate)
        return res;
    }

    Piece buildPieceForBinaryOperator(const PrometheusQueryTree::BinaryOperator * binary_operator)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", binary_operator->operator_name);
    }

    std::string_view getPromQLText(const PrometheusQueryTree::Node * node)
    {
        return getPromQLTree().getQuery(node);
    }

    ASTPtr makeGridFunction(const String & function_name, const Field & start_time, const Field & end_time, const Field & step,
                            ASTPtr timestamp_column, ASTPtr value_column)
    {
        auto aggregate_function = makeASTFunction(function_name, timestamp_column, value_column);
        aggregate_function->parameters = std::make_shared<ASTExpressionList>();
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(start_time));
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(end_time));
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(step));
        return makeASTFunction("timeSeriesGrid", std::make_shared<ASTLiteral>(start_time), std::make_shared<ASTLiteral>(step), aggregate_function);
    }

    /// Extracts a scalar value or an interval value.
    static Field nodeToField(const PrometheusQueryTree::Node * scalar_or_interval_node)
    {
        auto node_type = scalar_or_interval_node->node_type;
        if (node_type == NodeType::ScalarLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::ScalarLiteral *>(scalar_or_interval_node)->scalar};
        else if (node_type == NodeType::IntervalLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::IntervalLiteral *>(scalar_or_interval_node)->interval};
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a scalar literal or a interval literal, got {}", node_type);
    }

    /// Extracts a time range and a step based on operators @ offset and subqueries.
    void extractRangeAndStep(const PrometheusQueryTree::Node * node, Field & min_time, Field & max_time, Field & step)
    {
        min_time = Field{};
        max_time = Field{};
        step = Field{};
        Field max_time_offset;
        Field range;

        for (const auto * parent = node->parent; parent; parent = parent->parent)
        {
            if (parent->node_type == NodeType::At)
            {
                const auto * at_node = typeid_cast<const PrometheusQueryTree::At *>(parent);
                if (max_time.isNull())
                {
                    if (const auto * offset = at_node->getOffset())
                    {
                        auto casted_offset = castToIntervalDataType(nodeToField(offset));
                        if (max_time_offset.isNull())
                            max_time_offset = casted_offset;
                        else
                            max_time_offset = add(max_time_offset, casted_offset);
                    }
                    if (const auto * at = at_node->getAt())
                        max_time = castToTimestampDataType(nodeToField(at));
                }
            }
            else if (parent->node_type == NodeType::Subquery)
            {
                const auto * subquery_node = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                if (step.isNull())
                {
                    if (const auto * resolution = subquery_node->getResolution())
                        step = castToIntervalDataType(nodeToField(resolution));
                    else
                        step = castToIntervalDataType(getDefaultResolution());
                }
                auto subquery_range = castToIntervalDataType(nodeToField(subquery_node->getRange()));
                if (range.isNull())
                    range = subquery_range;
                else
                    range = add(range, subquery_range);
            }
        }

        if (max_time.isNull())
            max_time = castToTimestampDataType(getEvaluationTime());

        if (!max_time_offset.isNull())
            max_time = sub(max_time, max_time_offset);

        if (!range.isNull())
            min_time = sub(min_time, range);
    }

    /// Converts a scalar or an interval value to a timestamp compatible with the data types used in the TimeSeries table.
    Field castToTimestampDataType(const Field & field) const
    {
        const auto & timestamp_data_type = getTimeSeriesTableInfo().timestamp_data_type;
        switch (WhichDataType{*timestamp_data_type}.idx)
        {
            case TypeIndex::UInt32:
            case TypeIndex::DateTime:
            {

            }
            case TypeIndex::DateTime64:
            {
                UInt32 scale = 
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_AR)
            }
        }
    }

    /// Converts a scalar or an interval value to an interval compatible with the data types used in the TimeSeries table.
    Field castToIntervalDataType(const Field & field) const
    {
    }

    Field getDummyStep() const
    {

    }
};


PrometheusQueryToSQLConverter::PrometheusQueryToSQLConverter(
    const PrometheusQueryTree & promql_,
    const EvaluationTimeType & evaluation_time_,
    const TimeSeriesTableInfo & time_series_table_info_,
    const IntervalType & lookback_delta_,
    const IntervalType & default_resolution_)
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
        case ResultType::INTERVAL:
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
                    time_series_table_info.timestamp_data_type);
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Value,
                    time_series_table_info.value_data_type);
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
}

}
