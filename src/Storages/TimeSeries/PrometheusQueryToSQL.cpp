#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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
    size_t num_aliases = 0;

    const PrometheusQueryTree & getPromQLTree() const { return converter.promql; }
    const TimeSeriesTableInfo & getTimeSeriesTableInfo() const { return converter.time_series_table_info; }

    using NodeType = PrometheusQueryTree::NodeType;
    using ResultType = PrometheusQueryResultType;

    struct ExpressionAndAlias
    {
        ASTPtr expression;
        String alias;
    };

    /// Represents a SELECT query built for a node in a prometheus query tree.
    /// SELECT <tags>, <timestamp>, <value> FROM <from> [GROUP BY <group_by>]
    struct QueryPiece
    {
        ResultType type;
        bool empty = true;

        /// Expressions to select.
        ASTs select;

        /// Columns (nullptr if there is no such column).
        /// The names of these columns are always TimeSeriesColumnNames::ID and so on.
        ASTPtr id_column;
        ASTPtr group_column;
        ASTPtr tags_column;
        ASTPtr timestamp_column;
        ASTPtr value_column;
        ASTPtr time_series_column;
        ASTPtr scalar_column;
        ASTPtr string_column;
    
        size_t num_columns() const
        {
            return (id_column != nullptr) + (group_column != nullptr) + (tags_column != nullptr) + (timestamp_column != nullptr)
                + (value_column != nullptr) + (time_series_column != nullptr) + (scalar_column != nullptr) + (string_column != nullptr);
        }

        /// The table expression to read from.
        ASTPtr from_table_function;
        ASTPtr from_subquery;

        /// The GROUP BY expression.
        ASTs group_by;

        ASTPtr where;
    };

    /// Converts a QueryPiece to AST.
    static ASTPtr toAST(const QueryPiece & piece)
    {
        chassert(!piece.empty);
        auto select_query = std::make_shared<ASTSelectQuery>();

        auto select_list_exp = std::make_shared<ASTExpressionList>();
        auto & select_list = select_list_exp->children;
        if (piece.id_column)
            select_list.push_back(piece.id_column);
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

        if (piece.from_table_function || piece.from_subquery)
        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            if (piece.from_table_function)
                table_exp->table_function = piece.from_table_function;
            else
                table_exp->subquery = piece.from_subquery;
            table_exp->children.emplace_back(table_exp->database_and_table_name);
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

        return select_query;
    }

    /// Finalizes a QueryPiece built to execute a prometheus query.
    Piece finalize(Piece && piece)
    {
        switch (piece.type)
        {
            case ResultType::STRING: return finalizeWithStringResult(std::move(piece));
            case ResultType::SCALAR:
            case ResultType::INTERVAL: return finalizeWithScalarResult(std::move(piece));
            case ResultType::INSTANT_VECTOR: return finalizeWithInstantVectorResult(std::move(piece));
            case ResultType::RANGE_VECTOR: return finalizeWithRangeVectorResult(std::move(piece));
        }
    }

    Piece finalizeWithStringResult(Piece && piece)
    {
        if (piece.string_column && piece.num_columns() == 1)
            return piece;

        Piece res;
        res.type = piece.type;
        res.empty = false;
        res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);

        if (piece.empty)
            res.from_table_function = makeASTFunction("null", fmt::format("{} String", TimeSeriesColumnNames::String));
        else
            res.from_subquery = toAST(piece);

        return res;
    }

    Piece finalizeWithScalarResult(Piece && piece)
    {
        if (piece.empty)
        {
            Piece res;
            res.type = piece.type;
            res.empty = false;
            res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);
            res.from_table_function = makeASTFunction("null",
                fmt::format("{} {}", TimeSeriesColumnNames::Scalar, getTimeSeriesTableInfo().value_data_type));
            return res;
        }
        else if (piece.string_column && piece.num_columns() == 1)
        {
            return piece;
        }
        else
        {
            Piece res;
            res.type = piece.type;
            res.empty = false;
            res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);
            res.from_subquery = toAST(piece);
            return res;
        }
    }

    Piece finalizeWithInstantVectorResult(Piece && piece)
    {
        
    }

    Piece finalizeWithRangeVectorResult(Piece && piece)
    {
        
    }

    Piece finalize(Piece && piece)
    {
        switch (piece.type)
        {
            case ResultType::STRING:
            {
                if (piece.empty)
                {
                    Piece res;
                    res.type = piece.type;
                    res.empty = false;
                    res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);
                    res.from_table_function = makeASTFunction("null", fmt::format("{} String", TimeSeriesColumnNames::String));
                    return res;
                }
                else if (piece.string_column && piece.num_columns() == 1)
                {
                    return piece;
                }
                else
                {
                    Piece res;
                    res.type = piece.type;
                    res.empty = false;
                    res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);
                    res.from_subquery = toAST(piece);
                    return res;
                }
            }
            case ResultType::SCALAR:
            case ResultType::INTERVAL:
            {

            }
            case ResultType::INSTANT_VECTOR:
            {
                if (piece.empty)
                {
                    Piece res;
                    res.type = piece.type;
                    res.empty = false;
                    res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
                    res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
                    res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
                    res.from_table_function = makeASTFunction("null",
                        fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                                    TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                                    TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type));
                    return res;
                }
                else if (piece.tags_column && piece.timestamp_column && piece.value_column && piece.num_columns() == 3)
                {
                    return piece;
                }
                else
                {
                    Piece res;
                    res.type = piece.type;
                    res.empty = false;
                    if (piece.tags_column)
                    {
                        res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
                    }
                    else if (piece.group_column)
                    {
                        res.tags_column = makeASTFunction("timeSeriesGroupToTags", TimeSeriesColumnNames::Group);
                        res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
                        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                    }
                    else if (piece.id_column)
                    {
                        res.tags_column = makeASTFunction("timeSeriesIdToTags", TimeSeriesColumnNames::ID);
                        res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
                        res.group_by.push_back(makeASTFunction("timeSeriesIdToGroup", TimeSeriesColumnNames::ID));
                    }
                    if (piece.timestamp_column && piece.value_column)
                    {
                        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
                        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
                    }
                    else if (piece.time_series_column)
                    {
                        res.where = makeASTFunction("notEmpty", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
                        auto array_element = makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries), std::make_shared<ASTLiteral>(Field{1}));
                        res.timestamp_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{1}));
                        res.value_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{2}));
                    }
                    res.from_subquery = toAST(piece);
                    return res;
                }
            }


        }
            switch (piece.type)
            {
                case ResultType::STRING:
                {
                    res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);
                    structure = fmt::format("{} String", res.string_column);
                    break;
                }
                case ResultType::SCALAR:
                case ResultType::INTERVAL:
                {
                    res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);
                    structure = fmt::format("{} {}", TimeSeriesColumnNames::String, getTimeSeriesTableInfo().value_data_type);
                    break;
                }
                case ResultType::INSTANT_VECTOR:
                {
                    res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
                    res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
                    res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
                    structure = fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                                            TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type);
                    break;
                }
                case ResultType::RANGE_VECTOR:
                {
                    res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
                    res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
                    structure = fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::TimeSeries,
                                            getTimeSeriesTableInfo().timestamp_data_type, getTimeSeriesTableInfo().value_data_type);
                    break;
                }
            }
            res.from_table_function = makeASTFunction("null", structure);
            return res;
        }

        if (piece.type == ResultType::STRING)
        {
            if (piece.scalar_column && piece.num_columns() == 1)
                return piece;
            
            add
        }





        )
        else if (piece.sorted_array && res.id && )
        {
            if (res.sorted_array)
            if (res.)
        }
        return res;
    }

    /// Builds a query piece to execute a node in a prometheus query tree.
    Piece buildPiece(const PrometheusQueryTree::Node * node)
    {
        auto node_type = node->node_type;
        switch (node_type)
        {
            case NodeType::InstantSelector:
                return buildPieceForSelector(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query tree node {} is not implemented", node_type);
        }
    }

    /// Builds a query piece to execute an instant or range selector in a prometheus query tree.
    Piece buildPieceForSelector(const PrometheusQueryTree::InstantSelector * instant_selector)
    {
        Field max_time;
        Field max_time_offset;
        Field window = castToIntervalDataType(getLookbackDelta());
        Field range, step;
        bool apply_function_last = true;

        for (const auto * parent = instant_selector->parent; parent; parent = parent->parent)
        {
            if (parent->node_type == NodeType::RangeSelector)
            {
                const auto * range_selector = typeid_cast<const PrometheusQueryTree::RangeSelector *>(parent);
                window = castToIntervalDataType(scalarOrIntervalNodeToField(range_selector->range));
                apply_function_last = false;
            }
            else if (parent->node_type == NodeType::At)
            {
                const auto * at_node = typeid_cast<const PrometheusQueryTree::At *>(parent);
                if (max_time.isNull())
                {
                    if (const auto * at = at_node->getAt())
                        max_time = castToTimestampDataType(scalarOrIntervalNodeToField(at));
                    if (const auto * offset = at_node->getOffset())
                    {
                        auto casted_offset = castToIntervalDataType(scalarOrIntervalNodeToField(offset));
                        if (max_time_offset.isNull())
                            max_time_offset = casted_offset;
                        else
                            max_time_offset = add(max_time_offset, casted_offset);
                    }
                }
            }
            else if (parent->node_type == NodeType::Subquery)
            {
                const auto * subquery_node = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                if (step.isNull())
                {
                    if (const auto * resolution = subquery_node->getResolution())
                        step = castToIntervalDataType(scalarOrIntervalNodeToField(resolution));
                    else
                        step = castToIntervalDataType(getDefaultResolution());
                }
                auto subquery_range = castToIntervalDataType(scalarOrIntervalNodeToField(subquery_node->getRange()));
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
        Field min_time = sub(max_time, window);
        if (!range.isNull())
            min_time = sub(min_time, range);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector", getPromQLText(instant_selector), std::make_shared<ASTLiteral>(min_time), std::make_shared<ASTLiteral>(max_time));
        res.id = std::make_shared<ASTIdentifier>("id");
        res.timestamp = std::make_shared<ASTIdentifier>("timestamp");
        res.value = std::make_shared<ASTIdentifier>("value");
        res.empty = false;

        if (apply_function_last)
        {
            String alias = getAliasName();
            Field start_time = max_time;
            if (step.isNull())
            {
                step = getDummyStep();
            }
            else
            {
                start_time = alignStartTime(sub(max_time, range), max_time, step);
                if (!start_time)
                    return {}; /// Couldn't align by `step` within a specified range.
            }

            auto grid_function = makeGridFunction("timeSeriesLastToGrid", start_time, max_time, step, res.timestamp, res.value, alias);
            res.extra_columns.push_back(grid_function);
            res.timestamp = std::make_shared<ASTIdentifier>(Strings{alias, "1"});
            res.value = std::make_shared<ASTIdentifier>(Strings{alias, "2"});
            res.group_by = makeGroupByID(res.id);
            res.sorted_arrays = true;
        }

        return res;
    }

    std::string_view getPromQLText(const PrometheusQueryTree::Node * node)
    {
        return getPromQLTree().getQuery(node);
    }

    ASTPtr makeGridFunction(const String & function_name, const Field & start_time, const Field & end_time, const Field & step,
                            ASTPtr timestamp, ASTPtr value, const String & alias)
    {
        auto aggregate_function = std::make_shared<ASTFunction>();
        aggregate_function->name = function_name;
        aggregate_function->parameters = std::make_shared<ASTExpressionList>();
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(start_time));
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(end_time));
        aggregate_function->parameters->children.push_back(std::make_shared<ASTLiteral>(step));
        aggregate_function->arguments = std::make_shared<ASTExpressionList>();
        aggregate_function->arguments->children.push_back(timestamp);
        aggregate_function->arguments->children.push_back(value);
        auto grid_function = makeASTFunction("timeSeriesGrid", std::make_shared<ASTLiteral>(start_time), std::make_shared<ASTLiteral>(step), aggregate_function);
        grid_function->setAlias(alias);
        return grid_function;
    }

    ASTs makeGroupByID(ASTPtr id)
    {
        return {makeASTFunction("timeSeriesIdToGroup", id)};
    }

    String getAliasName()
    {
        return fmt::format("prom{}", ++num_aliases);
    }

    /// Extracts a scalar value or an interval value.
    static Field scalarOrIntervalNodeToField(const PrometheusQueryTree::Node * scalar_or_interval_node)
    {
        auto node_type = scalar_or_interval_node->node_type;
        if (node_type == NodeType::ScalarLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::ScalarLiteral *>(scalar_or_interval_node)->scalar};
        else if (node_type == NodeType::IntervalLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::IntervalLiteral *>(scalar_or_interval_node)->interval};
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a scalar literal or a interval literal, got {}", node_type);
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
    static Field sub(const Field & left, const Field & right)
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

    /// Increases the start time by some value to make it divisible by `step`.
    /// If such new aligned started time would be greater than `end_time` the function returns Null.
    static Field alignStartTime(const Field & start_time, const Field & end_time, const Field & step)
    {
        auto align_int = [](auto start_time_, auto end_time_, auto step_)
        {
            if (step_ <= 0)
                throw Exception();
            auto aligned = (start_time_ + step_ - 1) / step_;
            if (aligned > end_time_)
                return Field{};
            return Field{aligned};
        };

        if ((start_time.getType() == end_time.getType()) && (start_time.getType() == step.getType()))
        {
            switch (left.getType())
            {
                case Field::Types::Int64: return align_int(start_time.safeGet<Int64>(), end_time.safeGet<Int64>(), step.safeGet<Int64>());
                case Field::Types::UInt64: return align_int(start_time.safeGet<UInt64>(), end_time.safeGet<UInt64>(), step.safeGet<UInt64>());
                case Field::Types::Decimal32: return align_decimal(start_time.safeGet<Decimal32>(), end_time.safeGet<Decimal32>(), step.safeGet<Decimal32>());
                case Field::Types::Decimal64: return align_decimal(start_time.safeGet<Decimal64>(), end_time.safeGet<Decimal64>(), step.safeGet<Decimal64>());
                default: break;
            }
        }
        else if ((start_time.getType() == end_time.getType()) && (start_time.getType() == Field::Types::UInt64) && (step.getType() == Field::Types::Int64))
        {
            return align_int(start_time.safeGet<UInt64>(), end_time.safeGet<UInt64>(), step.safeGet<Int64>());
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot align start time of type {} by step of type {} with end time of type {}",
                        start_time.getType(), step.getType(), end_time.getType());
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







namespace
{



    template <typename TimestampType, typename IntervalType>
    class PromQLToSQLConverter
    {
    public:
        ASTPtr nodeToSQL(const PrometheusQueryTree::Node * node)
        {
            auto node_type = node->node_type;
            switch (node_type)
            {
                case NodeType::InstantSelector:
                    return instantSelectorToSQL(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));
            }
        }

    private:
        /// Converts a selector 
        ASTPtr instantSelectorToSQL(const PrometheusQueryTree::InstantSelector * instant_selector)
        {
            std::optional<TimestampType> evaluation_time;
            IntervalType evaluation_time_offset = 0;
            TimestampType range = lookback_delta;

            for (const auto * parent = instant_selector->parent; parent; parent = parent->parent)
            {
                if (parent->node_type == NodeType::RangeSelector)
                {
                    const auto * range_selector = typeid_cast<const PrometheusQueryTree::RangeSelector *>(parent);
                    range = range_selector->range;
                }
                else if (parent->node_type == NodeType::At)
                {
                    const auto * at_parent = typeid_cast<const PrometheusQueryTree::At *>(parent);
                    if (at_parent->at && !evaluation_time)
                        evaluation_time = *at_parent->at;
                    if ()
                }
                else if (parent->node_type == NodeType::Subquery)
                {
                    const auto * subquery_parent = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                    range = subquery_parent->
                }
            }
        }

        PrometheusQueryTree promql;
        StorageID time_series_table_id;
        TimestampType evaluation_time;
        OffsetType lookback_delta;

        ASTPtr makeSelector
    };
}

template <typename TimestampType, typename IntervalType>
ASTPtr prometheusQueryToSQL(const PrometheusQueryTree & promql,
                            const StorageID & time_series_table_id,
                            const TimestampType & evaluation_time,
                            const IntervalType & lookback_delta,
                            const IntervalType & default_resolution);

ASTPtr prometheusQueryToSQL(const PrometheusQueryTree & promql,
                            const StorageID & time_series_table_id,
                            const PrometheusQueryTree::Timestamp & evaluation_time,
                            const PrometheusQueryTree::OffsetType & lookback_delta)
{
    if (promql.empty())
        throw Exception();

    const auto * node = promql.getRoot();
    return PromQLToSQLConverter{promql, time_series_table_id, lookback_delta}.nodeToSQL(promql.getRoot(), evaluation_time);

}

}
