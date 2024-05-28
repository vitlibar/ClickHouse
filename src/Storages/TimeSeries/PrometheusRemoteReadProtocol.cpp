#include <Storages/TimeSeries/PrometheusRemoteReadProtocol.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int WRONG_TABLE_ENGINE;
    extern const int BAD_REQUEST_PARAMETER;
}


namespace
{
    /// Makes an ASTIdentifier for a column of the specified table.
    ASTPtr makeASTColumn(const StorageID & table_id, const String & column_name)
    {
        return std::make_shared<ASTIdentifier>(Strings{table_id.database_name, table_id.table_name, column_name});
    }

    /// Makes an AST for condition `tags_table.timestamp >= min_timestamp_ms`
    ASTPtr makeASTTimestampGreaterOrEquals(Int64 min_timestamp_ms, const StorageID & data_table_id)
    {
        return makeASTFunction("greaterOrEquals", makeASTColumn(data_table_id, "timestamp"),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{min_timestamp_ms}, 3}}));
    }

    /// Makes an AST for condition `tags_table.timestamp <= max_timestamp_ms`
    ASTPtr makeASTTimestampLessOrEquals(Int64 max_timestamp_ms, const StorageID & data_table_id)
    {
        return makeASTFunction("lessOrEquals", makeASTColumn(data_table_id, "timestamp"),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{max_timestamp_ms}, 3}}));
    }

    /// Makes an AST for the expression referencing a tag value.
    ASTPtr makeASTTagName(const String & tag_name, const StorageID & tags_table_id, const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        auto it = column_name_by_tag_name.find(tag_name);
        if (it != column_name_by_tag_name.end())
        {
            return makeASTColumn(tags_table_id, it->second);
        }
        /// arrayElement() can be used to extract a value from a Map too.
        return makeASTFunction("arrayElement", makeASTColumn(tags_table_id, "tags"), std::make_shared<ASTLiteral>(tag_name));
    }

    /// Makes an AST for a label matcher, for example `metric_name == 'value'` or `NOT match(labels['label_name'], 'regexp')`.
    ASTPtr makeASTLabelMatcher(
        const prometheus::LabelMatcher & label_matcher,
        const StorageID & tags_table_id,
        const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        const auto & label_name = label_matcher.name();
        const auto & label_value = label_matcher.value();
        auto type = label_matcher.type();

        if (type == prometheus::LabelMatcher::EQ)
            return makeASTFunction("equals", makeASTTagName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::NEQ)
            return makeASTFunction("notEquals", makeASTTagName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::RE)
            return makeASTFunction("match", makeASTTagName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::NRE)
            return makeASTFunction("not", makeASTFunction("match", makeASTTagName(label_name, tags_table_id, column_name_by_tag_name), std::make_shared<ASTLiteral>(label_value)));
        else
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Unexpected type of label matcher: {}", type);
    }

    /// Makes an AST checking that tags match a specifed label matcher and that timestamp is in range [min_timestamp_ms, max_timestamp_ms].
    ASTPtr makeASTFilterForReadingTimeSeries(
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
        Int64 min_timestamp_ms,
        Int64 max_timestamp_ms,
        const StorageID & data_table_id,
        const StorageID & tags_table_id,
        const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        ASTs filters;

        if (min_timestamp_ms)
            filters.push_back(makeASTTimestampGreaterOrEquals(min_timestamp_ms, data_table_id));

        if (max_timestamp_ms)
            filters.push_back(makeASTTimestampLessOrEquals(max_timestamp_ms, data_table_id));

        for (const auto & label_matcher_element : label_matcher)
            filters.push_back(makeASTLabelMatcher(label_matcher_element, tags_table_id, column_name_by_tag_name));

        if (filters.empty())
            return nullptr;
        
        return makeASTForLogicalAnd(std::move(filters));
    }

    /// The function builds a SELECT query for reading time series:
    /// SELECT tags_table.tag_column1, ... tags_table.tag_columnN, tags_table.tags,
    ///        groupArray(data_table.timestamp, data_table.value)
    /// FROM data_table
    /// SEMI LEFT JOIN tag_table ON data_table.id = tags_table.id
    /// WHERE filter
    /// GROUP BY tags_table.tag_column1, ..., tags_table.tag_columnN, tags_table.tags
    ASTPtr buildSelectQueryForReadingTimeSeries(
        const StorageID & data_table_id,
        const StorageID & tags_table_id,
        const std::unordered_map<String, String> & column_name_by_tag_name,
        Int64 min_timestamp_ms,
        Int64 max_timestamp_ms,
        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher)
    {
        auto select_query = std::make_shared<ASTSelectQuery>();

            /// SELECT any(tags_table.tag_column1), ... any(tags_table.tag_columnN), any(tags_table.tags),
            ///        groupArray(data_table.timestamp, data_table.value)
            {
            auto exp_list = std::make_shared<ASTExpressionList>();

            for (const auto & [_, column_name] : column_name_by_tag_name)
                exp_list->children.push_back(
                    makeASTColumn(tags_table_id, column_name));

            exp_list->children.push_back(
                makeASTColumn(tags_table_id, "tags"));

            exp_list->children.push_back(
                makeASTFunction("groupArray",
                                makeASTFunction("tuple",
                                                makeASTFunction("CAST", makeASTColumn(data_table_id, "timestamp"), std::make_shared<ASTLiteral>("DateTime64(3)")),
                                                makeASTFunction("CAST", makeASTColumn(data_table_id, "value"), std::make_shared<ASTLiteral>("Float64")))));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, exp_list);
        }

        /// FROM data_table
        auto tables = std::make_shared<ASTTablesInSelectQuery>();

        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(data_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);
        }

        /// SEMI LEFT JOIN tags_table ON data_table.id = tags_table.id
        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();

            auto table_join = std::make_shared<ASTTableJoin>();
            table_join->kind = JoinKind::Left;
            table_join->strictness = JoinStrictness::Semi;

            table_join->on_expression = makeASTFunction("equals", makeASTColumn(data_table_id, "id"), makeASTColumn(tags_table_id, "id"));
            table->table_join = table_join;

            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(tags_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE <filter>
        if (auto where = makeASTFilterForReadingTimeSeries(label_matcher, min_timestamp_ms, max_timestamp_ms, data_table_id, tags_table_id, column_name_by_tag_name))
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));

        /// GROUP BY tags_table.tag_column1, ..., tags_table.tag_columnN, tags_table.tags
        {
            auto exp_list = std::make_shared<ASTExpressionList>();
            for (const auto & [_, column_name] : column_name_by_tag_name)
            {
                exp_list->children.push_back(makeASTColumn(tags_table_id, column_name));
            }
            exp_list->children.push_back(makeASTColumn(tags_table_id, "tags"));
            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, exp_list);
        }

        return select_query;
    }

    /// Sorts a list of pairs {tag_name, tag_value} by tag name.
    void sortTagsByName(std::vector<std::pair<std::string_view /* tag_name */, std::string_view /* tag_value */>> & tags)
    {
        auto less_by_tag_name = [](const std::pair<std::string_view, std::string_view> & left, const std::pair<std::string_view, std::string_view> & right)
        {
            return left.first < right.first;
        };
        std::sort(tags.begin(), tags.end(), less_by_tag_name);
    }

    /// Sorts a list of pairs {timestamp, value} by timestamp.
    void sortTimeSeriesByTimestamp(std::vector<std::pair<Int64 /* timestamp_ms */, Float64 /* value */>> & time_series)
    {
        auto less_by_timestamp = [](const std::pair<Int64, Float64> & left, const std::pair<Int64, Float64> & right)
        {
            return left.first < right.first;
        };
        std::sort(time_series.begin(), time_series.end(), less_by_timestamp);
    }

    /// Converts a block generated by the SELECT query for reading time series to the protobuf format.
    void convertBlockToProtobuf(
        Block && block,
        google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series,
        const std::unordered_map<String, String> & column_name_by_tag_name)
    {
        size_t num_rows = block.rows();
        if (!num_rows)
            return;

        size_t column_index = 0;

        std::unordered_map<String, const IColumn *> column_by_tag_name;
        for (const auto & [tag_name, _] : column_name_by_tag_name)
            column_by_tag_name[tag_name] = block.getByPosition(column_index++).column.get();

        const auto & other_tags_column = typeid_cast<const ColumnMap &>(*block.getByPosition(column_index++).column);
        const auto & other_tags_names = other_tags_column.getNestedData().getColumn(0);
        const auto & other_tags_values = other_tags_column.getNestedData().getColumn(1);
        const auto & other_tags_offsets = other_tags_column.getNestedColumn().getOffsets();

        const auto & time_series_column = typeid_cast<const ColumnArray &>(*block.getByPosition(column_index++).column);
        const auto & time_series_timestamps = typeid_cast<const ColumnDecimal<DateTime64> &>(typeid_cast<const ColumnTuple &>(time_series_column.getData()).getColumn(0));
        const auto & time_series_values = typeid_cast<const ColumnFloat64 &>(typeid_cast<const ColumnTuple &>(time_series_column.getData()).getColumn(1));
        const auto & time_series_offsets = time_series_column.getOffsets();
        
        std::vector<std::pair<std::string_view, std::string_view>> tags;
        std::vector<std::pair<Int64, Float64>> time_series;

        for (size_t i = 0; i != num_rows; ++i)
        {
            size_t num_tags = 0;
            for (const auto & [_, column] : column_by_tag_name)
            {
                if (!column->getDataAt(i).empty())
                    ++num_tags;
            }

            size_t other_tags_start_offset = other_tags_offsets[i - 1];
            size_t other_tags_end_offset = other_tags_offsets[i];
            num_tags += other_tags_end_offset - other_tags_start_offset;

            tags.clear();
            tags.reserve(num_tags);

            for (const auto & [tag_name, column] : column_by_tag_name)
            {
                if (!column->getDataAt(i).empty())
                    tags.emplace_back(tag_name, column->getDataAt(i));
            }

            for (size_t j = other_tags_start_offset; j != other_tags_end_offset; ++j)
                tags.emplace_back(other_tags_names.getDataAt(j), other_tags_values.getDataAt(j));

            size_t time_series_start_offset = time_series_offsets[i - 1];
            size_t time_series_end_offset = time_series_offsets[i];
            size_t num_time_series = time_series_end_offset - time_series_start_offset;

            time_series.clear();
            time_series.reserve(num_time_series);

            for (size_t j = time_series_start_offset; j != time_series_end_offset; ++j)
                time_series.emplace_back(time_series_timestamps.getElement(j), time_series_values.getElement(j));

            sortTagsByName(tags);
            sortTimeSeriesByTimestamp(time_series);

            auto & new_time_series = *out_time_series.Add();

            for (const auto & [tag_name, tag_value] : tags)
            {
                auto & new_label = *new_time_series.add_labels();
                new_label.set_name(tag_name);
                new_label.set_value(tag_value);
            }
            
            for (const auto & [timestamp, value] : time_series)
            {
                auto & new_sample = *new_time_series.add_samples();
                new_sample.set_timestamp(timestamp);
                new_sample.set_value(value);
            }
        }
    }

    /// Makes a mapping from a tag name to a column name.
    std::unordered_map<String, String> makeMapColumnNameByTagName(const TimeSeriesSettings & storage_settings)
    {
        std::unordered_map<String, String> res;
        res["__name__"] = "metric_name"; /// The "__name__" tag is always stored as a column named "metric_name".
        const Map & tags_to_columns = storage_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            res[tag_name] = column_name;
        }
        return res;
    }
}


PrometheusRemoteReadProtocol::PrometheusRemoteReadProtocol(ConstStoragePtr time_series_storage_, const ContextPtr & context_)
    : context(context_)
    , log(getLogger("PrometheusRemoteReadProtocol"))
{
    time_series_storage = typeid_cast<std::shared_ptr<const StorageTimeSeries>>(time_series_storage_);
    if (!time_series_storage)
    {
        throw Exception(
            ErrorCodes::WRONG_TABLE_ENGINE,
            "PrometheusRemoteReadProtocol can be used with a TimeSeries table engine only, {} has engine {}",
            time_series_storage_->getStorageID().getNameForLogs(),
            time_series_storage_->getName());
    }
}

PrometheusRemoteReadProtocol::~PrometheusRemoteReadProtocol() = default;

void PrometheusRemoteReadProtocol::readTimeSeries(google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series,
                                                  Int64 start_timestamp_ms,
                                                  Int64 end_timestamp_ms,
                                                  const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
                                                  const prometheus::ReadHints &)
{
    out_time_series.Clear();

    auto storage_settings = time_series_storage->getStorageSettingsPtr();
    auto data_table_id = time_series_storage->getTargetTableId(TargetTableKind::kData);
    auto tags_table_id = time_series_storage->getTargetTableId(TargetTableKind::kTags);

    auto column_name_by_tag_name = makeMapColumnNameByTagName(*storage_settings);

    ASTPtr select_query = buildSelectQueryForReadingTimeSeries(
        data_table_id, tags_table_id, column_name_by_tag_name, start_timestamp_ms, end_timestamp_ms, label_matcher);

    LOG_TRACE(log, "Executing query {}", select_query);

    InterpreterSelectQuery interpreter(select_query, context, SelectQueryOptions{});
    BlockIO io = interpreter.execute();
    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (executor.pull(block))
    {
        LOG_TRACE(log, "Pulled block with {} columns and {} rows", block.columns(), block.rows());
        if (block)
            convertBlockToProtobuf(std::move(block), out_time_series, column_name_by_tag_name);
    }
}

}
