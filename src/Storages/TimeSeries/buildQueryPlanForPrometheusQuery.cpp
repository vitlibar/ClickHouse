#if 0
#include <Storages/TimeSeries/PrometheusQueryPlanBuilder.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

namespace
{
    /// Regular expressions in promql are always fully anchored. A match of mode=~"debug" is treated as mode=~"^debug$".
    /// Thus we need to add character '^' at the beginning of a pattern and character '$' at the end of it in order to use function match().
    String makeRegexpAnchored(const String & regexp)
    {
        String anchored = regexp;
        if (!anchored.starts_with("^"))
            anchored.insert(0, "^");
        if (!anchored.ends_with("$"))
            anchored.push_back("$");
        return anchored;
    }

    /// Builds an AST to evaluate a promql matcher.
    /// Example: for matcher `mode=~"debug"` the function returns `match(tags['mode'], '^debug$')`.
    ASTPtr makeASTForMatcher(const PrometheusQueryTree::Matcher & matcher)
    {
        std::string_view function_name;
        bool regexp = false;
        bool negate = false;
        switch (matcher.matcher_type)
        {
            case MatcherType::EQ:  function_name = "equals"; break;
            case MatcherType::NE:  function_name = "notEquals"; break;
            case MatcherType::RE:  function_name = "match"; regexp = true; break;
            case MatcherType::NRE: function_name = "match"; regexp = true; negate = true; break;
        }

        ASTPtr left_arg;
        if (matcher.label_name == "__name__")
            left_arg = std::make_shared<ASTIdentifier>("metric_name");
        else
            left_arg = makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>("tags"), matcher.label_name);

            ASTPtr right_arg;
        if (regexp)
            right_arg = std::make_shared<ASTLiteral>(makeRegexpAnchored(matcher.label_value));
        else
            right_arg = std::make_shared<ASTLiteral>(matcher.label_value);

        ASTPtr res = makeASTFunction(function_name, left_arg, right_arg);

        if (negate)
            res = makeASTFunction("not", res);

        return res;
    }

    /// Builds an AST to evaluate all matchers in a promql selector.
    /// Example: for selector `foo{mode=~"debug"}` the function returns
    /// (metric_name == 'foo') AND match(tags['mode'], '^debug$')
    ASTPtr makeASTForAllMatchersInSelector(const PrometheusQueryTree::Selector & selector)
    {
        ASTs matcher_asts;
        matcher_asts.reserve(selector.getMatchers().size());
        for (const auto * matcher_node : selector.getMatchers())
        {
            const auto & matcher = typeid_cast<const PrometheusQueryTree::Matcher &>(*matcher);
            matcher_asts.push_back(makeASTForMatcher(matcher));
        }

        if (matcher_asts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A selector must contain at least one matcher");

        if (matcher_asts.size() == 1)
            return matcher_asts[0];

        return makeASTFunction("and", std::move(matcher_asts));
    }

    /// Builds an AST to evaluate all matchers in a promql query, combined with OR.
    /// Example: for query `foo{mode=~"debug"} + bar{mode=~"debug"}` the function returns
    /// (metric_name == 'foo') AND match(tags['mode'], '^debug$') OR (metric_name == 'bar') AND match(tags['mode'], '^debug$')
    /// The function returns nullptr for a query without selectors, for example for query `time() - 1h30m`.
    ASTPtr makeASTForAllMatchers(const PrometheusQueryTree & promql_query)
    {
        ASTs asts;

        std::function<void(const PrometheusQueryTree::Node & node)> make_ast_for_all_selectors;
        make_ast_for_all_selectors = [&](const PrometheusQueryTree::Node & node)
        {
            auto node_type = node.node_type;
            if ((node_type == PrometheusQueryTree::NodeType::InstantSelector) || (node_type == PrometheusQueryTree::NodeType::RangeSelector))
            {
                const auto & selector = static_cast<const PrometheusQueryTree::Selector &>(node);
                res.push_back(makeASTForAllMatchersInSelector(selector));
            }
            else
            {
                for (const auto * child : node.children)
                    make_ast_for_all_selectors(*child);
            }
        };

        if (asts.empty())
            return nullptr;

        if (asts.size() == 1)
            return nullptr;

        return makeASTFunction("or", std::move(asts));
    }

    /// Builds an AST to read columns `id`, `metric_name`, `tags` from the tags table required to evaluate a promql query.
    /// Example: for query `foo{mode=~"debug"}` the function returns
    /// SELECT id, tags FROM tags_table WHERE (metric_name == 'foo') AND match(tags['mode'], '^debug$')
    /// The function returns nullptr for a query without selectors, for example for query `time() - 1h30m`.
    ASTPtr makeSelectQueryToReadFromTagsTable(const PrometheusQueryTree & promql_query, const StorageID & tags_storage_id)
    {
        ASTPtr condition_for_all_matchers = makeASTForAllMatchers(promql_query);
        if (!condition_for_all_matchers)
            return nullptr;

        auto columns = std::make_shared<ASTExpressionList>();
        columns->children.push_back(std::make_shared<ASTIdentifier>("id"));
        columns->children.push_back(std::make_shared<ASTIdentifier>("metric_name"));
        columns->children.push_back(std::make_shared<ASTIdentifier>("tags"));

        auto table_expression = std::make_shared<ASTTableExpression>();
        table_expression->children.push_back(std::make_shared<ASTTableIdentifier>(tags_storage_id));
        table_expression->database_and_table_name = table_expression->children.back();
        auto tables_in_select_query_element = std::make_shared<ASTTablesInSelectQueryElement>();
        tables_in_select_query_element->children.push_back(std::move(table_expression));
        tables_in_select_query_element->table_expression = tables_in_select_query_element->children.back();
        auto tables_in_select_query = std::make_shared<ASTTablesInSelectQuery>();
        tables_in_select_query->children.push_back(std::move(tables_in_select_query_element));

        auto select_query = std::make_shared<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(columns));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query));
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, condition_for_all_matchers);
        return select_query;
    }
}


PrometheusQueryPlanBuilder::PrometheusQueryPlanBuilder(
    std::shared_ptr<const PrometheusQueryTree> promql_query_, const StorageID & time_series_storage_id_)
    : promql_query(promql_query_)
    , time_series_storage_id(time_series_storage_id_)
{
}

PrometheusQueryPlanBuilder::~PrometheusQueryPlanBuilder() = default;


void PrometheusQueryPlanBuilder::buildQueryPlan(
    QueryPlan & query_plan, const ContextPtr & context, size_t max_block_size, size_t num_streams)
{
    /// TODO: Extract these from settings.
    auto now = std::chrono::system_clock::now();
    DecimalField<DateTime64> evaluation_time{std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count(), 3};

    PromQL

   //UInt64 lookback_delta_ms = 5 * 60 * 1000;

    //parsed_promql_query.findMatchersAndTimeRanges(evaluation_time, lookback_delta_ms);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto tags_storage = time_series_storage->getTargetTable(ViewTarget::Tags, context);
    auto tags_storage_snapshot = tags_storage->getStorageSnapshot(tags_storage->getInMemoryMetadataPtr(), context);

    auto select_query_to_read_from_tags_table = makeSelectQueryToReadFromTagsTable(*promql_query, tags_storage->getStorageID());

    if (!select_query_to_read_from_tags_table)
    {
        evaluatePromQLQueryWithoutSelectors(*promql_query);
    }

    Names column_names{"id", "metric_name", "tags"};
    SelectQueryInfo query_info;
    query_info.query = makeSelectQueryToReadFromTagsTable(*promql_query, tags_storage->getStorageID());
    tags_storage->read(
        query_plan, column_names, tags_storage_snapshot, query_info, context, QueryProcessingStage::Enum::Complete, max_block_size, num_streams);
}

}
#endif
