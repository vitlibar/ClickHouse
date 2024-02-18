#include <Interpreters/getOrCreateSystemFilledTable.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/StorageID.h>


namespace DB
{

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

namespace
{
    /// returns CREATE TABLE query, but with removed UUID
    /// That way it can be used to compare with the SystemLog::getCreateTableQuery()
    ASTPtr getCreateTableQueryClean(const StorageID & table_id, ContextPtr context)
    {
        DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        ASTPtr old_ast = database->getCreateTableQuery(table_id.table_name, context);
        auto & old_create_query_ast = old_ast->as<ASTCreateQuery &>();
        /// Reset UUID
        old_create_query_ast.uuid = UUIDHelpers::Nil;
        return old_ast;
    }
}


std::pair<StoragePtr, bool> getOrCreateSystemFilledTable(ContextPtr context, ASTPtr create_query, bool check_create_query_if_exists)
{
    const ASTCreateQuery & create_query_ref = create_query->as<const ASTCreateQuery &>();
    StorageID table_id{create_query_ref.getDatabase(), create_query_ref.getTable()};
    String description = table_id.getNameForLogs();
    auto logger = getLogger("getOrCreateSystemFilledTable");

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, context);
    bool created = false;

    if (table)
    {
        String create_query_str;
        String old_create_query;
        bool need_rename = false;

        if (check_create_query_if_exists)
        {
            create_query_str = serializeAST(*create_query);
            old_create_query = serializeAST(*getCreateTableQueryClean(table_id, context));

            /// TODO: Handle altering comment, because otherwise all table will be renamed.
            if (create_query_str != old_create_query)
                need_rename = true;
        }

        if (need_rename)
        {
            /// Rename the existing table.
            int suffix = 0;
            while (DatabaseCatalog::instance().isTableExist(
                {table_id.database_name, table_id.table_name + "_" + toString(suffix)}, context))
                ++suffix;

            auto rename = std::make_shared<ASTRenameQuery>();
            ASTRenameQuery::Element elem
            {
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name)
                },
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name + "_" + toString(suffix))
                }
            };

            LOG_DEBUG(
                logger,
                "Existing table {} has obsolete or different structure. Renaming it to {}.\nOld: {}\nNew: {}\n.",
                description,
                backQuoteIfNeed(elem.to.getTable()),
                old_create_query,
                create_query_str);

            rename->elements.emplace_back(std::move(elem));

            ActionLock merges_lock;
            if (DatabaseCatalog::instance().getDatabase(table_id.database_name)->getUUID() == UUIDHelpers::Nil)
                merges_lock = table->getActionLock(ActionLocks::PartsMerge);

            auto query_context = Context::createCopy(context);
            /// As this operation is performed automatically we don't want it to fail because of user dependencies on log tables
            query_context->setSetting("check_table_dependencies", Field{false});
            query_context->setSetting("check_referential_table_dependencies", Field{false});
            query_context->makeQueryContext();
            InterpreterRenameQuery(rename, query_context).execute();

            /// The required table will be created.
            table = nullptr;
        }
    }

    if (!table)
    {
        if (!DatabaseCatalog::instance().tryGetDatabase(table_id.database_name))
        {
            /// Create the database.
            LOG_DEBUG(logger, "Creating new database {} for table {}", table_id.database_name, description);
            auto create_database_ast = std::make_shared<ASTCreateQuery>();
            create_database_ast->setDatabase(table_id.database_name);
            create_database_ast->if_not_exists = true;

            auto query_context = Context::createCopy(context);
            query_context->makeQueryContext();
            InterpreterCreateQuery interpreter(create_database_ast, query_context);
            interpreter.setInternal(true);
            interpreter.execute();
        }

        {
            /// Create the table.
            LOG_DEBUG(logger, "Creating new table {}", description);

            auto query_context = Context::createCopy(context);
            query_context->makeQueryContext();

            InterpreterCreateQuery interpreter(create_query, query_context);
            interpreter.setInternal(true);
            interpreter.execute();

            table = DatabaseCatalog::instance().getTable(table_id, context);
            created = true;
        }
    }

    return {table, created};
}

}
