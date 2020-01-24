#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/IStorage.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/LiveView/LiveViewCommands.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <Common/typeid_cast.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
}


InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, const Context & context_)
    : query_ptr(query_ptr_), context(context_)
{
}

BlockIO InterpreterAlterQuery::execute()
{
    const auto & alter = query_ptr->as<ASTAlterQuery &>();

    if (!alter.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccess());

    context.checkAccess(getRequiredAccess());

    const String & table_name = alter.table;
    String database_name = alter.database.empty() ? context.getCurrentDatabase() : alter.database;
    StoragePtr table = context.getTable(database_name, table_name);

    /// Add default database to table identifiers that we can encounter in e.g. default expressions,
    /// mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(database_name);
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    LiveViewCommands live_view_commands;
    for (ASTAlterCommand * command_ast : alter.command_list->commands)
    {
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            if (partition_command->type == PartitionCommand::DROP_DETACHED_PARTITION
                && !context.getSettingsRef().allow_drop_detached)
                throw DB::Exception("Cannot execute query: DROP DETACHED PART is disabled "
                                    "(see allow_drop_detached setting)", ErrorCodes::SUPPORT_IS_DISABLED);
            partition_commands.emplace_back(std::move(*partition_command));
        }
        else if (auto mut_command = MutationCommand::parse(command_ast))
            mutation_commands.emplace_back(std::move(*mut_command));
        else if (auto live_view_command = LiveViewCommand::parse(command_ast))
            live_view_commands.emplace_back(std::move(*live_view_command));
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!mutation_commands.empty())
    {
        auto table_lock_holder = table->lockStructureForShare(false /* because mutation is executed asyncronously */, context.getCurrentQueryId());
        MutationsInterpreter(table, mutation_commands, context, false).validate(table_lock_holder);
        table->mutate(mutation_commands, context);
    }

    if (!partition_commands.empty())
    {
        partition_commands.validate(*table);
        table->alterPartition(query_ptr, partition_commands, context);
    }

    if (!live_view_commands.empty())
    {
        live_view_commands.validate(*table);
        for (const LiveViewCommand & command : live_view_commands)
        {
            auto live_view = std::dynamic_pointer_cast<StorageLiveView>(table);
            switch (command.type)
            {
                case LiveViewCommand::REFRESH:
                    live_view->refresh(context);
                    break;
            }
        }
    }

    if (!alter_commands.empty())
    {
        auto table_lock_holder = table->lockAlterIntention(context.getCurrentQueryId());
        StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
        alter_commands.validate(metadata, context);
        alter_commands.prepare(metadata, context);
        table->checkAlterIsPossible(alter_commands, context.getSettingsRef());
        table->alter(alter_commands, context, table_lock_holder);
    }

    return {};
}


AccessRightsElements InterpreterAlterQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    for (ASTAlterCommand * command_ast : alter.command_list->commands)
    {
        auto column_name = [&]() -> const String & { return command_ast->col_decl->as<ASTColumnDeclaration &>().name; };
        switch (command_ast->type)
        {
            case ASTAlterCommand::UPDATE:
            {
                std::vector<std::string_view> column_names;
                for (const ASTPtr & assignment_ast : command_ast->update_assignments->children)
                    column_names.emplace_back(assignment_ast->as<const ASTAssignment &>().column_name);
                required_access.emplace_back(AccessType::UPDATE, alter.database, alter.table, column_names);
                break;
            }
            case ASTAlterCommand::DELETE:
            {
                required_access.emplace_back(AccessType::DELETE, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::ADD_COLUMN:
            {
                required_access.emplace_back(AccessType::ADD_COLUMN, alter.database, alter.table, column_name());
                break;
            }
            case ASTAlterCommand::DROP_COLUMN:
            {
                if (command_ast->clear_column)
                    required_access.emplace_back(AccessType::CLEAR_COLUMN, alter.database, alter.table, column_name());
                else
                    required_access.emplace_back(AccessType::DROP_COLUMN, alter.database, alter.table, column_name());
                break;
            }
            case ASTAlterCommand::MODIFY_COLUMN:
            {
                required_access.emplace_back(AccessType::MODIFY_COLUMN, alter.database, alter.table, column_name());
                break;
            }
            case ASTAlterCommand::COMMENT_COLUMN:
            {
                required_access.emplace_back(AccessType::COMMENT_COLUMN, alter.database, alter.table, column_name());
                break;
            }
            case ASTAlterCommand::MODIFY_ORDER_BY:
            {
                required_access.emplace_back(AccessType::ALTER_ORDER_BY, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::ADD_INDEX:
            {
                required_access.emplace_back(AccessType::ADD_INDEX, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::DROP_INDEX:
            {
                if (command_ast->clear_index)
                    required_access.emplace_back(AccessType::CLEAR_INDEX, alter.database, alter.table);
                else
                    required_access.emplace_back(AccessType::DROP_INDEX, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::MATERIALIZE_INDEX:
            {
                required_access.emplace_back(AccessType::MATERIALIZE_INDEX, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::ADD_CONSTRAINT:
            {
                required_access.emplace_back(AccessType::ADD_CONSTRAINT, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::DROP_CONSTRAINT:
            {
                required_access.emplace_back(AccessType::DROP_CONSTRAINT, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::MODIFY_TTL:
            {
                required_access.emplace_back(AccessType::MODIFY_TTL, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::MODIFY_SETTING:
            {
                required_access.emplace_back(AccessType::MODIFY_SETTING, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::ATTACH_PARTITION:
            {
                required_access.emplace_back(AccessType::ATTACH_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::DROP_PARTITION:
            {
                if (command_ast->detach)
                    required_access.emplace_back(AccessType::DETACH_PARTITION, alter.database, alter.table);
                else
                    required_access.emplace_back(AccessType::DROP_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::DROP_DETACHED_PARTITION:
            {
                required_access.emplace_back(AccessType::DROP_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::MOVE_PARTITION:
            {
                if (command_ast->move_destination_type == PartDestinationType::DISK)
                    required_access.emplace_back(AccessType::MOVE_PARTITION_TO_DISK, alter.database, alter.table);
                else if (command_ast->move_destination_type == PartDestinationType::VOLUME)
                    required_access.emplace_back(AccessType::MOVE_PARTITION_TO_VOLUME, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::REPLACE_PARTITION:
            {
                required_access.emplace_back(AccessType::COPY_PARTITION, command_ast->from_database, command_ast->from_table);
                required_access.emplace_back(AccessType::DROP_PARTITION | AccessType::ATTACH_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::FETCH_PARTITION:
            {
                required_access.emplace_back(AccessType::FETCH_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::FREEZE_PARTITION: [[fallthrough]];
            case ASTAlterCommand::FREEZE_ALL:
            {
                required_access.emplace_back(AccessType::FREEZE_PARTITION, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::LIVE_VIEW_REFRESH:
            {
                required_access.emplace_back(AccessType::REFRESH_LIVE_VIEW, alter.database, alter.table);
                break;
            }
            case ASTAlterCommand::NO_TYPE: break;
        }
    }

    required_access.replaceDatabase("", AccessRightsElement::CurrentDatabaseTag{});
    return required_access;
}

}
