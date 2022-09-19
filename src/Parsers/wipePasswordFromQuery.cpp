#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/wipePasswordFromQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

void wipePasswordFromQuery(ASTPtr & ast)
{
    if (auto * create_query = ast->as<ASTCreateUserQuery>())
    {
        auto new_create_query = typeid_cast<std::shared_ptr<ASTCreateUserQuery>>(ast->clone());
        new_create_query->show_password = false;
        ast = new_create_query;
    }
}

}
