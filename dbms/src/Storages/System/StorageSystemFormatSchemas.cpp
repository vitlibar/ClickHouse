#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatSchemaLoader.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/System/StorageSystemFormatSchemas.h>


namespace DB
{
namespace
{
    bool extractFilterByPathFromWhereExpression(const IAST & where_expression, String & filter_by_path)
    {
        const ASTFunction* function = typeid_cast<const ASTFunction*>(&where_expression);
        if (!function || (function->name != "equals") || !function->arguments || (function->arguments->children.size() != 2))
            return false;
        const ASTIdentifier* identifier = typeid_cast<const ASTIdentifier*>(function->arguments->children[0].get());
        const ASTLiteral* literal = typeid_cast<const ASTLiteral*>(function->arguments->children[1].get());
        if (!identifier || (identifier->name != "path") || !literal || (literal->value.getType() != Field::Types::String))
            return false;
        filter_by_path = literal->value.get<String>();
        return true;
    }
}


StorageSystemFormatSchemas::StorageSystemFormatSchemas(const String & name_) : name(name_)
{
    NamesAndTypesList names_and_types{
        {"path", std::make_shared<DataTypeString>()},
        {"file_contents", std::make_shared<DataTypeString>()},
    };
    setColumns(ColumnsDescription(names_and_types));
}

BlockInputStreams StorageSystemFormatSchemas::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    check(column_names);
    Block sample_block = getSampleBlockForColumns(column_names);
    constexpr size_t npos = static_cast<size_t>(-1);
    const size_t path_pos = sample_block.has("path") ? sample_block.getPositionByName("path") : npos;
    const size_t file_contents_pos = sample_block.has("file_contents") ? sample_block.getPositionByName("file_contents") : npos;
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    FormatSchemaLoader & format_schema_loader = context.getFormatSchemaLoader();

    std::vector<String> paths;
    ASTSelectQuery query = typeid_cast<ASTSelectQuery &>(*query_info.query);
    String filter_by_path;
    if (query.where_expression && extractFilterByPathFromWhereExpression(*query.where_expression, filter_by_path)
            && format_schema_loader.schemaFileExists(filter_by_path))
        paths.emplace_back(filter_by_path);
    else
        paths = format_schema_loader.getAllPaths();

    for (const String & path : paths)
    {
        if (path_pos != npos)
            res_columns[path_pos]->insert(path);
        if (file_contents_pos != npos)
            res_columns[file_contents_pos]->insert(format_schema_loader.getSchemaFile(path));
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns))));
}

}
