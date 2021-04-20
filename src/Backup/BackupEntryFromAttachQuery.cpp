#include <Backup/BackupEntryFromAttachQuery.h>
#include <Backup/BackupRenaming.h>
#include <Common/escapeForFileName.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
    String readFileWithMetadata(const String & path)
    {
        auto read_buffer = createReadBufferFromFileBase(path, 0, 0, 0);
        String str;
        readStringUntilEOF(str, *read_buffer);
        return str;
    }
}

BackupEntryFromAttachQuery::BackupEntryFromAttachQuery(
    const String & path_in_backup_, const ASTPtr & attach_query_, const BackupRenamingPtr & renaming_)
    : IBackupEntry(path_in_backup_)
    , attach_query(attach_query_)
    , attach_query_raw_data()
    , path_to_file_with_attach_query()
    , renaming(renaming_)
{
}

BackupEntryFromAttachQuery::BackupEntryFromAttachQuery(
    const String & path_in_backup_, const String & path_to_file_with_attach_query_, const BackupRenamingPtr & renaming_)
    : IBackupEntry(path_in_backup_)
    , attach_query(nullptr)
    , attach_query_raw_data(readFileWithMetadata(path_to_file_with_attach_query_))
    , path_to_file_with_attach_query(path_to_file_with_attach_query_)
    , renaming(renaming_)
{
}

BackupEntryFromAttachQuery::~BackupEntryFromAttachQuery() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromAttachQuery::getReadBuffer() const
{
    prepareData();
    return std::make_unique<ReadBufferFromString>(data);
}

UInt64 BackupEntryFromAttachQuery::getDataSize() const
{
    prepareData();
    return data->size();
}

UInt128 BackupEntryFromAttachQuery::getChecksum() const
{
    if (!checksum)
    {
        prepareData();
        auto u128 = CityHash_v1_0_2::CityHash128WithSeed(data->data(), data->size(), {0, 0});
        checksum = UInt128{u128.first, u128.second};
    }
    return *checksum;
}

std::optional<UInt128> BackupEntryFromAttachQuery::tryGetChecksumFast() const
{
    return getChecksum();
}

void BackupEntryFromAttachQuery::prepareData() const
{
    if (data)
        return;

    ASTPtr ast;
    if (attach_query)
    {
        ast = attach_query->clone();
    }
    else
    {
        ParserCreateQuery parser;
        const char * pos = attach_query_raw_data.data();
        String error_message;
        auto ast = tryParseQuery(
            parser,
            pos,
            pos + attach_query_raw_data.size(),
            error_message,
            /* hilite = */ false,
            "in file " + path_to_file_with_attach_query,
            /* allow_multi_statements = */ false,
            0,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);
        if (!ast)
            throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
    }

    auto & create = ast->as<ASTCreateQuery &>();

    if ((create.table == TABLE_WITH_UUID_NAME_PLACEHOLDER) && (create.uuid != UUIDHelpers::Nil))
    {
        create.table = unescapeForFileName(std::filesystem::path(path_to_file_with_attach_query).stem());
    }

    //if (create.table.empty() && (create.database == TABLE_WITH_UUID_NAME_PLACEHOLDER) && (create.uuid != UUIDHelpers::Nil))
    //{
    //    create.database = unescapeForFileName(Poco::Path(path_to_file_with_attach_query).makeFile().getBaseName());
    //}

    create.uuid = UUIDHelpers::Nil;
    create.attach = true;

    if (create.table.empty())
    {
        create.database = renaming->getNewName(create.database);
    }
    else
    {
        std::tie(create.database, create.table) = renaming->getNewName(std::make_pair(create.database, create.table));
    }

    data = serializeAST(*ast);
}

}
