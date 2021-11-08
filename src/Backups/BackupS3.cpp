#include <Backups/BackupInDirectory.h>
#include <Backups/BackupFactory.h>
#include <Common/quoteString.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <Disks/DiskLocal.h>
#include <IO/copyData.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


BackupInDirectory::BackupInDirectory(
    const String & backup_name_,
    OpenMode open_mode_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, open_mode_, context_, base_backup_info_)
    , disk(disk_), path(path_)
{
    if (path.back() != '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to backup must end with '/', but {} doesn't.", getName(), quoteString(path));
    dir_path = fs::path(path).parent_path(); /// get path without terminating slash

    if (!disk)
    {
        auto fspath = fs::path{dir_path};
        if (!fspath.has_filename())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Backup {}: Path to a backup must be a directory path.", getName(), quoteString(path));
        path = fspath.filename() / "";
        dir_path = fs::path(path).parent_path(); /// get path without terminating slash
        String disk_path = fspath.remove_filename();
        disk = std::make_shared<DiskLocal>(disk_path, disk_path, 0);
    }

    open();
}


BackupInDirectory::~BackupInDirectory()
{
    close();
}

bool BackupInDirectory::backupExists() const
{
    return disk->isDirectory(dir_path);
}

void BackupInDirectory::startWriting()
{
    disk->createDirectories(dir_path);
}

void BackupInDirectory::removeAllFilesAfterFailure()
{
    if (disk->isDirectory(dir_path))
        disk->removeRecursive(dir_path);
}

BackupImpl::ReadBufferCreator BackupInDirectory::readFileImpl(const String & file_name) const
{
    String file_path = path + file_name;
    return [disk=disk, file_path]() -> std::unique_ptr<ReadBuffer> { return disk->readFile(file_path); };
}

void BackupInDirectory::addFileImpl(const String & file_name, ReadBuffer & read_buffer)
{
    String file_path = path + file_name;
    disk->createDirectories(fs::path(file_path).parent_path());
    auto out = disk->writeFile(file_path);
    copyData(read_buffer, *out);
}


void registerBackupEngineFile(BackupFactory & factory)
{
    auto creator_fn = [](const BackupFactory::CreateParams & params)
    {
        String backup_name = params.backup_info.toString();
        const String & engine_name = params.backup_info.backup_engine_name;
        const auto & args = params.backup_info.args;

        DiskPtr disk;
        String path;
        if (engine_name == "File")
        {
            if (args.size() != 1)
            {
                throw Exception(
                    "Backup engine 'File' requires 1 argument (path)",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            path = args[0].safeGet<String>();
        }
        else if (engine_name == "Disk")
        {
            if (args.size() < 1 || args.size() > 2)
            {
                throw Exception(
                    "Backup engine 'Disk' requires 1 or 2 arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            String disk_name = args[0].safeGet<String>();
            disk = params.context->getDisk(disk_name);
            if (args.size() >= 2)
                path = args[1].safeGet<String>();
        }

        return std::make_shared<BackupInDirectory>(backup_name, params.open_mode, disk, path, params.context, params.base_backup_info);
    };

    factory.registerBackupEngine("File", creator_fn);
    factory.registerBackupEngine("Disk", creator_fn);
}

}


context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri_.uri);

Aws::Auth::AWSCredentials credentials(upd.access_key_id, upd.secret_access_key);
HeaderCollection headers;
if (upd.access_key_id.empty())
{
    credentials = Aws::Auth::AWSCredentials(settings.access_key_id, settings.secret_access_key);
    headers = settings.headers;
}

S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
    settings.region,
    ctx->getRemoteHostFilter(), ctx->getGlobalContext()->getSettingsRef().s3_max_redirects);

client_configuration.endpointOverride = upd.uri.endpoint;
client_configuration.maxConnections = args.getLocalContext()->getSettingsRef().s3_max_connections;

auto max_single_read_retries = args.getLocalContext()->getSettingsRef().s3_max_single_read_retries;
auto min_upload_part_size = args.getLocalContext()->getSettingsRef().s3_min_upload_part_size;
auto max_single_part_upload_size = args.getLocalContext()->getSettingsRef().s3_max_single_part_upload_size;

upd.client = S3::ClientFactory::instance().create(
    client_configuration,
    upd.uri.is_virtual_hosted_style,
    credentials.GetAWSAccessKeyId(),
    credentials.GetAWSSecretKey(),
    "" /* settings.server_side_encryption_customer_key_base64*/,
    std::move(headers),
    settings.use_environment_credentials.value_or(ctx->getConfigRef().getBool("s3.use_environment_credentials", false)),
    settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)));



std::make_unique<WriteBufferFromS3>(client, uri.bucket, uri.key, min_upload_part_size, max_single_part_upload_size)

std::make_unique<ReadBufferFromS3>(client, bucket, current_key, max_single_read_retries, getContext()->getReadSettings()),




def isdir_s3(bucket, key: str) -> bool:
    """Returns T/F whether the directory exists."""
    objs = list(bucket.objects.filter(Prefix=key))
    return len(objs) > 1
