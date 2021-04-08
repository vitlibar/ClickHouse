#include <Backup/BackupEntryFromFile.h>
#include <Common/createHardLink.h>
#include <Common/filesystemHelpers.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Poco/Path.h>


namespace DB
{
namespace
{
    /// It's the maximum size of the file to store it in RAM.
    /// If the file is larger than this value a disk copy will be made.
    constexpr size_t MAX_SIZE_TO_STORE_MUTABLE_IN_MEMORY = 1024;
}


BackupEntryFromFile::BackupEntryFromFile(
    const String & path_in_backup_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_, Flags flags_)
    : BackupEntryFromFile(path_in_backup_, file_size_, checksum_, NoInitTag{})
{
    init(file_path_, flags_);
}

/// Internal constructor.
BackupEntryFromFile::BackupEntryFromFile(const String & path_in_backup_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_, NoInitTag)
    : IBackupEntry(path_in_backup_, file_size_, checksum_)
{
}

void BackupEntryFromFile::init(const String & file_path_, Flags flags_)
{
    file_path = file_path_;
    flags = flags_;
    assert(static_cast<size_t>(flags & MUTABLE) + static_cast<size_t>(flags & IMMUTABLE) + static_cast<size_t>(flags & APPENDABLE) == 1);
    assert(static_cast<size_t>(flags & REMOVABLE) + static_cast<size_t>(flags & ALWAYS_EXISTS) == 1);

    if (flags & MUTABLE)
    {
        if (getDataSize() <= MAX_SIZE_TO_STORE_MUTABLE_IN_MEMORY)
        {
            data.emplace();
            data->resize(getDataSize());
            createReadBufferFromFileBase(file_path, 0, 0, 0)->readStrict(data->data(), data->size());
        }
        else
        {
            createTemporaryFile(false);
            Poco::File(file_path).copyTo(temporary_file->path());
        }
        return;
    }

    if (flags & ALWAYS_EXISTS)
    {
        if (flags & APPENDABLE)
            getDataSize();
        return;
    }

    if (flags & REMOVABLE)
    {
        if (flags & APPENDABLE)
            getDataSize();
        createTemporaryFile(true);
        createHardLink(temporary_file->path(), file_path);
        return;
    }
}

BackupEntryFromFile::~BackupEntryFromFile() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromFile::getReadBuffer() const
{
    if (data)
        return std::make_unique<ReadBufferFromString>(*data);

    if (temporary_file)
    {
        if (flags & APPENDABLE)
            return std::make_unique<LimitReadBuffer>(createReadBufferFromFileBase(temporary_file->path(), 0, 0, 0), getDataSize(), false);
        else
            return createReadBufferFromFileBase(temporary_file->path(), 0, 0, 0);
    }

    if (flags & APPENDABLE)
        return std::make_unique<LimitReadBuffer>(createReadBufferFromFileBase(file_path, 0, 0, 0), getDataSize(), false);
    else
        return createReadBufferFromFileBase(file_path, 0, 0, 0);
}

UInt64 BackupEntryFromFile::calculateDataSize() const
{
    return getStatInfo().size;
}

const BackupEntryFromFile::StatInfo & BackupEntryFromFile::getStatInfo() const
{
    if (stat_info)
        return *stat_info;
    auto st = getStat(file_path);
    stat_info.emplace(StatInfo{static_cast<UInt64>(st.st_size), st.st_dev});
    return *stat_info;
}

void BackupEntryFromFile::createTemporaryFile(bool same_device_is_required)
{
    if (!same_device_is_required)
    {
        temporary_file = std::make_unique<TemporaryFile>();
        return;
    }

    static const auto temp_directory_device_id = getStat(Poco::Path::temp()).st_dev;
    if (getStatInfo().device_id == temp_directory_device_id)
    {
        temporary_file = std::make_unique<TemporaryFile>();
        return;
    }

    String directory_path = Poco::Path(file_path).setFileName("").toString();
    temporary_file = std::make_unique<TemporaryFile>(directory_path);
}

}
