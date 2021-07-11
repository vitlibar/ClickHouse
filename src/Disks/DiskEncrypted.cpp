#include <Disks/DiskEncrypted.h>

#if USE_SSL
#include <Interpreters/Context.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskEncrypted.h>

#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromFileEncrypted.h>
#include <IO/WriteBufferFromFileEncrypted.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DISK_INDEX;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

using DiskEncryptedPtr = std::shared_ptr<DiskEncrypted>;
using namespace FileEncryption;

class DiskEncryptedReservation : public IReservation
{
public:
    DiskEncryptedReservation(DiskEncryptedPtr disk_, std::unique_ptr<IReservation> reservation_)
        : disk(std::move(disk_)), reservation(std::move(reservation_))
    {
    }

    UInt64 getSize() const override { return reservation->getSize(); }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override { reservation->update(new_size); }

private:
    DiskEncryptedPtr disk;
    std::unique_ptr<IReservation> reservation;
};

ReservationPtr DiskEncrypted::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);
    if (!reservation)
        return {};
    return std::make_unique<DiskEncryptedReservation>(std::static_pointer_cast<DiskEncrypted>(shared_from_this()), std::move(reservation));
}

DiskEncrypted::DiskEncrypted(const String & name_, DiskPtr disk_, const String & key_, const String & path_)
    : DiskDecorator(disk_)
    , name(name_), key(key_), disk_path(path_)
    , disk_absolute_path(delegate->getPath() + disk_path)
{
    initialize();
}

void DiskEncrypted::initialize()
{
    // use wrapped_disk as an EncryptedDisk store
    if (disk_path.empty())
        return;

    if (disk_path.back() != '/')
        throw Exception("Disk path must ends with '/', but '" + disk_path + "' doesn't.", ErrorCodes::LOGICAL_ERROR);

    delegate->createDirectories(disk_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskEncrypted::readFile(
    const String & path,
    size_t buf_size,
    size_t estimated_size,
    size_t aio_threshold,
    size_t mmap_threshold,
    MMappedFileCache * mmap_cache) const
{
    auto wrapped_path = wrappedPath(path);
    auto buffer = delegate->readFile(wrapped_path, buf_size, estimated_size, aio_threshold, mmap_threshold, mmap_cache);

    InitVector iv;
    iv.read(*buffer);

    return std::make_unique<ReadBufferFromFileEncrypted>(buf_size, std::move(buffer), key, iv);
}

std::unique_ptr<WriteBufferFromFileBase> DiskEncrypted::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    InitVector iv;
    UInt64 old_file_size = 0;
    auto wrapped_path = wrappedPath(path);

    if (mode == WriteMode::Append && exists(path) && getFileSize(path))
    {
        auto read_buffer = delegate->readFile(wrapped_path, InitVector::kSize);
        iv.read(*read_buffer);
        old_file_size = getFileSize(path);
    }
    else
        iv = InitVector::random();

    auto buffer = delegate->writeFile(wrapped_path, buf_size, mode);
    return std::make_unique<WriteBufferFromFileEncrypted>(buf_size, std::move(buffer), key, iv, old_file_size);
}


size_t DiskEncrypted::getFileSize(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    size_t size = delegate->getFileSize(wrapped_path);
    return size > InitVector::kSize ? (size - InitVector::kSize) : 0;
}

void DiskEncrypted::truncateFile(const String & path, size_t size)
{
    auto wrapped_path = wrappedPath(path);
    delegate->truncateFile(wrapped_path, size ? (size + InitVector::kSize) : 0);
}

SyncGuardPtr DiskEncrypted::getDirectorySyncGuard(const String & path) const
{
    auto wrapped_path = wrappedPath(path);
    return delegate->getDirectorySyncGuard(wrapped_path);
}

void DiskEncrypted::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    ContextPtr /*context*/,
    const String & config_prefix,
    const DisksMap & map)
{
    String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
    if (wrapped_disk_name.empty())
        throw Exception("The wrapped disk name can not be empty. An encrypted disk is a wrapper over another disk. "
                        "Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    key = config.getString(config_prefix + ".key", "");
    if (key.empty())
        throw Exception("Encrypted disk key can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    auto wrapped_disk = map.find(wrapped_disk_name);
    if (wrapped_disk == map.end())
        throw Exception("The wrapped disk must have been announced earlier. No disk with name " + wrapped_disk_name + ". Disk " + name,
                        ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    delegate = wrapped_disk->second;

    disk_path = config.getString(config_prefix + ".path", "");
    initialize();
}

void registerDiskEncrypted(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr /*context*/,
                      const DisksMap & map) -> DiskPtr {

        String wrapped_disk_name = config.getString(config_prefix + ".disk", "");
        if (wrapped_disk_name.empty())
            throw Exception("The wrapped disk name can not be empty. An encrypted disk is a wrapper over another disk. "
                            "Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        String key = config.getString(config_prefix + ".key", "");
        if (key.empty())
            throw Exception("Encrypted disk key can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        FileEncryption::Encryptor::checkKeyLengthIsSupported(key.size());

        auto wrapped_disk = map.find(wrapped_disk_name);
        if (wrapped_disk == map.end())
            throw Exception("The wrapped disk must have been announced earlier. No disk with name " + wrapped_disk_name + ". Disk " + name,
                            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

        String relative_path = config.getString(config_prefix + ".path", "");

        return std::make_shared<DiskEncrypted>(name, wrapped_disk->second, key, relative_path);
    };
    factory.registerDiskType("encrypted", creator);
}

}


#endif
