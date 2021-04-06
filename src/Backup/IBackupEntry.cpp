#include <Backup/IBackupEntry.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

UInt64 IBackupEntry::getDataSize() const
{
    if (!data_size)
        data_size = calculateDataSize();
    return *data_size;
}

UInt128 IBackupEntry::getChecksum() const
{
    if (!checksum)
        checksum = calculateChecksum();
    return *checksum;
}

UInt64 IBackupEntry::calculateDataSize() const
{
    auto read_buffer = getReadBuffer();
    read_buffer->ignoreAll();
    return read_buffer->count();
}

UInt128 IBackupEntry::calculateChecksum() const
{
    auto read_buffer = getReadBuffer();
    HashingReadBuffer hashing_in{*read_buffer};
    hashing_in.ignoreAll();
    auto u128 = hashing_in.getHash();
    return UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
}

}
