#include <Backup/IBackupEntry.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

UInt128 IBackupEntry::getChecksum() const
{
    auto maybe_checksum = tryGetChecksumFast();
    if (maybe_checksum)
        return *maybe_checksum;
    auto read_buffer = getReadBuffer();
    HashingReadBuffer hashing_in{*read_buffer};
    hashing_in.ignoreAll();
    auto u128 = hashing_in.getHash();
    return UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
}

}
