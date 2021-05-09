#include <Backup/IBackupEntry.h>
#include <IO/HashingReadBuffer.h>


namespace DB
{

UInt128 IBackupEntry::calculateChecksum(std::unique_ptr<ReadBuffer> read_buffer)
{
    HashingReadBuffer hashing_in{*read_buffer};
    hashing_in.ignoreAll();
    auto u128 = hashing_in.getHash();
    return UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
}

}
