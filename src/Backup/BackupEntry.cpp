#include <Backup/BackupEntry.h>


namespace DB
{

BackupEntry::BackupEntry() = default;
BackupEntry::~BackupEntry() = default;
BackupEntry::BackupEntry(BackupEntry && src) = default;
BackupEntry & BackupEntry::operator =(BackupEntry && src) = default;

}
