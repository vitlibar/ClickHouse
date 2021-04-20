#pragma once

#include <Backup/IBackupEntry.h>


namespace DB
{
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class BackupRenaming;
using BackupRenamingPtr = std::shared_ptr<const BackupRenaming>;


/// Represents an attach query (for database or table) prepared to be included in a backup.
class BackupEntryFromAttachQuery : public IBackupEntry
{
public:
    BackupEntryFromAttachQuery(const String & path_in_backup_, const ASTPtr & attach_query_, const BackupRenamingPtr & renaming_);
    BackupEntryFromAttachQuery(const String & path_in_backup_, const String & path_to_file_with_attach_query_, const BackupRenamingPtr & renaming_);
    ~BackupEntryFromAttachQuery() override;

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
    UInt64 getDataSize() const override;
    UInt128 getChecksum() const override;
    std::optional<UInt128> tryGetChecksumFast() const override;

private:
    void prepareData() const;

    const ASTPtr attach_query;
    const String attach_query_raw_data;
    const String path_to_file_with_attach_query;
    const BackupRenamingPtr renaming;
    mutable std::optional<String> data;
    mutable std::optional<UInt128> checksum;
};

}
