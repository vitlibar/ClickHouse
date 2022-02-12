#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <optional>

#include <Common/escapeForFileName.h>
#include <Common/Exception.h>

#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedBufferHeader.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageStripeLog.h>
#include "StorageLogSettings.h"
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>

#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/IBackup.h>
#include <Backups/IRestoreTask.h>
#include <Backups/RestoreSettings.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <base/insertAtEnd.h>

#include <cassert>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int TIMEOUT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_RESTORE_TABLE;
}


namespace
{
    constexpr char DATA_FILE_NAME[] = "data.bin";
    constexpr char INDEX_FILE_NAME[] = "index.mrk";
    constexpr char SIZES_FILE_NAME[] = "sizes.json";
}


/// NOTE: The lock `StorageStripeLog::rwlock` is NOT kept locked while reading,
/// because we read ranges of data that do not change.
class StripeLogSource final : public SourceWithProgress
{
public:
    static Block getHeader(
        const StorageStripeLog & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & column_names,
        IndexForNativeFormat::Blocks::const_iterator index_begin,
        IndexForNativeFormat::Blocks::const_iterator index_end)
    {
        if (index_begin == index_end)
            return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());

        /// TODO: check if possible to always return storage.getSampleBlock()

        Block header;

        for (const auto & column : index_begin->columns)
        {
            auto type = DataTypeFactory::instance().get(column.type);
            header.insert(ColumnWithTypeAndName{ type, column.name });
        }

        return header;
    }

    StripeLogSource(
        const StorageStripeLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names,
        ReadSettings read_settings_,
        std::shared_ptr<const IndexForNativeFormat> indices_,
        IndexForNativeFormat::Blocks::const_iterator index_begin_,
        IndexForNativeFormat::Blocks::const_iterator index_end_,
        size_t file_size_)
        : SourceWithProgress(getHeader(storage_, metadata_snapshot_, column_names, index_begin_, index_end_))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , read_settings(std::move(read_settings_))
        , indices(indices_)
        , index_begin(index_begin_)
        , index_end(index_end_)
        , file_size(file_size_)
    {
    }

    String getName() const override { return "StripeLog"; }

protected:
    Chunk generate() override
    {
        Block res;
        start();

        if (block_in)
        {
            res = block_in->read();

            /// Freeing memory before destroying the object.
            if (!res)
            {
                block_in.reset();
                data_in.reset();
                indices.reset();
            }
        }

        return Chunk(res.getColumns(), res.rows());
    }

private:
    const StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    ReadSettings read_settings;

    std::shared_ptr<const IndexForNativeFormat> indices;
    IndexForNativeFormat::Blocks::const_iterator index_begin;
    IndexForNativeFormat::Blocks::const_iterator index_end;
    size_t file_size;

    Block header;

    /** optional - to create objects only on first reading
      *  and delete objects (release buffers) after the source is exhausted
      * - to save RAM when using a large number of sources.
      */
    bool started = false;
    std::optional<CompressedReadBufferFromFile> data_in;
    std::optional<NativeReader> block_in;

    void start()
    {
        if (!started)
        {
            started = true;

            String data_file_path = storage.table_path + "data.bin";
            data_in.emplace(storage.disk->readFile(data_file_path, read_settings.adjustBufferSize(file_size)));
            block_in.emplace(*data_in, 0, index_begin, index_end);
        }
    }
};


/// NOTE: The lock `StorageStripeLog::rwlock` is kept locked in exclusive mode while writing.
class StripeLogSink final : public SinkToStorage
{
public:
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    explicit StripeLogSink(
        StorageStripeLog & storage_, const StorageMetadataPtr & metadata_snapshot_, WriteLock && lock_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
        , data_out_compressed(storage.disk->writeFile(storage.data_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append))
        , data_out(std::make_unique<CompressedWriteBuffer>(
              *data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), storage.max_compress_block_size))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        /// Ensure that indices are loaded because we're going to update them.
        storage.loadIndices(lock);

        /// If there were no files, save zero file sizes to be able to rollback in case of error.
        storage.saveFileSizes(lock);

        size_t initial_data_size = storage.file_checker.getFileSize(storage.data_file_path);
        block_out = std::make_unique<NativeWriter>(*data_out, 0, metadata_snapshot->getSampleBlock(), false, &storage.indices, initial_data_size);
    }

    String getName() const override { return "StripeLogSink"; }

    ~StripeLogSink() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.

                /// No more writing.
                data_out.reset();
                data_out_compressed.reset();

                /// Truncate files to the older sizes.
                storage.file_checker.repair();

                /// Remove excessive indices.
                storage.removeUnsavedIndices(lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void consume(Chunk chunk) override
    {
        block_out->write(getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onFinish() override
    {
        if (done)
            return;

        data_out->next();
        data_out_compressed->next();
        data_out_compressed->finalize();

        /// Save the new indices.
        storage.saveIndices(lock);

        /// Save the new file sizes.
        storage.saveFileSizes(lock);

        done = true;

        /// unlock should be done from the same thread as lock, and dtor may be
        /// called from different thread, so it should be done here (at least in
        /// case of no exceptions occurred)
        lock.unlock();
    }

private:
    StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    WriteLock lock;

    std::unique_ptr<WriteBuffer> data_out_compressed;
    std::unique_ptr<CompressedWriteBuffer> data_out;
    std::unique_ptr<NativeWriter> block_out;

    bool done = false;
};


StorageStripeLog::StorageStripeLog(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage(table_id_)
    , disk(std::move(disk_))
    , table_path(relative_path_)
    , data_file_path(table_path + DATA_FILE_NAME)
    , index_file_path(table_path + INDEX_FILE_NAME)
    , file_checker(disk, table_path + SIZES_FILE_NAME)
    , max_compress_block_size(max_compress_block_size_)
    , log(&Poco::Logger::get("StorageStripeLog"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

    /// Ensure the file checker is initialized.
    if (file_checker.empty())
    {
        file_checker.setEmpty(data_file_path);
        file_checker.setEmpty(index_file_path);
    }

    if (!attach)
    {
        /// create directories if they do not exist
        disk->createDirectories(table_path);
    }
    else
    {
        try
        {
            file_checker.repair();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


StorageStripeLog::~StorageStripeLog() = default;


void StorageStripeLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        data_file_path = table_path + DATA_FILE_NAME;
        index_file_path = table_path + INDEX_FILE_NAME;
        file_checker.setPath(table_path + SIZES_FILE_NAME);
    }
    renameInMemory(new_table_id);
}


static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    Int64 lock_timeout = settings.lock_acquire_timeout.totalSeconds();
    if (settings.max_execution_time.totalSeconds() != 0 && settings.max_execution_time.totalSeconds() < lock_timeout)
        lock_timeout = settings.max_execution_time.totalSeconds();
    return std::chrono::seconds{lock_timeout};
}


Pipe StorageStripeLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    auto lock_timeout = getLockTimeout(context);
    loadIndices(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    size_t data_file_size = file_checker.getFileSize(data_file_path);
    if (!data_file_size)
        return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));

    auto indices_for_selected_columns
        = std::make_shared<IndexForNativeFormat>(indices.extractIndexForColumns(NameSet{column_names.begin(), column_names.end()}));

    size_t size = indices_for_selected_columns->blocks.size();
    if (num_streams > size)
        num_streams = size;

    ReadSettings read_settings = context->getReadSettings();
    Pipes pipes;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        IndexForNativeFormat::Blocks::const_iterator begin = indices_for_selected_columns->blocks.begin();
        IndexForNativeFormat::Blocks::const_iterator end = indices_for_selected_columns->blocks.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<StripeLogSource>(
            *this, metadata_snapshot, column_names, read_settings, indices_for_selected_columns, begin, end, data_file_size));
    }

    /// We do not keep read lock directly at the time of reading, because we read ranges of data that do not change.

    return Pipe::unitePipes(std::move(pipes));
}


SinkToStoragePtr StorageStripeLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    WriteLock lock{rwlock, getLockTimeout(context)};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return std::make_shared<StripeLogSink>(*this, metadata_snapshot, std::move(lock));
}


CheckResults StorageStripeLog::checkData(const ASTPtr & /* query */, ContextPtr context)
{
    ReadLock lock{rwlock, getLockTimeout(context)};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return file_checker.check();
}


void StorageStripeLog::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    disk->clearDirectory(table_path);

    indices.clear();
    file_checker.setEmpty(data_file_path);
    file_checker.setEmpty(index_file_path);

    indices_loaded = true;
    num_indices_saved = 0;
}


void StorageStripeLog::loadIndices(std::chrono::seconds lock_timeout)
{
    if (indices_loaded)
        return;

    /// We load indices with an exclusive lock (i.e. the write lock) because we don't want
    /// a data race between two threads trying to load indices simultaneously.
    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    loadIndices(lock);
}


void StorageStripeLog::loadIndices(const WriteLock & /* already locked exclusively */)
{
    if (indices_loaded)
        return;

    if (disk->exists(index_file_path))
    {
        CompressedReadBufferFromFile index_in(disk->readFile(index_file_path, ReadSettings{}.adjustBufferSize(4096)));
        indices.read(index_in);
    }

    indices_loaded = true;
    num_indices_saved = indices.blocks.size();
}


void StorageStripeLog::saveIndices(const WriteLock & /* already locked for writing */)
{
    size_t num_indices = indices.blocks.size();
    if (num_indices_saved == num_indices)
        return;

    size_t start = num_indices_saved;
    auto index_out_compressed = disk->writeFile(index_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    auto index_out = std::make_unique<CompressedWriteBuffer>(*index_out_compressed);

    for (size_t i = start; i != num_indices; ++i)
        indices.blocks[i].write(*index_out);

    index_out->next();
    index_out_compressed->next();
    index_out_compressed->finalize();

    num_indices_saved = num_indices;
}


void StorageStripeLog::removeUnsavedIndices(const WriteLock & /* already locked for writing */)
{
    if (indices.blocks.size() > num_indices_saved)
        indices.blocks.resize(num_indices_saved);
}


void StorageStripeLog::saveFileSizes(const WriteLock & /* already locked for writing */)
{
    file_checker.update(data_file_path);
    file_checker.update(index_file_path);
    file_checker.save();
}


BackupEntries StorageStripeLog::backup(ContextPtr context, const ASTs & partitions)
{
    if (!partitions.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support partitions", getName());

    auto lock_timeout = getLockTimeout(context);
    loadIndices(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    if (!file_checker.getFileSize(data_file_path))
        return {};

    auto temp_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, "tmp/backup_");
    auto temp_dir = temp_dir_owner->getPath();
    disk->createDirectories(temp_dir);

    BackupEntries backup_entries;

    /// data.bin
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String hardlink_file_path = temp_dir + "/" + DATA_FILE_NAME;
        disk->createHardLink(data_file_path, hardlink_file_path);
        backup_entries.emplace_back(
            DATA_FILE_NAME,
            std::make_unique<BackupEntryFromAppendOnlyFile>(
                disk, hardlink_file_path, file_checker.getFileSize(data_file_path), std::nullopt, temp_dir_owner));
    }

    /// index.mrk
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String hardlink_file_path = temp_dir + "/" + INDEX_FILE_NAME;
        disk->createHardLink(index_file_path, hardlink_file_path);
        backup_entries.emplace_back(
            INDEX_FILE_NAME,
            std::make_unique<BackupEntryFromAppendOnlyFile>(
                disk, hardlink_file_path, file_checker.getFileSize(index_file_path), std::nullopt, temp_dir_owner));
    }

    /// sizes.json
    String sizes_file_path = file_checker.getPath();
    backup_entries.emplace_back(SIZES_FILE_NAME, std::make_unique<BackupEntryFromSmallFile>(disk, sizes_file_path));

    /// columns.txt
    backup_entries.emplace_back(
        "columns.txt", std::make_unique<BackupEntryFromMemory>(getInMemoryMetadata().getColumns().getAllPhysical().toString()));

    /// count.txt
    size_t num_rows = 0;
    for (const auto & block : indices.blocks)
        num_rows += block.num_rows;
    backup_entries.emplace_back("count.txt", std::make_unique<BackupEntryFromMemory>(toString(num_rows)));

    return backup_entries;
}


class StripeLogRestoreTask : public IRestoreTask
{
    using WriteLock = StorageStripeLog::WriteLock;

public:
    StripeLogRestoreTask(
        const std::shared_ptr<StorageStripeLog> storage_,
        const ContextPtr & context_,
        const BackupPtr & backup_,
        const String & data_path_in_backup_,
        const StorageRestoreSettings & restore_settings_)
        : storage(storage_)
        , context(context_)
        , backup(backup_)
        , data_path_in_backup(data_path_in_backup_)
        , restore_settings(restore_settings_)
    {
    }

    RestoreTasks run() override
    {
        WriteLock lock{storage->rwlock, getLockTimeout(context)};
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        auto & file_checker = storage->file_checker;

        /// Load the indices if not loaded yet. We have to do that now because we're going to update these indices.
        storage->loadIndices(lock);

        if (restore_settings.throw_if_table_not_empty && !storage->indices.empty())
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "Cannot restore table {} because it's already not empty", storage->getStorageID().getFullTableName());

        /// If there were no files, save zero file sizes to be able to rollback in case of error.
        storage->saveFileSizes(lock);

        try
        {
            const auto & disk = storage->disk;
            const auto & data_file_path = storage->data_file_path;

            /// Read new indices.
            IndexForNativeFormat new_indices;
            {
                auto index_backup_entry = backup->readFile(data_path_in_backup + INDEX_FILE_NAME);
                auto index_in = index_backup_entry->getReadBuffer();
                CompressedReadBuffer index_compressed_in{*index_in};
                new_indices.read(index_compressed_in);
            }

            /// Start reading data from the backup.
            auto data_backup_entry = backup->readFile(data_path_in_backup + DATA_FILE_NAME);
            auto new_data = data_backup_entry->getReadBuffer();

            auto old_data_size = file_checker.getFileSize(data_file_path);
            if (old_data_size && restore_settings.deduplicate_data)
            {
                /// We need to deduplicate data, `new_data` should be seekable for this.
                std::optional<TemporaryFileOnDisk> temp_file_owner;
                auto new_data_seekable = makeReadBufferSeekable(std::move(new_data), context, temp_file_owner);

                /// Calculates checksums and find out which new data parts differs from the old data parts.
                auto old_data = disk->readFile(data_file_path, context->getReadSettings());
                auto old_parts = collectDataPartInfos(storage->indices, readCompressedBufferHeaders(*old_data));
                auto new_parts = collectDataPartInfos(new_indices, readCompressedBufferHeaders(*new_data_seekable));
                new_parts = getDiff(std::move(new_parts), std::move(old_parts));

                /// Copy the chosed data parts from `new_data` to `out_data`, and append the indices too.
                auto out_data = disk->writeFile(data_file_path, storage->max_compress_block_size, WriteMode::Append);
                size_t current_data_size = old_data_size;
                auto & indices = storage->indices;
                indices.blocks.reserve(indices.blocks.size() + new_parts.size());
                for (const auto & part : new_parts)
                {
                    new_data_seekable->seek(part.offset, SEEK_SET);
                    copyData(*new_data_seekable, *out_data, part.size);
                    auto & index_block = indices.blocks.emplace_back(new_indices.blocks[part.index]);
                    for (auto & column : index_block.columns)
                        column.location.offset_in_compressed_file += current_data_size - part.offset;
                    current_data_size += part.size;
                }
            }
            else
            {
                /// Copy all the data from `new_data` to `out_data`, and append the indices too.
                auto out_data = disk->writeFile(data_file_path, storage->max_compress_block_size, WriteMode::Append);
                copyData(*new_data, *out_data);
                auto & indices = storage->indices;
                indices.blocks.reserve(indices.blocks.size() + new_indices.blocks.size());
                for (size_t i = 0; i != new_indices.blocks.size(); ++i)
                {
                    auto & index_block = indices.blocks.emplace_back(new_indices.blocks[i]);
                    for (auto & column : index_block.columns)
                        column.location.offset_in_compressed_file += old_data_size;
                }
            }

            /// Finish writing.
            storage->saveIndices(lock);
            storage->saveFileSizes(lock);
            return {};
        }
        catch (...)
        {
            /// Rollback partial writes.
            file_checker.repair();
            storage->removeUnsavedIndices(lock);
            throw;
        }
    }

private:
    std::shared_ptr<StorageStripeLog> storage;
    ContextPtr context;
    BackupPtr backup;
    String data_path_in_backup;
    StorageRestoreSettings restore_settings;

    /// Converts a ReadBuffer into a SeekableReadBuffer. The function uses a temporary file if it's necessary.
    static std::unique_ptr<SeekableReadBuffer> makeReadBufferSeekable(std::unique_ptr<ReadBuffer> in, const ContextPtr & context, std::optional<TemporaryFileOnDisk> & temp_file)
    {
        if (dynamic_cast<SeekableReadBuffer *>(in.get()))
            return std::unique_ptr<SeekableReadBuffer>(static_cast<SeekableReadBuffer *>(in.release()));

        auto temp_disk = context->getTemporaryVolume()->getDisk();
        temp_file.emplace(temp_disk);
        auto out = temp_disk->writeFile(temp_file->getPath());
        copyData(*in, *out);
        out.reset();
        return temp_disk->readFile(temp_file->getPath(), context->getReadSettings());
    }

    /// Reads all compressed buffers' headers from a passed file.
    static CompressedBufferHeaders readCompressedBufferHeaders(SeekableReadBuffer & in)
    {
        CompressedBufferHeaders headers;
        while (!in.eof())
        {
            CompressedBufferHeader & header = headers.emplace_back();
            header.read(in);
            in.seek(header.getCompressedDataSize(), SEEK_CUR);
        }
        return headers;
    }

    /// Represents a part of table's data, used to find a difference between new and old data parts while restoring.
    struct DataPartInfo
    {
        size_t index;
        size_t offset;
        size_t size;
        CityHash_v1_0_2::uint128 checksum;

        struct LessOffset
        {
            bool operator()(const DataPartInfo & lhs, const DataPartInfo & rhs) const { return lhs.offset < rhs.offset; }
        };

        struct LessChecksum
        {
            bool operator()(const DataPartInfo & lhs, const DataPartInfo & rhs) const { return (lhs.size < rhs.size) || ((lhs.size == rhs.size) && (lhs.checksum < rhs.checksum)); }
        };
    };

    /// Collects information about data parts.
    static std::vector<DataPartInfo> collectDataPartInfos(const IndexForNativeFormat & indices, const CompressedBufferHeaders & headers)
    {
        if (indices.blocks.empty() || headers.empty())
            return {};

        std::vector<DataPartInfo> res;
        size_t current_offset = 0;
        size_t current_header = 0;

        for (size_t i = 0; i != indices.blocks.size(); ++i)
        {
            size_t size = 0;
            CityHash_v1_0_2::uint128 checksum = {0, 0};

            if (i < indices.blocks.size() - 1)
            {
                size_t next_offset = indices.blocks[i + 1].getMinOffsetInCompressedFile();
                while (current_offset + size != next_offset)
                {
                    if (((current_offset + size > next_offset) || (current_header >= headers.size()))
                        throw Exception("Unexpected data format", ErrorCodes::CANNOT_RESTORE_TABLE);
                    size += headers[current_header].getCompressedDataSize() + CompressedBufferHeader::kSize;
                    const auto & checksum_from_header = headers[current_header].checksum;
                    checksum = CityHash_v1_0_2::CityHash128WithSeed(reinterpret_cast<const char *>(&checksum_from_header), sizeof(checksum_from_header), checksum);
                    current_header++;
                }
            }
            else
            {
                while (current_header < headers.size())
                {
                    size += headers[current_header].getCompressedDataSize() + CompressedBufferHeader::kSize;
                    const auto & checksum_from_header = headers[current_header].checksum;
                    checksum = CityHash_v1_0_2::CityHash128WithSeed(reinterpret_cast<const char *>(&checksum_from_header), sizeof(checksum_from_header), checksum);
                    current_header++;
                }
            }

            auto & new_part_info = res.emplace_back();
            new_part_info.index = i;
            new_part_info.offset = current_offset;
            new_part_info.size = size;
            new_part_info.checksum = checksum;

            current_offset += size;
        }

        return res;
    }

    /// Finds data parts in `lhs` which are not contained in `rhs`.
    static std::vector<DataPartInfo> getDiff(std::vector<DataPartInfo> && lhs, std::vector<DataPartInfo> && rhs)
    {
        std::sort(lhs.begin(), lhs.end(), DataPartInfo::LessChecksum{});
        std::sort(rhs.begin(), rhs.end(), DataPartInfo::LessChecksum{});
        std::vector<DataPartInfo> diff;
        std::set_difference(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), std::back_inserter(diff), DataPartInfo::LessChecksum{});
        std::sort(diff.begin(), diff.end(), DataPartInfo::LessOffset{});
        return diff;
    }
};


RestoreTaskPtr StorageStripeLog::restoreFromBackup(ContextMutablePtr context, const ASTs & partitions, const BackupPtr & backup, const String & data_path_in_backup, const StorageRestoreSettings & restore_settings)
{
    if (!partitions.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support partitions", getName());

    return std::make_unique<StripeLogRestoreTask>(
        std::static_pointer_cast<StorageStripeLog>(shared_from_this()), context, backup, data_path_in_backup, restore_settings);
}


void registerStorageStripeLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    factory.registerStorage("StripeLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return StorageStripeLog::create(
            disk,
            args.relative_data_path,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.attach,
            args.getContext()->getSettings().max_compress_block_size);
    }, features);
}

}
