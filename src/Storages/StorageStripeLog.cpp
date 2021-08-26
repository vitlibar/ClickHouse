#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <optional>

#include <Common/escapeForFileName.h>
#include <Common/Exception.h>

#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>

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
#include <Processors/Pipe.h>

#include <Poco/ScopedUnlock.h>

#include <cassert>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int TIMEOUT_EXCEEDED;
}


class StripeLogSource final : public SourceWithProgress
{
public:
    static Block getHeader(
        StorageStripeLog & storage,
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
        StorageStripeLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names,
        size_t max_read_buffer_size_,
        const std::shared_ptr<const IndexForNativeFormat> & index_,
        IndexForNativeFormat::Blocks::const_iterator index_begin_,
        IndexForNativeFormat::Blocks::const_iterator index_end_)
        : SourceWithProgress(
            getHeader(storage_, metadata_snapshot_, column_names, index_begin_, index_end_))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_read_buffer_size(max_read_buffer_size_)
        , index(index_)
        , index_begin(index_begin_)
        , index_end(index_end_)
    {
    }

    String getName() const override { return "StripeLog"; }

protected:
    Chunk generate() override
    {
        if (storage.file_checker.empty())
            return {};

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
                index.reset();
            }
        }

        return Chunk(res.getColumns(), res.rows());
    }

private:
    StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_read_buffer_size;

    std::shared_ptr<const IndexForNativeFormat> index;
    IndexForNativeFormat::Blocks::const_iterator index_begin;
    IndexForNativeFormat::Blocks::const_iterator index_end;
    Block header;

    /** optional - to create objects only on first reading
      *  and delete objects (release buffers) after the source is exhausted
      * - to save RAM when using a large number of sources.
      */
    bool started = false;
    std::optional<CompressedReadBufferFromFile> data_in;
    std::optional<NativeBlockInputStream> block_in;

    void start()
    {
        if (!started)
        {
            started = true;

            const String & data_file_path = storage.data_file_path;
            size_t buffer_size = std::min(max_read_buffer_size, storage.disk->getFileSize(data_file_path));

            data_in.emplace(storage.disk->readFile(data_file_path, buffer_size));
            block_in.emplace(*data_in, 0, index_begin, index_end);
        }
    }
};


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

        /// Load the index if not loaded yet. We have to do that now because we're going to update this index.
        storage.loadIndex(lock);

        /// If there were no files, save zero file sizes to be able to rollback in case of error.
        storage.saveFileSizes(lock);

        block_out = std::make_unique<NativeBlockOutputStream>(*data_out, 0, metadata_snapshot->getSampleBlock(), false, storage.index, storage.disk->getFileSize(storage.data_file_path));
    }

    String getName() const override { return "StripeLogSink"; }

    ~StripeLogSink() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.
                data_out.reset();
                data_out_compressed.reset();

                storage.file_checker.repair();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void consume(Chunk chunk) override
    {
        block_out->write(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    }

    void onFinish() override
    {
        if (done)
            return;

        block_out->writeSuffix();
        data_out->next();
        data_out_compressed->next();
        data_out_compressed->finalize();

        /// Save the updated index.
        storage.saveIndex(lock);

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
    std::unique_ptr<NativeBlockOutputStream> block_out;

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
    , data_file_path(table_path + "data.bin")
    , index_file_path(table_path + "index.mrk")
    , file_checker(disk, table_path + "sizes.json")
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
        file_checker.setPath(table_path + "sizes.json");
    }
    renameInMemory(new_table_id);
}


static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
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
    auto lock_timeout = getLockTimeout(context);
    loadIndex(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    if (index.empty())
        return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));

    NameSet column_names_set(column_names.begin(), column_names.end());
    auto index_with_selected_columns = std::make_shared<IndexForNativeFormat>(index.getColumns(column_names_set));

    size_t size = index_with_selected_columns->blocks.size();
    if (num_streams > size)
        num_streams = size;

    Pipes pipes;
    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        IndexForNativeFormat::Blocks::const_iterator begin = index_with_selected_columns->blocks.begin();
        IndexForNativeFormat::Blocks::const_iterator end = index_with_selected_columns->blocks.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<StripeLogSource>(
            *this, metadata_snapshot, column_names, context->getSettingsRef().max_read_buffer_size,
            index_with_selected_columns, begin, end));
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
    file_checker = FileChecker{disk, table_path + "sizes.json"};

    index.clear();
    index_loaded = false;
    num_indices_for_blocks_saved = 0;
}


void StorageStripeLog::loadIndex(std::chrono::seconds lock_timeout)
{
    if (index_loaded)
        return;

    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    loadIndex(lock);
}


void StorageStripeLog::loadIndex(const WriteLock & /* already locked for writing */)
{
    if (index_loaded)
        return;

    if (disk->exists(index_file_path))
    {
        CompressedReadBufferFromFile index_in(disk->readFile(index_file_path, 4096));
        index.read(index_in);
    }

    index_loaded = true;
    num_indices_for_blocks_saved = index.blocks.size();
}


void StorageStripeLog::saveIndex(const WriteLock & /* already locked for writing */)
{
    size_t num_indices_for_blocks = index.blocks.size();
    if (num_indices_for_blocks_saved == num_indices_for_blocks)
        return;

    size_t start = num_indices_for_blocks_saved;
    WriteMode write_mode = start ? WriteMode::Append : WriteMode::Rewrite;
    auto index_out_compressed = disk->writeFile(index_file_path, DBMS_DEFAULT_BUFFER_SIZE, write_mode);
    auto index_out = std::make_unique<CompressedWriteBuffer>(*index_out_compressed);

    for (size_t i = start; i != num_indices_for_blocks; ++i)
        index.blocks[i].write(*index_out);

    index_out->next();
    index_out_compressed->next();
    index_out_compressed->finalize();

    num_indices_for_blocks_saved = num_indices_for_blocks;
}


void StorageStripeLog::saveFileSizes(const WriteLock & /* already locked for writing */)
{
    if (file_checker.empty())
    {
        /// If there were no files, save zero file sizes.
        file_checker.setEmpty(data_file_path);
        file_checker.setEmpty(index_file_path);
    }
    else
    {
        file_checker.update(data_file_path);
        file_checker.update(index_file_path);
    }
    file_checker.save();
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
