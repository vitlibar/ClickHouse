#include <Backups/BackupIO.h>

#include <IO/copyData.h>
#include <IO/WriteBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/runAsyncWithOnFinishCallback.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void IBackupWriter::copyDataToFile(
    const CreateReadBufferFunction & create_read_buffer, UInt64 offset, UInt64 size, const String & dest_file_name, const ThreadPoolCallbackRunner<void> &)
{
    auto read_buffer = create_read_buffer();
    if (offset)
        read_buffer->seek(offset, SEEK_SET);
    auto write_buffer = writeFile(dest_file_name);
    copyData(*read_buffer, *write_buffer, size);
    write_buffer->finalize();
}

void IBackupWriter::copyDataToFileAsync(
    const CreateReadBufferFunction & create_read_buffer,
    UInt64 offset,
    UInt64 size,
    const String & dest_file_name,
    const ThreadPoolCallbackRunner<void> & scheduler,
    std::function<void(std::exception_ptr)> on_finish_callback)
{
    auto job = [this, create_read_buffer, offset, size, dest_file_name, scheduler]
    {
        copyDataToFile(create_read_buffer, offset, size, dest_file_name, scheduler);
    };
    runAsyncWithOnFinishCallback(scheduler, job, std::move(on_finish_callback));
}

void IBackupWriter::copyFileNative(
    DiskPtr /* src_disk */,
    const String & /* src_file_name */,
    UInt64 /* src_offset */,
    UInt64 /* src_size */,
    const String & /* dest_file_name */,
    const ThreadPoolCallbackRunner<void> &)
{
    throw Exception{ErrorCodes::NOT_IMPLEMENTED, "Native copy not implemented for backup writer"};
}

void IBackupWriter::copyFileNativeAsync(
    DiskPtr src_disk,
    const String & src_file_name,
    UInt64 src_offset,
    UInt64 src_size,
    const String & dest_file_name,
    const ThreadPoolCallbackRunner<void> & scheduler,
    std::function<void(std::exception_ptr)> on_finish_callback)
{
    auto job = [this, src_disk, src_file_name, src_offset, src_size, dest_file_name, scheduler]
    {
        copyFileNative(src_disk, src_file_name, src_offset, src_size, dest_file_name, scheduler);
    };
    runAsyncWithOnFinishCallback(scheduler, job, std::move(on_finish_callback));
}
}
