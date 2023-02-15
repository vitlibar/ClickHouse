#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/StorageS3Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/types.h>
#include <functional>
#include <memory>


namespace DB
{
class SeekableReadBuffer;

struct CopyS3FileSettings
{
    S3Settings::RequestSettings request_settings;

    /// A part of the source to copy.
    size_t offset = 0;
    size_t size = static_cast<size_t>(-1); /// -1 here means the function copyS3File() will copy the whole file.

    /// Metadata to store on the target. If set the metadata on the target will be replaced with it.
    std::optional<std::map<String, String>> object_metadata;

    /// Whether the copy functions should return immediately after scheduling without waiting for completion.
    bool async = false;

    /// A callback which will be called on completion. Optional. If `async == true` this callback will be called from another thread.
    /// This callback will be called no matter if the operation is successful or not.
    std::function<void(std::exception_ptr)> on_finish_callback;

    /// Scheduler for uploading parts in parallel and for async execution.
    /// Optional if `async == false`, required if `async == true`.
    ThreadPoolCallbackRunner<void> scheduler;

    /// Used for profiling events only.
    bool for_disk_s3 = false;
};

/// Copies a file from S3 to S3.
/// The same functionality can be done by using the function copyData() and the classes ReadBufferFromS3 and WriteBufferFromS3
/// however copyS3File() is faster and spends less network traffic and memory.
void copyS3File(
    const std::shared_ptr<const S3::Client> & s3_client,
    String src_bucket,
    String src_key,
    String dest_bucket,
    String dest_key,
    CopyS3FileSettings copy_settings);

/// Copies data from any seekable source to S3.
/// The same functionality can be done by using the function copyData() and the class WriteBufferFromS3
/// however copyDataToS3File() is faster and spends less memory.
/// The callback `create_read_buffer` can be called from multiple threads in parallel, so that should be thread-safe.
void copyDataToS3File(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    String dest_bucket,
    String dest_key,
    CopyS3FileSettings copy_settings);

}

#endif
