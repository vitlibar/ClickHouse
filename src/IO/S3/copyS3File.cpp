#include <IO/S3/copyS3File.h>

#if USE_AWS_S3

#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <Common/runAsyncWithOnFinishCallback.h>
#include <IO/LimitSeekableReadBuffer.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/StdStreamFromReadBuffer.h>

#include <IO/S3/Requests.h>

namespace ProfileEvents
{
    extern const Event S3CreateMultipartUpload;
    extern const Event S3CompleteMultipartUpload;
    extern const Event S3PutObject;
    extern const Event S3CopyObject;
    extern const Event S3UploadPart;
    extern const Event S3UploadPartCopy;

    extern const Event DiskS3CreateMultipartUpload;
    extern const Event DiskS3CompleteMultipartUpload;
    extern const Event DiskS3PutObject;
    extern const Event DiskS3CopyObject;
    extern const Event DiskS3UploadPart;
    extern const Event DiskS3UploadPartCopy;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


namespace
{
    class BaseS3FileCopier : public std::enable_shared_from_this<BaseS3FileCopier>
    {
    public:
        BaseS3FileCopier(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            const CopyS3FileSettings & copy_settings_,
            Poco::Logger * log_)
            : client_ptr(client_ptr_)
            , dest_bucket(dest_bucket_)
            , dest_key(dest_key_)
            , offset(copy_settings_.offset)
            , size(copy_settings_.size)
            , request_settings(copy_settings_.request_settings)
            , upload_settings(request_settings.getUploadSettings())
            , object_metadata(copy_settings_.object_metadata)
            , schedule(copy_settings_.scheduler)
            , async(copy_settings_.async)
            , on_finish(copy_settings_.on_finish_callback)
            , for_disk_s3(copy_settings_.for_disk_s3)
            , log(log_)
        {
        }

        virtual ~BaseS3FileCopier() = default;

        /// Main function: makes a copy or starts making a copy.
        void run()
        {
            if (async && !schedule)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Async copying to S3 cannot work without a scheduler");

            /// Exceptions can be thrown only from this call stack.
            SCOPE_EXIT( on_finish.onExitInitialScope() );

            auto job = [this, keep_this_alive = shared_from_this()]
            {
                try
                {
                    if (size == static_cast<size_t>(-1))
                        size = calculateSize();

                    if (shouldUseMultipartCopy())
                        performMultipartCopy();
                    else
                        performSinglepartCopy();
                }
                catch(...)
                {
                    /// Couldn't calculate the size or make a copy.
                    onFinish(std::current_exception());
                }
            };

            if (async)
            {
                try
                {
                    schedule(job, 0);
                }
                catch (...)
                {
                    /// Couldn't schedule a job.
                    onFinish(std::current_exception());
                }
            }
            else
            {
                job();
            }
        }

    protected:
        const std::shared_ptr<const S3::Client> client_ptr;
        const String dest_bucket;
        const String dest_key;
        const size_t offset;
        size_t size;
        const S3Settings::RequestSettings request_settings;
        const S3Settings::RequestSettings::PartUploadSettings & upload_settings;
        const std::optional<std::map<String, String>> object_metadata;
        const ThreadPoolCallbackRunner<void> schedule;
        const bool async;
        OnFinishCallbackRunnerForAsyncJob<void> on_finish;
        const bool for_disk_s3;
        Poco::Logger * log;

        size_t num_parts =  0;
        size_t normal_part_size = 0;
        String multipart_upload_id;

        struct CopyPartTask
        {
            std::unique_ptr<Aws::AmazonWebServiceRequest> req;
            int part_number = 0;
            bool is_finished = false;
            String tag;
        };

        std::vector<CopyPartTask> TSA_GUARDED_BY(bg_tasks_mutex) bg_tasks;
        size_t num_finished_bg_tasks TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        size_t num_part_tags_received TSA_GUARDED_BY(bg_tasks_mutex) = 0;
        std::exception_ptr TSA_GUARDED_BY(bg_tasks_mutex) bg_error;
        mutable std::mutex bg_tasks_mutex;
        mutable Poco::Event bg_tasks_finished;

        /// Called at the end of copying no matter if it was successful or not.
        void onFinish(std::exception_ptr error = nullptr) { on_finish.run(error); }

        /// Called to determinine the size to copy if it's set to -1.
        virtual size_t calculateSize() const = 0;

        virtual bool shouldUseMultipartCopy() const = 0;

        enum class SinglepartCopyResult
        {
            OK,                      /// Singlepart copy has succeeded.
            TRY_SWITCH_TO_MULTIPART, /// Singlepart copy has failed but it seems we can try using multipart copy instead.
            /// In all other cases processSinglepartCopyRequest() just throws exceptions.
        };

        /// Copies the source in a single copy operation.
        void performSinglepartCopy()
        {
            chassert(size != static_cast<size_t>(-1)); /// size must be set or calculated at this point

            auto req = fillSinglepartCopyRequest();
            if (processSinglepartCopyRequest(*req) == SinglepartCopyResult::TRY_SWITCH_TO_MULTIPART)
            {
                performMultipartCopy();
                return;
            }
            checkObjectAfterUpload();
            onFinish();
        }

        virtual std::unique_ptr<Aws::AmazonWebServiceRequest> fillSinglepartCopyRequest() = 0;
        virtual SinglepartCopyResult processSinglepartCopyRequest(const Aws::AmazonWebServiceRequest & request) = 0;

        /// Copies the source using multiple parts which are copied in parallel.
        void performMultipartCopy()
        {
            chassert(size != static_cast<size_t>(-1)); /// size must be set or calculated at this point

            if (!size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart copy for an empty file. This must not happen");

            calculatePartSize();
            createMultipartUpload();
            copyParts();
            /// completeMultipartUpload() will be called by processCopyTask() after copying the last part.

            if (!async) /// If `async == true` then processCopyTask() will call onFinish() after copying the last part.
            {
                waitForBackgroundTasks();
                onFinish(get_background_error());
            }
        }

        void createMultipartUpload()
        {
            chassert(multipart_upload_id.empty()); /// createMultipartUpload() must not be called multiple times.

            S3::CreateMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request.SetContentType("binary/octet-stream");

            if (object_metadata.has_value())
                request.SetMetadata(object_metadata.value());

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request.SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            ProfileEvents::increment(ProfileEvents::S3CreateMultipartUpload);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3CreateMultipartUpload);

            auto outcome = client_ptr->CreateMultipartUpload(request);

            if (outcome.IsSuccess())
            {
                multipart_upload_id = outcome.GetResult().GetUploadId();
                LOG_TRACE(log, "Started multipart upload of {} parts to \"{}\". Part size: {}, Total size: {}, Bucket: {}, Upload id: {}",
                          num_parts, dest_key, normal_part_size, size, dest_bucket, multipart_upload_id);
            }
            else
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Couldn't start multipart upload of {} parts to \"{}\": {}. Part size: {}, Total size: {}, Bucket: {}",
                    num_parts, dest_key, outcome.GetError().GetMessage(), normal_part_size, size, dest_bucket);
        }

        void completeMultipartUpload()
        {
            std::lock_guard lock{bg_tasks_mutex};
            LOG_TRACE(log, "Completing multipart upload to \"{}\". Bucket: {}, Parts: {}, Upload id: {}",
                      dest_key, dest_bucket, num_part_tags_received, multipart_upload_id);

            /// Got no errors and finished all background tasks.
            chassert(!bg_error && (bg_tasks.size() == num_parts) && (num_finished_bg_tasks == num_parts) && (num_part_tags_received == num_parts));

            S3::CompleteMultipartUploadRequest request;
            request.SetBucket(dest_bucket);
            request.SetKey(dest_key);
            request.SetUploadId(multipart_upload_id);

            Aws::S3::Model::CompletedMultipartUpload multipart_upload;
            for (const auto & task : bg_tasks)
            {
                Aws::S3::Model::CompletedPart part;
                multipart_upload.AddParts(part.WithETag(task.tag).WithPartNumber(task.part_number));
            }

            request.SetMultipartUpload(multipart_upload);

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUpload);
                if (for_disk_s3)
                    ProfileEvents::increment(ProfileEvents::DiskS3CompleteMultipartUpload);

                auto outcome = client_ptr->CompleteMultipartUpload(request);

                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Completed multipart upload to \"{}\". Upload id: {}", dest_key, multipart_upload_id);
                    break;
                }

                bool can_retry = (retries < max_retries);
                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && can_retry)
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    /// BTW, NO_SUCH_UPLOAD is expected error and we shouldn't retry it
                    LOG_INFO(log, "Couldn't complete multipart upload to \"{}\": {}. Bucket: {}, Parts: {}, Upload id: {}. Will retry",
                             dest_key, outcome.GetError().GetMessage(), dest_bucket, num_parts, multipart_upload_id);
                    continue; /// will retry
                }

                throw S3Exception(outcome.GetError().GetErrorType(),
                                  "Couldn't complete multipart upload to \"{}\": {}. Bucket: {}, Parts: {}, Upload id: {}",
                                  dest_key, outcome.GetError().GetMessage(), dest_bucket, num_parts, multipart_upload_id);
            }
        }

        void abortMultipartUpload()
        {
            std::lock_guard lock{bg_tasks_mutex};
            LOG_TRACE(log, "Aborting multipart upload to \"{}\". Bucket: {}, Parts uploaded: {} / {}, Upload id: {}",
                      dest_key, dest_bucket, num_part_tags_received, num_parts, multipart_upload_id);

            chassert(bg_error && (bg_tasks.size() <= num_parts) && (num_finished_bg_tasks == bg_tasks.size()) && (num_part_tags_received < num_parts));

            try
            {
                S3::AbortMultipartUploadRequest abort_request;
                abort_request.SetBucket(dest_bucket);
                abort_request.SetKey(dest_key);
                abort_request.SetUploadId(multipart_upload_id);
                client_ptr->AbortMultipartUpload(abort_request);
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }
        }

        /// Checks the destination object exists after copying.
        void checkObjectAfterUpload()
        {
            if (!request_settings.check_objects_after_upload)
                return;
            LOG_TRACE(log, "Checking object \"{}\" exists after upload", dest_key);
            S3::checkObjectExists(*client_ptr, dest_bucket, dest_key, {}, request_settings, {}, "Immediately after upload");
            LOG_TRACE(log, "Object \"{}\" exists after upload", dest_key);
        }

        /// Calculates `normal_part_size` and `num_parts`.
        void calculatePartSize()
        {
            if (!size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chosen multipart upload for an empty file. This must not happen");

            auto max_part_number = upload_settings.max_part_number;
            auto min_upload_part_size = upload_settings.min_upload_part_size;
            auto max_upload_part_size = upload_settings.max_upload_part_size;

            if (!max_part_number)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_part_number must not be 0");
            else if (!min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "min_upload_part_size must not be 0");
            else if (max_upload_part_size < min_upload_part_size)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "max_upload_part_size must not be less than min_upload_part_size");

            size_t part_size = min_upload_part_size;
            num_parts = (size + part_size - 1) / part_size;

            if (num_parts > max_part_number)
            {
                part_size = (size + max_part_number - 1) / max_part_number;
                num_parts = (size + part_size - 1) / part_size;
            }

            if (part_size > max_upload_part_size)
            {
                part_size = max_upload_part_size;
                num_parts = (size + part_size - 1) / part_size;
            }

            if (num_parts < 1 || num_parts > max_part_number || part_size < min_upload_part_size
                || part_size > max_upload_part_size)
            {
                String msg;
                if (num_parts < 1)
                    msg = "Number of parts is zero";
                else if (num_parts > max_part_number)
                    msg = fmt::format("Number of parts exceeds {}", num_parts, max_part_number);
                else if (part_size < min_upload_part_size)
                    msg = fmt::format("Size of a part is less than {}", part_size, min_upload_part_size);
                else
                    msg = fmt::format("Size of a part exceeds {}", part_size, max_upload_part_size);

                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "{} while writing {} bytes to \"{}\". Check max_part_number = {}, min_upload_part_size = {}, max_upload_part_size = {}",
                    msg, size, dest_key, max_part_number, min_upload_part_size, max_upload_part_size);
            }

            /// We've calculated the size of a normal part (the final part can be smaller).
            normal_part_size = part_size;
        }

        /// Copies multiple parts in parallel.
        void copyParts()
        {
            chassert(size > 0 && num_parts > 0 && normal_part_size > 0);

            {
                /// copyPart() will add background tasks to `bg_tasks`.
                std::lock_guard lock{bg_tasks_mutex};
                bg_tasks.reserve(num_parts);
            }

            for (int part_number = 1; part_number <= static_cast<int>(num_parts); ++part_number)
            {
                if (!copyPart(part_number))
                    break;
            }
        }

        size_t get_part_offset(int part_number) const
        {
            return offset + (part_number - 1) * normal_part_size;
        }

        size_t get_part_size(int part_number) const
        {
            if (part_number < static_cast<int>(num_parts))
                return normal_part_size;
            else
                return size - (num_parts - 1) * normal_part_size;
        }

        bool copyPart(int part_number)
        {
            LOG_TRACE(log, "Writing part #{} to \"{}\". Part offset: {}, Part size: {}, Upload id: {}",
                      part_number, dest_key, get_part_offset(part_number), get_part_size(part_number), multipart_upload_id);

            CopyPartTask * task = nullptr;
            {
                std::lock_guard lock(bg_tasks_mutex);
                if (bg_error)
                    return false; /// Already got error from other tasks, no more tasks will be scheduled.
                task = &bg_tasks.emplace_back();
                task->part_number = part_number;
            }

            try
            {
                task->req = fillCopyPartRequest(part_number);

                if (schedule)
                    schedule([task, this, keep_this_alive = shared_from_this()] { processCopyTask(*task); }, 0);
                else
                    processCopyTask(*task);
                return true;
            }
            catch (...)
            {
                /// Couldn't copy or couldn't schedule.
                finishCopyTask(*task, {}, std::current_exception());
                return false;
            }
        }

        void processCopyTask(CopyPartTask & task)
        {
            if (get_background_error())
            {
                /// Already got error from other tasks, we have to call finishCopyTask() to increase `num_finished_bg_tasks` in order
                /// to call finishMultipartUpload() when it's the right time.
                finishCopyTask(task);
                return;
            }

            try
            {
                String tag = processCopyPartRequest(*task.req, task.part_number);
                finishCopyTask(task, tag);
            }
            catch (...)
            {
                finishCopyTask(task, {}, std::current_exception());
            }
        }

        void finishCopyTask(CopyPartTask & task, const String & tag = {}, std::exception_ptr error = nullptr)
        {
            bool finish_multipart_upload = false;
            bool abort_multipart_upload = false;

            {
                std::lock_guard lock(bg_tasks_mutex);
                bool is_first_error = false;

                if (error && !bg_error)
                {
                    bg_error = error;
                    is_first_error = true;
                }
                else if (task.tag.empty() && !tag.empty())
                {
                    task.tag = tag;
                    ++num_part_tags_received;
                }

                if (!task.is_finished)
                {
                    task.is_finished = true;
                    ++num_finished_bg_tasks;
                }

                if (!error)
                    LOG_TRACE(log, "Finished writing part #{} to \"{}\". Parts ready: {} / {}, Upload id: {}",
                              task.part_number, dest_key, num_part_tags_received, num_parts, multipart_upload_id);

                if (bg_error)
                {
                    if (num_finished_bg_tasks == bg_tasks.size())
                    {
                        finish_multipart_upload = true;
                    }
                    else if (is_first_error)
                    {
                        abort_multipart_upload = true;
                    }
                }
                else
                {
                    if (num_finished_bg_tasks == num_parts)
                    {
                        finish_multipart_upload = true;
                    }
                }
            }

            if (finish_multipart_upload)
                finishMultipartUpload();
            else if (abort_multipart_upload)
                abortMultipartUpload();
        }

        /// Called at the end of a multipart copying no matter if it was successful or not.
        void finishMultipartUpload()
        {
            bool ok = !get_background_error();

            if (ok)
            {
                try
                {
                    completeMultipartUpload();
                }
                catch (...)
                {
                    set_background_error(std::current_exception());
                    ok = false;
                }
            }

            if (!ok)
            {
                /// Maybe abortMultipartUpload() was already called but it can't cancel copying of parts which are already in progress,
                /// so we need to call abortMultipartUpload() here just in case.
                abortMultipartUpload();
            }

            if (ok)
            {
                try
                {
                    checkObjectAfterUpload();
                }
                catch (...)
                {
                    set_background_error(std::current_exception());
                }
            }

            if (async)
                onFinish(get_background_error());
            else
                bg_tasks_finished.set();
        }

        void waitForBackgroundTasks() const
        {
            if (!async && schedule)
                bg_tasks_finished.wait();
        }

        std::exception_ptr get_background_error() const
        {
            std::lock_guard lock{bg_tasks_mutex};
            return bg_error;
        }

        void set_background_error(std::exception_ptr error)
        {
            std::lock_guard lock{bg_tasks_mutex};
            if (!bg_error)
                bg_error = error;
        }

        virtual std::unique_ptr<Aws::AmazonWebServiceRequest> fillCopyPartRequest(int part_number) = 0;
        virtual String processCopyPartRequest(const Aws::AmazonWebServiceRequest & request, int part_number) = 0;
    };

    /// Helper class to help implementing copyDataToS3File().
    class DataToS3FileCopier : public BaseS3FileCopier
    {
    public:
        DataToS3FileCopier(
            const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer_,
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & dest_bucket_,
            const String & dest_key_,
            const CopyS3FileSettings & copy_settings_)
            : BaseS3FileCopier(client_ptr_, dest_bucket_, dest_key_, copy_settings_, &Poco::Logger::get("copyDataToS3File"))
            , create_read_buffer(create_read_buffer_)
        {
        }

    private:
        std::function<std::unique_ptr<SeekableReadBuffer>()> create_read_buffer;

        size_t calculateSize() const override
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "copyDataToS3File: Unknown size of data");
        }

        bool shouldUseMultipartCopy() const override
        {
            return size > request_settings.getUploadSettings().max_single_part_upload_size;
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> fillSinglepartCopyRequest() override
        {
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), offset, size);

            auto request = std::make_unique<S3::PutObjectRequest>();
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetContentLength(size);
            request->SetBody(std::make_unique<StdStreamFromReadBuffer>(std::move(read_buffer), size));

            if (object_metadata.has_value())
                request->SetMetadata(object_metadata.value());

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request->SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");

            return request;
        }

        SinglepartCopyResult processSinglepartCopyRequest(const Aws::AmazonWebServiceRequest & request) override
        {
            const auto & req = typeid_cast<const S3::PutObjectRequest &>(request);

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 0;; ++retries)
            {
                ProfileEvents::increment(ProfileEvents::S3PutObject);
                if (for_disk_s3)
                    ProfileEvents::increment(ProfileEvents::DiskS3PutObject);

                auto outcome = client_ptr->PutObject(req);

                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Completed singlepart upload to \"{}\". Bucket: {}, Size: {}", dest_key, dest_bucket, size);
                    return SinglepartCopyResult::OK;
                }
                
                if ((outcome.GetError().GetExceptionName() == "EntityTooLarge" || outcome.GetError().GetExceptionName() == "InvalidRequest") && size)
                {
                    // Can't come here with MinIO, MinIO allows single part upload for large objects.
                    LOG_INFO(log, "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}. Will retry with multipart upload",
                             dest_key, outcome.GetError().GetExceptionName(), dest_bucket, size);
                    return SinglepartCopyResult::TRY_SWITCH_TO_MULTIPART;
                }
                
                bool can_retry = (retries < max_retries);
                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && can_retry)
                {
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    LOG_INFO(log, "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}. Will retry",
                             dest_key, outcome.GetError().GetMessage(), dest_bucket, size);
                    continue; /// will retry
                }

                throw S3Exception(outcome.GetError().GetErrorType(),
                                  "Failed singlepart upload to \"{}\": {}. Bucket: {}, Size: {}",
                                  dest_key, outcome.GetError().GetMessage(), dest_bucket, size);
            }
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> fillCopyPartRequest(int part_number) override
        {
            size_t part_offset = get_part_offset(part_number);
            size_t part_size = get_part_size(part_number);
            auto read_buffer = std::make_unique<LimitSeekableReadBuffer>(create_read_buffer(), part_offset, part_size);

            /// Setup request.
            auto request = std::make_unique<S3::UploadPartRequest>();
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetPartNumber(part_number);
            request->SetUploadId(multipart_upload_id);
            request->SetContentLength(part_size);
            request->SetBody(std::make_unique<StdStreamFromReadBuffer>(std::move(read_buffer), part_size));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");

            return request;
        }

        String processCopyPartRequest(const Aws::AmazonWebServiceRequest & request, int part_number) override
        {
            const auto & req = typeid_cast<const S3::UploadPartRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPart);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPart);

            auto outcome = client_ptr->UploadPart(req);
            if (!outcome.IsSuccess())
                throw S3Exception(outcome.GetError().GetErrorType(),
                                  "Couldn't upload part #{} to \"{}\": {}. Part offset: {}, Part size: {}, Bucket: {}",
                                  part_number, dest_key, outcome.GetError().GetMessage(),
                                  get_part_offset(part_number), get_part_size(part_number), dest_bucket);

            return outcome.GetResult().GetETag();
        }
    };

    /// Helper class to help implementing copyS3File().
    class S3FileCopier : public BaseS3FileCopier
    {
    public:
        S3FileCopier(
            const std::shared_ptr<const S3::Client> & client_ptr_,
            const String & src_bucket_,
            const String & src_key_,
            const String & dest_bucket_,
            const String & dest_key_,
            const CopyS3FileSettings & copy_settings_)
            : BaseS3FileCopier(client_ptr_, dest_bucket_, dest_key_, copy_settings_, &Poco::Logger::get("copyS3File"))
            , src_bucket(src_bucket_)
            , src_key(src_key_)
        {
        }

    private:
        String src_bucket;
        String src_key;

        size_t calculateSize() const override
        {
            size_t object_size = S3::getObjectSize(*client_ptr, src_bucket, src_key, {}, request_settings, for_disk_s3);
            if (object_size < offset)
            {
                throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
                                "Offset {} exceeds the size {} of the object \"{}\". Bucket: {}",
                                offset, object_size, src_key, src_bucket);
            }
            return object_size - offset;
        }

        bool shouldUseMultipartCopy() const override
        {
            return (size > request_settings.getUploadSettings().max_single_operation_copy_size) || (offset > 0);
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> fillSinglepartCopyRequest() override
        {
            auto request = std::make_unique<S3::CopyObjectRequest>();
            request->SetCopySource(src_bucket + "/" + src_key);
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);

            if (object_metadata.has_value())
            {
                request->SetMetadata(object_metadata.value());
                request->SetMetadataDirective(Aws::S3::Model::MetadataDirective::REPLACE);
            }

            const auto & storage_class_name = upload_settings.storage_class_name;
            if (!storage_class_name.empty())
                request->SetStorageClass(Aws::S3::Model::StorageClassMapper::GetStorageClassForName(storage_class_name));

            /// If we don't do it, AWS SDK can mistakenly set it to application/xml, see https://github.com/aws/aws-sdk-cpp/issues/1840
            request->SetContentType("binary/octet-stream");
            return request;
        }

        SinglepartCopyResult processSinglepartCopyRequest(const Aws::AmazonWebServiceRequest & request) override
        {
            const auto & req = typeid_cast<const S3::CopyObjectRequest &>(request);

            size_t max_retries = std::max(request_settings.max_unexpected_write_error_retries, 1UL);
            for (size_t retries = 1;; ++retries)
            {
                auto outcome = client_ptr->CopyObject(req);
                if (outcome.IsSuccess())
                {
                    LOG_TRACE(log, "Completed singlepart copy from \"{}\" to \"{}\". {}, Size: {}", src_key, dest_key, getBucketsDescription(), size);
                    return SinglepartCopyResult::OK;
                }

                if ((outcome.GetError().GetExceptionName() == "EntityTooLarge" || outcome.GetError().GetExceptionName() == "InvalidRequest") && size)
                {
                    // Can't come here with MinIO, MinIO allows single part upload for large objects.
                    LOG_INFO(log, "Failed singlepart copy from \"{}\" to \"{}\": {}. {}, Size: {}. Will retry with multipart copy",
                             src_key, dest_key, outcome.GetError().GetExceptionName(), getBucketsDescription(), size);
                    return SinglepartCopyResult::TRY_SWITCH_TO_MULTIPART;
                }
                
                bool can_retry = (retries < max_retries);
                if ((outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) && can_retry)
                {
                    /// TODO: Is it true for copy requests?
                    /// For unknown reason, at least MinIO can respond with NO_SUCH_KEY for put requests
                    LOG_INFO(log, "Failed singlepart copy from \"{}\" to \"{}\": {}. {}, Size: {}. Will retry",
                             src_key, dest_key, outcome.GetError().GetExceptionName(), getBucketsDescription(), size);
                    continue; /// will retry
                }

                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Failed singlepart copy from \"{}\" to \"{}\": {}. {}, Size: {}, will retry",
                    src_key, dest_key, outcome.GetError().GetExceptionName(), getBucketsDescription(), size);
            }
        }

        std::unique_ptr<Aws::AmazonWebServiceRequest> fillCopyPartRequest(int part_number) override
        {
            /// Make a copy request to copy a part.
            auto request = std::make_unique<S3::UploadPartCopyRequest>();

            size_t part_offset = get_part_offset(part_number);
            size_t part_size = get_part_size(part_number);

            request->SetCopySource(src_bucket + "/" + src_key);
            request->SetBucket(dest_bucket);
            request->SetKey(dest_key);
            request->SetUploadId(multipart_upload_id);
            request->SetPartNumber(part_number);
            request->SetCopySourceRange(fmt::format("bytes={}-{}", part_offset, part_offset + part_size - 1));

            return request;
        }

        String processCopyPartRequest(const Aws::AmazonWebServiceRequest & request, int part_number) override
        {
            const auto & req = typeid_cast<const S3::UploadPartCopyRequest &>(request);

            ProfileEvents::increment(ProfileEvents::S3UploadPartCopy);
            if (for_disk_s3)
                ProfileEvents::increment(ProfileEvents::DiskS3UploadPartCopy);

            auto outcome = client_ptr->UploadPartCopy(req);
            if (!outcome.IsSuccess())
                throw S3Exception(
                    outcome.GetError().GetErrorType(),
                    "Couldn't copy part #{} from \"{}\" to \"{}\": {}. Part offset: {}, Part size: {}, {}",
                    part_number, src_key, dest_key, outcome.GetError().GetMessage(),
                    get_part_offset(part_number), get_part_size(part_number), getBucketsDescription());

            return outcome.GetResult().GetCopyPartResult().GetETag();
        }

        String getBucketsDescription() const
        {
            if (src_bucket == dest_bucket)
                return fmt::format("Bucket: {}", src_bucket);
            else
                return fmt::format("Source bucket: {}, Destination bucket: {}", src_bucket, dest_bucket);
        }
    };
}


void copyDataToS3File(
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & create_read_buffer,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings)
{
    auto copier = std::make_shared<DataToS3FileCopier>(create_read_buffer, dest_s3_client, dest_bucket, dest_key, copy_settings);
    copier->run();
}


void copyS3File(
    const std::shared_ptr<const S3::Client> & s3_client,
    const String & src_bucket,
    const String & src_key,
    const String & dest_bucket,
    const String & dest_key,
    const CopyS3FileSettings & copy_settings)
{
    auto copier = std::make_shared<S3FileCopier>(s3_client, src_bucket, src_key, dest_bucket, dest_key, copy_settings);
    copier->run();
}

}

#endif
