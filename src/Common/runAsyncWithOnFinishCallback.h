#include <Common/threadPoolCallbackRunner.h>
#include <Common/Exception.h>


namespace DB
{

/// Runs a passed job on a scheduler, and then call `on_finish_callback`.
/// Makes sure `on_finish_callback` is always called even if a passed job or a scheduler throw an exception.
void runAsyncWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & scheduler,
                                  std::function<void()> && job,
                                  std::function<void(std::exception_ptr)> && on_finish_callback);

template<typename Signature>
class ThreadSafeHolderForOnFinishCallback;

template <typename Result, typename... ArgTypes>
class ThreadSafeHolderForOnFinishCallback<Result(ArgTypes...)>
{
public:
    using CallbackType = std::function<Result(ArgTypes...)>;

    ThreadSafeHolderForOnFinishCallback(CallbackType && callback_) : callback(std::move(callback_)) {}

    ~ThreadSafeHolderForOnFinishCallback()
    {
        if (get())
        {
            LOG_ERROR(&Poco::Logger::get("runAsyncWithOnFinishCallback"), "on_finish_callback was never called!");
            chassert(false);
        }
    }

    void callOrThrow(std::exception_ptr error = nullptr)
    {
        chassert(error);
        if (auto cl = get())
            (std::move(cl))(error);
        else if (error)
            std::rethrow_exception(error);
    }

    void callOrLogError(std::exception_ptr error = nullptr)
    {
        chassert(error);
        if (auto cl = get())
        {
            try
            {
                (std::move(cl))(error);
            }
            catch (...)
            {
                tryLogCurrentException(&Poco::Logger::get("runAsyncWithOnFinishCallback"));
            }
        }
        else if(error)
            tryLogException(error, &Poco::Logger::get("runAsyncWithOnFinishCallback"));
    }

    CallbackType get()
    {
        std::lock_guard lock{mutex};
        CallbackType res;
        std::swap(res, callback);
        return res;
    }

private:
    CallbackType TSA_GUARDED_BY(mutex) callback;
    std::mutex mutex;
};

template <typename Result, typename... ArgTypes>
std::shared_ptr<ThreadSafeHolderForOnFinishCallback<Result(ArgTypes...)>> makeThreadSafeHolderForOnFinishCallback(std::function<Result(ArgTypes...)> && callback)
{
    return std::make_shared<ThreadSafeHolderForOnFinishCallback<Result(ArgTypes...)>>(std::move(callback));
}

inline void runAsyncWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & scheduler,
                                         std::function<void()> && job,
                                         const std::shared_ptr<ThreadSafeHolderForOnFinishCallback<void(std::exception_ptr)>> & on_finish_callback_holder)
{
    auto try_job = [job, on_finish_callback_holder]
    {
        try
        {
            job();
        }
        catch(...)
        {
            on_finish_callback_holder->callOrLogError(std::current_exception());
        }
    };

    try
    {
        scheduler(try_job, 0);
    }
    catch(...)
    {
        on_finish_callback_holder->callOrThrow(std::current_exception());
    }
}

inline void runAsyncWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & scheduler,
                                         std::function<void()> && job,
                                         std::function<void(std::exception_ptr)> && on_finish_callback)
{
    auto on_finish_callback_holder = makeThreadSafeHolderForOnFinishCallback(std::move(on_finish_callback));

    auto run_job = [job = std::move(job), on_finish_callback_holder]() mutable
    {
        job();
        on_finish_callback_holder->callOrLogError();
    };

    runAsyncWithOnFinishCallback(scheduler, std::move(run_job), on_finish_callback_holder);
}


class MultipleTasksTrackerWithOnFinishCallback
{
public:
    MultipleTasksTrackerWithOnFinishCallback(size_t num_tasks_, std::function<void(std::exception_ptr)> && on_finish_callback_)
        : num_tasks(num_tasks_), on_finish_callback_holder(makeThreadSafeHolderForOnFinishCallback(std::move(on_finish_callback_)))
    {
    }

    bool hasError() const
    {
        std::lock_guard lock{mutex};
        return error != nullptr;
    }

    [[nodiscard]] bool onTaskStarted()
    {
        std::lock_guard lock{mutex};
        if (error != nullptr || (num_tasks_started >= num_tasks))
            return false;
        ++num_tasks_started;
        return true;
    }

    void onTaskFinished(std::exception_ptr error_)
    {
        std::lock_guard lock{mutex};
        ++num_tasks_finished;
        if (!error)
            error = error_;
        size_t num_tasks_expected = error ? num_tasks_started : num_tasks;
        if (num_tasks_finished == num_tasks_expected)
            on_finish_callback_holder->callOrLogError(error);
    }

private:
    const size_t num_tasks;
    const std::shared_ptr<ThreadSafeHolderForOnFinishCallback<void(std::exception_ptr)>> on_finish_callback_holder;
    size_t TSA_GUARDED_BY(mutex) num_tasks_started = 0;
    size_t TSA_GUARDED_BY(mutex) num_tasks_finished;
    std::exception_ptr TSA_GUARDED_BY(mutex) error;
    mutable std::mutex mutex;
};


inline void runAsyncMultipleTasksWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & scheduler,
                                                      std::vector<std::function<void()>> & jobs,
                                                      std::function<void(std::exception_ptr)> on_finish_callback)
{
    auto tracker = std::make_shared<MultipleTasksTrackerWithOnFinishCallback>(jobs.size(), std::move(on_finish_callback));
    auto on_task_finished = [tracker](std::exception_ptr error_) { tracker->onTaskFinished(error_); };

    for (auto & job : jobs)
    {
        if (!tracker->onTaskStarted())
            break;
        runAsyncWithOnFinishCallback(scheduler, std::move(job), on_task_finished);
    }
}

}
