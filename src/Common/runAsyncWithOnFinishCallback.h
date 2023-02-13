#include <Common/threadPoolCallbackRunner.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Either `std::function<void(std::optional<Result>, std::exception_ptr)>` or `std::function<void(std::exception_ptr)>`.
/// See runAsyncWithOnFinishCallback().
template <typename Result>
using OnFinishCallbackForAsyncJob
    = std::function<std::conditional_t<std::is_void_v<Result>, void(std::exception_ptr), void(std::optional<Result>, std::exception_ptr)>>;

/// Runs a passed job on a scheduler, and then call `on_finish_callback`.
/// Makes sure `on_finish_callback` is always called even if a passed job or a scheduler throw an exception.
template <typename Result>
void runAsyncWithOnFinishCallback(
    const ThreadPoolCallbackRunner<void> & schedule,
    const std::function<Result()> & job,
    const OnFinishCallbackForAsyncJob<Result> & on_finish_callback);

/// Helper for implementation of runAsyncWithOnFinishCallback().
template <typename Result>
class OnFinishCallbackRunnerForAsyncJob
{
public:
    using OnFinishCallback = OnFinishCallbackForAsyncJob<Result>;
    static const constexpr bool result_is_void = std::is_void_v<Result>;

    OnFinishCallbackRunnerForAsyncJob(const OnFinishCallback & on_finish_callback_)
        : on_finish_callback(on_finish_callback_), thread_id_which_can_throw(std::this_thread::get_id())
    {
    }

    ~OnFinishCallbackRunnerForAsyncJob()
    {
        /// If there was `on_finish_callback` it must be called before calling this destructor.
        if (on_finish_callback && !on_finish_callback_called.load())
        {
            /// Though we can call `on_finish_callback` now, but this is not normal.
            run(Exception{ErrorCodes::LOGICAL_ERROR, "onFinish() must be called before ~OnFinishCallbackRunnerForAsyncJob. It's a bug"});
        }
    }

    template <typename T = Result>
    std::enable_if_t<std::is_same_v<T, Result> && !result_is_void> run(T result)
    {
        runImpl(std::move(result));
    }

    void run(std::exception_ptr error = nullptr)
    {
        runImpl(std::move(error));
    }

    void run(const std::exception & error)
    {
        runImpl(std::make_exception_ptr(error));
    }

    void onExitInitialScope()
    {
        std::lock_guard lock{mutex};
        thread_id_which_can_throw = std::thread::id{};
    }

private:
    template <typename T>
    void runImpl(T result_or_error)
    {
        constexpr bool is_error = std::is_same_v<T, std::exception_ptr>;
        if (on_finish_callback && !on_finish_callback_called.exchange(true))
        {
            try
            {
                if constexpr (result_is_void)
                    on_finish_callback(result_or_error);
                else if constexpr (is_error)
                    on_finish_callback(std::nullopt, result_or_error);
                else
                    on_finish_callback(result_or_error, nullptr);
            }
            catch(...)
            {
                if (can_throw())
                    throw;
                else
                    tryLogCurrentException("OnFinishCallbackRunner");
            }
        }
        else
        {
            if constexpr(is_error)
            {
                if (result_or_error)
                {
                    if (can_throw())
                        std::rethrow_exception(result_or_error);
                    else
                        tryLogException(result_or_error, "OnFinishCallbackRunner");
                }
            }
        }
    }

    bool can_throw() const
    {
        std::lock_guard lock{mutex};
        return thread_id_which_can_throw == std::this_thread::get_id();
    }

    OnFinishCallback on_finish_callback;
    std::atomic<bool> on_finish_callback_called = false;
    std::thread::id TSA_GUARDED_BY(mutex) thread_id_which_can_throw;
    mutable std::mutex mutex;
};


template <typename Result>
void runAsyncWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & schedule,
                                  const std::function<Result()> & job,
                                  const OnFinishCallbackForAsyncJob<Result> & on_finish_callback)
{
    auto on_finish = std::make_shared<OnFinishCallbackRunnerForAsyncJob<Result>>(on_finish_callback);
    SCOPE_EXIT( on_finish->onExitInitialScope(); );

    auto job_with_on_finish = [job, on_finish]
    {
        try
        {
            if constexpr (std::is_void_v<Result>)
            {
                job();
                on_finish->run();
            }
            else
            {
                on_finish->run(job());
            }
        }
        catch(...)
        {
            on_finish->run(std::current_exception());
        }
    };

    try
    {
        schedule(job_with_on_finish, 0);
    }
    catch(...)
    {
        on_finish->run(std::current_exception());
    }
}

template <typename Result>
void runAsyncWithOnFinishCallback(const ThreadPoolCallbackRunner<void> & schedule,
                                  const std::function<void(OnFinishCallbackForAsyncJob<Result>)> & job,
                                  const OnFinishCallbackForAsyncJob<Result> & on_finish_callback)
{
    auto on_finish = std::make_shared<OnFinishCallbackRunnerForAsyncJob<Result>>(on_finish_callback);
    SCOPE_EXIT( on_finish->onExitInitialScope(); );

    auto job_with_on_finish = [job, on_finish]
    {
        try
        {
            if constexpr (std::is_void_v<Result>)
            {
                auto 
                job();
                on_finish->run();
            }
            else
            {
                on_finish->run(job());
            }
        }
        catch(...)
        {
            on_finish->run(std::current_exception());
        }
    };

    try
    {
        schedule(job_with_on_finish, 0);
    }
    catch(...)
    {
        on_finish->run(std::current_exception());
    }
}

}
