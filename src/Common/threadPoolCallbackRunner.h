#pragma once

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <future>

namespace DB
{

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result, typename Callback = std::function<Result()>>
using ThreadPoolCallbackRunner = std::function<std::future<Result>(Callback &&, int64_t priority)>;

/// Wraps a scheduled 
template <typename Result, typename Callback = std::function<Result()>>
Result wrapScheduledCallback(Callback && callback, const String & thread_name, ThreadGroupStatusPtr thread_group)
{
    if (thread_group)
        CurrentThread::attachTo(thread_group);

    SCOPE_EXIT_SAFE({
        if (thread_group)
            CurrentThread::detachQueryIfNotDetached();
    });

    setThreadName(thread_name.data());

    return std::move(callback)(); 
}

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrow()'.
template <typename Result, typename Callback = std::function<Result()>>
ThreadPoolCallbackRunner<Result, Callback> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name)
{
    return [pool = &pool, thread_name, thread_group = CurrentThread::getGroup()](
               Callback && callback, int64_t priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>(
            [callback = std::move(callback), thread_name, thread_group]() mutable -> Result
            { return wrapScheduledCallback<Result>(std::move(callback), thread_name, thread_group); });

        auto future = task->get_future();

        /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
        pool->scheduleOrThrow([task = std::move(task)]{ (*task)(); }, -priority);

        return future;
    };
}

template <typename Result, typename T>
std::future<Result> scheduleFromThreadPool(T && task, ThreadPool & pool, const std::string & thread_name, int64_t priority = 0)
{
    auto schedule = threadPoolCallbackRunner<Result, T>(pool, thread_name);
    return schedule(std::move(task), priority);
}

}
