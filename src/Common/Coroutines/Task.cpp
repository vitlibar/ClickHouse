#include <Common/Coroutines/Task.h>
#include <Common/threadPoolCallbackRunner.h>


namespace DB::Coroutine
{

RunParams::RunParams(ThreadPool & thread_pool, const String & thread_name)
{
    scheduler = makeScheduler(thread_pool, thread_name);
    scheduler_for_parallel_tasks = makeScheduler(thread_pool, thread_name, /* allow_fallback_to_sync_run= */ true);
    scheduler_for_delayed_tasks = makeScheduler(thread_pool, thread_name, /* allow_fallback_to_sync_run= */ false, /* ignore_queue_size= */ true);
}

RunParams::RunParams(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner)
{
    scheduler = makeScheduler(thread_pool_callback_runner);
    scheduler_for_parallel_tasks = scheduler;
    scheduler_for_delayed_tasks = scheduler;
}

std::function<void(std::function<void()>)> RunParams::makeScheduler(ThreadPool & thread_pool, const String & thread_name, bool allow_fallback_to_sync_run, bool /* ignore_queue_size = false */)
{
    if (allow_fallback_to_sync_run)
    {
        return [pool = &thread_pool, thread_name, thread_group = CurrentThread::getGroup()](std::function<void()> callback)
        {
            if (pool->trySchedule(
                    [callback, thread_name, thread_group]()
                    { wrapScheduledCallback<void>(std::move(callback), thread_name, thread_group); },
                    0))
                return;

            callback();
        };
    }
    else
    {
        return [pool = &thread_pool, thread_name, thread_group = CurrentThread::getGroup()](std::function<void()> callback)
        {
            pool->scheduleOrThrow(
                [callback = std::move(callback), thread_name, thread_group]() mutable
                { wrapScheduledCallback<void>(std::move(callback), thread_name, thread_group); },
                0);
        };
    }
}

std::function<void(std::function<void()>)> RunParams::makeScheduler(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner)
{
    return [thread_pool_callback_runner](std::function<void()> callback) { return thread_pool_callback_runner(std::move(callback), 0); };
}


void CancelStatus::setIsCancelled(std::exception_ptr cancel_exception_) noexcept
{
    std::lock_guard lock{mutex};
    if (cancel_exception)
        return;
    cancel_exception = cancel_exception_;
    for (auto & subscription : subscriptions)
    {
        try
        {
            subscription(cancel_exception_);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void CancelStatus::setIsCancelled(const String & message_) noexcept
{
    setIsCancelled(std::make_exception_ptr(Exception{ErrorCodes::COROUTINE_TASK_CANCELLED, "{}", message_.empty() ? "Task cancelled" : message_}));
}

std::exception_ptr CancelStatus::getCancelException() const noexcept
{
    std::lock_guard lock{mutex};
    return cancel_exception;
}

scope_guard CancelStatus::subscribe(const std::function<void(std::exception_ptr)> & callback)
{
    std::lock_guard lock{mutex};
    if (cancel_exception)
    {
        callback(cancel_exception);
        return {};
    }
    else
    {
        subscriptions.emplace_front(callback);
        auto it = subscriptions.begin();

        return [this, keep_this_from_destruction = shared_from_this(), it]
        {
            std::lock_guard lock2{mutex};
            subscriptions.erase(it);
        };
    }
}

namespace details
{
    bool ContinuationList::add(std::coroutine_handle<> new_item)
    {
        std::lock_guard lock{mutex};
        if (!processed)
        {
            items.push_back(std::move(new_item));
            return true;
        }
        return false;
    }

    void ContinuationList::process()
    {
        std::list<std::coroutine_handle<>> items_to_process;
        {
            std::lock_guard lock{mutex};
            items_to_process = std::exchange(items, {});
            processed = true;
        }
        for (auto & item : items_to_process)
            item.resume();
    }

    bool ContinuationList::isProcessed() const
    {
        std::lock_guard lock{mutex};
        return processed;
    }

    void logErrorStateMustBeFinishedAfterWait(State state)
    {
        LOG_ERROR(
            &Poco::Logger::get(__PRETTY_FUNCTION__),
            "After co_await the state of the coroutine must be FINISHED, but it's {}. It's a bug. Call stack: {}",
            magic_enum::enum_name(state),
            StackTrace{}.toString());
    }

    void logErrorControllerDestructsWhileRunning()
    {
        LOG_ERROR(
            &Poco::Logger::get(__PRETTY_FUNCTION__),
            "~Controller must not be called with the coroutine still running. It's a bug. Call stack: {}",
            StackTrace{}.toString());
    }
}
}
