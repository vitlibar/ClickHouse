#include <Common/Coroutines/Task.h>
#include <Common/threadPoolCallbackRunner.h>


namespace DB::Coroutine
{

RunParams::RunParams(ThreadPool & thread_pool, const String & thread_name)
{
    scheduler = makeScheduler(thread_pool, thread_name);
    scheduler_for_parallel_subtasks = makeScheduler(thread_pool, thread_name, /* allow_fallback_to_sync_run= */ true);
}

RunParams::RunParams(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner)
{
    scheduler = makeScheduler(thread_pool_callback_runner);
    scheduler_for_parallel_subtasks = scheduler;
}

std::function<void(std::function<void()>)> RunParams::makeScheduler(ThreadPool & thread_pool, const String & thread_name, bool allow_fallback_to_sync_run)
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


    void CancelStatus::setIsCancelled(std::exception_ptr cancel_exception_) noexcept
    {
        CallbacksWithIDs subscriptions_to_notify;

        {
            std::lock_guard lock{mutex};
            if (cancel_exception)
                return;
            cancel_exception = cancel_exception_;
            subscriptions_to_notify = std::exchange(subscriptions, {});
        }

        for (auto & subscription : subscriptions_to_notify)
        {
            try
            {
                subscription.callback(cancel_exception_);
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
        std::exception_ptr exception;

        {
            std::lock_guard lock{mutex};
            if (cancel_exception)
            {
                exception = cancel_exception;
            }
            else
            {
                size_t current_id = ++last_id;
                subscriptions.emplace_back(CallbackWithID{callback, current_id});

                return [this, keep_this_from_destruction = shared_from_this(), current_id]
                {
                    std::lock_guard lock2{mutex};
                    for (auto it = subscriptions.begin(); it != subscriptions.end(); ++it)
                    {
                        if (it->id == current_id)
                        {
                            subscriptions.erase(it);
                            break;
                        }
                    }
                };
            }
        }

        callback(exception);
        return {};
    }


    Timer::~Timer()
    {
        if (!timed_queue.empty())
            LOG_ERROR(
                &Poco::Logger::get(__PRETTY_FUNCTION__),
                "Timed queue is not empty on destruction of timer. It's a bug. Stack trace: {}",
                StackTrace{}.toString());
    }

    scope_guard Timer::scheduleDelayedTask(uint64_t microseconds, std::function<void()> callback)
    {
        std::lock_guard lock{mutex};
        std::cout << "scheduleDelayedTask " << microseconds << std::endl;
        Poco::Timestamp now;
        Poco::Timestamp timestamp = now + microseconds;
        size_t id = ++last_id;
        timed_queue[timestamp].emplace_back(CallbackWithID{std::move(callback), id});

        timed_queue_updated.set();
        return [this, timestamp, id] { unscheduleDelayedTask(timestamp, id); };
    }

    void Timer::unscheduleDelayedTask(Poco::Timestamp timestamp, uint64_t id)
    {
        std::cout << "unscheduleDelayedTask " << (timestamp - Poco::Timestamp{}) << std::endl;
        std::lock_guard lock{mutex};
        auto it = timed_queue.find(timestamp);
        if (it == timed_queue.end())
            return;

        auto & callbacks_with_ids = it->second;
        for (size_t i = 0; i != callbacks_with_ids.size(); ++i)
        {
            auto & callback_with_id = callbacks_with_ids[i];
            if (callback_with_id.id == id)
            {
                callbacks_with_ids.erase(callbacks_with_ids.begin() + i);
                if (callbacks_with_ids.empty())
                {
                    timed_queue.erase(it);
                    if (timed_queue.empty())
                        timed_queue_updated.set();
                }
                break;
            }
        }
    }

    void Timer::run()
    {
        long wait_time = 0;

        {
            std::lock_guard lock{mutex};
            if (timed_queue.empty() || is_running)
                return;

            is_running = true;
            Poco::Timestamp now;
            Poco::Timestamp first_time = timed_queue.begin()->first;
            wait_time = (now < first_time) ? (first_time - now) : 0;
        }

        while (true)
        {
            /// Wait with `mutex` unlocked.
            timed_queue_updated.tryWait(wait_time / 1000);

            Poco::Timestamp now;

            std::vector<std::function<void()>> callbacks;
            SCOPE_EXIT({
                for (auto & callback : callbacks)
                {
                    try
                    {
                        std::move(callback)();
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            });

            {
                std::lock_guard lock{mutex};

                while (!timed_queue.empty() && (now >= timed_queue.begin()->first))
                {
                    auto it = timed_queue.begin();
                    callbacks.reserve(callbacks.size() + it->second.size());
                    for (auto & callback_with_id : it->second)
                        callbacks.push_back(std::move(callback_with_id.callback));
                    timed_queue.erase(it);
                }

                if (timed_queue.empty())
                {
                    is_running = false;
                    return;
                }

                Poco::Timestamp next_time = timed_queue.begin()->first;
                wait_time = (now < next_time) ? (next_time - now) : 0;
            }
        }
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
