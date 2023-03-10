#include <Common/Coroutines/Task.h>


namespace DB::Coroutine::details
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
