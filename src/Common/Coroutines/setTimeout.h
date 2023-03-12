#pragma once

#include <Common/Coroutines/Task.h>
#include <Common/Coroutines/Timer.h>


namespace DB::Coroutine
{

/// Sets a timeout for a task - if a specified `task` will be executed longer than specified amount of time then
/// it will be cancelled when the time is out, and an exception of type COROUTINE_TASK_TIMEOUT will be thrown.
template <typename ResultType>
Task<ResultType> setTimeoutInSeconds(Task<ResultType> task, uint64_t seconds);

template <typename ResultType>
Task<ResultType> setTimeoutInMilliseconds(Task<ResultType> task, uint64_t milliseconds);

template <typename ResultType>
Task<ResultType> setTimeoutInMicroseconds(Task<ResultType> task, uint64_t microseconds);


/// Implementation details.
namespace details
{
    enum class TimeUnit
    {
        SECONDS,
        MILLISECONDS,
        MICROSECONDS
    };

    std::exception_ptr makeTimeoutException(TimeUnit time_unit, uint64_t count);
    std::string_view getTimeUnitsName(TimeUnit time_unit);
    uint64_t toMicroseconds(TimeUnit time_unit, uint64_t count);

    template <typename ResultType>
    Task<ResultType> setTimeoutImpl(Task<ResultType> subtask, TimeUnit time_unit, uint64_t count)
    {
        auto params = co_await RunParams{};

        /// Make a separate cancel status for the subtask, so we'll able to cancel it if the main task is not cancelled.
        auto main_task_cancel_status = params.cancel_status;
        auto subtask_cancel_status = std::make_shared<CancelStatus>();
        auto main_task_cancel_status_subscription = main_task_cancel_status->subscribe(
            [subtask_cancel_status](std::exception_ptr exception) { subtask_cancel_status->setIsCancelled(exception); });
        params.cancel_status = subtask_cancel_status;

        /// Cancel `subtask` when the time is out.
        auto cancel_after_timeout
            = [subtask_cancel_status, time_unit, count] { subtask_cancel_status->setIsCancelled(makeTimeoutException(time_unit, count)); };

        auto timer_subscription = Timer::instance().scheduleDelayedTask(toMicroseconds(time_unit, count), cancel_after_timeout);

        /// Run the subtask.
        auto awaiter = subtask.run(params);
        if constexpr (std::is_void_v<ResultType>)
            co_await awaiter;
        else
            co_return co_await awaiter;
    }
}

template <typename ResultType>
Task<ResultType> setTimeoutInSeconds(Task<ResultType> task, uint64_t seconds)
{
    return details::setTimeoutImpl(std::move(task), details::TimeUnit::SECONDS, seconds);
}

template <typename ResultType>
Task<ResultType> setTimeoutInMilliseconds(Task<ResultType> task, uint64_t milliseconds)
{
    return details::setTimeoutImpl(std::move(task), details::TimeUnit::MILLISECONDS, milliseconds);
}

template <typename ResultType>
Task<ResultType> setTimeoutInMicroseconds(Task<ResultType> task, uint64_t microseconds)
{
    return details::setTimeoutImpl(std::move(task), details::TimeUnit::MICROSECONDS, microseconds);
}

}
