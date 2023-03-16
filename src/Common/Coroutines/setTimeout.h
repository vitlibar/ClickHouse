#pragma once

#include <Common/Coroutines/Task.h>
#include <Common/Coroutines/sleep.h>


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
    std::exception_ptr makeTimeoutException(TimeUnit time_unit, uint64_t count);

    /// The implementation of setTimeout*() coroutines.
    template <typename ResultType>
    Task<ResultType> setTimeoutImpl(Task<ResultType> subtask, TimeUnit time_unit, uint64_t count)
    {
        auto params = co_await RunParams{};

        if (!params.scheduler_for_parallel_subtasks)
            co_return co_await subtask; /// Cannot set timeout, just run the subtask.

        /// We'll run two subtasks - the first will do the `subtask` itself, and the second will be just sleeping for specified amount of time.
        auto subtask_params = params;
        auto sleeping_params = params;

        /// `subtask` will be executed in parallel with waiting for the timeout's end.
        subtask_params.scheduler = subtask_params.scheduler_for_parallel_subtasks;

        /// Make separate cancel statuses for the subtask and the sleeping, so we'll able to cancel them if the main task is not cancelled.
        auto main_task_cancel_status = params.cancel_status;

        auto subtask_cancel_status = std::make_shared<CancelStatus>();
        subtask_params.cancel_status = subtask_cancel_status;

        auto sleeping_cancel_status = std::make_shared<CancelStatus>();
        sleeping_params.cancel_status = sleeping_cancel_status;

        auto cancel_subscription = main_task_cancel_status->subscribe(
            [subtask_cancel_status, sleeping_cancel_status](std::exception_ptr exception)
            {
                subtask_cancel_status->setIsCancelled(exception);
                sleeping_cancel_status->setIsCancelled(exception);
            });

        /// The result will be stored here.
        ResultHolder<ResultType> result_holder;

        auto do_subtask = [/* a lambda function without captures can be a coroutine */](
                              Task<ResultType> subtask_,
                              details::ResultHolder<ResultType> & result_holder_,
                              std::shared_ptr<CancelStatus> sleeping_cancel_status_) -> Task<>
        {
            SCOPE_EXIT({
                std::cout << "Cancelling waiting for timeout" << std::endl;
                sleeping_cancel_status_->setIsCancelled("Cancelled waiting for the timeout's end because the task finished first");
                std::cout << "Cancelled waiting for timeout" << std::endl;
                });
            std::cout << "Doing subtask" << std::endl;
            if constexpr (std::is_void_v<ResultType>)
                co_await subtask_;
            else
                result_holder_.setResult(co_await subtask_);
            std::cout << "Done subtask" << std::endl;
        };

        /// Run the `subtask`.
        auto subtask_awaiter = do_subtask(std::move(subtask), result_holder, sleeping_cancel_status).run(params);

        /// `subtask` might be finished already. In that case we don't need any sleeping.
        if (!subtask_awaiter.isReady())
        {
            try
            {
                co_await sleepForMicroseconds(toMicroseconds(time_unit, count)).run(sleeping_params);
            }
            catch (Exception & e)
            {
                /// `co_await sleepForMicroseconds()` could throw the exception "Cancelled waiting for the timeout's end because the task finished first"
                if (e.code() != ErrorCodes::COROUTINE_TASK_CANCELLED)
                    throw;
            }

            if (!subtask_awaiter.isReady())
            {
                /// Time is out, let's cancel `subtask`.
                subtask_cancel_status->setIsCancelled(makeTimeoutException(time_unit, count));
                
                /// Wait until the subtask finishes processing.
                co_await subtask_awaiter;
            }
        }

        if constexpr(!std::is_void_v<ResultType>)
            co_return std::move(*result_holder.getResultPtr());
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
