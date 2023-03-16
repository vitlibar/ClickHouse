#pragma once

#include <Common/Coroutines/Task.h>


namespace DB::Coroutine
{

/// Runs multiple tasks in parallel and returns all the results in the same order.
/// The returning value must be the same as for sequential().
template <typename ResultType>
Task<std::vector<ResultType>> parallel(std::vector<Task<ResultType>> tasks);

Task<> parallel(std::vector<Task<>> tasks);


/// Implementation details.
namespace details
{
    template <
        typename ContainerType,
        typename SubTaskType = typename ContainerType::value_type,
        typename SubTaskResultType = typename SubTaskType::result_type,
        typename ResultType = std::conditional_t<std::is_void_v<SubTaskResultType>, void, std::vector<SubTaskResultType>>>
    Task<ResultType> parallelImpl(ContainerType subtasks)
    {
        /// No subtasks is an easy case.
        if (subtasks.empty())
        {
            if constexpr (std::is_void_v<ResultType>)
                co_return;
            else
                co_return {};
        }

        /// A single subtask is an easy case.
        if (subtasks.size() == 1)
        {
            if constexpr (std::is_void_v<ResultType>)
            {
                co_await subtasks[0];
                co_return;
            }
            else
                co_return {co_await subtasks[0]};
        }

        /// We'll collect the results of the subtasks here in `result_holder`.
        details::ResultHolder<ResultType> result_holder;
        if constexpr (!std::is_void_v<ResultType>)
            result_holder.getResultPtr()->resize(subtasks.size());

        /// The same parameters will be used to run all the subtasks.
        auto params = co_await RunParams{};

        /// Run the subtasks in parallel if the scheduler allows it.
        params.scheduler = params.scheduler_for_parallel_subtasks;

        /// Make a separate cancel status for subtasks, so we'll able to cancel them if the main task is not cancelled.
        auto main_task_cancel_status = params.cancel_status;
        auto subtask_cancel_status = std::make_shared<CancelStatus>();
        auto main_task_cancel_status_subscription = main_task_cancel_status->subscribe(
            [subtask_cancel_status](std::exception_ptr exception) { subtask_cancel_status->setIsCancelled(exception); });
        params.cancel_status = subtask_cancel_status;

        /// We only need the first exception.
        std::exception_ptr exception;
        std::mutex exception_mutex;

        auto store_current_exception = [&exception, &exception_mutex, &subtask_cancel_status]
        {
            std::lock_guard lock{exception_mutex};
            if (!exception)
            {
                /// Cancel all the subtasks if any of them fail.
                exception = std::current_exception();
                subtask_cancel_status->setIsCancelled("Cancelled because a parallel task failed");
            }
        };

        /// First we start or schedule all the tasks, then we'll wait for them to finish.
        std::vector<Task<>::Awaiter> awaiters;
        awaiters.reserve(subtasks.size());

        try
        {
            /// Start each task.
            for (size_t i = 0; i != subtasks.size(); ++i)
            {
                co_await StopIfCancelled{};

                auto do_subtask = [/* a lambda function without captures can be a coroutine */](
                                      SubTaskType subtask_, auto & result_holder_, size_t i_, auto & store_current_exception_) -> Task<>
                {
                    try
                    {
                        co_await StopIfCancelled{};

                        if constexpr (std::is_void_v<ResultType>)
                            co_await subtask_;
                        else
                            (*result_holder_.getResultPtr())[i_] = co_await subtask_;
                    }
                    catch (...)
                    {
                        store_current_exception_();
                    }
                };

                awaiters.emplace_back(do_subtask(std::move(subtasks[i]), result_holder, i, store_current_exception).run(params));
            }
        }
        catch (...)
        {
            store_current_exception();
        }

        /// Wait until each subtask finishes. We continue doing that even after exceptions because the subtasks are parallel and we don't want to leave them hanging
        /// after returning from parallelImpl().
        for (auto & awaiter : awaiters)
        {
            try
            {
                co_await awaiter;
            }
            catch (...)
            {
                store_current_exception();
            }
        }

        {
            std::lock_guard lock{exception_mutex};
            if (exception)
                std::rethrow_exception(exception);
        }

        /// Returns the results of the subtasks as a vector (if it's not void).
        if constexpr (!std::is_void_v<ResultType>)
            co_return std::move(*result_holder.getResultPtr());
    }
}

template <typename ResultType>
Task<std::vector<ResultType>> parallel(std::vector<Task<ResultType>> subtasks)
{
    return details::parallelImpl(std::move(subtasks));
}

}
