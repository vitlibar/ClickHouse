#pragma once

#include <Common/Coroutines/Task.h>


namespace DB::Coroutine
{

/// Runs multiple tasks one after another and returns all the results in the same order.
template <typename Result>
Task<std::vector<Result>> sequential(std::vector<Task<Result>> tasks);

Task<> sequential(std::vector<Task<>> tasks);


/// Implementation details.
namespace details
{
    template <
        typename ContainerType,
        typename SubTaskType = typename ContainerType::value_type,
        typename SubTaskResultType = typename SubTaskType::result_type,
        typename ResultType = std::conditional_t<std::is_void_v<SubTaskResultType>, void, std::vector<SubTaskResultType>>>
    Task<ResultType> sequentialImpl(ContainerType subtasks)
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
            result_holder.getResultPtr()->reserve(subtasks.size());

        /// The same parameters will be used to run all the subtasks.
        auto params = co_await RunParams{};

        /// Run all the subtasks in a sequence.
        for (const auto & subtask : subtasks)
        {
            co_await StopIfCancelled{};
            if constexpr (std::is_void_v<ResultType>)
                co_await std::move(subtask).runWithParams(params);
            else
                result_holder.getResultPtr()->emplace_back(co_await std::move(subtask).runWithParams(params));
        }

        /// Returns the results of the subtasks as a vector (if it's not void).
        if constexpr (!std::is_void_v<ResultType>)
            co_return std::move(*result_holder.getResultPtr());
    }
}

template <typename ResultType>
Task<std::vector<ResultType>> sequential(std::vector<Task<ResultType>> subtasks)
{
    return details::sequentialImpl(std::move(subtasks));
}

}
