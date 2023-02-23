#include <Common/CoTask.h>
#include <Interpreters/threadPoolCallbackRunner.h>


namespace DB::Co
{

bool details::ContinuationList::add(std::coroutine_handle<> new_item)
{
    std::lock_guard lock{mutex};
    if (!processed)
    {
        items.push_back(std::move(new_item));
        return true;
    }
    return false;
}

bool details::ContinuationList::process()
{
    std::list<std::coroutine_handle<>> items_to_process;
    {
        std::lock_guard lock{mutex};
        if (processed)
            return false;
        items_to_process = std::exchange(items, {});
        processed = true;
    }
    if (items_to_process.empty())
        return false;
    for (auto & item : items_to_process)
        item.resume();
    return true;
}

bool details::ContinuationList::isProcessed() const
{
    std::lock_guard lock{mutex};
    return processed;
}

bool details::ContinuationList::markProcessedIfEmpty()
{
    std::lock_guard lock{mutex};
    if (items.empty())
    {
        processed = true;
        return true;
    }
    return false;
}

Task<> sequential(std::vector<Task<>> && tasks)
{
    if (tasks.empty())
        co_return;

    SCOPE_EXIT_SAFE( tasks.clear(); );
    auto scheduler = co_await Scheduler{};

    for (auto & task : tasks)
    {
        co_await StopIfCancelled{};
        co_await std::move(task).run(scheduler);
    }
}

Task<> parallel(std::vector<Task<>> && tasks)
{
    if (tasks.empty())
        co_return;

    SCOPE_EXIT_SAFE( tasks.clear(); );

    auto scheduler = co_await Scheduler{};

    struct AwaitInfo
    {
        Task<>::Awaiter awaiter;
        bool ready = false;
    };

    std::vector<AwaitInfo> await_infos;
    await_infos.reserve(tasks.size());

    std::exception_ptr exception;

    try
    {
        for (auto & task : tasks)
        {
            await_infos.emplace_back(AwaitInfo{std::move(task).run(scheduler)});
            auto & info = await_infos.back();

            /// After `task.run()` the `task` can be already finished.
            if (info.awaiter.await_ready())
            {
                info.ready = true;
                co_await info.awaiter; /// Call co_await anyway because it can throw an exception.
            }
        }

        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                co_await StopIfCancelled{};
                info.ready = true;
                co_await info.awaiter;
            }
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    if (exception)
    {
        for (auto & info : await_infos)
        {
            if (!info.ready)
                info.awaiter.tryCancelTask();
        }
        
        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                try
                {
                    co_await info.awaiter;
                }
                catch (...)
                {
                }
            }
        }
        
        std::rethrow_exception(exception);
    }
}

}
