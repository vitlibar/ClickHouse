#pragma once

#include <coroutine>
#include <utility>
#include <Common/scope_guard_safe.h>
#include <Interpreters/threadPoolCallbackRunner.h>


namespace DB::Co
{

/// This file declares utilities to deal with C++20 coroutines.
/// Example: (prints integers using three threads in parallel)
///
/// Co::Task<> print_numbers(size_t count)
/// {
///    for (size_t i = 0; i != count; ++i)
///    {
///        std::cout << "thread=" << std::this_thread::get_id() << ", i=" << i << std::endl;
///        sleepForMilliseconds(100);
///    }
///    co_return;
/// }
/// 
/// Co::Task<> print_numbers_in_parallel()
/// {
///    std::vector<Co::Task<>> tasks;
///    tasks.push_back(print_numbers(5));
///    tasks.push_back(print_numbers(10));
///    tasks.push_back(print_numbers(12));
///
///    auto scheduler = co_await Co::Scheduler{}; // get the scheduler of the current coroutine to pass it into subcoroutines.
///    co_await Co::parallel(std::move(tasks)).run(scheduler);
/// }
/// 
/// 
/// void main()
/// {
///     ThreadPool thread_pool{10};
///     Co::Scheduler scheduler{thread_pool};
///     auto main_task = print_numbers_in_parallel(); /// create a main task to execute but don't execute it yet
///     std::move(main_task).syncRun(scheduler); /// execute the main task and block the current thread until it finishes or throws an exception.
///  }

/// Scheduler is used to schedule tasks to run them on another thread.
class Scheduler
{
public:
    Scheduler() : schedule_function(&Scheduler::defaultScheduleFunction) { }

    explicit Scheduler(const std::function<void(std::function<void()>)> & schedule_function_) : Scheduler()
    {
        if (schedule_function_)
            schedule_function = schedule_function_;
    }

    explicit Scheduler(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) : Scheduler()
    {
        if (thread_pool_callback_runner)
            schedule_function = [thread_pool_callback_runner](std::function<void()> callback)
            { thread_pool_callback_runner(std::move(callback), 0); };
    }

    explicit Scheduler(ThreadPool & thread_pool, const std::string & thread_name = "Co::Scheduler")
        : Scheduler(threadPoolCallbackRunner<void>(thread_pool, thread_name))
    {
    }

    void schedule(std::function<void()> callback) const { schedule_function(std::move(callback)); }

private:
    static void defaultScheduleFunction(std::function<void()> callback) { std::move(callback)(); }
    std::function<void(std::function<void()>)> schedule_function;
};


/// Keeps an asynchronous task, used as a return type in coroutines.
template <typename ResultType = void>
class [[nodiscard]] Task
{
    class Promise;
    class PromiseBase;
    class AwaiterControl;
    class FinalSuspend;

public:
    using promise_type = Promise;

    /// Creates a non-initialized task, it cannot be executed until assigned.
    Task() = default;

    /// Tasks are initialized by promise_type.
    explicit Task(const std::shared_ptr<AwaiterControl> & awaiter_control_) : awaiter_control(awaiter_control_) {}

    /// Task is a move-only class.
    Task(const Task &) = delete;
    Task & operator=(const Task &) = delete;

    Task(Task&& task_) noexcept : awaiter_control(std::exchange(task_.awaiter_control, {})) {}

    Task & operator=(Task && task_) noexcept
    {
        if (this == &task_)
            return *this;
        awaiter_control = std::exchange(task_.awaiter_control, {});
        return *this;
    }

    /// Whether this is an initialized task ready to execute.
    /// Returns false if the task is either created by the default constructor or has already run.
    operator bool() const { return static_cast<bool>(awaiter_control); }

    class Awaiter;

    /// Runs a task on a passed scheduler, returns a waiter which can be checked to find out when it's done.
    /// Example of usage: `co_await task.run(scheduler)`.
    Awaiter run(const Scheduler & scheduler) &&
    {
        chassert(awaiter_control);
        auto & promise = awaiter_control->getHandle().promise();
        auto awaiter = promise.run(scheduler);
        awaiter_control.reset();
        return awaiter;
    }

    /// Runs a task and blocks the current thread until it's done.
    /// Prefer run() to this function if possible.
    ResultType syncRun(const Scheduler & scheduler) &&;

private:
    std::shared_ptr<AwaiterControl> awaiter_control;
};


/// Represents an asynchronous waiter for a task to be finished.
/// This class has functions await_ready(), await_suspend(), await_resume() so it can an argument of the `co_await` operator.
template <typename ResultType>
class [[nodiscard]] Task<ResultType>::Awaiter
{
public:
    Awaiter(const std::shared_ptr<AwaiterControl> & awaiter_control_) : awaiter_control(awaiter_control_) {}

    bool await_ready()
    {
        return awaiter_control->isContinuationProcessed();
    }
    bool await_suspend(std::coroutine_handle<> handle_) { return awaiter_control->addContinuation(handle_); }

    ResultType await_resume()
    {
        if (auto exception = awaiter_control->getException())
            std::rethrow_exception(exception);
        if constexpr (!std::is_void_v<ResultType>)
            return awaiter_control->getResult();
    }

    /// Blocks the current thread until the coroutine finishes or throws an exception.
    ResultType syncWait();

    /// Tries to cancel the coroutine. If succeeded the coroutine stops with Co::CancelledException.
    /// That doesn't act immediately because the coroutine decides by itself where to stop.
    /// After calling tryCancelTask() the coroutine can still finish successfully if it desides so.
    void tryCancelTask() { awaiter_control->setIsCancelled(); }

private:
    std::shared_ptr<AwaiterControl> awaiter_control;
};


/// Runs multiple tasks one after another.
template <typename Result>
Task<std::vector<Result>> sequential(std::vector<Task<Result>> && tasks);

Task<> sequential(std::vector<Task<>> && tasks);


/// Runs multiple tasks in parallel.
template <typename Result>
Task<std::vector<Result>> parallel(std::vector<Task<Result>> && tasks);

Task<> parallel(std::vector<Task<>> && tasks);


/// This exception is thrown by a coroutine when it's cancelled.
class CancelledException : public Exception
{
public:
    CancelledException() = default;
};

/// Usage: `co_await Co::IsCancelled{}` inside a coroutine returns a boolean meaning whether the coroutine must be cancelled.
struct IsCancelled
{
    bool value = false;
    operator bool() const { return value; }
};

/// Usage: `co_await Co::StopIfCancelled{}` inside a coroutine stops the coroutine by throwing an exception of type `CancelledException`
/// if the coroutine must be cancelled.
struct StopIfCancelled {};


/// The following are implementation details.
namespace details
{
    /// Keeps a result for the promise type.
    template <typename ResultType>
    class ResultHolder
    {
    public:
        void setResult(ResultType result_) { result = std::move(result_); }
        ResultType getResult() const { return result; }
        ResultType * getResultPtr() { return &result; }

    private:
        ResultType result;
    };

    template <>
    class ResultHolder<void>
    {
    public:
        void * getResultPtr() { return nullptr; }
        void return_void() {}
    };

    /// ContinuationList is used to run resume waiting coroutines after the task has finished.
    class ContinuationList
    {
    public:
        /// Adds a new item if the list has not been processed yet, otherwise the function returns false.
        bool add(std::coroutine_handle<> new_item);

        /// Process all items in the list. The function returns false if they were already processed or there were no items in the list.
        bool process();

        bool isProcessed() const;

        /// It the list is empty then the function marks it as processed, otherwise the function returns false.
        bool markProcessedIfEmpty();

    private:
        std::list<std::coroutine_handle<>> TSA_GUARDED_BY(mutex) items;
        bool TSA_GUARDED_BY(mutex) processed = false;
        mutable std::mutex mutex;
    };

    /// The simpliest coroutine task possible.
    struct DummyTask
    {
        struct promise_type
        {
            auto get_return_object() { return DummyTask{}; }
            std::suspend_never initial_suspend() { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void unhandled_exception() { }
            void return_void() { }
        };
    };

    /// A helper coroutine, part of the implementation of syncWait() and syncRun().
    template <typename ResultType>
    static DummyTask syncWaitImpl(typename Task<ResultType>::Awaiter * awaiter, ResultType * result, std::exception_ptr * exception, Poco::Event * event)
    {
        try
        {
            if constexpr(std::is_void_v<ResultType>)
                co_await *awaiter;
            else
                *result = co_await *awaiter;
        }
        catch (...)
        {
            *exception = std::current_exception();
        }
        event->set();
        co_return;
    }
}

/// Waits for the result in synchronous (blocking) manner.
template <typename ResultType>
ResultType Task<ResultType>::Awaiter::syncWait()
{
    Poco::Event event;
    std::exception_ptr exception;
    details::ResultHolder<ResultType> result_holder;
    details::syncWaitImpl(this, result_holder.getResultPtr(), &exception, &event);
    event.wait();
    if (exception)
        std::rethrow_exception(exception);
    if constexpr (!std::is_void_v<ResultType>)
        return std::move(*result_holder.getResultPtr());
}

template <typename ResultType>
ResultType Task<ResultType>::syncRun(const Scheduler & scheduler) &&
{
    if constexpr(std::is_void_v<ResultType>)
        std::move(*this).run(scheduler).syncWait();
    else
        return std::move(*this).run(scheduler).syncWait();
}

template <typename ResultType>
class Task<ResultType>::PromiseBase
{
public:
    void return_value(ResultType result_) { awaiter_control->setResult(std::move(result_)); }

protected:
    std::shared_ptr<AwaiterControl> awaiter_control;
};

template <>
class Task<void>::PromiseBase
{
public:
    void return_void() {}

protected:
    std::shared_ptr<AwaiterControl> awaiter_control;
};

/// Represents the `promise_type` for coroutine tasks.
template <typename ResultType>
class Task<ResultType>::Promise : public Task<ResultType>::PromiseBase
{
public:
    Task get_return_object()
    {
        this->awaiter_control = std::make_shared<AwaiterControl>(std::coroutine_handle<Promise>::from_promise(*this));
        return Task(getAwaiterControl());
    }

    Awaiter run(const Scheduler & scheduler_)
    {
        /// We store `scheduler_` in the promise so it will be later accessible with `co_await Co::Scheduler{}`
        scheduler = scheduler_;

        return getAwaiterControl()->run(scheduler_);
    }

    auto initial_suspend() { return std::suspend_always{}; }

    auto final_suspend() noexcept { return FinalSuspend{getAwaiterControl()}; }
    void unhandled_exception() { getAwaiterControl()->setException(std::current_exception()); }

    auto await_transform(Scheduler) noexcept
    {
        struct GetSchedulerAwaiter : std::suspend_never
        {
            Scheduler scheduler;
            Scheduler await_resume() const noexcept { return scheduler; }
        };
        return GetSchedulerAwaiter{{}, scheduler};
    }

    auto await_transform(IsCancelled) noexcept
    {
        struct CheckIfCancelledAwaiter : std::suspend_never
        {
            bool is_cancelled;
            IsCancelled await_resume() const noexcept { return IsCancelled{is_cancelled}; }
        };
        return CheckIfCancelledAwaiter{{}, getAwaiterControl()->isCancelled()};
    }

    auto await_transform(StopIfCancelled)
    {
        struct StopIfCancelledAwaiter : std::suspend_never
        {
            bool is_cancelled;
            void await_resume() const
            {
                if (is_cancelled)
                    throw CancelledException{};
            }
        };
        return StopIfCancelledAwaiter{{}, getAwaiterControl()->isCancelled()};
    }

    template <typename AwaiterType>
    AwaiterType await_transform(AwaiterType && awaiter) noexcept
    {
        return std::move(awaiter);
    }

    template <typename AwaiterType>
    AwaiterType & await_transform(AwaiterType & awaiter) noexcept
    {
        return awaiter;
    }

private:
    std::shared_ptr<AwaiterControl> getAwaiterControl() const { return this->awaiter_control; }
    Scheduler scheduler;
};

/// Keeps std::coroutine_handle<> and destroys it in the destructor if the coroutine has not finished
/// (if it has finished it will be destroyed automatically).
template <typename ResultType>
class Task<ResultType>::AwaiterControl : public details::ResultHolder<ResultType>, public std::enable_shared_from_this<AwaiterControl>
{
public:
    AwaiterControl(std::coroutine_handle<Promise> handle_) : handle(handle_) { }
    AwaiterControl(const AwaiterControl &) = delete;
    AwaiterControl & operator=(const AwaiterControl &) = delete;

    ~AwaiterControl()
    {
        if (!is_finalized)
            handle.destroy();
    }

    std::coroutine_handle<Promise> getHandle() const { return handle; }

    Awaiter run(const Scheduler & scheduler_)
    {
        auto awaiter = Awaiter(this->shared_from_this());

        scheduler_.schedule([this, keep_this_from_destruction = this->shared_from_this()]
        {
            if (isCancelled())
            {
                /// Go straight to the continuation list without resuming this task.
                setException(std::make_exception_ptr(CancelledException{}));
                processContinuation();
                return;
            }

            handle.resume();
        });

        return awaiter;
    }

    bool isContinuationProcessed() const { return continuation_list.isProcessed(); }
    bool addContinuation(std::coroutine_handle<> handle_) { return continuation_list.add(handle_); }
    bool markContinuationProcessedIfEmpty() { return continuation_list.markProcessedIfEmpty(); }
    bool processContinuation() { return continuation_list.process(); }

    void setIsCancelled() { is_cancelled.store(true); }
    bool isCancelled() const { return is_cancelled.load(); }

    void setIsFinalized() { is_finalized = true; }
    void setException(std::exception_ptr exception_) { exception = exception_; }
    std::exception_ptr getException() const { return exception; }

private:
    std::coroutine_handle<Promise> handle;
    details::ContinuationList continuation_list;
    std::atomic<bool> is_cancelled;
    bool is_finalized = false; /// Synchronization is not needed, `is_finalized` is never accessed concurrently.
    std::exception_ptr exception; /// Synchronization is not needed, `exception` is never accessed concurrently.
};

/// FinalSuspend is used to resume waiting coroutines stored in the contunuation list after the task has finished.
template <typename ResultType>
class Task<ResultType>::FinalSuspend
{
public:
    FinalSuspend(std::shared_ptr<AwaiterControl> awaiter_control_) : awaiter_control(awaiter_control_) {}

    bool await_ready() noexcept
    {
        /// The coroutine has finished normally, maybe with an exception, but it wasn't cancelled,
        /// so ~AwaiterControl must not destroy it.
        awaiter_control->setIsFinalized();

        return awaiter_control->markContinuationProcessedIfEmpty();
    }

    bool await_suspend(std::coroutine_handle<>) noexcept { return awaiter_control->processContinuation(); }
    void await_resume() noexcept {}

private:
    std::shared_ptr<AwaiterControl> awaiter_control;
};


template <typename ResultType>
Task<std::vector<ResultType>> sequential(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE( tasks.clear(); );
    auto scheduler = co_await Scheduler{};
    results.reserve(tasks.size());

    for (auto & task : tasks)
    {
        co_await StopIfCancelled{};
        results.emplace_back(co_await std::move(task).run(scheduler));
    }

    co_return results;
}

template <typename ResultType>
Task<std::vector<ResultType>> parallel(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE( tasks.clear(); );

    auto scheduler = co_await Scheduler{};
    results.reserve(tasks.size());

    struct AwaitInfo
    {
        typename Task<ResultType>::Awaiter awaiter;
        bool ready = false;
        size_t index_in_results = static_cast<size_t>(-1);
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
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter);
            }
        }

        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                co_await StopIfCancelled{};
                info.ready = true;
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter);
            }
        }

        for (size_t i = 0; i != tasks.size(); ++i)
        {
            std::swap(results[i], results[await_infos[i].index_in_results]);
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

    co_return results;
}

}
