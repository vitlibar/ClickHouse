#pragma once

#include <Common/CoTask_fwd.h>
#include <Common/scope_guard_safe.h>
#include <Common/threadPoolCallbackRunner.h>
#include <coroutine>
#include <utility>


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
///    co_await Co::parallel(std::move(tasks));
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

    void schedule(std::function<void()> && callback) const { schedule_function(std::move(callback)); }

private:
    static void defaultScheduleFunction(std::function<void()> callback) { std::move(callback)(); }
    std::function<void(std::function<void()>)> schedule_function;
};

struct TaskRunParams;


/// Co::Task keeps an asynchronous task, must be specified as a result type when declaring a coroutine.
/// For example,
///     Co::Task<> doSomething {}
///     Co::Task<int> readInt() { co_return 5; }
template <typename ResultType /* = void */>
class [[nodiscard]] Task
{
    class Promise;

public:
    /// Creates a non-initialized task, it cannot be executed until assigned.
    Task() = default;

    /// Task is a copyable class.
    Task(const Task &);
    Task & operator=(const Task &) = delete;

    Task(Task&& task_) noexcept : controller(std::exchange(task_.controller, {})) {}
    Task & operator=(Task && task_) noexcept;

    /// It's allowed to destroy a task anytime (before starting, while running, after completion).
    ~Task();

    /// Whether this is an initialized task ready to execute.
    /// Returns false if the task is either created by the default constructor or has already run.
    operator bool() const { return static_cast<bool>(controller); }

    class Awaiter;

    /// Runs a task on a passed scheduler, returns a waiter which can be checked to find out when it's done.
    /// Example of usage: `co_await task.run(scheduler)`.
    Awaiter run(const Scheduler & scheduler) &&;

    /// `RunParams` is a structure used to run a coroutine from another coroutine.
    /// `co_await Co::TaskRunParams{}` can be used to get `RunParams` inside a coroutine.
    using RunParams = TaskRunParams;

    /// Runs a coroutine from another coroutine. Normally shouldn't be called because the `co_await` operator should be used instead.
    /// The following two calls are the same
    /// `co_await subCoroutineName(a, b)`
    /// `co_await subCoroutineName(a, b).runWithParams(co_await Co::TaskRunParams{})`
    Awaiter runWithParams(const RunParams & params) &&;

    /// Runs a task and blocks the current thread until it's done.
    /// Prefer run() to this function if possible.
    ResultType syncRun(const Scheduler & scheduler) &&;

    /// We should define `promise_type` so C++ compiler will be able to use Co::Task<...> with coroutines.
    using promise_type = Promise;

private:
    class Controller;
    class PromiseBase;

    /// Tasks are initialized by promise_type.
    friend class Promise;
    explicit Task(const std::shared_ptr<Controller> & controller_) : controller(controller_) {}

    std::shared_ptr<Controller> controller;
};


/// Represents an asynchronous waiter for a task to be finished.
/// This class has functions await_ready(), await_suspend(), await_resume() so it can an argument of the `co_await` operator.
template <typename ResultType>
class [[nodiscard]] Task<ResultType>::Awaiter
{
public:
    /// Awaiters are copyable, no problem with that.

    /// Returns true if the task has already finished.
    bool isReady() const { return controller->isContinuationProcessed(); }

    /// Blocks the current thread until the coroutine finishes or throws an exception.
    ResultType syncWait();

    /// Tries to cancel the coroutine. If succeeded the coroutine stops with a specified exception.
    /// That doesn't act immediately because the coroutine decides by itself where to stop (by calling coawait Co::StopIfCancelled{}).
    /// After calling tryCancelTask() the coroutine can still finish successfully if it decides so.
    void tryCancelTask(std::exception_ptr cancel_exception_) { controller->getCancelStatus()->setCancelException(cancel_exception_); }
    void tryCancelTask(const String & message_ = {}) { controller->getCancelStatus()->setCancelException(message_); }

    /// To be a proper awaiter this class defines await_ready(), await_suspend(), await_resume() functions.
    /// Those functions are required by compiler to generate code for the statement `co_await awaiter`.
    bool await_ready() { return controller->isContinuationProcessed(); }
    bool await_suspend(std::coroutine_handle<> handle_) { return controller->addContinuation(handle_); }

    ResultType await_resume()
    {
        if (auto exception = controller->getException())
            std::rethrow_exception(exception);
        if constexpr (!std::is_void_v<ResultType>)
            return controller->getResult();
    }

private:
    /// Awaiter can be made by Task::run() only.
    friend class Task<ResultType>;
    Awaiter(const std::shared_ptr<Controller> & controller_) : controller(controller_) {}

    std::shared_ptr<Controller> controller;
};


/// Runs multiple tasks one after another and returns all the results in the same order.
template <typename Result>
Task<std::vector<Result>> sequential(std::vector<Task<Result>> && tasks);

Task<> sequential(std::vector<Task<>> && tasks);


/// Runs multiple tasks in parallel and returns all the results in the same order.
template <typename Result>
Task<std::vector<Result>> parallel(std::vector<Task<Result>> && tasks);

Task<> parallel(std::vector<Task<>> && tasks);


/// Usage: `co_await Co::IsCancelled{}` inside a coroutine returns a boolean meaning whether the coroutine is ordered to cancel
/// (by calling awaiter.tryCancelTask()). Then the coroutine can decide by itself what to do with that: it can either ignore that and continue working
/// or finish working by returning or throwing an exception.
struct IsCancelled
{
    bool value = false;
    operator bool() const { return value; }
};

/// Usage: `co_await Co::StopIfCancelled{}` inside a coroutine checks whether the coroutine is ordered to cancel, and if so,
/// stops the coroutine by throwing the exception specified in Awaiter::tryCancelTask().
struct StopIfCancelled {};


/// The following are implementation details.
namespace details
{
    /// ContinuationList is used to resume waiting coroutines after the task has finished.
    class ContinuationList
    {
    public:
        /// Adds a new item if the list has not been processed yet, otherwise the function returns false.
        bool add(std::coroutine_handle<> new_item);

        /// Process all items in the list. The function returns false if they were already processed or there were no items in the list.
        void process();

        /// Returns whether the list is already processed.
        bool isProcessed() const;

    private:
        std::list<std::coroutine_handle<>> TSA_GUARDED_BY(mutex) items;
        bool TSA_GUARDED_BY(mutex) processed = false;
        mutable std::mutex mutex;
    };

    /// Keeps the result of a coroutine (which can be void).
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

    /// Checks whether a specified type is an awaiter, i.e. has the member function await_ready().
    template<typename F>
    concept AwaiterConcept = requires(F f)
    {
        {f.await_ready()} -> std::same_as<bool>;
    };

    /// Holds a cancel exception to helps implementing Awaiter::tryCancelTask().
    class CancelStatus
    {
    public:
        CancelStatus() = default;
        virtual ~CancelStatus() = default;

        void setCancelException(const String & message_ = {});
        void setCancelException(std::exception_ptr cancel_exception_);
        std::exception_ptr getCancelException() const;
        bool isCancelled() const { return static_cast<bool>(getCancelException()); }

        /// Registers this cancel status as a child of another cancel status, so when setCancelException() is called on parent it's also called on child.
        /// This function is used only in complex scenarious, like parallel().
        void addChild(std::shared_ptr<CancelStatus> child_);

    protected:
        /// Inhetrited classes can do extra work here.
        virtual void onCancelled(std::exception_ptr /* cancel_exception_ */) {}

    private:
        mutable std::mutex mutex;
        std::exception_ptr TSA_GUARDED_BY(mutex) cancel_exception;
        std::list<std::shared_ptr<CancelStatus>> TSA_GUARDED_BY(mutex) children;
    };

    /// A helper task, part of the implementation of syncWait() and syncRun().
    class SyncWaitTask
    {
    public:
        template <typename ResultType>
        static SyncWaitTask runAndSetEvent(typename Task<ResultType>::Awaiter & awaiter, ResultHolder<ResultType> & result, std::exception_ptr & exception, Poco::Event & event)
        {
            try
            {
                if constexpr (std::is_void_v<ResultType>)
                    co_await awaiter;
                else
                    result.setResult(co_await awaiter);
            }
            catch (...)
            {
                exception = std::current_exception();
            }
            event.set();
        }

        struct promise_type
        {
            auto get_return_object() { return SyncWaitTask{}; }
            std::suspend_never initial_suspend() { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void unhandled_exception() { }
            void return_void() {}
        };
    };
}

/// Waits for the result in synchronous (blocking) manner.
template <typename ResultType>
ResultType Task<ResultType>::Awaiter::syncWait()
{
    Poco::Event event;
    std::exception_ptr exception;
    details::ResultHolder<ResultType> result_holder;
    details::SyncWaitTask::runAndSetEvent(*this, result_holder, exception, event);
    event.wait();
    if (exception)
        std::rethrow_exception(exception);
    if constexpr (!std::is_void_v<ResultType>)
        return std::move(*result_holder.getResultPtr());
}

template <typename ResultType>
ResultType Task<ResultType>::syncRun(const Scheduler & scheduler) &&
{
    if constexpr (std::is_void_v<ResultType>)
        std::move(*this).run(scheduler).syncWait();
    else
        return std::move(*this).run(scheduler).syncWait();
}

template <typename ResultType>
Task<ResultType>::~Task()
{
    if (controller)
        controller->onTaskDestroyedBeforeStart();
}

template <typename ResultType>
Task<ResultType> & Task<ResultType>::operator=(Task && task_) noexcept
{
    if (this == &task_)
        return *this;
    if (controller)
        controller->onTaskDestroyedBeforeStart();
    controller = std::exchange(task_.controller, {});
    return *this;
}

/// TaskRunParams contains parameters to run a coroutine, see Task::runWithParams().
struct TaskRunParams
{
    bool reschedule = true;
    Scheduler scheduler;
    std::shared_ptr<details::CancelStatus> cancel_status;
};

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::run(const Scheduler & scheduler) &&
{
    TaskRunParams params;
    params.scheduler = scheduler;
    return std::move(*this).runWithParams(params);
}

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::runWithParams(const TaskRunParams & params) &&
{
    chassert(controller);
    auto ctrl = std::exchange(controller, nullptr);
    return ctrl->run(params);
}

/// PromiseBase is a helper which is used as a base class for Promise.
template <typename ResultType>
class Task<ResultType>::PromiseBase
{
public:
    void return_value(ResultType result_) { controller->setResult(std::move(result_)); }
protected:
    std::shared_ptr<typename Task<ResultType>::Controller> controller;
};

template <>
class Task<void>::PromiseBase
{
public:
    void return_void() {}
protected:
    std::shared_ptr<typename Task<>::Controller> controller;
};

/// Represents the `promise_type` for coroutine tasks.
/// There is always a single instance of Promise per each coroutine running.
/// When a coroutine starts the compiler-generated code:
/// 1) creates an instance of the `promise_type` and calls `Promise::Promise()`
/// 2) creates a task by calling `Promise::get_return_object()`.
/// 3) suspends the coroutine because `initial_suspend()` returns `std::suspend_always{}`
/// 4) the coroutine stays in suspended state until `Task::run()` or `Task::syncRun()` will resume it.
template <typename ResultType>
class Task<ResultType>::Promise : public PromiseBase
{
public:
    Promise() { this->controller = std::make_shared<Controller>(std::coroutine_handle<Promise>::from_promise(*this)); }

    Task get_return_object() { return Task(getController()); }

    auto initial_suspend() { return std::suspend_always{}; }

    class FinalSuspend;
    auto final_suspend() noexcept { return FinalSuspend{getController()}; }

    void unhandled_exception() { getController()->setException(std::current_exception()); }

    auto await_transform(RunParams)
    {
        struct Getter : std::suspend_never
        {
            RunParams params;
            RunParams await_resume() const noexcept { return params; }
        };
        return Getter{{}, getController()->getRunParams()};
    }

    auto await_transform(IsCancelled)
    {
        struct Getter : std::suspend_never
        {
            bool is_cancelled;
            IsCancelled await_resume() const noexcept { return {is_cancelled}; }
        };
        return Getter{{}, static_cast<bool>(getController()->getCancelStatus()->isCancelled())};
    }

    auto await_transform(StopIfCancelled)
    {
        struct Getter : std::suspend_never
        {
            std::exception_ptr cancel_exception;
            void await_resume() const
            {
                if (cancel_exception)
                    std::rethrow_exception(cancel_exception);
            }
        };
        return Getter{{}, getController()->getCancelStatus()->getCancelException()};
    }

    template <details::AwaiterConcept AwaiterType>
    auto await_transform(AwaiterType && awaiter)
    {
        return std::forward<AwaiterType>(awaiter);
    }

    template <typename SubTaskResultType>
    typename Task<SubTaskResultType>::Awaiter await_transform(Task<SubTaskResultType> && sub_task)
    {
        return std::move(sub_task).runWithParams(getController()->getRunParams());
    }

private:
    std::shared_ptr<typename Task<ResultType>::Controller> getController() { return this->controller; }
};

/// FinalSuspend is used to resume waiting coroutines stored in the contunuation list after the task has finished.
///
/// `co_await subtask_awaiter` is transformed by the compiler into something like:
/// if (!subtask_awaiter.await_ready())
/// {
///     subtask_awaiter.suspend(this_task_handle); // Stores <resume_point> in the continuation list of `subtask`. Later when `subtask` is done,
///                                                // its `FinalSuspend` will process the continuation list and resume `this_task` at <resume_point>.
///     <resume_point>
/// }
/// subtask_awaiter.resume(); /// This will rethrow an exception from `subtask`.
///
template <typename ResultType>
class Task<ResultType>::Promise::FinalSuspend : public std::suspend_never
{
public:
    FinalSuspend(std::shared_ptr<Controller> controller_) : controller(controller_) {}

    bool await_ready() noexcept
    {
        /// Give control to the code waiting for this coroutine to be finished.
        controller->processContinuation();
        return true;
    }

private:
    std::shared_ptr<Controller> controller;
};

/// The core of the coroutine. An instance of Controller is shared between Task, Promise, and Awaiter (there can be 0 or 1 or more Awaiters).
template <typename ResultType>
class Task<ResultType>::Controller : public details::ResultHolder<ResultType>, public std::enable_shared_from_this<Controller>
{
public:
    Controller(std::coroutine_handle<Promise> handle_) : handle(handle_) { }
    Controller(const Controller &) = delete;
    Controller & operator=(const Controller &) = delete;

    /// This is possible that a task is destroyed without calling run().
    void onTaskDestroyedBeforeStart() { destroyHandleIfNeverResumed(); }

    Awaiter run(const RunParams & params_)
    {
        /// The coroutine handle will be destroyed along with the `job` if it's never resumed.
        auto destroy_handle_if_never_resumed = std::make_shared<scope_guard>([this, keep_this_from_destruction = this->shared_from_this()]
                                                                             { destroyHandleIfNeverResumed(); });

        auto job = [this, destroy_handle_if_never_resumed]
        {
            if (auto cancel_exception = getCancelStatus()->getCancelException())
            {
                /// Go straight to the continuation list without resuming this task.
                setException(cancel_exception);
                processContinuation();
                return;
            }

            resumed = true;
            handle.resume();
        };

        scheduler = params_.scheduler;
        cancel_status = params_.cancel_status;

        if (!cancel_status)
            cancel_status = std::make_shared<details::CancelStatus>();

        if (params_.reschedule)
            scheduler.schedule(job);
        else
            job();

        return Awaiter(this->shared_from_this());
    }

    /// Gets parameter to run a subtask.
    RunParams getRunParams() const { return {false, scheduler, cancel_status}; }

    bool isContinuationProcessed() const { return continuation_list.isProcessed(); }
    bool addContinuation(std::coroutine_handle<> handle_) { return continuation_list.add(handle_); }
    void processContinuation() { continuation_list.process(); }

    void setException(std::exception_ptr exception_) { exception = exception_; }
    std::exception_ptr getException() const { return exception; }
    std::shared_ptr<details::CancelStatus> getCancelStatus() const { return cancel_status; }

private:
    /// std::coroutine_handle<>::destroy() must be called only if the coroutine is never resumed.
    /// (Because if the coroutine is resumed and executes normally it will destroy itself after finish.)
    void destroyHandleIfNeverResumed()
    {
        if (handle && !resumed)
        {
            handle.destroy();
            handle = nullptr;
        }
    }

    std::coroutine_handle<Promise> handle;
    Scheduler scheduler;
    std::shared_ptr<details::CancelStatus> cancel_status;
    std::atomic<bool> resumed = false; /// The coroutine is resumed in run(), but it might be never resumed.
    details::ContinuationList continuation_list;

    /// Mutex is not needed for `exception` because `exception` is accessed in separate moments of time (so no concurrent access possible):
    /// Controller::run(), Promise::unhandled_exception(), Awaiter::await_resume().
    std::exception_ptr exception;
};

template <typename ResultType>
Task<std::vector<ResultType>> sequential(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE({ tasks.clear(); });
    results.reserve(tasks.size());

    /// Use the same run parameters to start all the tasks.
    auto params = co_await TaskRunParams{};

    for (auto & task : tasks)
    {
        co_await StopIfCancelled{}; /// Stop starting tasks if cancelled.
        results.emplace_back(co_await std::move(task).runWithParams(params));
    }

    co_return results;
}

template <typename ResultType>
Task<std::vector<ResultType>> parallel(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE({ tasks.clear(); });
    results.reserve(tasks.size());

    /// Use the same run parameters to start all the tasks.
    auto params = co_await TaskRunParams{};
    params.reschedule = (tasks.size() > 1);
    auto cancel_status = std::make_shared<details::CancelStatus>();
    params.cancel_status->addChild(cancel_status);
    params.cancel_status = cancel_status;

    struct AwaitInfo
    {
        typename Task<ResultType>::Awaiter awaiter;
        bool ready = false;
        size_t index_in_results = static_cast<size_t>(-1); /// we reorder results in the correct order at finish
    };
    std::vector<AwaitInfo> await_infos;
    await_infos.reserve(tasks.size());

    std::exception_ptr exception;

    try
    {
        /// Start or schedule all the tasks.  
        for (auto & task : tasks)
        {
            co_await StopIfCancelled{}; /// can cancel before each task.

            await_infos.emplace_back(AwaitInfo{std::move(task).runWithParams(params)});
            auto & info = await_infos.back();

            /// After `task.runWithParams()` the `task` can be already finished.
            if (info.awaiter.isReady())
            {
                info.ready = true;
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter); /// Call co_await here to extract the result from the finished task.
            }
        }

        /// Wait for each task to finish.
        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                co_await StopIfCancelled{}; /// can cancel before each task.
                info.ready = true;
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter);
            }
        }

        /// Reorder the results in the correct order.
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
        try
        {
            cancel_status->setCancelException("Task was cancelled because a parallel task failed");
        }
        catch (...)
        {
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
