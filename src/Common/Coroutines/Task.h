#pragma once

#include <Common/Coroutines/Task_fwd.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/scope_guard_safe.h>
#include <coroutine>
#include <utility>


namespace DB::ErrorCodes
{
    extern const int COROUTINE_TASK_CANCELLED;
}


namespace DB::Coroutine
{
struct RunParams;

/// This file declares utilities to deal with C++20 coroutines.
/// Example: (prints integers using three threads in parallel)
///
/// Coroutine::Task<> print_numbers(size_t count)
/// {
///    for (size_t i = 0; i != count; ++i)
///    {
///        std::cout << "thread=" << std::this_thread::get_id() << ", i=" << i << std::endl;
///        sleepForMilliseconds(100);
///    }
///    co_return;
/// }
///
/// Coroutine::Task<> print_numbers_in_parallel()
/// {
///    std::vector<Coroutine::Task<>> tasks;
///    tasks.push_back(print_numbers(5));
///    tasks.push_back(print_numbers(10));
///    tasks.push_back(print_numbers(12));
///    co_await Coroutine::parallel(std::move(tasks));
/// }
///
///
/// void main()
/// {
///     ThreadPool thread_pool{10};
///     auto main_task = print_numbers_in_parallel(); /// create a main task to execute but don't execute it yet
///     std::move(main_task).syncRun(thread_pool); /// execute the main task and block the current thread until it finishes or throws an exception.
///  }

/// Coroutine::Task keeps an asynchronous task, must be specified as a result type when declaring a coroutine.
/// For example,
///     Coroutine::Task<> doNothing {}
///     Coroutine::Task<int> getFive() { co_return 5; }
/// Coroutine::Task<> means the coroutine returns void.
///
template <typename ResultType /* = void */>
class [[nodiscard]] Task
{
    class Promise;

public:
    using result_type = ResultType;

    /// Creates a non-initialized task, it cannot be executed until assigned.
    Task() = default;

    /// Task is a copyable class, however all copies represent the same coroutine run.
    /// So the following will execute `task1` only once.
    /// auto task2 = task1;
    /// co_await task1;
    /// co_await task2;
    Task(const Task &) = default;
    Task & operator=(const Task &) = default;
    Task(Task &&) noexcept = default;
    Task & operator=(Task &&) = default;

    ~Task()
    {
        if (controller && controller.use_count() == 1)
            controller->onDestroy(); /// No more tasks or awaiters, notify the controller.
    }

    class Awaiter;

    /// Starts executing the task using a thread pool, returns a waiter which must be used to find out when it's done.
    /// Example of usage: `co_await task.run(thread_pool)`.
    /// NOTE: A repeated call of run() doesn't execute a coroutine again, it just returns a copy of the first awaiter.
    Awaiter run(ThreadPool & thread_pool, const String & thread_name = "CoroutineWorker") const;
    Awaiter run(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) const;

    /// Starts executing the task using specified parameters.
    /// The parameters can be either constructed from scratch, or extracted from inside of another coroutine with `co_await Coroutine::RunParams{}`.
    /// Usually this function shouldn't be called because the `co_await` operator can be used instead.
    /// The following two calls do the same
    /// `co_await subCoroutine(a, b, c)`
    /// `co_await subCoroutine(a, b, c).run(co_await Coroutine::RunParams{})`
    Awaiter run(const RunParams & params) const;

    /// Makes an awaiter without starting execution of the task.
    /// That means the task must be either already started by calling run() or syncRun() or it can be started soon.
    Awaiter getAwaiter() const;

    /// Runs a task and blocks the current thread until it's done.
    /// Prefer run() or the `co_await` operator to this function if possible.
    ResultType syncRun() const;
    ResultType syncRun(ThreadPool & thread_pool, const String & thread_name = "CoroutineWorker") const;
    ResultType syncRun(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) const;

    /// We should define `promise_type` so C++ compiler will be able to use Coroutine::Task<...> with coroutines.
    using promise_type = Promise;

private:
    class Controller;
    class ControllerHolderForPromise;
    class PromiseBase;

    /// Tasks are initialized by promise_type.
    friend class Promise;
    explicit Task(const std::shared_ptr<Controller> & controller_) : controller(controller_) {}

    std::shared_ptr<Controller> controller;
};


namespace details { class CancelStatus; class Timer; }


/// RunParams contains parameters to execute a coroutine, see Task::run().
struct RunParams
{
    /// Scheduler which will be called to execute the coroutine. The coroutine will be executed on the current thread if `scheduler` is not set.
    std::function<void(std::function<void()>)> scheduler;

    /// Scheduler which will be called to execute parallel subtasks when it's necessary to do that.
    /// See for example the coroutines parallel(), setTimeout().
    std::function<void(std::function<void()>)> scheduler_for_parallel_subtasks;

    /// A cancel status is used to cancel execution of the coroutine. Managed internally.
    std::shared_ptr<details::CancelStatus> cancel_status;

    /// A timer is used to implement the coroutines sleepForSeconds() and setTimeout(). Managed internally.
    std::shared_ptr<details::Timer> timer;

    /// The default constructor makes parameters to execute the coroutine and all its subtasks on the current thread.
    /// It's not the best but still possible choice.
    RunParams() = default;

    /// Makes parameters to execute the coroutine and its subtasks on a specified thread pool.
    RunParams(ThreadPool & thread_pool, const String & thread_name = "CoroutineWorker");
    RunParams(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner);

    static std::function<void(std::function<void()>)> makeScheduler(ThreadPool & thread_pool, const String & thread_name = "CoroutineWorker", bool allow_fallback_to_sync_run = false);
    static std::function<void(std::function<void()>)> makeScheduler(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner);
};


/// Represents an asynchronous waiter for a task to be finished.
/// This class has functions await_ready(), await_suspend(), await_resume() so it can an argument of the `co_await` operator.
template <typename ResultType>
class [[nodiscard]] Task<ResultType>::Awaiter
{
public:
    /// Creates a non-initialized awaiter, it cannot be used until assigned.
    Awaiter() = default;

    /// Awaiters are copyable, no problem with that.
    Awaiter(const Awaiter &) = default;
    Awaiter & operator =(const Awaiter &) = default;
    Awaiter(Awaiter &&) = default;
    Awaiter & operator =(Awaiter &&) = default;

    ~Awaiter()
    {
        if (controller && controller.use_count() == 1)
            controller->onDestroy(); /// No more tasks or awaiters, notify the controller.
    }

    /// Returns true if the task has already finished.
    bool isReady() const { return controller->isFinished(); }

    /// Blocks the current thread until the coroutine finishes or throws an exception.
    ResultType syncWait() const;

    /// Tries to cancel the coroutine. If succeeded the coroutine stops with a specified exception.
    /// That doesn't act immediately because the coroutine decides by itself where to stop (by calling co_await Co::StopIfCancelled{}).
    /// After calling tryCancelTask() the coroutine can still finish successfully if it decides so.
    void tryCancelTask(std::exception_ptr cancel_exception_) const noexcept { controller->getCancelStatus()->setIsCancelled(cancel_exception_); }
    void tryCancelTask(const String & message_ = {}) const noexcept { controller->getCancelStatus()->setIsCancelled(message_); }

    /// To be a proper awaiter this class defines await_ready(), await_suspend(), await_resume() functions.
    /// Those functions are required by compiler to generate code for the statement `co_await awaiter`.
    bool await_ready() const { return controller->isFinished(); }
    bool await_suspend(std::coroutine_handle<> handle_) const { return controller->addContinuation(handle_); }

    ResultType await_resume() const
    {
        if (auto result_exception = controller->getResultException())
            std::rethrow_exception(result_exception);
        if constexpr (!std::is_void_v<ResultType>)
            return controller->getResult();
    }

private:
    /// Awaiter normally are constructed by Task::run().
    friend class Task<ResultType>;
    Awaiter(const std::shared_ptr<Controller> & controller_) : controller(controller_) {}

    std::shared_ptr<Controller> controller;
};


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
    /// Checks whether a specified type is an awaiter, i.e. has the member function await_ready().
    template<typename F>
    concept AwaiterConcept = requires(F f)
    {
        {f.await_ready()} -> std::same_as<bool>;
    };

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
    };

    /// State of the controller.
    enum class State
    {
        INITIALIZED,
        STARTED,  /// run() was called.
        RUNNING,  /// the coroutine was resumed in run().
        FINISHED, /// the coroutine finished and the continuation list was processed.
    };

    /// Keeps the cancel status of one or more coroutines. Used to implement Awaiter::tryCancelTask().
    class CancelStatus : public std::enable_shared_from_this<CancelStatus>
    {
    public:
        void setIsCancelled(const String & message_ = {}) noexcept;
        void setIsCancelled(std::exception_ptr cancel_exception_) noexcept;
        bool isCancelled() const noexcept { return static_cast<bool>(getCancelException()); }
        std::exception_ptr getCancelException() const noexcept;
        scope_guard subscribe(const std::function<void(std::exception_ptr)> & callback);

    protected:
        std::exception_ptr TSA_GUARDED_BY(mutex) cancel_exception;
        struct CallbackWithID
        {
            std::function<void(std::exception_ptr)> callback;
            size_t id;
        };
        using CallbacksWithIDs = std::vector<CallbackWithID>;
        CallbacksWithIDs TSA_GUARDED_BY(mutex) subscriptions;
        size_t TSA_GUARDED_BY(mutex) last_id = 0;
        mutable std::mutex mutex;
    };

    /// An internal timer used in the coroutines like `sleepForSeconds()`.
    class Timer
    {
    public:
        ~Timer();

        /// Schedules a delayed task, returns a scope_guard which can be destroyed to unsubscribe.
        scope_guard scheduleDelayedTask(uint64_t microseconds, std::function<void()> callback);

        /// Waits for the first scheduled time, calls the callback, then waits for the next scheduled time, calls the next callback, and so on
        /// until the timed queue is empty.
        void run();

    private:
        void unscheduleDelayedTask(Poco::Timestamp timestamp, uint64_t id);

        struct CallbackWithID
        {
            std::function<void()> callback;
            size_t id;
        };
        using CallbacksWithIDs = std::vector<CallbackWithID>;

        bool TSA_GUARDED_BY(mutex) is_running = false;
        std::map<Poco::Timestamp, CallbacksWithIDs> TSA_GUARDED_BY(mutex) timed_queue;
        size_t TSA_GUARDED_BY(mutex) last_id = 0;
        Poco::Event timed_queue_updated;
        std::mutex mutex;
    };

    void logErrorStateMustBeFinishedAfterWait(State state);
    void logErrorControllerDestructsWhileRunning();

    /// A helper task, part of the implementation of syncWait() and syncRun().
    class SyncWaitTask
    {
    public:
        template <typename ResultType>
        static SyncWaitTask runAndSetEvent(const typename Task<ResultType>::Awaiter & awaiter, ResultHolder<ResultType> & result, std::exception_ptr & exception, Poco::Event & event)
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
ResultType Task<ResultType>::Awaiter::syncWait() const
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
ResultType Task<ResultType>::syncRun() const
{
    auto awaiter = run(RunParams{});
    if constexpr (std::is_void_v<ResultType>)
        awaiter.syncWait();
    else
        return awaiter.syncWait();
}

template <typename ResultType>
ResultType Task<ResultType>::syncRun(ThreadPool & thread_pool, const String & thread_name) const
{
    auto awaiter = run(thread_pool, thread_name);
    if constexpr (std::is_void_v<ResultType>)
        awaiter.syncWait();
    else
        return awaiter.syncWait();
}

template <typename ResultType>
ResultType Task<ResultType>::syncRun(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) const
{
    auto awaiter = run(thread_pool_callback_runner);
    if constexpr (std::is_void_v<ResultType>)
        awaiter.syncWait();
    else
        return awaiter.syncWait();
}

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::run(ThreadPool & thread_pool, const String & thread_name) const
{
    return run(RunParams{thread_pool, thread_name});
}

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::run(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) const
{
    return run(RunParams{thread_pool_callback_runner});
}

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::run(const RunParams & params) const
{
    chassert(controller);
    return controller->run(params);
}

template <typename ResultType>
typename Task<ResultType>::Awaiter Task<ResultType>::getAwaiter() const
{
    chassert(controller);
    return controller->getAwaiter();
}

/// PromiseBase is a helper which is used as a base class for Promise.
template <typename ResultType>
class Task<ResultType>::ControllerHolderForPromise
{
protected:
    /// A controller owns a promise, not other way around. See ~Controller().
    typename Task<ResultType>::Controller * controller = nullptr;
};

template <typename ResultType>
class Task<ResultType>::PromiseBase : public ControllerHolderForPromise
{
public:
    void return_value(ResultType result_) { this->controller->setResult(std::move(result_)); }
};

template <>
class Task<void>::PromiseBase : public ControllerHolderForPromise
{
public:
    void return_void() {}
};

/// Represents the `promise_type` for coroutine tasks.
/// There is always a single instance of Promise per each coroutine running.
/// When a coroutine starts the compiler-generated code:
/// 1) creates an instance of the `promise_type`, i.e. `Promise`.
/// 2) creates a task by calling `Promise::get_return_object()`.
/// 3) suspends the coroutine because `initial_suspend()` returns `std::suspend_always{}`
/// 4) the coroutine stays in suspended state until `Task::run()` or `Task::syncRun()` will resume it.
template <typename ResultType>
class Task<ResultType>::Promise : public PromiseBase
{
public:
    Task get_return_object()
    {
        auto ctrl = std::make_shared<Controller>(std::coroutine_handle<Promise>::from_promise(*this));
        this->controller = ctrl.get();
        return Task(ctrl);
    }

    auto initial_suspend() const { return std::suspend_always{}; }

    class FinalSuspend;
    auto final_suspend() const noexcept { return FinalSuspend{*(this->controller)}; }

    void unhandled_exception() const noexcept
    {
        this->controller->setResultException(std::current_exception());
    }

    auto await_transform(RunParams) const
    {
        struct Getter : std::suspend_never
        {
            RunParams params;
            RunParams await_resume() const noexcept { return params; }
        };
        return Getter{{}, this->controller->getRunParams()};
    }

    auto await_transform(IsCancelled) const
    {
        struct Getter : std::suspend_never
        {
            bool is_cancelled;
            IsCancelled await_resume() const noexcept { return {is_cancelled}; }
        };
        return Getter{{}, static_cast<bool>(this->controller->getCancelStatus()->isCancelled())};
    }

    auto await_transform(StopIfCancelled) const
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
        return Getter{{}, this->controller->getCancelStatus()->getCancelException()};
    }

    template <details::AwaiterConcept AwaiterType>
    auto await_transform(AwaiterType && awaiter) const
    {
        return std::forward<AwaiterType>(awaiter);
    }

    template <typename SubTaskResultType>
    typename Task<SubTaskResultType>::Awaiter await_transform(const Task<SubTaskResultType> & sub_task) const
    {
        return sub_task.run(this->controller->getRunParams());
    }
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
    FinalSuspend(Controller & controller_) : controller(controller_) {}

    bool await_ready() const noexcept
    {
        /// Give control to the code waiting for this coroutine to be finished.
        controller.processContinuation();
        return true;
    }

private:
    Controller & controller;
};


/// The core of the coroutine. An instance of Controller is shared between Task, Promise, and Awaiter (there can be 0 or 1 or more Awaiters).
template <typename ResultType>
class Task<ResultType>::Controller : public details::ResultHolder<ResultType>, public std::enable_shared_from_this<Controller>
{
public:
    explicit Controller(std::coroutine_handle<Promise> coroutine_handle_) : coroutine_handle(coroutine_handle_) { }

    Controller(const Controller &) = delete;
    Controller & operator=(const Controller &) = delete;

    using State = details::State;

    /// Called by ~Task or ~Awaiter when this controller is about to be destroyed.
    void onDestroy()
    {
        if (state == State::RUNNING)
        {
            /// The coroutine was resumed but not finished yet.
            /// Probably all awaiters were destroyed and the result of that coroutine is no longer needed.
            if (!getCancelStatus()->isCancelled())
                getCancelStatus()->setIsCancelled("Cancelled because none awaits us");
            try
            {
                getAwaiter().syncWait();
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::COROUTINE_TASK_CANCELLED)
                    tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            if (state != State::FINISHED)
                details::logErrorStateMustBeFinishedAfterWait(state);
        }
    }

    ~Controller()
    {
        if (state == State::INITIALIZED || state == State::STARTED)
        {
            /// std::coroutine_handle<>::destroy() must be called only if the coroutine was never resumed.
            /// (Because if the coroutine is resumed it will destroy itself after finish.)
            coroutine_handle.destroy();
        }
        else if (state == State::RUNNING)
        {
            /// onDestroy must have processed this case.
            details::logErrorControllerDestructsWhileRunning();
        }
    }

    Awaiter run(const RunParams & params_)
    {
        auto old_state_initialized = State::INITIALIZED;
        if (state.compare_exchange_strong(old_state_initialized, State::STARTED))
        {
            scheduler_for_parallel_subtasks = params_.scheduler_for_parallel_subtasks;
            cancel_status = params_.cancel_status;
            timer = params_.timer;

            if (!cancel_status)
                cancel_status = std::make_shared<details::CancelStatus>();

            if (!timer)
                timer = std::make_shared<details::Timer>();

            auto job = [this, keep_this_from_destruction = this->shared_from_this()]
            {
                auto old_state_started = State::STARTED;
                if (state.compare_exchange_strong(old_state_started, State::RUNNING))
                    coroutine_handle.resume();
            };

            if (params_.scheduler)
                params_.scheduler(job);
            else
                job();
        }

        return getAwaiter();
    }

    Awaiter getAwaiter() { return Awaiter{this->shared_from_this()}; }

    /// Gets parameter to run a subtask.
    RunParams getRunParams() const
    {
        RunParams params_for_subtask;
        /// We don't set `params_for_subtask.scheduler` here because normally a subtask can be executed on the same thread as the main task.
        params_for_subtask.scheduler_for_parallel_subtasks = scheduler_for_parallel_subtasks;
        params_for_subtask.cancel_status = cancel_status;
        params_for_subtask.timer = timer;
        return params_for_subtask;
    }

    bool addContinuation(std::coroutine_handle<> handle_) { return (state != State::FINISHED) && continuation_list.add(handle_); }

    void processContinuation() noexcept { state = State::FINISHED; continuation_list.process(); }

    bool isFinished() const noexcept { return state == State::FINISHED; }

    void setResultException(std::exception_ptr result_exception_) noexcept
    {
        if (!result_exception)
            result_exception = result_exception_;
    }

    std::exception_ptr getResultException() const noexcept { return result_exception; }

    std::shared_ptr<details::CancelStatus> getCancelStatus() const noexcept { return cancel_status; }

private:
    std::atomic<details::State> state = State::INITIALIZED;
    std::coroutine_handle<Promise> coroutine_handle;
    std::function<void(std::function<void()>)> scheduler_for_parallel_subtasks;
    std::shared_ptr<details::CancelStatus> cancel_status;
    std::shared_ptr<details::Timer> timer;
    std::exception_ptr result_exception;
    details::ContinuationList continuation_list;
};

}
