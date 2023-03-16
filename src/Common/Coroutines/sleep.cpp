#include <Common/Coroutines/sleep.h>

#include <Common/Coroutines/Task.h>


namespace DB::Coroutine
{

namespace
{
    using TimeUnit = details::TimeUnit;

    /// Synchronous waiting as a special case.
    /// This function is used when `scheduler_for_parallel_subtasks` is not set so rescheduling is not possible.
    void syncSleep(uint64_t microseconds, std::shared_ptr<details::CancelStatus> cancel_status)
    {
        /// `params.scheduler` is not set so we will do synchronous waiting for the specified time until the task is cancelled.
        auto event = std::make_shared<Poco::Event>();

        /// Make a separate cancel status for the subtask, so we'll able to cancel it if the main task is not cancelled.
        auto cancel_status_subscription = cancel_status->subscribe([event](std::exception_ptr) { event->set(); });

        if (event->tryWait(microseconds))
            throw cancel_status->getCancelException();
    }

    /// Special awaiter to implement asynchronous waiting.
    /// Uses details::Timer to reschedule after the specified time passes.
    class SleepAwaiter
    {
    public:
        SleepAwaiter(
            uint64_t microseconds_,
            std::shared_ptr<details::Timer> timer_,
            std::function<void(std::function<void()>)> scheduler_,
            std::shared_ptr<details::CancelStatus> cancel_status_)
            : timer(timer_), microseconds(microseconds_), cancel_status(cancel_status_), state(std::make_shared<State>(scheduler_))
        {
        }

        bool await_ready() const { return false; }

        void await_suspend(std::coroutine_handle<> handle_)
        {
            std::cout << "SleepAwaiter::await_suspend: " << reinterpret_cast<size_t>(this) << std::endl;

            auto do_resume = [handle_, state = state]
            {
                std::cout << "sleep - do_resume" << std::endl;
                std::lock_guard lock{state->mutex};
                /// We must not resume the coroutine twice.
                /// (There can be a race between timer_subscription and cancel_status_subscription).
                if (!state->is_resumed)
                {
                    state->is_resumed = true;
                    state->cancel_subscription.reset();
                    state->timer_subscription.reset();
                    std::cout << "sleep - do_resume-scheduling" << std::endl;
                    state->scheduler([handle_] {
                        std::cout << "sleep - do_resume-resuming, thread_id=" << std::this_thread::get_id() << std::endl;
                        handle_.resume();
                    });
                }
            };

            auto timer_subscription = timer->scheduleDelayedTask(microseconds, do_resume);
            auto cancel_subscription = cancel_status->subscribe([do_resume](std::exception_ptr) { do_resume(); });

            {
                std::lock_guard lock{state->mutex};
                if (!state->is_resumed) /// If it's resumed already we don't need to store those subscriptions anywhere.
                {
                    state->timer_subscription = std::move(timer_subscription);
                    state->cancel_subscription = std::move(cancel_subscription);
                }
            }

            /// Will proceed scheduled tasks on timer if some other thread isn't already processing them.
            timer->run();
        }

        void await_resume() const
        {
            std::cout << "SleepAwaiter::await_resume: " << reinterpret_cast<size_t>(this) << std::endl;

            /// If the waiting was cancelled we need to rethrow the exception.
            if (auto exception = cancel_status->getCancelException())
                std::rethrow_exception(exception);
        }

    private:
        struct State
        {
            std::function<void(std::function<void()>)> scheduler;
            bool TSA_GUARDED_BY(mutex) is_resumed = false;
            scope_guard TSA_GUARDED_BY(mutex) timer_subscription;
            scope_guard TSA_GUARDED_BY(mutex) cancel_subscription;
            std::mutex mutex;
            State(std::function<void(std::function<void()>)> scheduler_) : scheduler(scheduler_) {}
        };

        std::shared_ptr<details::Timer> timer;
        uint64_t microseconds;
        std::shared_ptr<details::CancelStatus> cancel_status;
        std::shared_ptr<State> state;
    };

    /// The implementation of the sleepFor* coroutines.
    Task<> sleepImpl(TimeUnit time_unit, uint64_t count)
    {
        auto microseconds = details::toMicroseconds(time_unit, count);
        auto params = co_await RunParams{};

        if (params.scheduler_for_parallel_subtasks)
        {
            co_await SleepAwaiter{microseconds, params.timer, params.scheduler_for_parallel_subtasks, params.cancel_status};
        }
        else
        {
            syncSleep(microseconds, params.cancel_status);
        }
    }
}


namespace details
{
    std::string_view getTimeUnitsName(TimeUnit time_unit)
    {
        switch (time_unit)
        {
            case TimeUnit::SECONDS: return "seconds";
            case TimeUnit::MILLISECONDS: return "milliseconds";
            case TimeUnit::MICROSECONDS: return "microseconds";
        }
        UNREACHABLE();
    }

    uint64_t toMicroseconds(TimeUnit time_unit, uint64_t count)
    {
        switch (time_unit)
        {
            case TimeUnit::SECONDS: return count * 1000000;
            case TimeUnit::MILLISECONDS: return count * 1000;
            case TimeUnit::MICROSECONDS: return count;
        }
        UNREACHABLE();
    }
}


Task<> sleepForSeconds(uint64_t seconds)
{
    return sleepImpl(TimeUnit::SECONDS, seconds);
}

Task<> sleepForMilliseconds(uint64_t milliseconds)
{
    return sleepImpl(TimeUnit::MILLISECONDS, milliseconds);
}

Task<> sleepForMicroseconds(uint64_t microseconds)
{
    return sleepImpl(TimeUnit::MICROSECONDS, microseconds);
}

}
