#include <Common/Coroutines/sleep.h>

#include <Common/Coroutines/Task.h>
#include <Common/Coroutines/Timer.h>
#include <Common/Coroutines/setTimeout.h>


namespace DB::Coroutine
{

namespace
{
    using TimeUnit = details::TimeUnit;

    void syncSleep(uint64_t microseconds, std::shared_ptr<CancelStatus> cancel_status)
    {
        /// `params.scheduler` is not set so we will do synchronous waiting for the specified time until the task is cancelled.
        auto event = std::make_shared<Poco::Event>();

        /// Make a separate cancel status for the subtask, so we'll able to cancel it if the main task is not cancelled.
        auto cancel_status_subscription = cancel_status->subscribe([event](std::exception_ptr) { event->set(); });

        if (event->tryWait(microseconds))
            throw cancel_status->getCancelException();
    }
    
    class SleepAwaiter
    {
    public:
        SleepAwaiter(
            uint64_t microseconds_,
            std::function<void(std::function<void()>)> scheduler_,
            std::shared_ptr<CancelStatus> cancel_status_)
            : microseconds(microseconds_), scheduler(scheduler_), cancel_status(cancel_status_)
        {
        }

        bool await_ready() const { return false; }

        void await_suspend(std::coroutine_handle<> handle_)
        {
            auto schedule_resume = [handle_, scheduler = scheduler, is_resumed = is_resumed]
            {
                if (!is_resumed->exchange(true))
                    scheduler([handle_] { handle_.resume(); });
            };

            auto schedule_resume_with_arg = [schedule_resume](std::exception_ptr) { schedule_resume(); };

            timer_subscription = Timer::instance().scheduleDelayedTask(microseconds, schedule_resume);
            cancel_status_subscription = cancel_status->subscribe(schedule_resume_with_arg);
        }

        void await_resume() const
        {
            if (auto exception = cancel_status->getCancelException())
                std::rethrow_exception(exception);
        }

    private:
        uint64_t microseconds;
        std::function<void(std::function<void()>)> scheduler;
        std::shared_ptr<std::atomic<bool>> is_resumed = std::make_shared<std::atomic<bool>>(false);
        std::shared_ptr<CancelStatus> cancel_status;
        scope_guard timer_subscription;
        scope_guard cancel_status_subscription;
    };

    Task<> sleepImpl(TimeUnit time_unit, uint64_t count)
    {
        auto microseconds = details::toMicroseconds(time_unit, count);
        auto params = co_await RunParams{};

        if (params.scheduler_for_delayed_tasks)
        {
            co_await SleepAwaiter{microseconds, params.scheduler_for_delayed_tasks, params.cancel_status};
        }
        else
        {
            syncSleep(microseconds, params.cancel_status);
        }
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
