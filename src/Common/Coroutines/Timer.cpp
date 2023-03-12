#include <Common/Coroutines/Timer.h>

#include <Common/setThreadName.h>


namespace DB::Coroutine
{

namespace
{
    std::unique_ptr<Timer> timer;
}

void Timer::initialize(const String & thread_name_, bool keep_thread_if_no_delayed_tasks_)
{
    chassert(!timer);
    timer = static_cast<std::unique_ptr<Timer>>(new Timer(thread_name_, keep_thread_if_no_delayed_tasks_));
}

Timer & Timer::instance()
{
    static Timer & timer_ref = []() -> Timer &
    {
        if (!timer)
            initialize();
        return *timer;
    }();
    return timer_ref;
}

void Timer::shutdown()
{
    if (timer)
        timer->do_shutdown();
}

Timer::~Timer()
{
    do_shutdown();
}

void Timer::do_shutdown()
{
    {
        std::lock_guard lock{mutex};
        need_shutdown = true;
        timed_queue_updated_or_need_shutdown.set();
        if (!thread.has_value())
            return;
    }

    thread_stopped_on_shutdown.wait(); /// Wait with `mutex` unlocked.
}

scope_guard Timer::scheduleDelayedTask(uint64_t microseconds, std::function<void()> callback)
{
    std::lock_guard lock{mutex};
    if (need_shutdown)
        return {};

    Poco::Timestamp now;
    Poco::Timestamp timestamp = now + microseconds;
    size_t id = ++last_id;
    timed_queue[timestamp].emplace_back(CallbackWithID{std::move(callback), id});

    if (!thread)
        thread.emplace([this] { threadRun(); });

    timed_queue_updated_or_need_shutdown.set();

    return [this, timestamp, id] { unscheduleDelayedTask(timestamp, id); };
}

void Timer::unscheduleDelayedTask(Poco::Timestamp timestamp, uint64_t id)
{
    std::lock_guard lock{mutex};
    auto it = timed_queue.find(timestamp);
    if (it == timed_queue.end())
        return;

    auto & callbacks_with_ids = it->second;
    for (size_t i = 0; i != callbacks_with_ids.size(); ++i)
    {
        auto & callback_with_id = callbacks_with_ids[i];
        if (callback_with_id.id == id)
        {
            callbacks_with_ids.erase(callbacks_with_ids.begin() + i);
            if (callbacks_with_ids.empty())
            {
                timed_queue.erase(it);
                if (timed_queue.empty())
                    timed_queue_updated_or_need_shutdown.set();
            }
            break;
        }
    }
}

void Timer::threadRun()
{
    setThreadName(thread_name.c_str());

    long wait_time = 0;
    constexpr const long unlimited = std::numeric_limits<long>::max();

    while (true)
    {
        /// Wait with `mutex` unlocked.
        if (wait_time == unlimited)
            timed_queue_updated_or_need_shutdown.wait();
        else
            timed_queue_updated_or_need_shutdown.tryWait(wait_time);

        Poco::Timestamp now;

        std::vector<std::function<void()>> callbacks;
        SCOPE_EXIT({
            for (auto & callback : callbacks)
            {
                try
                {
                    std::move(callback)();
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        });

        {
            std::lock_guard lock{mutex};

            while (!timed_queue.empty() && (now >= timed_queue.begin()->first))
            {
                auto it = timed_queue.begin();
                callbacks.reserve(callbacks.size() + it->second.size());
                for (auto & callback_with_id : it->second)
                    callbacks.push_back(std::move(callback_with_id.callback));
                timed_queue.erase(it);
            }

            if ((timed_queue.empty() && !keep_thread_if_no_delayed_tasks) || need_shutdown)
            {
                std::exchange(thread, {}).value().detach();
                if (need_shutdown)
                    thread_stopped_on_shutdown.set();
                return;
            }

            if (timed_queue.empty())
                wait_time = unlimited;
            else
                wait_time = timed_queue.begin()->first - now;
        }
    }
}

}
