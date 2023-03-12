#pragma once

#include <Common/Coroutines/Task_fwd.h>
#include <Common/ThreadPool.h>


namespace DB::Coroutine
{

/// An internal timer used in the coroutines like `sleepForSeconds()` and `setTimeoutForSeconds()`.
/// It's a very simple timer, it doesn't even use a thread pool to run passed callbacks, it just runs them on a single thread
/// and that's enough because those callbacks either just set some state (the case of `setTimeoutForSeconds()`)
/// or immediately reschedule the execution on some thread pool (the case of `sleepForSeconds()`).
class Timer
{
public:
    static void initialize(const String & thread_name_ = "CoroutineTimer", bool keep_thread_if_no_delayed_tasks_ = false);
    static void shutdown();

    static Timer & instance();

    /// Schedules a delayed task, returns a scope_guard which can be destroyed to unsubscribe.
    scope_guard scheduleDelayedTask(uint64_t microseconds, std::function<void()> callback);

    ~Timer();
private:
    Timer(const String & thread_name_, bool keep_thread_if_no_delayed_tasks_);

    void unscheduleDelayedTask(Poco::Timestamp timestamp, size_t id);
    void threadRun();
    void do_shutdown();

    struct CallbackWithID
    {
        std::function<void()> callback;
        size_t id;
    };
    using CallbacksWithIDs = std::vector<CallbackWithID>;

    String thread_name;
    bool keep_thread_if_no_delayed_tasks = false;
    std::optional<ThreadFromGlobalPool> TSA_GUARDED_BY(mutex) thread;
    std::map<Poco::Timestamp, CallbacksWithIDs> TSA_GUARDED_BY(mutex) timed_queue;
    Poco::Event timed_queue_updated_or_need_shutdown;
    size_t TSA_GUARDED_BY(mutex) last_id = 0;
    bool TSA_GUARDED_BY(mutex) need_shutdown = false;
    Poco::Event thread_stopped_on_shutdown;
    std::mutex mutex;
};

}
