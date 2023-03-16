#pragma once

#include <Common/Coroutines/Task_fwd.h>


namespace DB::Coroutine
{

/// Waits for specified time using the coroutine approach.
/// Usage:
///     co_await Coroutine::sleepForSeconds(5);
///
/// A longer example:
///     doSomething();
///     co_await Coroutine::sleepForSeconds(5);
///     doAnotherThing();
///
/// After waiting doAnotherThing() will be called on (probably) another thread from the thread pool.
///
/// With syncRun() or syncWait() we can write for example
/// `Coroutine::sleepForSeconds(5).syncRun()`
/// `Coroutine::sleepForSeconds(5).run(RunParams{}).syncWait()`
/// both are the same as just
/// `::sleepForSeconds(5)`
/// This example is just for illustration, `Coroutine::sleepForSeconds()` is meant to be used in coroutines with the co_await operator,
/// and not as a replacement for usual `sleepForSeconds()`.
///
Task<> sleepForSeconds(uint64_t seconds);
Task<> sleepForMilliseconds(uint64_t milliseconds);
Task<> sleepForMicroseconds(uint64_t microseconds);


/// Implementation details.
namespace details
{
    enum class TimeUnit
    {
        SECONDS,
        MILLISECONDS,
        MICROSECONDS
    };

    uint64_t toMicroseconds(TimeUnit time_unit, uint64_t count);

    /// Returns "seconds" or "milliseconds", etc.
    std::string_view getTimeUnitsName(TimeUnit time_unit);
}

}


!!! make Timer the singleton again as it was before, and if timer can't schedule, it should reschedule for later time

Timer(const String & thread_name, bool keep_threads_if_no_work,
      uint64_t initial_backoff_microseconds = 1000;
      float backoff_scale_factor = 1.1;
      uint64_t max_backoff_microseconds = 180000; //30 min

function<bool(std::function<void()>)> scheduler_for_delayed_subtasks
scheduler_for_delayed_subtasks() returns false if cannot schedule

!!!
