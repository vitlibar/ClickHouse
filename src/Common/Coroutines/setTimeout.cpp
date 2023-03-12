#include <Common/Coroutines/setTimeout.h>

#include <Common/Coroutines/Task.h>


namespace DB::ErrorCodes
{
    extern const int COROUTINE_TASK_TIMEOUT;
}

namespace DB::Coroutine
{

namespace details
{
std::exception_ptr makeTimeoutException(TimeUnit time_unit, uint64_t count)
{
    return std::make_exception_ptr(Exception{
        ErrorCodes::COROUTINE_TASK_TIMEOUT,
        "A coroutine task was cancelled because the timeout {} {} has reached",
        count,
        getTimeUnitsName(time_unit)});
}

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

template Task<> setTimeoutInSeconds(Task<> task, uint64_t seconds);
template Task<> setTimeoutInMilliseconds(Task<> task, uint64_t milliseconds);
template Task<> setTimeoutInMicroseconds(Task<> task, uint64_t microseconds);

}
