#include <Common/Coroutines/parallel.h>


namespace DB::Coroutine
{

Task<> parallel(std::vector<Task<>> subtasks)
{
    return details::parallelImpl(std::move(subtasks));
}

}
