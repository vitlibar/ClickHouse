#include <Common/Coroutines/sequential.h>


namespace DB::Coroutine
{

Task<> sequential(std::vector<Task<>> subtasks)
{
    return details::sequentialImpl(std::move(subtasks));
}

}
