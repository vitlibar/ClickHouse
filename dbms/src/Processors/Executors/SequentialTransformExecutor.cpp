#include <Processors/Executors/SequentialTransformExecutor.h>

namespace DB
{

void SequentialTransformExecutor::execute(Block & block)
{
    output.push(std::move(block));
    input.setNeeded();

    auto status = executor.prepare();
    while (status != IProcessor::Status::NeedData)
    {
        if (status == IProcessor::Status::Ready)
            executor.work();
        else if (status == IProcessor::Status::Async)
        {
            EventCounter watch;
            executor.schedule(watch);
            watch.wait();
        }
        else
            throw Exception("Unexpected status for SequentialTransformExecutor: " + std::to_string(int(status)),
                            ErrorCodes::LOGICAL_ERROR);

        status = executor.prepare();
    }

    block = input.pull();
}

}
