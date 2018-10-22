#pragma once

#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Common/EventCounter.h>

namespace DB
{

class SequentialTransformExecutor
{

public:
    SequentialTransformExecutor(Processors processors, InputPort & input, OutputPort & output);
    void execute(Block & block);

private:
    ProcessorPtr executor;
    Block * input_block;
    Block * output_block;
};

using SequentialTransformExecutorPtr = std::shared_ptr<SequentialTransformExecutor>;

}
