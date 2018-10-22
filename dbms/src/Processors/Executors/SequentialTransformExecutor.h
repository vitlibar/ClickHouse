#pragma once

#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Common/EventCounter.h>

namespace DB
{

class SequentialTransformExecutor : public IProcessor
{
private:


public:
    SequentialTransformExecutor(const Processors & processors, InputPort & pipeline_input, OutputPort & pipeline_output)
        : IProcessor({pipeline_output.getHeader()}, {pipeline_input.getHeader()})
        , executor(processors)
        , input(inputs.front())
        , output(outputs.front())
        , pipeline_input(pipeline_input)
        , pipeline_output(pipeline_output)
    {
        connect(output, pipeline_input);
        connect(pipeline_output, input);
    }

    void execute(Block & block);

private:
    SequentialPipelineExecutor executor;
    InputPort & input;
    OutputPort & output;
    InputPort & pipeline_input;
    OutputPort & pipeline_output;
};

using SequentialTransformExecutorPtr = std::shared_ptr<SequentialTransformExecutor>;

}
