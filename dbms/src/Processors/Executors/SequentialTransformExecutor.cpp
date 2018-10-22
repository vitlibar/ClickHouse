#include <Processors/Executors/SequentialTransformExecutor.h>
#include <Processors/ISink.h>
#include <Processors/ISource.h>

namespace DB
{

namespace
{

class SequentialTransformSource : public ISource
{
public:
    explicit SequentialTransformSource(Block header) : ISource(std::move(header)) {}

    Block generate() override { return std::move(input_block); }

    Block input_block;
};

class SequentialTransformSink : public ISink
{
public:
    explicit SequentialTransformSink(Block header) : ISink(std::move(header)) {}

    void consume(Block block) override { output_block = std::move(block); }

    Block output_block;
};

}

SequentialTransformExecutor::SequentialTransformExecutor(Processors processors, InputPort & input, OutputPort & output)
{
    auto source = std::make_shared<SequentialTransformSource>(input.getHeader());
    auto sink = std::make_shared<SequentialTransformSink>(output.getHeader());

    connect(source->getOutputs().front(), input);
    connect(output, sink->getInputs().front());

    input_block = &source->input_block;
    output_block = &sink->output_block;

    processors.emplace_back(std::move(source));
    processors.emplace_back(std::move(sink));

    executor = std::make_shared<SequentialPipelineExecutor>(processors);
}

void SequentialTransformExecutor::execute(Block & block)
{
    *input_block = std::move(block);
    output_block->clear();

    auto status = executor->prepare();
    while (!(*output_block))
    {
        if (status == IProcessor::Status::Ready)
            executor->work();
        else if (status == IProcessor::Status::Async)
        {
            EventCounter watch;
            executor->schedule(watch);
            watch.wait();
        }
        else
            throw Exception("Unexpected status for SequentialTransformExecutor: " + std::to_string(int(status)),
                            ErrorCodes::LOGICAL_ERROR);

        status = executor->prepare();
    }

    block = std::move(*output_block);
}

}
