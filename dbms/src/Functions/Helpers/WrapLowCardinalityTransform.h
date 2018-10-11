#pragma once

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

class PreparedFunctionLowCardinalityResultCache;
using PreparedFunctionLowCardinalityResultCachePtr = std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

namespace DB
{

class WrapLowCardinalityTransform : public ITransform
{
public:
    WrapLowCardinalityTransform(
        Blocks input_headers,
        const ColumnNumbers & column_numbers,
        size_t result,
        bool can_be_executed_on_default_arguments,
        PreparedFunctionLowCardinalityResultCachePtr cache);

protected:
    Blocks transform(Blocks && blocks) override;

private:
    const ColumnNumbers & column_numbers;
    size_t result;

    bool can_be_executed_on_default_arguments;
    PreparedFunctionLowCardinalityResultCachePtr cache;
};
