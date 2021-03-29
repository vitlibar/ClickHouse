#pragma once

#include <common/types.h>
#include <IO/SeekableReadBuffer.h>


namespace DB
{

/** Allows to read from another SeekableReadBuffer up to the specified offset.
  * Note that the nested SeekableReadBuffer may read slightly more data internally to fill its buffer.
  */
class LimitSeekableReadBuffer : public SeekableReadBuffer
{
public:
    LimitSeekableReadBuffer(SeekableReadBuffer & in_, off_t limit_);
    LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, off_t limit_);
    ~LimitSeekableReadBuffer() override;

private:
    LimitSeekableReadBuffer(SeekableReadBuffer * in_, bool owns, off_t limit_);
    bool nextImpl() override;
    off_t seek(off_t offset, int whence) override;
    off_t getPosition() override;

    SeekableReadBuffer * in;
    const bool owns_in;
    const off_t limit;
};

}
