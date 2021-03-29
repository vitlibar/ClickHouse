#include <IO/LimitSeekableReadBuffer.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


bool LimitSeekableReadBuffer::nextImpl()
{
    assert(in->position() <= position());
    in->position() = position();

    assert(in->getPosition() <= limit);
    if (in->getPosition() >= limit)
        return false;

    if (!in->next())
    {
        working_buffer = in->buffer();
        return false;
    }

    working_buffer = in->buffer();

    off_t end_pos = in->getPosition() + in->available();
    if (end_pos > limit)
        working_buffer.resize(working_buffer.size() - (end_pos - limit));

    return true;
}


off_t LimitSeekableReadBuffer::seek(off_t offset, int whence)
{
    if (whence == SEEK_SET)
    {
        if ((offset < 0) || (offset > limit))
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(limit),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    }
    else if (whence == SEEK_CUR)
    {
        in->position() = position();
        off_t new_pos = in->getPosition() + offset;
        if ((new_pos < 0) || (new_pos > limit))
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(new_pos) + ", Max: " + std::to_string(limit),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else
        throw Exception("Only SEEK_SET and SEEK_CUR seek modes allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    return in->seek(offset, whence);
}

off_t LimitSeekableReadBuffer::getPosition()
{
    return in->getPosition();
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer * in_, bool owns, off_t limit_)
    : SeekableReadBuffer(in_->buffer().begin(), in_->buffer().size(), in_->offset())
    , in(in_)
    , owns_in(owns)
    , limit(limit_)
{
    assert(in);

    off_t end_pos = in->getPosition() + in->available();
    if (end_pos > limit)
        working_buffer.resize(working_buffer.size() - (end_pos - limit));
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(SeekableReadBuffer & in_, off_t limit_)
    : LimitSeekableReadBuffer(&in_, false, limit_)
{
}


LimitSeekableReadBuffer::LimitSeekableReadBuffer(std::unique_ptr<SeekableReadBuffer> in_, off_t limit_)
    : LimitSeekableReadBuffer(in_.release(), true, limit_)
{
}


LimitSeekableReadBuffer::~LimitSeekableReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (!working_buffer.empty())
        in->position() = position();

    if (owns_in)
        delete in;
}

}
