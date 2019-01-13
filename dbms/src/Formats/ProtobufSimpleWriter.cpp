#include <Formats/ProtobufSimpleWriter.h>

namespace DB
{


namespace
{

UInt32 zigZag(Int32 value)
{
    return static_cast<UInt32>((value << 1) ^ (value >> 31));
}

UInt64 zigZag(Int64 value)
{
    return static_cast<UInt64>((value << 1) ^ (value >> 63));
}

StringRef flushBuffer(WriteBufferFromOwnString & buffer)
{
    const char* data = buffer.str().data();
    StringRef str(data, buffer.position() - data);
    buffer.set(str.data(), str.size());
    return str;
}

void writeVariant(WriteBuffer & buf, Int32 value)
{
    writeVariant(buf, static_cast<UInt32>(value));
}

void writeVariant(WriteBuffer & buf, UInt32 value)
{
    while (value >= 0x80)
    {
      buf.write(static_cast<char>(value | 0x80));
      value >>= 7;
    }
    buf.write(static_cast<char>(value));
}

void writeVariant(WriteBuffer & buf, Int64 value)
{
    writeVariant(buf, static_cast<UInt64>(value));
}

void writeVariant(WriteBuffer & buf, UInt64 value)
{
    while (value >= 0x80)
    {
      buf.write(static_cast<char>(value | 0x80));
      value >>= 7;
    }
    buf.write(static_cast<char>(value));
}

void writeLittleEndian(WriteBuffer & buf, Int32 value)
{
    writeLittleEndian(buf, static_cast<UInt32>(value));
}

void writeLittleEndian(WriteBuffer & buf, UInt32 value)
{
    boost::endian::native_to_little_inplace(value);
    writeBytes(buf, &value, sizeof(value));
}

void writeLittleEndian(WriteBuffer & buf, float value)
{
    writeLittleEndian(buf, *reinterpret_cast<UInt32*>(&value));
}

void writeLittleEndian(WriteBuffer & buf, Int64 value)
{
    writeLittleEndian(buf, static_cast<UInt64>(value));
}

void writeLittleEndian(WriteBuffer & buf, UInt64 value)
{
    boost::endian::native_to_little_inplace(value);
    writeBytes(buf, &value, sizeof(value));
}

void writeLittleEndian(WriteBuffer & buf, double value)
{
    writeLittleEndian(buf, *reinterpret_cast<UInt64*>(&value));
}

void writeBytes(WriteBuffer & buf, const void* data, size_t size)
{
    buf.write(reinterpret_cast<const char*>(data), size);
}

}


enum ProtobufSimpleWriter::WireType: UInt32
{
    VARIANT = 0,
    BITS64 = 1,
    LENGTH_DELIMITED = 2,
    BITS32= 5
};

ProtobufSimpleWriter::ProtobufSimpleWriter(WriteBuffer & out_)
    : out(out_) {}

~ProtobufSimpleWriter()
{
    flush();
}

void ProtobufSimpleWriter::setPackRepeatedAllowed(bool allowed)
{
    pack_repeated_allowed = allowed;
}

void ProtobufSimpleWriter::newMessage()
{
    finishCurrentMessage();
    current_field_number = 0;
}

void ProtobufSimpleWriter::flush()
{
    finishCurrentMessage();
}

void ProtobufSimpleWriter::finishCurrentMessage()
{
    finishCurrentField();
    StringRef str = flushBuffer(message_buffer);
    writeVariant(out, str.size);
    writeBytes(out, str.data, str.size);
}

void ProtobufSimpleWriter::setCurrentField(UInt32 field_number, bool is_repeated)
{
    finishCurrentField();
    assert(current_field_number < field_number);
    current_field_number = field_number;
    try_pack_repeated = is_repeated && pack_repeated_allowed;
    num_values = 0;
}

void ProtobufSimpleWriter::finishCurrentField()
{
    if (try_pack_repeated && (num_values > 0))
    {
        StringRef str = flushBuffer(repeated_packing_buffer);
        if (str.size)
        {
            writeKey(message_buffer, LENGTH_DELIMITED);
            writeVariant(message_buffer, str.size);
            writeRawBytes(message_buffer, str.data, str.size);
        }
    }
}

void ProtobufSimpleWriter::addInt32(Int32 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addUInt32(UInt32 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addSInt32(Int32 value)
{
    assert(current_field_number);
    ++num_values;
    UInt32 zigzag_encoded = zigZag(value);
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, zigzag_encoded);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, zigzag_encoded);
    }
}

void ProtobufSimpleWriter::addInt64(Int64 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addUInt64(UInt64 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addSInt64(Int64 value)
{
    assert(current_field_number);
    ++num_values;
    UInt64 zigzag_encoded = zigZag(value);
    if (try_pack_repeated)
    {
        writeVariant(packing_buffer, zigzag_encoded);
    }
    else
    {
        writeKey(message_buffer, VARIANT);
        writeVariant(message_buffer, zigzag_encoded);
    }
}

void ProtobufSimpleWriter::addFixed32(UInt32 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS32);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addSFixed32(Int32 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS32);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addFloat(float value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS32);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addFixed64(UInt64 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS64);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addSFixed64(Int64 value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS64);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addDouble(double value)
{
    assert(current_field_number);
    ++num_values;
    if (try_pack_repeated)
    {
        writeLittleEndian(packing_buffer, value);
    }
    else
    {
        writeKey(message_buffer, BITS64);
        writeLittleEndian(message_buffer, value);
    }
}

void ProtobufSimpleWriter::addBool(bool b)
{
    addUInt32(b);
}

void ProtobufSimpleWriter::addBytes(const void* data, size_t size)
{
    assert(current_field_number);
    ++num_values;
    writeKey(message_buffer, LENGTH_DELIMITED);
    writeVariant(message_buffer, size);
    writeBytes(message_buffer, data, size);
}

void ProtobufSimpleWriter::addString(const StringRef& str)
{
    addBytes(str.data, str.size);
}

void ProtobufSimpleWriter::writeKey(WriteBuffer & buf, WireType wire_type)
{
    writeVariant(buf, (current_field_number << 3) | wire_type);
}

}
