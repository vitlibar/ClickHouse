#pragma once


namespace DB
{

class ProtobufSimpleWriter : private boost::noncopyable
{
public:
    ProtobufSimpleWriter(WriteBuffer & out_);
    ~ProtobufSimpleWriter();

    void setPackRepeatedAllowed(bool allowed);

    void newMessage();
    void flush();

    void setCurrentField(UInt32 field_number, bool is_repeated);
    UInt32 currentFieldNumber() const { return current_field_number; }
    size_t numValues() const { return num_values; }

    void addInt32NoTypeCheck(Int32 value);
    void addUInt32(UInt32 value);
    void addSInt32(Int32 value);

    void addInt64(Int64 value);
    void addUInt64(UInt64 value);
    void addSInt64(Int64 value);

    void addFixed32(UInt32 value);
    void addSFixed32(Int32 value);
    void addFloat(float value);

    void addFixed64(UInt64 value);
    void addSFixed(Int64 value);
    void addDouble(double value);

    void addBool(bool b);
    void addBytes(const void* data, size_t size);
    void addString(const StringRef& str);

private:
    void finishCurrentMessage();
    void finishCurrentField();

    enum WireType: UInt32;
    void writeKey(WriteBuffer & buf, WireType wire_type);

    WriteBuffer & out;
    bool pack_repeated_allowed = false;
    std::function<void()> required_field_not_set_handler;
    WriteBufferFromOwnString message_buffer;
    UInt32 current_field_number = 0;
    size_t num_values = 0;
    bool try_pack_repeated = false;
    WriteBufferFromOwnString repeated_packing_buffer;
};

}
