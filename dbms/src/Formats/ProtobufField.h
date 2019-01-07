#pragma once

#include "Core/Types.h"


namespace google
{
namespace protobuf
{
class FieldDescriptor;
class Message;
}
}


namespace DB
{

class ProtobufField
{
public:
    using Message = google::protobuf::Message;

    ProtobufField(const google::protobuf::FieldDescriptor* descriptor_);

    void SetString(Message & destination, const String & str) const;

#if 0
    std::string GetString(const Message & source) const;

    void SetInt8(Message & destination, Int8 value) const;
    void SetUInt8(Message & destination, UInt8 value) const;
    void SetInt16(Message & destination, Int16 value) const;
    void SetUInt16(Message & destination, UInt16 value) const;

    template <typename T>
    void SetNumber(Message & destination, T value) const;

    Int8 GetInt8(const Message & source) const;
#endif

private:
    const google::protobuf::FieldDescriptor * descriptor;
};


}
