#pragma once

#include <boost/core/noncopyable.hpp>
#include <common/DayNum.h>
#include <Common/UInt128.h>
#include <Core/Types.h>
#include <Core/UUID.h>


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

class ProtobufFieldWriter : private boost::noncopyable
{
public:
    static std::unique_ptr<ProtobufFieldWriter> create(const google::protobuf::FieldDescriptor* descriptor);
    virtual ~ProtobufFieldWriter();

    void setDestinationMessage(google::protobuf::Message* message_) { message = message_; }

    virtual void writeString(const String & value);

    virtual void writeNumber(Int8 value);
    virtual void writeNumber(UInt8 value);
    virtual void writeNumber(Int16 value);
    virtual void writeNumber(UInt16 value);
    virtual void writeNumber(Int32 value);
    virtual void writeNumber(UInt32 value);
    virtual void writeNumber(Int64 value);
    virtual void writeNumber(UInt64 value);
    virtual void writeNumber(UInt128 value);
    virtual void writeNumber(Float32 value);
    virtual void writeNumber(Float64 value);

    virtual void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values);
    virtual void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values);
    virtual void writeEnum(Int8 value);
    virtual void writeEnum(Int16 value);

    virtual void writeUUID(const UUID & value);
    virtual void writeDate(DayNum date);
    virtual void writeDateTime(time_t tm);

    virtual void writeDecimal(Decimal32 decimal, UInt32 scale);
    virtual void writeDecimal(Decimal64 decimal, UInt32 scale);
    virtual void writeDecimal(Decimal128 decimal, UInt32 scale);

protected:
    ProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_);

    void throwCannotConvertType(const String & type_name);

    template<typename T>
    void throwCannotConvertValue(const T & value);

    void checkCanWriteField();

    const google::protobuf::FieldDescriptor* descriptor;
    google::protobuf::Message* message;
};

}
