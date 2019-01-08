#pragma once

#include <boost/core/noncopyable.hpp>
#include <Common/UInt128.h>
#include <Core/Types.h>


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

    void writeNumber(Int8 value) { writeInt8(value); }
    void writeNumber(UInt8 value) { writeUInt8(value); }
    void writeNumber(Int16 value) { writeInt16(value); }
    void writeNumber(UInt16 value) { writeUInt16(value); }
    void writeNumber(Int32 value) { writeInt32(value); }
    void writeNumber(UInt32 value) { writeUInt32(value); }
    void writeNumber(Int64 value) { writeInt64(value); }
    void writeNumber(UInt64 value) { writeUInt64(value); }
    void writeNumber(UInt128 value) { writeUInt128(value); }
    void writeNumber(Float32 value) { writeFloat32(value); }
    void writeNumber(Float64 value) { writeFloat64(value); }

    void prepareEnumValueMapping(const std::vector<std::pair<std::string, Int8>> & data_type_enum_values)
    {
        prepareEnumValueMappingInt8(data_type_enum_values);
    }

    void prepareEnumValueMapping(const std::vector<std::pair<std::string, Int16>> & data_type_enum_values)
    {
        prepareEnumValueMappingInt16(data_type_enum_values);
    }

    virtual void writeEnum(Int16 value);

protected:
    ProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_);

    virtual void writeInt8(Int8 value);
    virtual void writeUInt8(UInt8 value);
    virtual void writeInt16(Int16 value);
    virtual void writeUInt16(UInt16 value);
    virtual void writeInt32(Int32 value);
    virtual void writeUInt32(UInt32 value);
    virtual void writeInt64(Int64 value);
    virtual void writeUInt64(UInt64 value);
    virtual void writeUInt128(UInt128 value);
    virtual void writeFloat32(Float32 value);
    virtual void writeFloat64(Float64 value);

    virtual void prepareEnumValueMappingInt8(const std::vector<std::pair<std::string, Int8>> & data_type_enum_values);
    virtual void prepareEnumValueMappingInt16(const std::vector<std::pair<std::string, Int16>> & data_type_enum_values);

    void throwCannotConvertType(const String & type_name);
    void throwCannotConvertValue(const String & value);

    const google::protobuf::FieldDescriptor* descriptor;
    google::protobuf::Message* message;
};

}
