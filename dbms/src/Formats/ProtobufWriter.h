#pragma once

#include <Formats/ProtobufSimpleWriter.h>


namespace DB
{

class ProtobufWriter : private boost::noncopyable
{
public:
    ProtobufWriter(WriteBuffer & out, const google::protobuf::FileDescriptor* file_descriptor);

    void setMessageType(const google::protobuf::Descriptor* descriptor);
    const std::vector<const google::protobuf::FieldDescriptor*> & fieldsInWriteOrder() const;

    void newMessage();
    void flush();

    const google::protobuf::FieldDescription* currentField() const;
    bool nextField();

    void addNumber(Int8 value);
    void addNumber(UInt8 value);
    void addNumber(Int16 value);
    void addNumber(UInt16 value);
    void addNumber(Int32 value);
    void addNumber(UInt32 value);
    void addNumber(Int64 value);
    void addNumber(UInt64 value);
    void addNumber(UInt128 value);
    void addNumber(Float32 value);
    void addNumber(Float64 value);

    void addString(const StringRef & value);

    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values);
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values);
    void addEnum(Int8 value);
    void addEnum(Int16 value);

    void addUUID(const UUID & value);
    void addDate(DayNum date);
    void addDateTime(time_t tm);

    void addDecimal(Decimal32 decimal, UInt32 scale);
    void addDecimal(Decimal64 decimal, UInt32 scale);
    void addDecimal(Decimal128 decimal, UInt32 scale);

private:
    void finishCurrentMessage();
    void finishCurrentField();

    ProtobufSimpleWriter simple_writer;
    std::vector<const google::protobuf::FieldDescriptor*> fields_in_write_order;
    size_t current_field_index = -1;

    class FieldSetter;
    class StringFieldSetter;
    template<typename T> class NumberFieldSetter;
    class BoolFieldSetter;
    class EnumFieldSetter;

    FieldSetter* currentFieldSetter();

    std::vector<std::unique_ptr<FieldSetter>> field_setters;
};

}
