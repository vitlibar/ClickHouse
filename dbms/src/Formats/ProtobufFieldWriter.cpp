#include <boost/numeric/conversion/cast.hpp>
#include <Common/Exception.h>
#include <Formats/ProtobufFieldWriter.h>
#include <google/protobuf/message.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONVERT_TO_PROTOBUF_TYPE;
}


// ProtobufFieldWriter ----------------------------------------------------------------------------

ProtobufFieldWriter::ProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
    : descriptor(descriptor_), message(nullptr) {}

ProtobufFieldWriter::~ProtobufFieldWriter() = default;

void ProtobufFieldWriter::writeString(const String &) { throwCannotConvertType("String"); }

void ProtobufFieldWriter::writeNumber(Int8 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(UInt8 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(Int16 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(UInt16 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(Int32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(UInt32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(Int64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(UInt64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(UInt128 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(Float32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeNumber(Float64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }

void ProtobufFieldWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> &) {}
void ProtobufFieldWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> &) {}

void ProtobufFieldWriter::writeEnum(Int8) { throwCannotConvertType("Enum"); }
void ProtobufFieldWriter::writeEnum(Int16) { throwCannotConvertType("Enum"); }

void ProtobufFieldWriter::writeUUID(const UUID &) { throwCannotConvertType("UUID"); }
void ProtobufFieldWriter::writeDate(DayNum) { throwCannotConvertType("Date"); }
void ProtobufFieldWriter::writeDateTime(time_t) { throwCannotConvertType("DateTime"); }

void ProtobufFieldWriter::writeDecimal(Decimal32 decimal, UInt32) { throwCannotConvertType(TypeName<decltype(decimal)>::get()); }
void ProtobufFieldWriter::writeDecimal(Decimal64 decimal, UInt32) { throwCannotConvertType(TypeName<decltype(decimal)>::get()); }
void ProtobufFieldWriter::writeDecimal(Decimal128 decimal, UInt32) { throwCannotConvertType(TypeName<decltype(decimal)>::get()); }

void ProtobufFieldWriter::throwCannotConvertType(const String & type_name)
{
    throw Exception("Could not convert data type " + type_name + " to protobuf type " + descriptor->type_name() +
                    " (field: " + descriptor->name() + ").",
                    ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}

template<typename T>
void ProtobufFieldWriter::throwCannotConvertValue(const T & value)
{
    throw Exception("Could not convert value " + toString(value) + " to protobuf type " + descriptor->type_name() +
                    " (field: " + descriptor->name() + ").",
                    ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}

void ProtobufFieldWriter::checkCanWriteField()
{
    if (!descriptor->is_repeated() && message->GetReflection()->HasField(message, descriptor))
        throw Exception("Cannot add more than single value to the non-repeated field " + descriptor->name(),
                        ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}



// StringProtobufFieldWriter ----------------------------------------------------------------------------

class StringProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    StringProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override { writeField(value); }

    void writeNumber(Int8 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt8 value) override { writeNumberTemplate(value); }
    void writeNumber(Int16 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt16 value) override { writeNumberTemplate(value); }
    void writeNumber(Int32 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt32 value) override { writeNumberTemplate(value); }
    void writeNumber(Int64 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt64 value) override { writeNumberTemplate(value); }
    void writeNumber(Float32 value) override { writeNumberTemplate(value); }
    void writeNumber(Float64 value) override { writeNumberTemplate(value); }

    void prepareEnumMapping(const std::vector<std::pair<String, Int8>> & enum_values) override { prepareEnumMappingTemplate(enum_values); }
    void prepareEnumMapping(const std::vector<std::pair<String, Int16>> & enum_values) override { prepareEnumMappingTemplate(enum_values); }
    void writeEnum(Int8 value) override { writeEnum(static_cast<Int16>(value)); }

    void writeEnum(Int16 value) override
    {
        auto it = enum_value_to_name_map->find(value);
        if (it == enum_value_to_name_map->end())
            throwCannotConvertValue(value);
        writeField(it->second);
    }

    void writeUUID(const UUID & value) override { writeString(toString(value)); }
    void writeDate(DayNum date) override { writeString(toString(date)); }
    void writeDateTime(time_t tm) override { writeString(toString(tm)); }

    void writeDecimal(Decimal32 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }
    void writeDecimal(Decimal64 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }
    void writeDecimal(Decimal128 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }

private:
    void writeField(const String & value)
    {
        checkCanWriteField();
        if (descriptor->is_repeated())
            message->GetReflection()->AddString(message, descriptor, value);
        else
            message->GetReflection()->SetString(message, descriptor, value);
    }

    template<typename T>
    void writeNumberTemplate(T number)
    {
        writeField(toString(number));
    }

    template<typename T>
    void writeDecimalTemplate(Decimal<T> decimal, UInt32 scale)
    {
        WriteBufferFromOwnString buf;
        writeText(decimal, scale, buf);
        writeField(buf.str());
    }

    template<typename T>
    void prepareEnumMappingTemplate(const std::vector<std::pair<String, T>> & enum_values)
    {
        if (enum_value_to_name_map.has_value())
            return;
        enum_value_to_name_map.emplace();
        for (const auto & name_value_pair : enum_values)
            enum_value_to_name_map->emplace(name_value_pair.second, name_value_pair.first);
    }

    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};



// NumberProtobufFieldWriter ----------------------------------------------------------------------------

template<typename ProtobufFieldType>
class NumberProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    NumberProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override
    {
        ProtobufFieldType parsed;
        try
        {
            parsed = parseFromString<ProtobufFieldType>(value);
        }
        catch(...)
        {
            throwCannotConvertValue("'" + value + "'");
        }
        writeField(parsed);
    }

    void writeNumber(Int8 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt8 value) override { writeNumberTemplate(value); }
    void writeNumber(Int16 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt16 value) override { writeNumberTemplate(value); }
    void writeNumber(Int32 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt32 value) override { writeNumberTemplate(value); }
    void writeNumber(Int64 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt64 value) override { writeNumberTemplate(value); }
    void writeNumber(Float32 value) override { writeNumberTemplate(value); }
    void writeNumber(Float64 value) override { writeNumberTemplate(value); }

    void writeEnum(Int8 value) override { writeEnum(static_cast<Int16>(value)); }

    void writeEnum(Int16 value) override
    {
        if constexpr (!std::is_integral_v<ProtobufFieldType>)
           throwCannotConvertType("Enum");
        writeNumber(value);
    }

    void writeDate(DayNum date) override { writeNumber(date); }
    void writeDateTime(time_t tm) override { writeNumber(tm); }

    void writeDecimal(Decimal32 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }
    void writeDecimal(Decimal64 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }
    void writeDecimal(Decimal128 decimal, UInt32 scale) override { writeDecimalTemplate(decimal, scale); }

private:
    template<typename T>
    void writeDecimalTemplate(Decimal<T> decimal, UInt32 scale)
    {
        if constexpr (std::is_integral_v<ProtobufFieldType>)
            writeNumberTemplate(decimal.value / decimalScaleMultiplier<T>(scale));
        else
            writeNumberTemplate(double(decimal.value) * pow(10., -double(scale)));
    }

    template<typename T>
    void writeNumberTemplate(T value)
    {
        ProtobufFieldType converted_value;
        try
        {
            converted_value = boost::numeric_cast<ProtobufFieldType>(value);
        }
        catch (boost::numeric::bad_numeric_cast &)
        {
            throwCannotConvertValue(value);
        }

        writeField(converted_value);
    }

    void writeField(ProtobufFieldType value)
    {
        checkCanWriteField();
        if constexpr (std::is_same_v<ProtobufFieldType, Int32>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddInt32(message, descriptor, value);
            else
                message->GetReflection()->SetInt32(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<ProtobufFieldType, UInt32>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddUInt32(message, descriptor, value);
            else
                message->GetReflection()->SetUInt32(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<ProtobufFieldType, Int64>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddInt64(message, descriptor, value);
            else
                message->GetReflection()->SetInt64(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<ProtobufFieldType, UInt64>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddUInt64(message, descriptor, value);
            else
                message->GetReflection()->SetUInt64(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<ProtobufFieldType, float>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddFloat(message, descriptor, value);
            else
                message->GetReflection()->SetFloat(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<ProtobufFieldType, double>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddDouble(message, descriptor, value);
            else
                message->GetReflection()->SetDouble(message, descriptor, value);
        }
    }
};



// BoolProtobufFieldWriter ----------------------------------------------------------------------------

class BoolProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    BoolProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override
    {
        if (value == "true")
            writeField(true);
        else if (value == "false")
            writeField(false);
        else
            throwCannotConvertValue("'" + value + "'");
    }

    void writeNumber(Int8 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt8 value) override { writeNumberTemplate(value); }
    void writeNumber(Int16 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt16 value) override { writeNumberTemplate(value); }
    void writeNumber(Int32 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt32 value) override { writeNumberTemplate(value); }
    void writeNumber(Int64 value) override { writeNumberTemplate(value); }
    void writeNumber(UInt64 value) override { writeNumberTemplate(value); }
    void writeNumber(Float32 value) override { writeNumberTemplate(value); }
    void writeNumber(Float64 value) override { writeNumberTemplate(value); }

private:
    template<typename T>
    void writeNumberTemplate(T value)
    {
        writeField(value != 0);
    }

    void writeField(bool value)
    {
        checkCanWriteField();
        if (descriptor->is_repeated())
            message->GetReflection()->AddBool(message, descriptor, value);
        else
            message->GetReflection()->SetBool(message, descriptor, value);
    }
};



// EnumProtobufFieldWriter ----------------------------------------------------------------------------

class EnumProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    EnumProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override
    {
        const auto* enum_descriptor = descriptor->enum_type()->FindValueByName(value);
        if (!enum_descriptor)
            throwCannotConvertValue("'" + value + "'");
        writeField(enum_descriptor);
    }

    void writeNumber(Int8 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(UInt8 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(Int16 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(UInt16 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(Int32 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(UInt32 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(Int64 value) override { writeNumberTemplate(toInt(value)); }
    void writeNumber(UInt64 value) override { writeNumberTemplate(toInt(value)); }

    void prepareEnumMapping(const std::vector<std::pair<String, Int8>> & enum_values) override { prepareEnumMappingTemplate(enum_values); }
    void prepareEnumMapping(const std::vector<std::pair<String, Int16>> & enum_values) override { prepareEnumMappingTemplate(enum_values); }
    void writeEnum(Int8 value) override { writeEnum(static_cast<Int16>(value)); }

    void writeEnum(Int16 value) override
    {
        auto it = enum_value_to_descriptor_map->find(value);
        if (it == enum_value_to_descriptor_map->end())
            throwCannotConvertValue(value);
        writeField(it->second);
    }

private:
    template<typename T>
    void writeNumberTemplate(T value)
    {
        int value_as_int;
        try
        {
            value_as_int = boost::numeric_cast<int>(value);
        }
        catch (boost::numeric::bad_numeric_cast &)
        {
            throwCannotConvertValue(value);
        }

        const auto* enum_descriptor = descriptor->enum_type()->FindValueByNumber(value_as_int);
        if (!enum_descriptor)
            throwCannotConvertValue(value);
        writeField(enum_descriptor);
    }

    void writeField(const google::protobuf::EnumValueDescriptor* enum_descriptor)
    {
        checkCanWriteField();
        if (descriptor->is_repeated())
            message->GetReflection()->AddEnum(message, descriptor, enum_descriptor);
        else
            message->GetReflection()->SetEnum(message, descriptor, enum_descriptor);
    }

    template<typename T>
    void prepareEnumMappingTemplate(const std::vector<std::pair<String, T>> & enum_values)
    {
        if (enum_value_to_descriptor_map.has_value())
            return;
        enum_value_to_descriptor_map.emplace();
        for (const auto & name_value_pair : enum_values)
        {
            Int16 value = name_value_pair.second;
            const auto* enum_descriptor = descriptor->enum_type()->FindValueByName(name_value_pair.first);
            if (enum_descriptor)
                enum_value_to_descriptor_map->emplace(value, enum_descriptor);
        }
    }

    std::optional<std::unordered_map<Int16, const google::protobuf::EnumValueDescriptor*>> enum_value_to_descriptor_map;
};



// ProtobufFieldWriter::create -----------------------------------------------------------------------------------------

std::unique_ptr<ProtobufFieldWriter> ProtobufFieldWriter::create(const google::protobuf::FieldDescriptor* descriptor_)
{
    switch (descriptor_->cpp_type())
    {
        case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
            return std::make_unique<StringProtobufFieldWriter>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
            return std::make_unique<NumberProtobufFieldWriter<Int32>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
            return std::make_unique<NumberProtobufFieldWriter<UInt32>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
            return std::make_unique<NumberProtobufFieldWriter<Int64>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
            return std::make_unique<NumberProtobufFieldWriter<UInt64>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
            return std::make_unique<NumberProtobufFieldWriter<float>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
            return std::make_unique<NumberProtobufFieldWriter<double>>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
            return std::make_unique<BoolProtobufFieldWriter>(descriptor_);
        case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
            return std::make_unique<EnumProtobufFieldWriter>(descriptor_);
        default:
            throw Exception(String("Protobuf type ") + descriptor_->type_name() + " isn't supported.",
                            ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
    }
}

}
