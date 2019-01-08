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
void ProtobufFieldWriter::writeInt8(Int8 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeUInt8(UInt8 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeInt16(Int16 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeUInt16(UInt16 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeInt32(Int32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeUInt32(UInt32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeInt64(Int64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeUInt64(UInt64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeUInt128(UInt128 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeFloat32(Float32 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }
void ProtobufFieldWriter::writeFloat64(Float64 value) { throwCannotConvertType(TypeName<decltype(value)>::get()); }

void ProtobufFieldWriter::prepareEnumValueMappingInt8(const std::vector<std::pair<std::string, Int8>> &) {}
void ProtobufFieldWriter::prepareEnumValueMappingInt16(const std::vector<std::pair<std::string, Int16>> &) {}

void ProtobufFieldWriter::writeEnum(Int16) { throwCannotConvertType("Enum"); }

void ProtobufFieldWriter::throwCannotConvertType(const String & type_name)
{
    throw Exception("Could not convert data type " + type_name + " to protobuf type " + descriptor->type_name() +
                    " (field: " + descriptor->name() + ").",
                    ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}

void ProtobufFieldWriter::throwCannotConvertValue(const String & value)
{
    throw Exception("Could not convert value " + value + " to protobuf type " + descriptor->type_name() +
                    " (field: " + descriptor->name() + ").",
                    ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}



// StringProtobufFieldWriter ----------------------------------------------------------------------------

class StringProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    StringProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override
    {
        if (descriptor->is_repeated())
            message->GetReflection()->AddString(message, descriptor, value);
        else
            message->GetReflection()->SetString(message, descriptor, value);
    }

    void writeInt8(Int8 value) override { writeString(toString(value)); }
    void writeUInt8(UInt8 value) override { writeString(toString(value)); }
    void writeInt16(Int16 value) override { writeString(toString(value)); }
    void writeUInt16(UInt16 value) override { writeString(toString(value)); }
    void writeInt32(Int32 value) override { writeString(toString(value)); }
    void writeUInt32(UInt32 value) override { writeString(toString(value)); }
    void writeInt64(Int64 value) override { writeString(toString(value)); }
    void writeUInt64(UInt64 value) override { writeString(toString(value)); }
    void writeFloat32(Float32 value) override { writeString(toString(value)); }
    void writeFloat64(Float64 value) override { writeString(toString(value)); }

    void prepareEnumValueMappingInt8(const std::vector<std::pair<String, Int8>> & data_type_enum_values) override
    {
        prepareEnumValueMappingInternal(data_type_enum_values);
    }

    void prepareEnumValueMappingInt16(const std::vector<std::pair<String, Int16>> & data_type_enum_values) override
    {
        prepareEnumValueMappingInternal(data_type_enum_values);
    }

    void writeEnum(Int16 value) override
    {
        auto it = enum_value_to_name_map->find(value);
        if (it == enum_value_to_name_map->end())
            throwCannotConvertValue(toString(value));
        writeString(it->second);
    }

private:
    template<typename T>
    void prepareEnumValueMappingInternal(const std::vector<std::pair<String, T>> & data_type_enum_values)
    {
        if (enum_value_to_name_map.has_value())
            return;
        enum_value_to_name_map.emplace();
        for (const auto & name_value_pair : data_type_enum_values)
            enum_value_to_name_map->emplace(name_value_pair.second, name_value_pair.first);
    }

    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};



// NumberProtobufFieldWriter ----------------------------------------------------------------------------

template<typename T>
class NumberProtobufFieldWriter : public ProtobufFieldWriter
{
public:
    NumberProtobufFieldWriter(const google::protobuf::FieldDescriptor* descriptor_)
        : ProtobufFieldWriter(descriptor_) {}

    void writeString(const String & value) override { writeNumberInternal(parseFromString<T>(value)); }

    void writeInt8(Int8 value) override { writeNumberInternal(castNumeric(value)); }
    void writeUInt8(UInt8 value) override { writeNumberInternal(castNumeric(value)); }
    void writeInt16(Int16 value) override { writeNumberInternal(castNumeric(value)); }
    void writeUInt16(UInt16 value) override { writeNumberInternal(castNumeric(value)); }
    void writeInt32(Int32 value) override { writeNumberInternal(castNumeric(value)); }
    void writeUInt32(UInt32 value) override { writeNumberInternal(castNumeric(value)); }
    void writeInt64(Int64 value) override { writeNumberInternal(castNumeric(value)); }
    void writeUInt64(UInt64 value) override { writeNumberInternal(castNumeric(value)); }
    void writeFloat32(Float32 value) override { writeNumberInternal(castNumeric(value)); }
    void writeFloat64(Float64 value) override { writeNumberInternal(castNumeric(value)); }

    void writeEnum(Int16 value) override
    {
        if constexpr (!std::is_integral_v<T>)
           throwCannotConvertType("Enum");
        writeInt16(value);
    }

private:
    template<typename From>
    T castNumeric(From value)
    {
        if constexpr (std::is_same_v<T, From>)
            return value;
        if constexpr (std::is_integral_v<T> && std::is_integral_v<From>)
        {
            T converted = static_cast<T>(value);
            if (static_cast<From>(converted) != value)
                throwCannotConvertValue(toString(value));
            return converted;
        }
        if constexpr (std::is_integral_v<T> && std::is_floating_point_v<From>)
        {
            if (!std::isfinite(value) || (value < std::numeric_limits<T>::min()) || (value > std::numeric_limits<T>::max()))
                throwCannotConvertValue(toString(value));
        }
        return static_cast<T>(value);
    }

    void writeNumberInternal(T value)
    {
        if constexpr (std::is_same_v<T, Int32>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddInt32(message, descriptor, value);
            else
                message->GetReflection()->SetInt32(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<T, UInt32>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddUInt32(message, descriptor, value);
            else
                message->GetReflection()->SetUInt32(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<T, Int64>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddInt64(message, descriptor, value);
            else
                message->GetReflection()->SetInt64(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<T, UInt64>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddUInt64(message, descriptor, value);
            else
                message->GetReflection()->SetUInt64(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddFloat(message, descriptor, value);
            else
                message->GetReflection()->SetFloat(message, descriptor, value);
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            if (descriptor->is_repeated())
                message->GetReflection()->AddDouble(message, descriptor, value);
            else
                message->GetReflection()->SetDouble(message, descriptor, value);
        }
        //else
        //    std::static_assert(false);
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
            writeBoolInternal(true);
        else if (value == "false")
            writeBoolInternal("false");
        else
            throwCannotConvertValue("'" + value + "'");
    }

    void writeInt8(Int8 value) override { writeBoolInternal(value != 0); }
    void writeUInt8(UInt8 value) override { writeBoolInternal(value != 0); }
    void writeInt16(Int16 value) override { writeBoolInternal(value != 0); }
    void writeUInt16(UInt16 value) override { writeBoolInternal(value != 0); }
    void writeInt32(Int32 value) override { writeBoolInternal(value != 0); }
    void writeUInt32(UInt32 value) override { writeBoolInternal(value != 0); }
    void writeInt64(Int64 value) override { writeBoolInternal(value != 0); }
    void writeUInt64(UInt64 value) override { writeBoolInternal(value != 0); }
    void writeFloat32(Float32 value) override { writeBoolInternal(value != 0); }
    void writeFloat64(Float64 value) override { writeBoolInternal(value != 0); }

private:
    void writeBoolInternal(bool value)
    {
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
        writeEnumInternal(enum_descriptor);
    }

    void writeInt8(Int8 value) override { writeEnumInternal(toInt(value)); }
    void writeUInt8(UInt8 value) override { writeEnumInternal(toInt(value)); }
    void writeInt16(Int16 value) override { writeEnumInternal(toInt(value)); }
    void writeUInt16(UInt16 value) override { writeEnumInternal(toInt(value)); }
    void writeInt32(Int32 value) override { writeEnumInternal(toInt(value)); }
    void writeUInt32(UInt32 value) override { writeEnumInternal(toInt(value)); }
    void writeInt64(Int64 value) override { writeEnumInternal(toInt(value)); }
    void writeUInt64(UInt64 value) override { writeEnumInternal(toInt(value)); }

    void prepareEnumValueMappingInt8(const std::vector<std::pair<String, Int8>> & data_type_enum_values) override
    {
        prepareEnumValueMappingInternal(data_type_enum_values);
    }

    void prepareEnumValueMappingInt16(const std::vector<std::pair<String, Int16>> & data_type_enum_values) override
    {
        prepareEnumValueMappingInternal(data_type_enum_values);
    }

    void writeEnum(Int16 value) override
    {
        auto it = enum_value_to_descriptor_map->find(value);
        if (it == enum_value_to_descriptor_map->end())
            throwCannotConvertValue(toString(value));
        writeEnumInternal(it->second);
    }

private:
    void writeEnumInternal(int value)
    {
        const auto* enum_descriptor = descriptor->enum_type()->FindValueByNumber(value);
        if (!enum_descriptor)
            throwCannotConvertValue(toString(value));
        writeEnumInternal(enum_descriptor);
    }

    void writeEnumInternal(const google::protobuf::EnumValueDescriptor* enum_descriptor)
    {
        if (descriptor->is_repeated())
            message->GetReflection()->AddEnum(message, descriptor, enum_descriptor);
        else
            message->GetReflection()->SetEnum(message, descriptor, enum_descriptor);
    }

    template<typename T>
    int toInt(T value)
    {
        int value_as_int = static_cast<int>(value);
        if (static_cast<T>(value_as_int) != value)
            throwCannotConvertValue(toString(value));
        return value_as_int;
    }

    template<typename T>
    void prepareEnumValueMappingInternal(const std::vector<std::pair<String, T>> & data_type_enum_values)
    {
        if (enum_value_to_descriptor_map.has_value())
            return;
        enum_value_to_descriptor_map.emplace();
        for (const auto & name_value_pair : data_type_enum_values)
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
