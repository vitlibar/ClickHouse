#include <Formats/ProtobufWriter.h>


namespace DB
{

// FieldSetter ---------------------------------------------------------------------------------------------------------

class ProtobufWriter::FieldSetter : private boost::noncopyable
{
public:
    FieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : simple_writer(simple_writer_), field(field_) {}

    virtual void addString(const String & value) { cannotConvertType("String"); }

    virtual void addInt8(Int8 value) { cannotConvertType("Int8"); }
    virtual void addUInt8(UInt8 value) { cannotConvertType("UInt8"); }
    virtual void addInt16(Int16 value) { cannotConvertType("Int16"); }
    virtual void addUInt16(UInt16 value) { cannotConvertType("UInt16"); }
    virtual void addInt32(Int32 value) { cannotConvertType("Int32"); }
    virtual void addUInt32(UInt32 value) { cannotConvertType("UInt32"); }
    virtual void addInt64(Int64 value) { cannotConvertType("Int64"); }
    virtual void addUInt64(UInt64 value) { cannotConvertType("UInt64"); }
    virtual void addUInt128(UInt128 value) { cannotConvertType("UInt128"); }
    virtual void addFloat32(Float32 value) { cannotConvertType("Float32"); }
    virtual void addFloat64(Float64 value) { cannotConvertType("Float64"); }

    virtual void prepareEnumMappingInt8(const std::vector<std::pair<std::string, Int8>> & enum_values) {}
    virtual void prepareEnumMappingInt16(const std::vector<std::pair<std::string, Int16>> & enum_values) {}
    virtual void addEnumInt8(Int8 value) { cannotConvertType("Enum"); }
    virtual void addEnumInt16(Int16 value) { cannotConvertType("Enum"); }

    virtual void addUUID(const UUID & value) { cannotConvertType("UUID"); }
    virtual void addDate(DayNum date) { cannotConvertType("Date"); }
    virtual void addDateTime(time_t tm) { cannotConvertType("DateTime"); }

    virtual void addDecimal32(Decimal32 decimal, UInt32 scale) { cannotConvertType("Decimal32"); }
    virtual void addDecimal64(Decimal64 decimal, UInt32 scale) { cannotConvertType("Decimal64"); }
    virtual void addDecimal128(Decimal128 decimal, UInt32 scale) { cannotConvertType("Decimal128"); }

    virtual void addDefaultIfDefined() {}

protected:
    void cannotConvertType(const String & type_name)
    {
        throw Exception("Could not convert data type " + type_name + " to protobuf type " + descriptor->type_name() +
                        " (field: " + descriptor->name() + ").",
                        ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
    }

    void cannotConvertValue(const String & value_as_string)
    {
        throw Exception("Could not convert value " + value_as_string + " to protobuf type " + descriptor->type_name() +
                        " (field: " + descriptor->name() + ").",
                        ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
    }

    template<typename To, typename From>
    To numericCast(From value)
    {
        To result;
        try
        {
            result = boost::numeric_cast<To>(value);
        }
        catch (boost::numeric::bad_numeric_cast &)
        {
            cannotConvertValue(toString(value));
        }
        return result;
    }

    template<typename To>
    To parseFromString(const StringRef & str)
    {
        To result;
        try
        {
            parsed = ::DB::parse<To>(str.data, str.size());
        }
        catch(...)
        {
            cannotConvertValue("'" + str.toString() + "'");
        }
        return result;
    }

    ProtobufSimpleWriter & simple_writer;
    const google::protobuf::FieldDescriptor* descriptor;
};



// StringFieldSetter ---------------------------------------------------------------------------------------------------

class ProtobufWriter::StringFieldSetter : public FieldSetter
{
public:
    StringFieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : FieldSetter(simple_writer_, field_) {}

    void addString(const StringRef & str) override { addStringImpl(str); }

    void addInt8(Int8 value) override { convertToStringAndAdd(value); }
    void addUInt8(UInt8 value) override { convertToStringAndAdd(value); }
    void addInt16(Int16 value) override { convertToStringAndAdd(value); }
    void addUInt16(UInt16 value) override { convertToStringAndAdd(value); }
    void addInt32(Int32 value) override { convertToStringAndAdd(value); }
    void addUInt32(UInt32 value) override { convertToStringAndAdd(value); }
    void addInt64(Int64 value) override { convertToStringAndAdd(value); }
    void addUInt64(UInt64 value) override { convertToStringAndAdd(value); }
    void addFloat32(Float32 value) override { convertToStringAndAdd(value); }
    void addFloat64(Float64 value) override { convertToStringAndAdd(value); }

    void prepareEnumMappingInt8(const std::vector<std::pair<String, Int8>> & enum_values) override { prepareEnumMapping(enum_values); }
    void prepareEnumMappingInt16(const std::vector<std::pair<String, Int16>> & enum_values) override { prepareEnumMapping(enum_values); }

    void addEnumInt8(Int8 value) override { addEnumInt16(value); }

    void addEnumInt16(Int16 value) override
    {
        auto it = enum_value_to_name_map->find(value);
        if (it == enum_value_to_name_map->end())
            cannotConvertValue(toString(value));
        addStringImpl(it->second);
    }

    void addUUID(const UUID & value) override { convertToStringAndAdd(value); }
    void addDate(DayNum date) override { convertToStringAndAdd(value); }
    void addDateTime(time_t tm) override { convertToStringAndAdd(value); }

    void addDecimal32(Decimal32 decimal, UInt32 scale) override { addDecimal(decimal, scale); }
    void addDecimal64(Decimal64 decimal, UInt32 scale) override { addDecimal(decimal, scale); }
    void addDecimal128(Decimal128 decimal, UInt32 scale) override { addDecimal(decimal, scale); }

private:
    void addStringImpl(const StringRef & str)
    {
        simple_writer.addString(str);
    }

    template<typename T>
    void convertToStringAndAdd(T value)
    {
        writeText(to_string_conversion_buffer, value);
        addStringImpl(flushBuffer());
    }

    template<typename T>
    void addDecimal(const Decimal<T> & decimal, UInt32 scale)
    {
        writeText(decimal, scale, to_string_conversion_buffer);
        addStringImpl(flushBuffer());
    }

    StringRef flushBuffer()
    {
        const char* data = to_string_conversion_buffer.str().data();
        StringRef str(data, to_string_conversion_buffer.position() - data);
        to_string_conversion_buffer.set(data, buffer.str().size());
        return str;
    }

    template<typename T>
    void prepareEnumMapping(const std::vector<std::pair<String, T>> & enum_values)
    {
        if (enum_value_to_name_map.has_value())
            return;
        enum_value_to_name_map.emplace();
        for (const auto & name_value_pair : enum_values)
            enum_value_to_name_map->emplace(name_value_pair.second, name_value_pair.first);
    }

    WriteBufferFromOwnString to_string_conversion_buffer;
    std::optional<std::unordered_map<Int16, String>> enum_value_to_name_map;
};



// BytesFieldSetter ---------------------------------------------------------------------------------------------------

class ProtobufWriter::BytesFieldSetter : public FieldSetter
{
public:
    BytesFieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : FieldSetter(simple_writer_, field_) {}
};



// NumberProtobufFieldWriter -------------------------------------------------------------------------------------------

template<typename T>
class ProtobufWriter::NumberFieldSetter : public FieldSetter
{
public:
    NumberFieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : FieldSetter(simple_writer_, field_) {
        setupAddNumberImplFunction();
    }

    void addString(const StringRef & str) override { addNumberImpl(parseFromString<T>(str)); }

    void addInt8(Int8 value) override { castNumericAndAdd(value); }
    void addUInt8(UInt8 value) override { castNumericAndAdd(value); }
    void addInt16(Int16 value) override { castNumericAndAdd(value); }
    void addUInt16(UInt16 value) override { castNumericAndAdd(value); }
    void addInt32(Int32 value) override { castNumericAndAdd(value); }
    void addUInt32(UInt32 value) override { castNumericAndAdd(value); }
    void addInt64(Int64 value) override { castNumericAndAdd(value); }
    void addUInt64(UInt64 value) override { castNumericAndAdd(value); }
    void addFloat32(Float32 value) override { castNumericAndAdd(value); }
    void addFloat64(Float64 value) override { castNumericAndAdd(value); }

    void addEnumInt8(Int8 value) override { addEnumInt16(value); }

    void addEnumInt16(Int16 value) override
    {
        if constexpr (!std::is_integral_v<T>)
           cannotConvertType("Enum");
        castNumericAndAdd(value);
    }

    void addDate(DayNum date) override { castNumericAndAdd(date); }
    void addDateTime(time_t tm) override { castNumericAndAdd(tm); }

    void addDecimal32(Decimal32 decimal, UInt32 scale) override { addDecimal(decimal, scale); }
    void addDecimal64(Decimal64 decimal, UInt32 scale) override { addDecimal(decimal, scale); }
    void addDecimal128(Decimal128 decimal, UInt32 scale) override { addDecimal(decimal, scale); }

private:
    template<typename From>
    void castNumericAndAdd(From value)
    {
        addNumberImpl(numericCast<T>(value));
    }

    template<typename S>
    void addDecimal(const Decimal<S> & decimal, UInt32 scale)
    {
        if constexpr (std::is_integral_v<T>)
            castNumberAndAdd(decimal.value / decimalScaleMultiplier<S>(scale));
        else
            castNumberAndAdd(double(decimal.value) * pow(10., -double(scale)));
    }

    void addNumberImpl(T value)
    {
        (simple_writer.*add_number_impl_function)(value);
    }

    void setupAddNumberImplFunction()
    {
        if constexpr (std::is_same_v<T, Int32>)
        {
            switch (field_->type())
            {
                case TYPE_INT32: add_number_impl_function = &ProtobufSimpleWriter::addInt32; break;
                case TYPE_SINT32: add_number_impl_function = &ProtobufSimpleWriter::addSInt32; break;
                case TYPE_SFIXED32: add_number_impl_function = &ProtobufSimpleWriter::addSFixed32; break;
                default: assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, UInt32>)
        {
            switch (field_->type())
            {
                case TYPE_UINT32: add_number_impl_function = &ProtobufSimpleWriter::addUInt32; break;
                case TYPE_FIXED32: add_number_impl_function = &ProtobufSimpleWriter::addFixed32; break;
                default: assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, Int64>)
        {
            switch (field_->type())
            {
                case TYPE_INT64: add_number_impl_function = &ProtobufSimpleWriter::addInt64; break;
                case TYPE_SINT64: add_number_impl_function = &ProtobufSimpleWriter::addSInt64; break;
                case TYPE_SFIXED64: add_number_impl_function = &ProtobufSimpleWriter::addSFixed64; break;
                default: assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, UInt64>)
        {
            switch (field_->type())
            {
                case TYPE_UINT64: add_number_impl_function = &ProtobufSimpleWriter::addUInt64; break;
                case TYPE_FIXED64: add_number_impl_function = &ProtobufSimpleWriter::addFixed64; break;
                default: assert(false);
            }
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            add_number_impl_function = &ProtobufSimpleWriter::addFloat;
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            add_number_impl_function = &ProtobufSimpleWriter::addDouble;
        }
        else
            assert(false);
    }

    void (ProtobufSimpleWriter::*add_number_impl_function(T value);
};



// BoolFieldSetter -----------------------------------------------------------------------------------------------------

class ProtobufField::BoolFieldSetter : public FieldSetter
{
public:
    BoolFieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : FieldSetter(simple_writer_, field_) {}

    void addString(const StringRef & str) override
    {
        if (str == "true")
            addBoolImpl(true);
        else if (str == "false")
            addBoolImpl(false);
        else
            cannotConvertValue("'" + str + "'");
    }

    void addInt8(Int8 value) override { convertToBoolAndAdd(value); }
    void addUInt8(UInt8 value) override { convertToBoolAndAdd(value); }
    void addInt16(Int16 value) override { convertToBoolAndAdd(value); }
    void addUInt16(UInt16 value) override { convertToBoolAndAdd(value); }
    void addInt32(Int32 value) override { convertToBoolAndAdd(value); }
    void addUInt32(UInt32 value) override { convertToBoolAndAdd(value); }
    void addInt64(Int64 value) override { convertToBoolAndAdd(value); }
    void addUInt64(UInt64 value) override { convertToBoolAndAdd(value); }
    void addFloat32(Float32 value) override { convertToBoolAndAdd(value); }
    void addFloat64(Float64 value) override { convertToBoolAndAdd(value); }

private:
    template<typename T>
    void convertToBoolAndAdd(T value)
    {
        addBoolImpl(value != 0);
    }

    void addBoolImpl(bool b)
    {
        simple_writer.addBool(b);
    }
};



// EnumFieldSetter -----------------------------------------------------------------------------------------------------

class ProtobufWriter::EnumFieldSetter : public FieldSetter
{
public:
    BoolFieldSetter(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* field_)
        : FieldSetter(simple_writer_, field_) {}

    void addString(const StringRef & str) override
    {
        prepareEnumNameToNumberMap();
        auto it = enum_name_to_number_map->find(value);
        if (it == enum_name_to_number_map->end())
            cannotConvertValue("'" + str + "'");
        addEnumImpl(it->second);
    }

    void addInt8(Int8 value) override { convertToEnumAndAdd(value); }
    void addUInt8(UInt8 value) override { convertToEnumAndAdd(value); }
    void addInt16(Int16 value) override { convertToEnumAndAdd(value); }
    void addUInt16(UInt16 value) override { convertToEnumAndAdd(value); }
    void addInt32(Int32 value) override { convertToEnumAndAdd(value); }
    void addUInt32(UInt32 value) override { convertToEnumAndAdd(value); }
    void addInt64(Int64 value) override { convertToEnumAndAdd(value); }
    void addUInt64(UInt64 value) override { convertToEnumAndAdd(value); }

    void prepareEnumMappingInt8(const std::vector<std::pair<String, Int8>> & enum_values) override { prepareEnumMapping(enum_values); }
    void prepareEnumMappingInt16(const std::vector<std::pair<String, Int16>> & enum_values) override { prepareEnumMapping(enum_values); }

    void addEnumInt8(Int8 value) override { addEnumInt16(value); }

    void addEnumInt16(Int16 value) override
    {
        auto it = enum_value_to_number_map->find(value);
        if (it == enum_value_to_number_map->end())
            cannotConvertValue(toString(value));
        addEnumImpl(it->second);
    }

private:
    template<typename T>
    void convertToEnumAndAdd(T value)
    {
        const auto* enum_descriptor = descriptor->enum_type()->FindValueByNumber(numericCast<int>(value));
        if (!enum_descriptor)
            cannotConvertValue(toString(value));
        addEnumImpl(enum_descriptor->number());
    }

    void addEnumImpl(int enum_number)
    {
        simple_writer.addInt32(enum_number);
    }

    void prepareEnumNameToNumberMap()
    {
        if (enum_name_to_number_map.has_value())
            return;
        enum_name_to_number_map.emplace();
        const auto* enum_type = field->enum_type();)
        for (int i = 0; i != enum_type->value_count(); ++i)
        {
            const auto* enum_value = enum_type->value(i);
            enum_name_to_number_map->emplace(enum_value->name(), enum_value->number());
        }
    }

    template<typename T>
    void prepareEnumMapping(const std::vector<std::pair<String, T>> & enum_values)
    {
        if (enum_value_to_number_map.has_value())
            return;
        enum_value_to_number_map.emplace();
        for (const auto & name_value_pair : enum_values)
        {
            Int16 value = name_value_pair.second;
            const auto* enum_descriptor = descriptor->enum_type()->FindValueByName(name_value_pair.first);
            if (enum_descriptor)
                enum_value_to_number_map->emplace(value, enum_descriptor->number());
        }
    }

    std::optional<std::unordered_map<StringRef, int>> enum_name_to_number_map;
    std::optional<std::unordered_map<Int16, int>> enum_value_to_number_map;
};



// ProtobufWriter ------------------------------------------------------------------------------------------------------

ProtobufWriter::ProtobufWriter(WriteBuffer & buf, const google::protobuf::FileDescriptor* file_descriptor)
   : simple_writer(out)
{
    simple_writer.setRequiredFieldNotSetHandler(std::bind(&ProtobufWriter::requiredFieldNotSet, this));
    simple_writer.setPackRepeatedAllowed((file_descriptor->syntax() == FileDescriptor::SYNTAX_PROTO3) ||
                                         file_descriptor->options().packed());
}

ProtobufWriter::~ProtobufWriter()
{
    finishCurrentMessage();
}

void ProtobufWriter::setMessageType(const google::protobuf::Descriptor* descriptor)
{
    assert(fields_in_write_order.empty());
    fields_in_write_order.reserve(descriptor->field_count());
    for (int i = 0; i < descriptor->field_count(); ++i)
        fields_in_write_order.emplace_back(descriptor->field(i));

    std::sort(fields_in_write_order.begin(), fields_in_write_order.end(),
              [](const google::protobuf::FieldDescriptor* left, const google::protobuf::FieldDescriptor* right)
    {
        return left->number() < right->number();
    });

    assert(field_setters.empty());
    field_setters.reserve(fields_in_write_order.size());
    for (int i = 0; i != fields_in_write_order.size(); ++i)
    {
        const auto* field = fields_in_write_order[i];
        std::unique_ptr<FieldSetter> setter;
        switch (field->type())
        {
            case TYPE_INT32:
            case TYPE_SINT32:
            case TYPE_SFIXED32: setter = std::make_unique<NumberFieldSetter<Int32>>(simple_writer, field); break;
            case TYPE_UINT32:
            case TYPE_FIXED32: setter = std::make_unique<NumberFieldSetter<UInt32>>(simple_writer, field); break;
            case TYPE_INT64:
            case TYPE_SFIXED64:
            case TYPE_SINT64: setter = std::make_unique<NumberFieldSetter<Int64>>(simple_writer, field); break;
            case TYPE_UINT64:
            case TYPE_FIXED64: setter = std::make_unique<NumberFieldSetter<UInt64>>(simple_writer, field); break;
            case TYPE_FLOAT: setter = std::make_unique<NumberFieldSetter<float>>(simple_writer, field); break;
            case TYPE_DOUBLE: setter = std::make_unique<NumberFieldSetter<double>>(simple_writer, field); break;
            case TYPE_BOOL: setter = std::make_unique<BoolFieldSetter>(simple_writer, field); break;
            case TYPE_ENUM: setter = std::make_unique<EnumFieldSetter>(simple_writer, field); break;
            case TYPE_STRING: setter = std::make_unique<StringFieldSetter>(simple_writer, field); break;
            case TYPE_BYTES: setter = std::make_unique<BytesFieldSetter>(simple_writer, field); break;
            default: throw Exception(String("Protobuf type ") + field->type_name() + " isn't supported.",
                                     ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
        }
        field_setters.emplace_back(std::move(setter));
    }
}

const std::vector<const google::protobuf::FieldDescriptor*> & ProtobufWriter::fieldsInWriteOrder() const
{
    return fields_in_write_order;
}

void ProtobufWriter::newMessage()
{
    finishCurrentMessage();
    simple_writer.newMessage();
    if (fields_in_write_order.empty())
    {
        current_field_index = -1;
        return;
    }
    current_field_index = 0;
    simple_writer.setCurrentField(currentField()->number(), currentField()->is_repeated());
}

void ProtobufWriter::finishCurrentMessage()
{
    if (current_field_index != -1)
    {
        assert(current_field_index == fields_in_write_order.size() - 1);
        finishCurrentField();
    }
}

const google::protobuf::FieldDescription* ProtobufWriter::currentField() const
{
    return fields_in_write_order[current_field_index];
}

ProtobufWriter::FieldSetter* ProtobufWriter::currentFieldSetter() const
{
    return field_setters[current_field_index];
}

bool ProtobufWriter::nextField()
{
    if (current_field_index >= fields_in_write_order.size() - 1)
        return false;

    finishCurrentField();
    ++current_field_index;
    simple_writer.setCurrentField(currentField()->number(), currentField()->is_repeated());
    return true;
}

void ProtobufWriter::finishCurrentField()
{
    size_t num_values = simple_writer.numValues();
    if (num_values == 0)
    {
        if (currentField()->is_required())
            throw Exception("No data for the required field " + currentField->name(),
                            ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
        else
            currentField()->addDefaultIfDefined();
    }
    else if (num_values > 1 && !currentField()->is_repeated())
    {
        throw Exception("Cannot add more than single value to the non-repeated field " + currentField()->name(),
                        ErrorCodes::PROTOBUF_FIELD_NOT_REPEATED);
    }
}

void ProtobufWriter::addNumber(Int8 value)
{
    currentFieldSetter()->addInt8(value);
}

void ProtobufWriter::addNumber(UInt8 value)
{
    currentFieldSetter()->addUInt8(value);
}

void ProtobufWriter::addNumber(Int16 value)
{
    currentFieldSetter()->addInt16(value);
}

void ProtobufWriter::addNumber(UInt16 value)
{
    currentFieldSetter()->addUInt16(value);
}

void ProtobufWriter::addNumber(Int32 value)
{
    currentFieldSetter()->addInt32(value);
}

void ProtobufWriter::addNumber(UInt32 value)
{
    currentFieldSetter()->addUInt32(value);
}

void ProtobufWriter::addNumber(Int64 value)
{
    currentFieldSetter()->addInt64(value);
}

void ProtobufWriter::addNumber(UInt64 value)
{
    currentFieldSetter()->addUInt64(value);
}

void ProtobufWriter::addNumber(UInt128 value)
{
    currentFieldSetter()->addUInt128(value);
}

void ProtobufWriter::addNumber(Float32 value)
{
    currentFieldSetter()->addFloat32(value);
}

void ProtobufWriter::addNumber(Float64 value)
{
    currentFieldSetter()->addFloat64(value);
}

void ProtobufWriter::addString(const StringRef & str)
{
    currentFieldSetter()->addString(str);
}

void ProtobufWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & enum_values)
{
    currentFieldSetter()->prepareEnumMappingInt8(enum_values);
}

void ProtobufWriter::prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & enum_values)
{
    currentFieldSetter()->prepareEnumMappingInt16(enum_values);
}

void ProtobufWriter::addEnum(Int8 value)
{
    currentFieldSetter()->addEnumInt8(value);
}

void ProtobufWriter::addEnum(Int16 value)
{
    currentFieldSetter()->addEnumInt16(value);
}

void ProtobufWriter::addUUID(const UUID & uuid)
{
    currentFieldSetter()->addUUID(uuid);
}

void ProtobufWriter::addDate(DayNum date)
{
    currentFieldSetter()->addUUID(value);
}

void ProtobufWriter::addDateTime(time_t tm)
{
    currentFieldSetter()->addUUID(value);
}

void ProtobufWriter::addDecimal(Decimal32 decimal, UInt32 scale)
{
    currentFieldSetter()->addDecimal32(decimal, scale);
}

void ProtobufWriter::addDecimal(Decimal128 decimal, UInt32 scale)
{
    currentFieldSetter()->addDecimal64(decimal, scale);
}

void ProtobufWriter::addDecimal(Decimal128 decimal, UInt32 scale)
{
    currentFieldSetter()->addDecimal128(decimal, scale);
}

}
