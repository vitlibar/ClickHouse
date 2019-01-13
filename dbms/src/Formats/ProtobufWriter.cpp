#include <Formats/ProtobufWriter.h>


namespace DB
{

// FieldSetter ---------------------------------------------------------------------------------------------------------

class ProtobufWriter::FieldSetter : private boost::noncopyable
{
public:
    FieldSetter() = default;
    void init(ProtobufSimpleWriter & simple_writer_, const google::protobuf::FieldDescriptor* descriptor_);

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

    virtual void addDefaultIfDefined() = 0;

protected:
    void cannotConvertType(const String & type_name);

    template<typename T>
    void cannotConvertValue(const T & value);

    template<typename To, typename From>
    To numericCast(From value);

    template<typename To>
    To parseFromString(const StringRef& str);

    ProtobufSimpleWriter* simple_writer = nullptr;
    const google::protobuf::FieldDescriptor* descriptor = nullptr;
};


class ProtobufWriter::StringFieldSetter : public FieldSetter
{

};

class SInt32FieldSetter : public NumberFieldSetter<Int32>
{
public:
    void addFieldValue(Int32 value) override { simple_writer.addSInt32(value); }
};

class Int32FieldSetter : public NumberFieldSetter<Int32>
{
public:
    void addFieldValue(Int32 value) override { simple_writer.addInt32(value); }
};

class SFixed32FieldSetter : public NumberFieldSetter<Int32>
{
public:
    void addFieldValue(Int32 value) override { simple_writer.addSFixed32(value); }
};

class FloatFieldSetter : public NumberFieldSetter<Float32>
{
public:
    void addFieldValue(Float32 value) override { simple_writer.addFloat(value); }
};



// ProtobufWriter ------------------------------------------------------------------------------------------------------

ProtobufWriter::ProtobufWriter(WriteBuffer & buf, const google::protobuf::FileDescriptor* file_descriptor)
   : simple_writer(out)
{
    simple_writer.setRequiredFieldNotSetHandler(std::bind(&ProtobufWriter::requiredFieldNotSet, this));
    simple_writer.setPackRepeatedAllowed((file_descriptor->syntax() == FileDescriptor::SYNTAX_PROTO3) ||
                                  file_descriptor->options().packed());
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
            case TYPE_INT32: setter = std::make_unique<Int32FieldSetter>(); break;
            case TYPE_UINT32: setter = std::make_unique<UInt32FieldSetter>(); break;
            case TYPE_SINT32: setter = std::make_unique<SInt32FieldSetter>(); break;
            case TYPE_FIXED32: setter = std::make_unique<Fixed32FieldSetter>(); break;
            case TYPE_SFIXED32: setter = std::make_unique<SFixed32FieldSetter>(); break;
            case TYPE_INT64: setter = std::make_unique<Int64FieldSetter>(); break;
            case TYPE_SINT64: setter = std::make_unique<SInt64FieldSetter>(); break;
            case TYPE_FIXED64: setter = std::make_unique<Fixed64FieldSetter>(); break;
            case TYPE_SFIXED64: setter = std::make_unique<SFixed64FieldSetter>(); break;
            case TYPE_BOOL: setter = std::make_unique<BoolFieldSetter>(); break;
            case TYPE_FLOAT: setter = std::make_unique<FloatFieldSetter>(); break;
            case TYPE_DOUBLE: setter = std::make_unique<DoubleFieldSetter>(); break;
            case TYPE_ENUM: setter = std::make_unique<EnumFieldSetter>(); break;
            case TYPE_STRING: setter = std::make_unique<StringFieldSetter>(); break;
            case TYPE_BYTES: setter = std::make_unique<BytesFieldSetter>(); break;
            default: throw FIELD_NOT_SUPPORTED;
        }
        setter->init(simple_writer, field);
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

void ProtobufWriter::flush()
{
    finishCurrentMessage();
    simple_writer.flush();
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
    if (simple_writer.numValues() == 0)
    {
        if (currentField()->is_required())
            throw REQUIRED_FIELD_NOT_SET
        else
            currentField()->addDefaultIfDefined();
    }
}

void ProtobufWriter::checkCanAddValue()
{
    if ((simple.numValues() > 0) && !currentField()->is_repeated())
        throw TOO_MANY_FOR_NOT_REPEATED;
}

void ProtobufWriter::addNumber(Int8 value)
{
    checkCanAddValue();
    currentFieldSetter()->addInt8(value);
}

void ProtobufWriter::addNumber(UInt8 value)
{
    checkCanAddValue();
    currentFieldSetter()->addUInt8(value);
}

void ProtobufWriter::addNumber(Int16 value)
{
    checkCanAddValue();
    currentFieldSetter()->addInt16(value);
}

void ProtobufWriter::addNumber(UInt16 value)
{
    checkCanAddValue();
    currentFieldSetter()->addUInt16(value);
}

void ProtobufWriter::addNumber(Int32 value)
{
    checkCanAddValue();
    currentFieldSetter()->addInt32(value);
}

void ProtobufWriter::addNumber(UInt32 value)
{
    checkCanAddValue();
    currentFieldSetter()->addUInt32(value);
}

void ProtobufWriter::addNumber(Int64 value)
{
    checkCanAddValue();
    currentFieldSetter()->addInt64(value);
}

void ProtobufWriter::addNumber(UInt64 value)
{
    checkCanAddValue();
    currentFieldSetter()->addUInt64(value);
}

void ProtobufWriter::addNumber(UInt128 value)
{
    checkCanAddValue();
    currentFieldSetter()->addUInt128(value);
}

void ProtobufWriter::addNumber(Float32 value)
{
    checkCanAddValue();
    currentFieldSetter()->addFloat32(value);
}

void ProtobufWriter::addNumber(Float64 value)
{
    checkCanAddValue();
    currentFieldSetter()->addFloat64(value);
}

void ProtobufWriter::addString(const StringRef & str)
{
    checkCanAddValue();
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
    checkCanAddValue();
    currentFieldSetter()->addEnumInt8(value);
}

void ProtobufWriter::addEnum(Int16 value)
{
    checkCanAddValue();
    currentFieldSetter()->addEnumInt16(value);
}

void ProtobufWriter::addUUID(const UUID & uuid)
{
    checkCanAddValue();
    currentFieldSetter()->addUUID(uuid);
}

void ProtobufWriter::addDate(DayNum date)
{
    checkCanAddValue();
    currentFieldSetter()->addUUID(value);
}

void ProtobufWriter::addDateTime(time_t tm)
{
    checkCanAddValue();
    currentFieldSetter()->addUUID(value);
}

void ProtobufWriter::addDecimal(Decimal32 decimal, UInt32 scale)
{
    checkCanAddValue();
    currentFieldSetter()->addDecimal32(decimal, scale);
}

void ProtobufWriter::addDecimal(Decimal128 decimal, UInt32 scale)
{
    checkCanAddValue();
    currentFieldSetter()->addDecimal64(decimal, scale);
}

void ProtobufWriter::addDecimal(Decimal128 decimal, UInt32 scale)
{
    checkCanAddValue();
    currentFieldSetter()->addDecimal128(decimal, scale);
}
