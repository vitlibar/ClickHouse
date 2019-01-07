#include <Common/Exception.h>
#include <Formats/ProtobufField.h>
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONVERT_TO_PROTOBUF_TYPE;
}


ProtobufField::ProtobufField(const google::protobuf::FieldDescriptor* descriptor_)
    : descriptor(descriptor_) {}

void ProtobufField::SetString(Message & destination, const String & str) const
{
    if (descriptor->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING)
    {
        if (descriptor->is_repeated())
            destination.GetReflection()->AddString(&destination, descriptor, str);
        else
            destination.GetReflection()->SetString(&destination, descriptor, str);
        return;
    }

    if (!google::protobuf::TextFormat::ParseFieldValueFromString(str, descriptor, &destination))
        throw Exception("Cannot convert '" + str + " to protobuf type " + descriptor->type_name(),
                        ErrorCodes::CANNOT_CONVERT_TO_PROTOBUF_TYPE);
}

}
