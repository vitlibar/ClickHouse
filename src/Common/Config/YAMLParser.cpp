#include <Common/config.h>

#if USE_YAML_CPP
#include "YAMLParser.h"

#include <vector>

#include <Poco/DOM/Document.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/AutoPtr.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/DOM/Text.h>
#include <Common/Exception.h>

#include <yaml-cpp/yaml.h>

using namespace Poco::XML;

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_YAML;
}

namespace
{
    /// A prefix symbol in yaml key
    /// We add attributes to nodes by using a prefix symbol in the key part.
    /// Currently we use @ as a prefix symbol. Note, that @ is reserved
    /// by YAML standard, so we need to write a key-value pair like this: "@attribute": attr_value
    const char YAML_ATTRIBUTE_PREFIX = '@';

    /// Version of the internal converter from YAML to XML.
    /// By default the version is 1.
    const std::string_view YAML_PARSER_VERSION = "yaml_parser_version";

    /// Version which switched to using more convenient conversion of a sequence of mappings.
    ///
    /// The following yaml:
    ///
    /// seq:
    ///   - k1: val1
    ///   - k2: val2
    ///
    /// can be converted either to the following xml (if version < 2):
    ///
    /// <seq>
    ///     <k1>val1</k1>
    ///     <k2>val2</k2>
    /// </seq>
    ///
    /// or to the following (if version >= 2):
    ///
    /// <seq>
    ///     <k1>val1</k1>
    /// </seq>
    /// <seq>
    ///     <k2>val2</k2>
    /// </seq>
    const int CLONE_PARENT_XML_NODE_FOR_EACH_MAPPING_IN_SEQUENCE = 2;

    /// Latest supported version of the converter.
    const int LATEST_VERSION = 2;

    Poco::AutoPtr<Poco::XML::Element> cloneXMLNode(const Poco::XML::Element & original_node)
    {
        Poco::AutoPtr<Poco::XML::Element> clone_node = original_node.ownerDocument()->createElement(original_node.nodeName());
        original_node.parentNode()->appendChild(clone_node);
        return clone_node;
    }
}


void YAMLParser::processNode(const YAML::Node & node, Poco::XML::Element & parent_xml_node)
{
    auto * xml_document = parent_xml_node.ownerDocument();
    switch (node.Type())
    {
        case YAML::NodeType::Scalar:
        {
            std::string value = node.as<std::string>();
            Poco::AutoPtr<Poco::XML::Text> xml_value = xml_document->createTextNode(value);
            parent_xml_node.appendChild(xml_value);
            break;
        }

        /// For sequences we repeat the parent xml node. For example,
        /// seq:
        ///     - val1
        ///     - val2
        /// is converted into the following xml:
        /// <seq>val1</seq>
        /// <seq>val2</seq>
        ///
        /// And since version 2 a sequence of mappings is converted in the same way:   
        /// seq:
        ///     - k1: val1
        ///       k2: val2
        ///     - k3: val3
        /// is converted into the following xml:
        /// <seq><k1>val1</k1><k2>val2</k2></seq>
        /// <seq><k3>val3</k3></seq>
        case YAML::NodeType::Sequence:
        {
            size_t i = 0;
            for (auto it = node.begin(); it != node.end(); ++it, ++i)
            {
                const auto & child_node = *it;

                bool need_clone_parent_xml_node = (parent_xml_node.hasChildNodes() && !child_node.IsMap())
                    || ((i > 0) && (version >= CLONE_PARENT_XML_NODE_FOR_EACH_MAPPING_IN_SEQUENCE));

                if (need_clone_parent_xml_node)
                {
                    /// Create a new parent node with same tag for each child node
                    processNode(child_node, *cloneXMLNode(parent_xml_node));
                }
                else
                {
                    /// Map, so don't recreate the parent node but add directly
                    processNode(child_node, parent_xml_node);
                }
            }
            break;
        }

        case YAML::NodeType::Map:
        {
            for (const auto & key_value_pair : node)
            {
                const auto & key_node = key_value_pair.first;
                const auto & value_node = key_value_pair.second;
                std::string key = key_node.as<std::string>();
                bool is_attribute = (key.starts_with(YAML_ATTRIBUTE_PREFIX) && value_node.IsScalar());
                if (is_attribute)
                {
                    /// we use substr(1) here to remove YAML_ATTRIBUTE_PREFIX from key
                    auto attribute_name = key.substr(1);
                    std::string value = value_node.as<std::string>();
                    parent_xml_node.setAttribute(attribute_name, value);
                }
                else if (key == YAML_PARSER_VERSION)
                {
                    /// Get a required version of the converter from YAML.
                    int new_version = value_node.as<int>(INT_MAX); /// INT_MAX is a fallback for non-integer
                    if (new_version == INT_MAX)
                        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "{} specifies wrong version, must be an integer in range 1..{}", YAML_PARSER_VERSION, LATEST_VERSION);
                    if (new_version < 1)
                        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "{} specifies unsupported version {}, must be >= 1", YAML_PARSER_VERSION, new_version);
                    if (new_version > LATEST_VERSION)
                        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "{} specifies unsupported version {}, must be <= {}", YAML_PARSER_VERSION, new_version, LATEST_VERSION);
                    version = new_version;
                }
                else
                {
                    Poco::AutoPtr<Poco::XML::Element> xml_key = xml_document->createElement(key);
                    parent_xml_node.appendChild(xml_key);
                    processNode(value_node, *xml_key);
                }
            }
            break;
        }

        case YAML::NodeType::Null: break;
        case YAML::NodeType::Undefined:
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "YAMLParser has encountered node with undefined type and cannot continue parsing of the file");
        }
    }
}

Poco::AutoPtr<Poco::XML::Document> YAMLParser::parse(const String& path)
{
    YAML::Node node_yml;
    try
    {
        node_yml = YAML::LoadFile(path);
    }
    catch (const YAML::ParserException& e)
    {
        /// yaml-cpp cannot parse the file because its contents are incorrect
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "Unable to parse YAML configuration file {}, {}", path, e.what());
    }
    catch (const YAML::BadFile&)
    {
        /// yaml-cpp cannot open the file even though it exists
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open YAML configuration file {}", path);
    }
    Poco::AutoPtr<Poco::XML::Document> xml = new Document;
    Poco::AutoPtr<Poco::XML::Element> root_node = xml->createElement("clickhouse");
    xml->appendChild(root_node);
    try
    {
        processNode(node_yml, *root_node);
    }
    catch (const YAML::TypedBadConversion<std::string>&)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_YAML, "YAMLParser has encountered node with key or value which cannot be represented as string and cannot continue parsing of the file");
    }
    return xml;
}

}
#endif
