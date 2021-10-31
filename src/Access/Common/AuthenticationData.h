#pragma once

#include <Common/Exception.h>
#include <Common/OpenSSLHelpers.h>
#include <Poco/SHA1Engine.h>
#include <base/types.h>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


enum class AuthenticationType
{
    /// User doesn't have to enter password.
    NO_PASSWORD,

    /// Password is stored as is.
    PLAINTEXT_PASSWORD,

    /// Password is encrypted in SHA256 hash.
    SHA256_PASSWORD,

    /// SHA1(SHA1(password)).
    /// This kind of hash is used by the `mysql_native_password` authentication plugin.
    DOUBLE_SHA1_PASSWORD,

    /// Password is checked by a [remote] LDAP server. Connection will be made at each authentication attempt.
    LDAP,

    /// Kerberos authentication performed through GSS-API negotiation loop.
    KERBEROS,

    MAX_TYPE,
};

struct AuthenticationTypeInfo
{
    const char * const raw_name;
    const String name; /// Lowercased with underscores, e.g. "sha256_password".
    static const AuthenticationTypeInfo & get(AuthenticationType type_);
};


/// Stores data for checking password when a user logins.
class AuthenticationData
{
public:
    using Digest = std::vector<uint8_t>;

    AuthenticationData(AuthenticationType type_ = AuthenticationType::NO_PASSWORD) : type(type_) {}
    AuthenticationData(const AuthenticationData & src) = default;
    AuthenticationData & operator =(const AuthenticationData & src) = default;
    AuthenticationData(AuthenticationData && src) = default;
    AuthenticationData & operator =(AuthenticationData && src) = default;

    AuthenticationType getType() const { return type; }

    /// Sets the password and encrypt it using the authentication type set in the constructor.
    void setPassword(const String & password_);

    /// Returns the password. Allowed to use only for Type::PLAINTEXT_PASSWORD.
    String getPassword() const;

    /// Sets the password as a string of hexadecimal digits.
    void setPasswordHashHex(const String & hash);
    String getPasswordHashHex() const;

    /// Sets the password in binary form.
    void setPasswordHashBinary(const Digest & hash);
    const Digest & getPasswordHashBinary() const { return password_hash; }

    /// Sets the server name for authentication type LDAP.
    const String & getLDAPServerName() const;
    void setLDAPServerName(const String & name);

    /// Sets the realm name for authentication type KERBEROS.
    const String & getKerberosRealm() const;
    void setKerberosRealm(const String & realm);

    friend bool operator ==(const AuthenticationData & lhs, const AuthenticationData & rhs) { return (lhs.type == rhs.type) && (lhs.password_hash == rhs.password_hash); }
    friend bool operator !=(const AuthenticationData & lhs, const AuthenticationData & rhs) { return !(lhs == rhs); }

    struct Util
    {
        static Digest encodePlainText(const std::string_view & text) { return Digest(text.data(), text.data() + text.size()); }
        static Digest encodeSHA256(const std::string_view & text);
        static Digest encodeSHA1(const std::string_view & text);
        static Digest encodeSHA1(const Digest & text) { return encodeSHA1(std::string_view{reinterpret_cast<const char *>(text.data()), text.size()}); }
        static Digest encodeDoubleSHA1(const std::string_view & text) { return encodeSHA1(encodeSHA1(text)); }
        static Digest encodeDoubleSHA1(const Digest & text) { return encodeSHA1(encodeSHA1(text)); }
    };

private:
    AuthenticationType type = AuthenticationType::NO_PASSWORD;
    Digest password_hash;
    String ldap_server_name;
    String kerberos_realm;
};


inline const AuthenticationTypeInfo & AuthenticationTypeInfo::get(AuthenticationType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        return AuthenticationTypeInfo{raw_name_, std::move(init_name)};
    };

    switch (type_)
    {
        case AuthenticationType::NO_PASSWORD:
        {
            static const auto info = make_info("NO_PASSWORD");
            return info;
        }
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            static const auto info = make_info("PLAINTEXT_PASSWORD");
            return info;
        }
        case AuthenticationType::SHA256_PASSWORD:
        {
            static const auto info = make_info("SHA256_PASSWORD");
            return info;
        }
        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            static const auto info = make_info("DOUBLE_SHA1_PASSWORD");
            return info;
        }
        case AuthenticationType::LDAP:
        {
            static const auto info = make_info("LDAP");
            return info;
        }
        case AuthenticationType::KERBEROS:
        {
            static const auto info = make_info("KERBEROS");
            return info;
        }
        case AuthenticationType::MAX_TYPE:
            break;
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type_)), ErrorCodes::LOGICAL_ERROR);
}

inline String toString(AuthenticationType type_)
{
    return AuthenticationTypeInfo::get(type_).raw_name;
}


inline AuthenticationData::Digest AuthenticationData::Util::encodeSHA256(const std::string_view & text [[maybe_unused]])
{
#if USE_SSL
    Digest hash;
    hash.resize(32);
    ::DB::encodeSHA256(text, hash.data());
    return hash;
#else
    throw DB::Exception(
        "SHA256 passwords support is disabled, because ClickHouse was built without SSL library",
        DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

inline AuthenticationData::Digest AuthenticationData::Util::encodeSHA1(const std::string_view & text)
{
    Poco::SHA1Engine engine;
    engine.update(text.data(), text.size());
    return engine.digest();
}


inline void AuthenticationData::setPassword(const String & password_)
{
    switch (type)
    {
        case AuthenticationType::PLAINTEXT_PASSWORD:
            return setPasswordHashBinary(Util::encodePlainText(password_));

        case AuthenticationType::SHA256_PASSWORD:
            return setPasswordHashBinary(Util::encodeSHA256(password_));

        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
            return setPasswordHashBinary(Util::encodeDoubleSHA1(password_));

        case AuthenticationType::NO_PASSWORD:
        case AuthenticationType::LDAP:
        case AuthenticationType::KERBEROS:
            throw Exception("Cannot specify password for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case AuthenticationType::MAX_TYPE:
            break;
    }
    throw Exception("setPassword(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}


inline String AuthenticationData::getPassword() const
{
    if (type != AuthenticationType::PLAINTEXT_PASSWORD)
        throw Exception("Cannot decode the password", ErrorCodes::LOGICAL_ERROR);
    return String(password_hash.data(), password_hash.data() + password_hash.size());
}


inline void AuthenticationData::setPasswordHashHex(const String & hash)
{
    Digest digest;
    digest.resize(hash.size() / 2);
    boost::algorithm::unhex(hash.begin(), hash.end(), digest.data());
    setPasswordHashBinary(digest);
}

inline String AuthenticationData::getPasswordHashHex() const
{
    if (type == AuthenticationType::LDAP || type == AuthenticationType::KERBEROS)
        throw Exception("Cannot get password hex hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

    String hex;
    hex.resize(password_hash.size() * 2);
    boost::algorithm::hex(password_hash.begin(), password_hash.end(), hex.data());
    return hex;
}


inline void AuthenticationData::setPasswordHashBinary(const Digest & hash)
{
    switch (type)
    {
        case AuthenticationType::PLAINTEXT_PASSWORD:
        {
            password_hash = hash;
            return;
        }

        case AuthenticationType::SHA256_PASSWORD:
        {
            if (hash.size() != 32)
                throw Exception(
                    "Password hash for the 'SHA256_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 32 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case AuthenticationType::DOUBLE_SHA1_PASSWORD:
        {
            if (hash.size() != 20)
                throw Exception(
                    "Password hash for the 'DOUBLE_SHA1_PASSWORD' authentication type has length " + std::to_string(hash.size())
                        + " but must be exactly 20 bytes.",
                    ErrorCodes::BAD_ARGUMENTS);
            password_hash = hash;
            return;
        }

        case AuthenticationType::NO_PASSWORD:
        case AuthenticationType::LDAP:
        case AuthenticationType::KERBEROS:
            throw Exception("Cannot specify password binary hash for authentication type " + toString(type), ErrorCodes::LOGICAL_ERROR);

        case AuthenticationType::MAX_TYPE:
            break;
    }
    throw Exception("setPasswordHashBinary(): authentication type " + toString(type) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

inline const String & AuthenticationData::getLDAPServerName() const
{
    return ldap_server_name;
}

inline void AuthenticationData::setLDAPServerName(const String & name)
{
    ldap_server_name = name;
}

inline const String & AuthenticationData::getKerberosRealm() const
{
    return kerberos_realm;
}

inline void AuthenticationData::setKerberosRealm(const String & realm)
{
    kerberos_realm = realm;
}

}
