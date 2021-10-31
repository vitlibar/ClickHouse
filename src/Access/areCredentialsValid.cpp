#include <Access/Common/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>
#include <Common/Exception.h>
#include <Poco/SHA1Engine.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    using Digest = Authentication::Digest;
    using Util = Authentication::Util;

    bool checkPasswordPlainText(const String & password, const Digest & password_plaintext)
    {
        return (Util::encodePlainText(password) == password_plaintext);
    }

    bool checkPasswordDoubleSHA1(const std::string_view & password, const Digest & password_double_sha1)
    {
        return (Util::encodeDoubleSHA1(password) == password_double_sha1);
    }

    bool checkPasswordSHA256(const std::string_view & password, const Digest & password_sha256)
    {
        return Util::encodeSHA256(password) == password_sha256;
    }

    bool checkPasswordDoubleSHA1MySQL(const std::string_view & scramble, const std::string_view & scrambled_password, const Digest & password_double_sha1)
    {
        /// scrambled_password = SHA1(password) XOR SHA1(scramble <concat> SHA1(SHA1(password)))

        constexpr size_t scramble_length = 20;
        constexpr size_t sha1_size = Poco::SHA1Engine::DIGEST_SIZE;

        if ((scramble.size() < scramble_length) || (scramble.size() > scramble_length + 1)
            || ((scramble.size() == scramble_length + 1) && (scramble[scramble_length] != 0))
            || (scrambled_password.size() != sha1_size) || (password_double_sha1.size() != sha1_size))
            return false;

        Poco::SHA1Engine engine;
        engine.update(scramble.data(), scramble_length);
        engine.update(password_double_sha1.data(), sha1_size);
        const Poco::SHA1Engine::Digest & digest = engine.digest();

        Poco::SHA1Engine::Digest calculated_password_sha1(sha1_size);
        for (size_t i = 0; i < sha1_size; i++)
            calculated_password_sha1[i] = scrambled_password[i] ^ digest[i];

        auto calculated_password_double_sha1 = Util::encodeSHA1(calculated_password_sha1);
        return calculated_password_double_sha1 == password_double_sha1;
    }

    bool checkPasswordPlainTextMySQL(const std::string_view & scramble, const std::string_view & scrambled_password, const Digest & password_plaintext)
    {
        return checkPasswordDoubleSHA1MySQL(scramble, scrambled_password, Util::encodeDoubleSHA1(password_plaintext));
    }
}


bool areCredentialsValid(const Credentials & credentials, const Authentication & auth, const ExternalAuthenticators & external_authenticators)
{
    if (!credentials.isReady())
        return false;

    if (const auto * gss_acceptor_context = typeid_cast<const GSSAcceptorContext *>(&credentials))
    {
        switch (auth.getType())
        {
            case Authentication::NO_PASSWORD:
            case Authentication::PLAINTEXT_PASSWORD:
            case Authentication::SHA256_PASSWORD:
            case Authentication::DOUBLE_SHA1_PASSWORD:
            case Authentication::LDAP:
                throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");

            case Authentication::KERBEROS:
                return external_authenticators.checkKerberosCredentials(auth.getKerberosRealm(), *gss_acceptor_context);

            case Authentication::MAX_TYPE:
                break;
        }
    }

    if (const auto * mysql_credentials = typeid_cast<const MySQLNative41Credentials *>(&credentials))
    {
        switch (auth.getType())
        {
            case Authentication::NO_PASSWORD:
                return true; // N.B. even if the password is not empty!

            case Authentication::PLAINTEXT_PASSWORD:
                return checkPasswordPlainTextMySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth.getPasswordHashBinary());

            case Authentication::DOUBLE_SHA1_PASSWORD:
                return checkPasswordDoubleSHA1MySQL(mysql_credentials->getScramble(), mysql_credentials->getScrambledPassword(), auth.getPasswordHashBinary());

            case Authentication::SHA256_PASSWORD:
            case Authentication::LDAP:
            case Authentication::KERBEROS:
                throw Authentication::Require<BasicCredentials>("ClickHouse Basic Authentication");

            case Authentication::MAX_TYPE:
                break;
        }
    }

    if (const auto * basic_credentials = typeid_cast<const BasicCredentials *>(&credentials))
    {
        switch (auth.getType())
        {
            case Authentication::NO_PASSWORD:
                return true; // N.B. even if the password is not empty!

            case Authentication::PLAINTEXT_PASSWORD:
                return checkPasswordPlainText(basic_credentials->getPassword(), auth.getPasswordHashBinary());

            case Authentication::SHA256_PASSWORD:
                return checkPasswordSHA256(basic_credentials->getPassword(), auth.getPasswordHashBinary());

            case Authentication::DOUBLE_SHA1_PASSWORD:
                return checkPasswordDoubleSHA1(basic_credentials->getPassword(), auth.getPasswordHashBinary());

            case Authentication::LDAP:
                return external_authenticators.checkLDAPCredentials(auth.getLDAPServerName(), *basic_credentials);

            case Authentication::KERBEROS:
                throw Authentication::Require<GSSAcceptorContext>(auth.getKerberosRealm());

            case Authentication::MAX_TYPE:
                break;
        }
    }

    if ([[maybe_unused]] const auto * always_allow_credentials = typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return true;

    throw Exception("areCredentialsValid(): authentication type " + toString(auth.getType()) + " not supported", ErrorCodes::NOT_IMPLEMENTED);
}

}
