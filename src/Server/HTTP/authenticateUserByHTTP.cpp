#include <Server/HTTP/authenticateUserByHTTP.h>

#include <Access/Authentication.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>

#include <Poco/MemoryStream.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPBasicCredentials.h>

#if USE_SSL
#include <Poco/Net/X509Certificate.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{
    String base64Decode(const String & encoded)
    {
        String decoded;
        Poco::MemoryInputStream istr(encoded.data(), encoded.size());
        Poco::Base64Decoder decoder(istr);
        Poco::StreamCopier::copyToString(decoder, decoded);
        return decoded;
    }

    String base64Encode(const String & decoded)
    {
        std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ostr.exceptions(std::ios::failbit);
        Poco::Base64Encoder encoder(ostr);
        encoder.rdbuf()->setLineLength(0);
        encoder << decoded;
        encoder.close();
        return ostr.str();
    }
}


bool authenticateUserByHTTP(
    const HTTPServerRequest & request,
    const HTMLForm & params,
    HTTPServerResponse & response,
    Session & session,
    std::unique_ptr<Credentials> & request_credentials,
    ContextPtr global_context,
    LoggerPtr log)
{
    /// Get the credentials created by the previous call of authenticateUserByHTTP() while handling the previous HTTP request.
    auto current_credentials = std::move(request_credentials);

    /// The user and password can be passed by headers (similar to X-Auth-*),
    /// which is used by load balancers to pass authentication information.
    std::string user = request.get("X-ClickHouse-User", "");
    std::string password = request.get("X-ClickHouse-Key", "");
    std::string quota_key = request.get("X-ClickHouse-Quota", "");

    /// The header 'X-ClickHouse-SSL-Certificate-Auth: on' enables checking the common name
    /// extracted from the SSL certificate used for this connection instead of checking password.
    bool has_ssl_certificate_auth = (request.get("X-ClickHouse-SSL-Certificate-Auth", "") == "on");
    bool has_auth_headers = !user.empty() || !password.empty() || has_ssl_certificate_auth;

    /// User name and password can be passed using HTTP Basic auth or query parameters
    /// (both methods are insecure).
    bool has_http_credentials = request.hasCredentials();
    bool has_credentials_in_query_params = params.has("user") || params.has("password");

    std::string spnego_challenge;
    std::string certificate_common_name;

    if (has_auth_headers)
    {
        /// It is prohibited to mix different authorization schemes.
        if (has_http_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use SSL certificate authentication and Authorization HTTP header simultaneously");
        if (has_credentials_in_query_params)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use SSL certificate authentication and authentication via parameters simultaneously simultaneously");

        if (has_ssl_certificate_auth)
        {
#if USE_SSL
            if (!password.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                                "Invalid authentication: it is not allowed "
                                "to use SSL certificate authentication and authentication via password simultaneously");

            if (request.havePeerCertificate())
                certificate_common_name = request.peerCertificate().commonName();

            if (certificate_common_name.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                                "Invalid authentication: SSL certificate authentication requires nonempty certificate's Common Name");
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "SSL certificate authentication disabled because ClickHouse was built without SSL library");
#endif
        }
    }
    else if (has_http_credentials)
    {
        /// It is prohibited to mix different authorization schemes.
        if (has_credentials_in_query_params)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Invalid authentication: it is not allowed "
                            "to use Authorization HTTP header and authentication via parameters simultaneously");

        std::string scheme;
        std::string auth_info;
        request.getCredentials(scheme, auth_info);

        if (Poco::icompare(scheme, "Basic") == 0)
        {
            Poco::Net::HTTPBasicCredentials credentials(auth_info);
            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else if (Poco::icompare(scheme, "Negotiate") == 0)
        {
            spnego_challenge = auth_info;

            if (spnego_challenge.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: SPNEGO challenge is empty");
        }
        else
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: '{}' HTTP Authorization scheme is not supported", scheme);
        }
    }
    else
    {
        /// If the user name is not set we assume it's the 'default' user.
        user = params.get("user", "default");
        password = params.get("password", "");
    }

    if (!certificate_common_name.empty())
    {
        if (!current_credentials)
            current_credentials = std::make_unique<SSLCertificateCredentials>(user, certificate_common_name);

        auto * certificate_credentials = dynamic_cast<SSLCertificateCredentials *>(current_credentials.get());
        if (!certificate_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected SSL certificate authorization scheme");
    }
    else if (!spnego_challenge.empty())
    {
        if (!current_credentials)
            current_credentials = global_context->makeGSSAcceptorContext();

        auto * gss_acceptor_context = dynamic_cast<GSSAcceptorContext *>(current_credentials.get());
        if (!gss_acceptor_context)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: unexpected 'Negotiate' HTTP Authorization scheme expected");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        const auto spnego_response = base64Encode(gss_acceptor_context->processToken(base64Decode(spnego_challenge), log));
#pragma clang diagnostic pop

        if (!spnego_response.empty())
            response.set("WWW-Authenticate", "Negotiate " + spnego_response);

        if (!gss_acceptor_context->isFailed() && !gss_acceptor_context->isReady())
        {
            if (spnego_response.empty())
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: 'Negotiate' HTTP Authorization failure");

            response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
            response.send();
            /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
            request_credentials = std::move(current_credentials);
            return false;
        }
    }
    else // I.e., now using user name and password strings ("Basic").
    {
        if (!current_credentials)
            current_credentials = std::make_unique<BasicCredentials>();

        auto * basic_credentials = dynamic_cast<BasicCredentials *>(current_credentials.get());
        if (!basic_credentials)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid authentication: expected 'Basic' HTTP Authorization scheme");

        basic_credentials->setUserName(user);
        basic_credentials->setPassword(password);
    }

    if (params.has("quota_key"))
    {
        if (!quota_key.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Invalid authentication: it is not allowed "
                            "to use quota key as HTTP header and as parameter simultaneously");

        quota_key = params.get("quota_key");
    }

    /// Set client info. It will be used for quota accounting parameters in 'setUser' method.

    session.setHTTPClientInfo(request);
    session.setQuotaClientKey(quota_key);

    /// Extract the last entry from comma separated list of forwarded_for addresses.
    /// Only the last proxy can be trusted (if any).
    String forwarded_address = session.getClientInfo().getLastForwardedFor();
    try
    {
        if (!forwarded_address.empty() && global_context->getConfigRef().getBool("auth_use_forwarded_address", false))
            session.authenticate(*current_credentials, Poco::Net::SocketAddress(forwarded_address, request.clientAddress().port()));
        else
            session.authenticate(*current_credentials, request.clientAddress());
    }
    catch (const Authentication::Require<BasicCredentials> & required_credentials)
    {
        current_credentials = std::make_unique<BasicCredentials>();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Basic");
        else
            response.set("WWW-Authenticate", "Basic realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }
    catch (const Authentication::Require<GSSAcceptorContext> & required_credentials)
    {
        current_credentials = global_context->makeGSSAcceptorContext();

        if (required_credentials.getRealm().empty())
            response.set("WWW-Authenticate", "Negotiate");
        else
            response.set("WWW-Authenticate", "Negotiate realm=\"" + required_credentials.getRealm() + "\"");

        response.setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
        response.send();
        /// Keep the credentials for next HTTP request. A client can handle HTTP_UNAUTHORIZED and send us more credentials with the next HTTP request.
        request_credentials = std::move(current_credentials);
        return false;
    }

    return true;
}

}
