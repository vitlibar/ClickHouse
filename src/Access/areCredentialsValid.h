#pragma once


namespace DB
{
class Credentials;
class Authentication;
class ExternalAuthenticators;

/// Checks the credentials (passwords, readiness, etc.)
bool areCredentialsValid(const Credentials & credentials, const Authentication & auth, const ExternalAuthenticators & external_authenticators);

}
