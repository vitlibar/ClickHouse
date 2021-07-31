#pragma once

#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context_fwd.h>

#include <chrono>
#include <memory>
#include <optional>

namespace Poco::Net { class SocketAddress; }

namespace DB
{
class Credentials;
class Authentication;
struct NamedSessionData;
class NamedSessionsStorage;
class User;
using UserPtr = std::shared_ptr<const User>;

/** Represents user-session from the server perspective,
 *  basically it is just a smaller subset of Context API, simplifies Context management.
 *
 * Holds session context, facilitates acquisition of NamedSession and proper creation of query contexts.
 * Adds log in, log out and login failure events to the SessionLog.
 */
class Session
{
    static std::optional<NamedSessionsStorage> named_sessions;

public:
    /// Allow to use named sessions. The thread will be run to cleanup sessions after timeout has expired.
    /// The method must be called at the server startup.
    static void startupNamedSessions();

    Session(const ContextPtr & global_context_, ClientInfo::Interface interface_);
    Session(Session &&);
    ~Session();

    Session(const Session &) = delete;
    Session& operator=(const Session &) = delete;

    void setUser(const Credentials & credentials_, const Poco::Net::SocketAddress & address_);
    void setUser(const String & user_name_, const String & password_, const Poco::Net::SocketAddress & address_);
    Authentication getUserAuthentication(const String & user_name_) const;

    ClientInfo & getClientInfo() { return client_info; }

    ContextMutablePtr makeSessionContext();
    ContextMutablePtr makeSessionContext(const String & session_id_, std::chrono::steady_clock::duration timeout_, bool session_check_);
    ContextMutablePtr sessionContext() { return session_context; }
    ContextPtr sessionContext() const { return session_context; }

    ContextMutablePtr makeQueryContext() const;

private:
    friend void performLoginActions(Session & session, std::function<void(bool &)> func);

    void onLogOut();

    void initContextWithUserInfo(ContextMutablePtr context) const;

    /** Promotes current session to a named session.
     *
     *  that is: re-uses or creates NamedSession and then piggybacks on it's context,
     *  retaining ClientInfo of current session_context.
     *  Acquired named_session is then released in the destructor.
     */
    void promoteToNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check);

    /// Early release a NamedSession.
    void releaseNamedSession();

    const ContextPtr global_context;

    /// interface, current_user, current_password, quota_key, current_address, forwarded_for, http_method,
    /// current_query_id, client_trace_context
    ClientInfo client_info;

    mutable UserPtr user;
    std::optional<UUID> user_id;

    String session_id;
    ContextMutablePtr session_context;
    std::shared_ptr<NamedSessionData> named_session;
};

}
