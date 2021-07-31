#include <Interpreters/Session.h>

#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/Credentials.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Interpreters/Context.h>

#include <cassert>

#include <chrono>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <deque>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SESSION_NOT_FOUND;
    extern const int SESSION_IS_LOCKED;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    void adjustClientInfoIfInitialQuery(ClientInfo & client_info)
    {
        if (client_info.query_kind == ClientInfo::QueryKind::NO_QUERY)
            client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;

        if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            client_info.initial_user = client_info.current_user;
            client_info.initial_address = client_info.current_address;
            client_info.initial_query_id = client_info.current_query_id;
        }
    }
}

class NamedSessionsStorage;

/// User name and session identifier. Named sessions are local to users.
using NamedSessionKey = std::pair<String, String>;

/// Named sessions. The user could specify session identifier to reuse settings and temporary tables in subsequent requests.
struct NamedSessionData
{
    NamedSessionKey key;
    UInt64 close_cycle = 0;
    ContextMutablePtr context;
    std::chrono::steady_clock::duration timeout;
    NamedSessionsStorage & parent;

    NamedSessionData(NamedSessionKey key_, ContextPtr context_, std::chrono::steady_clock::duration timeout_, NamedSessionsStorage & parent_)
        : key(std::move(key_)), context(Context::createCopy(context_)), timeout(timeout_), parent(parent_)
    {}

    void release();
};

class NamedSessionsStorage
{
public:
    using Key = NamedSessionKey;

    ~NamedSessionsStorage()
    {
        try
        {
            {
                std::lock_guard lock{mutex};
                quit = true;
            }

            cond.notify_one();
            thread.join();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    /// Find existing session or create a new.
    std::shared_ptr<NamedSessionData> acquireSession(
        const String & session_id,
        ContextMutablePtr context,
        std::chrono::steady_clock::duration timeout,
        bool throw_if_not_found)
    {
        std::unique_lock lock(mutex);

        const auto & client_info = context->getClientInfo();
        const auto & user_name = client_info.current_user;

        if (user_name.empty())
            throw Exception("Empty user name.", ErrorCodes::LOGICAL_ERROR);

        Key key(user_name, session_id);

        auto it = sessions.find(key);
        if (it == sessions.end())
        {
            if (throw_if_not_found)
                throw Exception("Session not found.", ErrorCodes::SESSION_NOT_FOUND);

            /// Create a new session from current context.
            it = sessions.insert(std::make_pair(key, std::make_shared<NamedSessionData>(key, context, timeout, *this))).first;
        }
        else if (it->second->key.first != client_info.current_user)
        {
            throw Exception("Session belongs to a different user", ErrorCodes::SESSION_IS_LOCKED);
        }

        /// Use existing session.
        const auto & session = it->second;

        if (!session.unique())
            throw Exception("Session is locked by a concurrent client.", ErrorCodes::SESSION_IS_LOCKED);

        session->context->getClientInfo() = client_info;

        return session;
    }

    void releaseSession(NamedSessionData & session)
    {
        std::unique_lock lock(mutex);
        scheduleCloseSession(session, lock);
    }

private:
    class SessionKeyHash
    {
    public:
        size_t operator()(const Key & key) const
        {
            SipHash hash;
            hash.update(key.first);
            hash.update(key.second);
            return hash.get64();
        }
    };

    /// TODO it's very complicated. Make simple std::map with time_t or boost::multi_index.
    using Container = std::unordered_map<Key, std::shared_ptr<NamedSessionData>, SessionKeyHash>;
    using CloseTimes = std::deque<std::vector<Key>>;
    Container sessions;
    CloseTimes close_times;
    std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();
    UInt64 close_cycle = 0;

    void scheduleCloseSession(NamedSessionData & session, std::unique_lock<std::mutex> &)
    {
        /// Push it on a queue of sessions to close, on a position corresponding to the timeout.
        /// (timeout is measured from current moment of time)

        const UInt64 close_index = session.timeout / close_interval + 1;
        const auto new_close_cycle = close_cycle + close_index;

        if (session.close_cycle != new_close_cycle)
        {
            session.close_cycle = new_close_cycle;
            if (close_times.size() < close_index + 1)
                close_times.resize(close_index + 1);
            close_times[close_index].emplace_back(session.key);
        }
    }

    void cleanThread()
    {
        setThreadName("SessionCleaner");
        std::unique_lock lock{mutex};

        while (true)
        {
            auto interval = closeSessions(lock);

            if (cond.wait_for(lock, interval, [this]() -> bool { return quit; }))
                break;
        }
    }

    /// Close sessions, that has been expired. Returns how long to wait for next session to be expired, if no new sessions will be added.
    std::chrono::steady_clock::duration closeSessions(std::unique_lock<std::mutex> & lock)
    {
        const auto now = std::chrono::steady_clock::now();

        /// The time to close the next session did not come
        if (now < close_cycle_time)
            return close_cycle_time - now;  /// Will sleep until it comes.

        const auto current_cycle = close_cycle;

        ++close_cycle;
        close_cycle_time = now + close_interval;

        if (close_times.empty())
            return close_interval;

        auto & sessions_to_close = close_times.front();

        for (const auto & key : sessions_to_close)
        {
            const auto session = sessions.find(key);

            if (session != sessions.end() && session->second->close_cycle <= current_cycle)
            {
                if (!session->second.unique())
                {
                    /// Skip but move it to close on the next cycle.
                    session->second->timeout = std::chrono::steady_clock::duration{0};
                    scheduleCloseSession(*session->second, lock);
                }
                else
                    sessions.erase(session);
            }
        }

        close_times.pop_front();
        return close_interval;
    }

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread{&NamedSessionsStorage::cleanThread, this};
};


void NamedSessionData::release()
{
    parent.releaseSession(*this);
}

std::optional<NamedSessionsStorage> Session::named_sessions = std::nullopt;

void Session::startupNamedSessions()
{
    named_sessions.emplace();
}

Session::Session(const ContextPtr & global_context_, ClientInfo::Interface interface_)
    : global_context(global_context_)
{
    client_info.interface = interface_;
}

Session::Session(Session &&) = default;

Session::~Session()
{
    session_context.reset();
    releaseNamedSession();
}

void Session::setUser(const Credentials & credentials_, const Poco::Net::SocketAddress & address_)
{
    if (session_context)
        throw Exception("Must not be called after making session context", ErrorCodes::LOGICAL_ERROR);

    client_info.current_user = credentials_.getUserName();
    client_info.current_address = address_;

#if defined(ARCADIA_BUILD)
    /// This is harmful field that is used only in foreign "Arcadia" build.
    if (const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials_))
        client_info.current_password = basic_credentials->getPassword();
#endif

    user_id = global_context->getAccessControlManager().login(credentials_, client_info.current_address.host());
}

void Session::setUser(const String & user_name_, const String & password_, const Poco::Net::SocketAddress & address_)
{
    setUser(BasicCredentials(user_name_, password_), address_);
}

Authentication Session::getUserAuthentication(const String & user_name_) const
{
    return global_context->getAccessControlManager().read<User>(user_name_)->authentication;
}

ContextMutablePtr Session::makeSessionContext()
{
    if (session_context)
        throw Exception("Session context already exists", ErrorCodes::LOGICAL_ERROR);

    session_context = Context::createCopy(global_context);
    session_context->makeSessionContext();
    session_context->getClientInfo() = client_info;

    if (user_id)
    {
        session_context->setUser(*user_id);
        user = session_context->getUser();
    }

    adjustClientInfoIfInitialQuery(session_context->getClientInfo());

    return session_context;
}

ContextMutablePtr Session::makeSessionContext(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check)
{
    if (session_context)
        return session_context;
    // TODO: Fix excessive copying of context and calling setUser() for a session context which will be replaced.
    makeSessionContext();
    promoteToNamedSession(session_id, timeout, session_check);
    return session_context;
}

ContextMutablePtr Session::makeQueryContext() const
{
    ContextMutablePtr query_context = Context::createCopy(session_context ? session_context : global_context);
    query_context->makeQueryContext();
    auto & query_client_info = query_context->getClientInfo();

    if (session_context)
    {
        query_client_info.client_trace_context = client_info.client_trace_context;
    }
    else
    {
        query_client_info = client_info;
        if (user_id)
        {
            query_context->setUser(*user_id);
            user = query_context->getUser();
        }
    }

    query_context->setCurrentQueryId(client_info.current_query_id);

    adjustClientInfoIfInitialQuery(query_context->getClientInfo());

    return query_context;
}

void Session::promoteToNamedSession(const String & session_id, std::chrono::steady_clock::duration timeout, bool session_check)
{
    if (!named_sessions)
        throw Exception("Support for named sessions is not enabled", ErrorCodes::NOT_IMPLEMENTED);

    auto new_named_session = named_sessions->acquireSession(session_id, session_context, timeout, session_check);

    // Must retain previous client info cause otherwise source client address and port,
    // and other stuff are reused from previous user of the said session.
    const ClientInfo prev_client_info = session_context->getClientInfo();

    session_context = new_named_session->context;
    session_context->getClientInfo() = prev_client_info;
    session_context->makeSessionContext();

    named_session.swap(new_named_session);
}

/// Early release a NamedSessionData.
void Session::releaseNamedSession()
{
    if (named_session)
    {
        named_session->release();
        named_session.reset();
    }
}

}
