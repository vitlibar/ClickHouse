#include "GRPCServer.h"
#if USE_GRPC

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/SettingsChanges.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/executeQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Server/IServer.h>
#include <Storages/IStorage.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <grpc++/server_builder.h>


using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
using GRPCQueryInfo = clickhouse::grpc::QueryInfo;
using GRPCResult = clickhouse::grpc::Result;
using GRPCException = clickhouse::grpc::Exception;
using GRPCProgress = clickhouse::grpc::Progress;

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
    extern const int NO_DATA_TO_INSERT;
    extern const int NETWORK_ERROR;
    extern const int INVALID_SESSION_TIMEOUT;
}


namespace
{
    std::chrono::steady_clock::duration getSessionTimeout(const GRPCQueryInfo & query_info, const Poco::Util::AbstractConfiguration & config)
    {
        auto session_timeout = query_info.session_timeout();
        if (session_timeout)
        {
            auto max_session_timeout = config.getUInt("max_session_timeout", 3600);
            if (session_timeout > max_session_timeout)
                throw Exception(
                    "Session timeout '" + std::to_string(session_timeout) + "' is larger than max_session_timeout: "
                        + std::to_string(max_session_timeout) + ". Maximum session timeout could be modified in configuration file.",
                    ErrorCodes::INVALID_SESSION_TIMEOUT);
        }
        else
            session_timeout = config.getInt("default_session_timeout", 60);
        return std::chrono::seconds(session_timeout);
    }

    /// Requests a connection and provides low-level interface for reading and writing.
    class Responder
    {
    public:
        Responder(
            GRPCService & grpc_service_, grpc::ServerCompletionQueue & new_call_queue_, grpc::ServerCompletionQueue & notification_queue_)
            : tag(this)
        {
            grpc_service_.RequestExecuteQuery(&grpc_context, &reader_writer, &new_call_queue_, &notification_queue_, tag);
        }

        void setTag(void * tag_) { tag = tag_; }

        void read(GRPCQueryInfo & query_info_)
        {
            reader_writer.Read(&query_info_, tag);
        }

        void write(const GRPCResult & result_)
        {
            reader_writer.Write(result_, tag);
        }

        void writeAndFinish(const GRPCResult & result_, const grpc::Status & status_)
        {
            reader_writer.WriteAndFinish(result_, {}, status_, tag);
        }

        Poco::Net::SocketAddress getClientAddress() const
        {
            String peer = grpc_context.peer();
            return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)};
        }

    private:
        grpc::ServerContext grpc_context;
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
        void * tag;
    };


    /// Handles a connection after a responder is ready.
    class Call : public std::enable_shared_from_this<Call>
    {
    public:
        Call(
            IServer & iserver_,
            Poco::Logger * log_,
            std::unique_ptr<Responder> responder_,
            const std::function<void(Call &)> & on_finish_callback_);
        ~Call();

        std::shared_ptr<Call> getPtr() { return shared_from_this(); }

        void sync(bool ok);

    private:
        void run();
        void waitForSync();

        void receiveQuery();
        void executeQuery();
        void processInsertQuery();
        void processOrdinaryQuery();
        void processOrdinaryQueryWithProcessors();
        void finishQuery();
        void onException(const Exception & exception);
        void onFatalError();
        void close();

        void addOutputToResult(const Block & block);
        void addProgressToResult();
        void addTotalsToResult(const Block & totals);
        void addExtremesToResult(const Block & extremes);
        void addLogsToResult();
        void sendResult();
        void sendFinalResult();
        void sendException(const Exception & exception);

        IServer & iserver;
        Poco::Logger * log = nullptr;
        InternalTextLogsQueuePtr logs_queue;
        std::unique_ptr<Responder> responder;
        std::function<void(Call &)> on_finish_callback;

        ThreadFromGlobalPool runner;
        std::condition_variable signal;
        std::atomic<size_t> num_syncs_pending = 0;
        std::atomic<bool> sync_failed = false;

        GRPCQueryInfo query_info;
        GRPCResult result;

        std::shared_ptr<NamedSession> session;
        std::optional<Context> query_context;
        std::optional<CurrentThread::QueryScope> query_scope;
        ASTPtr ast;
        String input_format;
        String output_format;
        uint64_t interactive_delay;
        bool send_exception_with_stacktrace = false;

        BlockIO io;
        Progress progress;
    };

    Call::Call(
        IServer & iserver_,
        Poco::Logger * log_,
        std::unique_ptr<Responder> responder_,
        const std::function<void(Call &)> & on_finish_callback_)
        : iserver(iserver_), log(log_), responder(std::move(responder_)), on_finish_callback(on_finish_callback_)
    {
        responder->setTag(this);

        auto runner_function = [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException("GRPCServer");
            }
            on_finish_callback(*this);
        };
        runner = ThreadFromGlobalPool(runner_function);
    }

    Call::~Call()
    {
        if (runner.joinable())
            runner.join();
    }

    void Call::sync(bool ok)
    {
        ++num_syncs_pending;
        if (!ok)
            sync_failed = true;
        signal.notify_one();
    }

    void Call::waitForSync()
    {
        std::mutex mutex;
        std::unique_lock lock{mutex};
        signal.wait(lock, [&] { return (num_syncs_pending > 0) || sync_failed; });
        if (sync_failed)
            throw Exception("Client has gone away or network failure", ErrorCodes::NETWORK_ERROR);
        --num_syncs_pending;
    }

    void Call::run()
    {
        try
        {
            receiveQuery();
            executeQuery();

            bool need_receive_data_for_insert = (io.out != nullptr);

            /// Does the request require receive data from client?
            if (need_receive_data_for_insert)
                processInsertQuery();
            else
                processOrdinaryQuery();

            finishQuery();
        }
        catch (Exception & exception)
        {
            onException(exception);
        }
        catch (Poco::Exception & exception)
        {
            onException(Exception{Exception::CreateFromPocoTag{}, exception});
        }
        catch (std::exception & exception)
        {
            onException(Exception{Exception::CreateFromSTDTag{}, exception});
        }
    }

    void Call::receiveQuery()
    {
        responder->read(query_info);
        waitForSync();
    }

    void Call::executeQuery()
    {
        /// Retrieve user credentials.
        std::string user = query_info.user_name();
        std::string password = query_info.password();
        std::string quota_key = query_info.quota();
        Poco::Net::SocketAddress user_address = responder->getClientAddress();

        if (user.empty())
        {
            user = "default";
            password = "";
        }

        /// Create context.
        query_context.emplace(iserver.context());
        query_scope.emplace(*query_context);

        /// Authentication.
        query_context->setUser(user, password, user_address);
        query_context->setCurrentQueryId(query_info.query_id());
        if (!quota_key.empty())
            query_context->setQuotaKey(quota_key);

        /// The user could specify session identifier and session timeout.
        /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.
        if (!query_info.session_id().empty())
        {
            session = query_context->acquireNamedSession(
                query_info.session_id(), getSessionTimeout(query_info, iserver.config()), query_info.session_check());
            query_context = session->context;
            query_context->setSessionContext(session->context);
        }

        /// Set client info.
        ClientInfo & client_info = query_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        client_info.interface = ClientInfo::Interface::GRPC;
        client_info.initial_user = client_info.current_user;
        client_info.initial_query_id = client_info.current_query_id;
        client_info.initial_address = client_info.current_address;

        /// Prepare settings.
        SettingsChanges settings_changes;
        for (const auto & [key, value] : query_info.settings())
        {
            settings_changes.push_back({key, value});
        }
        query_context->checkSettingsConstraints(settings_changes);
        query_context->applySettingsChanges(settings_changes);
        const Settings & settings = query_context->getSettingsRef();

        /// Prepare for sending exceptions and logs.
        send_exception_with_stacktrace = query_context->getSettingsRef().calculate_text_stack_trace;
        const auto client_logs_level = query_context->getSettingsRef().send_logs_level;
        if (client_logs_level != LogsLevel::none)
        {
            logs_queue = std::make_shared<InternalTextLogsQueue>();
            logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
            CurrentThread::attachInternalTextLogsQueue(logs_queue, client_logs_level);
            CurrentThread::setFatalErrorCallback([this]{ onFatalError(); });
        }

        /// Set the current database if specified.
        if (!query_info.database().empty())
        {
            if (!DatabaseCatalog::instance().isDatabaseExist(query_info.database()))
            {
                Exception e("Database " + query_info.database() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            }
            query_context->setCurrentDatabase(query_info.database());
        }

        /// The interactive delay will be used to show progress.
        interactive_delay = query_context->getSettingsRef().interactive_delay;
        query_context->setProgressCallback([this](const Progress & value) { return progress.incrementPiecewiseAtomically(value); });

        /// Parse the query.
        const char * begin = query_info.query().data();
        const char * end = begin + query_info.query().size();
        ParserQuery parser(end, settings.enable_debug_queries);
        ast = ::DB::parseQuery(parser, begin, end, "", settings.max_query_size, settings.max_parser_depth);

        /// Choose output format.
        query_context->setDefaultFormat(query_info.format());
        if (const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
            ast_query_with_output && ast_query_with_output->format)
        {
            output_format = getIdentifierName(ast_query_with_output->format);
        }
        if (output_format.empty())
            output_format = query_context->getDefaultFormat();

        /// Start executing the query.
        auto * insert_query = ast->as<ASTInsertQuery>();
        const auto * query_end = end;
        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
        }
        String query(begin, query_end);
        io = ::DB::executeQuery(query, *query_context, false, QueryProcessingStage::Complete, true, true);
    }

    void Call::processInsertQuery()
    {
        auto * insert_query = ast->as<ASTInsertQuery>();
        if (!insert_query)
            throw Exception("Query requires data to insert, but it is not an INSERT query", ErrorCodes::NO_DATA_TO_INSERT);

        if (!insert_query->data && query_info.input_data().empty() && !query_info.use_next_input_data())
            throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);

        /// Choose input format.
        input_format = insert_query->format;
        if (input_format.empty())
            input_format = "Values";

        /// Prepare read buffer with data to insert.
        ConcatReadBuffer::ReadBuffers buffers;
        std::shared_ptr<ReadBufferFromMemory> insert_query_data_buffer;
        std::shared_ptr<ReadBufferFromMemory> input_data_buffer;
        if (insert_query->data)
        {
            insert_query_data_buffer = std::make_shared<ReadBufferFromMemory>(insert_query->data, insert_query->end - insert_query->data);
            buffers.push_back(insert_query_data_buffer.get());
        }
        if (!query_info.input_data().empty())
        {
            input_data_buffer = std::make_shared<ReadBufferFromMemory>(query_info.input_data().data(), query_info.input_data().size());
            buffers.push_back(input_data_buffer.get());
        }
        auto input_buffer_contacenated = std::make_unique<ConcatReadBuffer>(buffers);
        auto res_stream = query_context->getInputFormat(
            input_format, *input_buffer_contacenated, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

        /// Add default values if necessary.
        auto table_id = query_context->resolveStorageID(insert_query->table_id, Context::ResolveOrdinary);
        if (query_context->getSettingsRef().input_format_defaults_for_omitted_fields && table_id)
        {
            StoragePtr storage = DatabaseCatalog::instance().getTable(table_id, *query_context);
            const auto & columns = storage->getInMemoryMetadataPtr()->getColumns();
            if (!columns.empty())
                res_stream = std::make_shared<AddingDefaultsBlockInputStream>(res_stream, columns, *query_context);
        }

        /// Read input data.
        io.out->writePrefix();

        while (auto block = res_stream->read())
            io.out->write(block);

        while (query_info.use_next_input_data())
        {
            responder->read(query_info);
            waitForSync();
            if (!query_info.input_data().empty())
            {
                const char * begin = query_info.input_data().data();
                const char * end = begin + query_info.input_data().size();
                ReadBufferFromMemory data_in(begin, end - begin);
                res_stream = query_context->getInputFormat(
                    input_format, data_in, io.out->getHeader(), query_context->getSettings().max_insert_block_size);

                while (auto block = res_stream->read())
                    io.out->write(block);
            }
        }

        io.out->writeSuffix();

        processOrdinaryQuery();
    }

    void Call::processOrdinaryQuery()
    {
        if (io.pipeline.initialized())
        {
            processOrdinaryQueryWithProcessors();
            return;
        }

        if (io.in)
        {
            AsynchronousBlockInputStream async_in(io.in);
            Stopwatch after_send_progress;

            async_in.readPrefix();
            while (true)
            {
                if (async_in.poll(interactive_delay / 1000))
                {
                    const auto block = async_in.read();
                    if (!block)
                        break;

                    if (!io.null_format)
                        addOutputToResult(block);
                }

                if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
                {
                    addProgressToResult();
                    after_send_progress.restart();
                }

                addLogsToResult();

                if (!result.output().empty() || result.has_progress() || result.logs_size())
                    sendResult();
            }
            async_in.readSuffix();

            addTotalsToResult(io.in->getTotals());
            addExtremesToResult(io.in->getExtremes());
        }
    }

    void Call::processOrdinaryQueryWithProcessors()
    {
        auto executor = std::make_shared<PullingAsyncPipelineExecutor>(io.pipeline);
        Stopwatch after_send_progress;

        Block block;
        while (executor->pull(block, interactive_delay / 1000))
        {
            if (block)
            {
                if (!io.null_format)
                    addOutputToResult(block);
            }

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            addLogsToResult();

            if (!result.output().empty() || result.has_progress() || result.logs_size())
                sendResult();
        }

        addTotalsToResult(executor->getTotalsBlock());
        addExtremesToResult(executor->getExtremesBlock());
    }

    void Call::finishQuery()
    {
        io.onFinish();
        addProgressToResult();
        query_scope->logPeakMemoryUsage();
        addLogsToResult();
        sendFinalResult();
        close();
    }

    void Call::onException(const Exception & exception)
    {
        io.onException();

        LOG_ERROR(log, "Code: {}, e.displayText() = {}, Stack trace:\n\n{}", exception.code(), exception.displayText(), exception.getStackTraceString());

        if (responder)
        {
            try
            {
                /// Try to send logs to client, but it could be risky too.
                addLogsToResult();
            }
            catch (...)
            {
                LOG_WARNING(log, "Can't send logs to client");
            }

            try
            {
                sendException(exception);
            }
            catch (...)
            {
                LOG_WARNING(log, "Couldn't send exception information to the client");
            }
        }

        close();
    }

    void Call::onFatalError()
    {
        if (!responder)
            return;
        try
        {
            addLogsToResult();
            sendFinalResult();
        }
        catch (...)
        {
        }
    }

    void Call::close()
    {
        io = {};
        query_scope.reset();
        query_context.reset();
        if (session)
            session->release();
        session.reset();
        responder.reset();
    }

    void Call::addOutputToResult(const Block & block)
    {
        WriteBufferFromString buf{*result.mutable_output()};
        auto stream = query_context->getOutputFormat(output_format, buf, block);
        stream->write(block);
    }

    void Call::addProgressToResult()
    {
        auto values = progress.fetchAndResetPiecewiseAtomically();
        if (!values.read_rows && !values.read_bytes && !values.total_rows_to_read && !values.written_rows && !values.written_bytes)
            return;
        auto & grpc_progress = *result.mutable_progress();
        grpc_progress.set_read_rows(values.read_rows);
        grpc_progress.set_read_bytes(values.read_bytes);
        grpc_progress.set_total_rows_to_read(values.total_rows_to_read);
        grpc_progress.set_written_rows(values.written_rows);
        grpc_progress.set_written_bytes(values.written_bytes);
    }

    void Call::addTotalsToResult(const Block & totals)
    {
        if (!totals)
            return;

        WriteBufferFromString buf{*result.mutable_totals()};
        auto stream = query_context->getOutputFormat(output_format, buf, totals);
        stream->write(totals);
    }

    void Call::addExtremesToResult(const Block & extremes)
    {
        if (!extremes)
            return;

        WriteBufferFromString buf{*result.mutable_extremes()};
        auto stream = query_context->getOutputFormat(output_format, buf, extremes);
        stream->write(extremes);
    }

    void Call::addLogsToResult()
    {
        if (!logs_queue)
            return;

        static_assert(::clickhouse::grpc::LogEntry_Priority_FATAL       == static_cast<int>(Poco::Message::PRIO_FATAL));
        static_assert(::clickhouse::grpc::LogEntry_Priority_CRITICAL    == static_cast<int>(Poco::Message::PRIO_CRITICAL));
        static_assert(::clickhouse::grpc::LogEntry_Priority_ERROR       == static_cast<int>(Poco::Message::PRIO_ERROR));
        static_assert(::clickhouse::grpc::LogEntry_Priority_WARNING     == static_cast<int>(Poco::Message::PRIO_WARNING));
        static_assert(::clickhouse::grpc::LogEntry_Priority_NOTICE      == static_cast<int>(Poco::Message::PRIO_NOTICE));
        static_assert(::clickhouse::grpc::LogEntry_Priority_INFORMATION == static_cast<int>(Poco::Message::PRIO_INFORMATION));
        static_assert(::clickhouse::grpc::LogEntry_Priority_DEBUG       == static_cast<int>(Poco::Message::PRIO_DEBUG));
        static_assert(::clickhouse::grpc::LogEntry_Priority_TRACE       == static_cast<int>(Poco::Message::PRIO_TRACE));

        MutableColumns columns;
        while (logs_queue->tryPop(columns))
        {
            if (columns.empty() || columns[0]->empty())
                continue;

            size_t col = 0;
            const auto & column_event_time = typeid_cast<const ColumnUInt32 &>(*columns[col++]);
            const auto & column_event_time_microseconds = typeid_cast<const ColumnUInt32 &>(*columns[col++]);
            const auto & column_host_name = typeid_cast<const ColumnString &>(*columns[col++]);
            const auto & column_query_id = typeid_cast<const ColumnString &>(*columns[col++]);
            const auto & column_thread_id = typeid_cast<const ColumnUInt64 &>(*columns[col++]);
            const auto & column_priority = typeid_cast<const ColumnInt8 &>(*columns[col++]);
            const auto & column_source = typeid_cast<const ColumnString &>(*columns[col++]);
            const auto & column_text = typeid_cast<const ColumnString &>(*columns[col++]);
            size_t num_rows = column_event_time.size();

            for (size_t row = 0; row != num_rows; ++row)
            {
                auto & log_entry = *result.add_logs();
                log_entry.set_event_time(column_event_time.getElement(row));
                log_entry.set_event_time_microseconds(column_event_time_microseconds.getElement(row));
                StringRef host_name = column_host_name.getDataAt(row);
                log_entry.set_host_name(host_name.data, host_name.size);
                StringRef query_id = column_query_id.getDataAt(row);
                log_entry.set_query_id(query_id.data, query_id.size);
                log_entry.set_thread_id(column_thread_id.getElement(row));
                log_entry.set_priority(static_cast<::clickhouse::grpc::LogEntry_Priority>(column_priority.getElement(row)));
                StringRef source = column_source.getDataAt(row);
                log_entry.set_source(source.data, source.size);
                StringRef text = column_text.getDataAt(row);
                log_entry.set_text(text.data, text.size);
            }
        }
    }

    void Call::sendResult()
    {
        responder->write(result);
        waitForSync();
        result.Clear();
    }

    void Call::sendFinalResult()
    {
        responder->writeAndFinish(result, {});
        waitForSync();
        result.Clear();
        responder.reset(); /// We must not use the `responder` after calling WriteAndFinish().
    }

    void Call::sendException(const Exception & exception)
    {
        auto & grpc_exception = *result.mutable_exception();
        grpc_exception.set_code(exception.code());
        grpc_exception.set_name(exception.name());
        grpc_exception.set_display_text(exception.displayText());
        if (send_exception_with_stacktrace)
            grpc_exception.set_stack_trace(exception.getStackTraceString());
        sendFinalResult();
    }


    /// Container like std::set<Call> but thread-safe.
    class Calls
    {
    public:
        Calls() = default;
        ~Calls() = default;

        void insert(const std::shared_ptr<Call> & call)
        {
            std::lock_guard lock{mutex};
            calls.insert(call);
        }

        void remove(const std::shared_ptr<Call> & call)
        {
            std::lock_guard lock{mutex};
            calls.erase(call);
        }

        size_t size() const
        {
            std::lock_guard lock{mutex};
            return calls.size();
        }

    private:
        std::unordered_set<std::shared_ptr<Call>> calls;
        mutable std::mutex mutex;
    };
}


class GRPCServer::QueueRunner
{
public:
    QueueRunner(GRPCServer & owner_) : owner(owner_)
    {
        /// We run queue in a separate thread.
        auto runner_function = [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException("GRPCServer");
            }
        };
        runner = ThreadFromGlobalPool{runner_function};
    }

    ~QueueRunner()
    {
        if (runner.joinable())
            runner.join();
    }

    size_t getNumCurrentCalls() const { return calls.size(); }

private:
    void run()
    {
        /// Make a responder.
        responder = std::make_unique<Responder>(owner.grpc_service, *owner.queue, *owner.queue);

        while (true)
        {
            bool ok = false;
            void * tag = nullptr;
            if (!owner.queue->Next(&tag, &ok))
            {
                /// Queue shutted down.
                break;
            }

            if (tag == responder.get())
            {
                /// Connection established and the responder is ready.
                /// So we pass this responder to a Call and make another responder for next connection.
                auto old_responder = std::exchange(responder, std::make_unique<Responder>(owner.grpc_service, *owner.queue, *owner.queue));
                if (ok)
                {
                    auto on_finish_call = [this](Call & call) { calls.remove(call.getPtr()); };
                    calls.insert(std::make_shared<Call>(owner.iserver, owner.log, std::move(old_responder), on_finish_call));
                }
                continue;
            }

            /// Continue handling a Call.
            auto call = static_cast<Call *>(tag)->getPtr();
            call->sync(ok);
        }
    }

    GRPCServer & owner;
    ThreadFromGlobalPool runner;
    std::unique_ptr<Responder> responder;
    Calls calls;
};


GRPCServer::GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_)
    : iserver(iserver_), address_to_listen(address_to_listen_), log(&Poco::Logger::get("GRPCServer"))
{}

GRPCServer::~GRPCServer() = default;

void GRPCServer::start()
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_to_listen.toString(), grpc::InsecureServerCredentials());
    //keepalive pings default values
    builder.RegisterService(&grpc_service);
    builder.SetMaxReceiveMessageSize(INT_MAX);

    queue = builder.AddCompletionQueue();
    grpc_server = builder.BuildAndStart();
    queue_runner = std::make_unique<QueueRunner>(*this);
}


void GRPCServer::stop()
{
    grpc_server->Shutdown();
    queue->Shutdown();
    queue_runner.reset();
}

size_t GRPCServer::currentConnections() const
{
    return queue_runner->getNumCurrentCalls();
}

}
#endif
