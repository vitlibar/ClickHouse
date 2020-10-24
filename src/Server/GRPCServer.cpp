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
    /// Gets session's timeout from query info or from the server config.
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

    using CompletionCallback = std::function<void(bool)>;

    /// Requests a connection and provides low-level interface for reading and writing.
    class Responder
    {
    public:
        void start(
            GRPCService & grpc_service,
            grpc::ServerCompletionQueue & new_call_queue,
            grpc::ServerCompletionQueue & notification_queue,
            const CompletionCallback & callback)
        {
            grpc_service.RequestExecuteQuery(&grpc_context, &reader_writer, &new_call_queue, &notification_queue, getCallbackPtr(callback));
        }

        void read(GRPCQueryInfo & query_info_, const CompletionCallback & callback)
        {
            reader_writer.Read(&query_info_, getCallbackPtr(callback));
        }

        void write(const GRPCResult & result, const CompletionCallback & callback)
        {
            reader_writer.Write(result, getCallbackPtr(callback));
        }

        void writeAndFinish(const GRPCResult & result, const grpc::Status & status, const CompletionCallback & callback)
        {
            reader_writer.WriteAndFinish(result, {}, status, getCallbackPtr(callback));
        }

        Poco::Net::SocketAddress getClientAddress() const
        {
            String peer = grpc_context.peer();
            return Poco::Net::SocketAddress{peer.substr(peer.find(':') + 1)};
        }

    private:
        CompletionCallback * getCallbackPtr(const CompletionCallback & callback)
        {
            /// It would be better to pass callbacks to gRPC calls.
            /// However gRPC calls can be tagged with `void *` tags only.
            /// The map `callbacks` here is used to keep callbacks until they're called.
            size_t callback_id = next_callback_id++;
            auto & callback_in_map = callbacks[callback_id];
            callback_in_map = [this, callback, callback_id](bool ok)
            {
                auto callback_to_call = callback;
                callbacks.erase(callback_id);
                callback_to_call(ok);
            };
            return &callback_in_map;
        }
        grpc::ServerContext grpc_context;
        grpc::ServerAsyncReaderWriter<GRPCResult, GRPCQueryInfo> reader_writer{&grpc_context};
        std::unordered_map<size_t, CompletionCallback> callbacks;
        size_t next_callback_id = 0;
        /// This class needs no mutex because it's operated from a single thread at any time.
    };


    /// Handles a connection after a responder is started (i.e. after getting a new call).
    class Call
    {
    public:
        Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_);
        ~Call();

        void start(const std::function<void(void)> & on_finish_callback);

    private:
        void run();

        void receiveQuery();
        void executeQuery();
        void processInput();
        void generateOutput();
        void generateOutputWithProcessors();
        void finishQuery();
        void onException(const Exception & exception);
        void onFatalError();
        void close();

        void readQueryInfo();
        void addOutputToResult(const Block & block);
        void addProgressToResult();
        void addTotalsToResult(const Block & totals);
        void addExtremesToResult(const Block & extremes);
        void addLogsToResult();
        void sendResult();
        void throwIfFailedToSendResult();
        void sendFinalResult();
        void sendException(const Exception & exception);

        std::unique_ptr<Responder> responder;
        IServer & iserver;
        Poco::Logger * log = nullptr;
        InternalTextLogsQueuePtr logs_queue;

        ThreadFromGlobalPool call_thread;
        std::condition_variable signal;
        std::mutex dummy_mutex; /// Doesn't protect anything.

        GRPCQueryInfo query_info;
        size_t query_info_index = static_cast<size_t>(-1);
        GRPCResult result;

        std::shared_ptr<NamedSession> session;
        std::optional<Context> query_context;
        std::optional<CurrentThread::QueryScope> query_scope;
        ASTPtr ast;
        String input_format;
        String output_format;
        uint64_t interactive_delay;
        bool send_exception_with_stacktrace = false;

        std::atomic<bool> failed_to_send_result = false; /// atomic because it can be accessed both from call_thread and queue_thread

        BlockIO io;
        Progress progress;
    };

    Call::Call(std::unique_ptr<Responder> responder_, IServer & iserver_, Poco::Logger * log_)
        : responder(std::move(responder_)), iserver(iserver_), log(log_)
    {
    }

    Call::~Call()
    {
        if (call_thread.joinable())
            call_thread.join();
    }

    void Call::start(const std::function<void(void)> & on_finish_call_callback)
    {
        auto runner_function = [this, on_finish_call_callback]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException("GRPCServer");
            }
            on_finish_call_callback();
        };
        call_thread = ThreadFromGlobalPool(runner_function);
    }

    void Call::run()
    {
        try
        {
            receiveQuery();
            executeQuery();
            processInput();
            generateOutput();
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
        LOG_INFO(log, "Handling call ExecuteQuery()");

        readQueryInfo();

        auto get_query_text = [&]
        {
            std::string_view query = query_info.query();
            const size_t MAX_QUERY_LENGTH_TO_LOG = 64;
            if (query.length() > MAX_QUERY_LENGTH_TO_LOG)
                query.remove_suffix(query.length() - MAX_QUERY_LENGTH_TO_LOG);
            if (size_t format_pos = query.find(" FORMAT "); format_pos != String::npos)
                query.remove_suffix(query.length() - format_pos - strlen(" FORMAT "));
            if (query == query_info.query())
                return String{query};
            else
                return String{query} + "...";
        };
        LOG_DEBUG(log, "Received initial QueryInfo: query_id: {}, query: {}", query_info.query_id(), get_query_text());
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
                throw Exception("Database " + query_info.database() + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
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
        query_context->setDefaultFormat(query_info.output_format());
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

    void Call::processInput()
    {
        if (!io.out)
            return;

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
            readQueryInfo();
            LOG_DEBUG(log, "Received extra QueryInfo with input data: {} bytes", query_info.input_data().size());
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
    }

    void Call::generateOutput()
    {
        if (io.pipeline.initialized())
        {
            generateOutputWithProcessors();
            return;
        }

        if (!io.in)
            return;

        AsynchronousBlockInputStream async_in(io.in);
        Stopwatch after_send_progress;

        async_in.readPrefix();
        while (true)
        {
            Block block;
            if (async_in.poll(interactive_delay / 1000))
            {
                block = async_in.read();
                if (!block)
                    break;
            }

            throwIfFailedToSendResult();

            if (block && !io.null_format)
                addOutputToResult(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            addLogsToResult();

            throwIfFailedToSendResult();

            if (!result.output().empty() || result.has_progress() || result.logs_size())
                sendResult();
        }
        async_in.readSuffix();

        addTotalsToResult(io.in->getTotals());
        addExtremesToResult(io.in->getExtremes());
    }

    void Call::generateOutputWithProcessors()
    {
        if (!io.pipeline.initialized())
            return;

        auto executor = std::make_shared<PullingAsyncPipelineExecutor>(io.pipeline);
        Stopwatch after_send_progress;

        Block block;
        while (executor->pull(block, interactive_delay / 1000))
        {
            throwIfFailedToSendResult();

            if (block && !io.null_format)
                addOutputToResult(block);

            if (after_send_progress.elapsedMicroseconds() >= interactive_delay)
            {
                addProgressToResult();
                after_send_progress.restart();
            }

            addLogsToResult();

            throwIfFailedToSendResult();

            if (!result.output().empty() || result.has_progress() || result.logs_size())
                sendResult();
        }

        addTotalsToResult(executor->getTotalsBlock());
        addExtremesToResult(executor->getExtremesBlock());
    }

    void Call::finishQuery()
    {
        throwIfFailedToSendResult();
        io.onFinish();
        addProgressToResult();
        query_scope->logPeakMemoryUsage();
        addLogsToResult();
        throwIfFailedToSendResult();
        sendFinalResult();
        close();
        LOG_INFO(log, "Finished call ExecuteQuery()");
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
                LOG_WARNING(log, "Couldn't send logs to client");
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
        responder.reset();
        io = {};
        query_scope.reset();
        query_context.reset();
        if (session)
            session->release();
        session.reset();
    }

    void Call::readQueryInfo()
    {
        bool ok = false;
        bool completed = false;

        responder->read(query_info, [&](bool ok_)
        {
            /// Called on queue_thread.
            ok = ok_;
            completed = true;
            signal.notify_one();
        });

        std::unique_lock lock{dummy_mutex};
        signal.wait(lock, [&] { return completed; });

        if (!ok)
        {
            if (query_info_index == static_cast<size_t>(-1))
                throw Exception("Failed to read initial QueryInfo", ErrorCodes::NETWORK_ERROR);
            else
                throw Exception("Failed to read extra QueryInfo with input data", ErrorCodes::NETWORK_ERROR);
        }

        ++query_info_index;
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
        /// Send intermediate result without waiting.
        LOG_DEBUG(log, "Sending intermediate result to the client");
        responder->write(result, [this](bool ok)
        {
            if (!ok)
                failed_to_send_result = true;
        });

        /// gRPC has already retrieved all data from `result`, so we don't have to keep it.
        result.Clear();
    }

    void Call::throwIfFailedToSendResult()
    {
        if (failed_to_send_result)
            throw Exception("Failed to send result to the client", ErrorCodes::NETWORK_ERROR);
    }

    void Call::sendFinalResult()
    {
        /// Send final result and wait until it's actually sent.
        LOG_DEBUG(log, "Sending final result to the client");
        bool completed = false;

        responder->writeAndFinish(result, {}, [&](bool ok)
        {
            /// Called on queue_thread.
            if (!ok)
                failed_to_send_result = true;
            completed = true;
            signal.notify_one();
        });

        result.Clear();
        std::unique_lock lock{dummy_mutex};
        signal.wait(lock, [&] { return completed; });

        throwIfFailedToSendResult();
        LOG_TRACE(log, "Final result has been sent to the client");
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
}


class GRPCServer::Runner
{
public:
    Runner(GRPCServer & owner_) : owner(owner_) {}

    ~Runner()
    {
        if (queue_thread.joinable())
            queue_thread.join();
    }

    void start()
    {
        startReceivingNewCalls();

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
        queue_thread = ThreadFromGlobalPool{runner_function};
    }

    void stop() { stopReceivingNewCalls(); }

    size_t getNumCurrentCalls() const
    {
        std::lock_guard lock{mutex};
        return current_calls.size();
    }

private:
    void startReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        makeResponderForNewCall();
    }

    void makeResponderForNewCall()
    {
        /// `mutex` is already locked.
        responder_for_new_call = std::make_unique<Responder>();
        responder_for_new_call->start(owner.grpc_service, *owner.queue, *owner.queue, [this](bool ok) { onNewCall(ok); });
    }

    void stopReceivingNewCalls()
    {
        std::lock_guard lock{mutex};
        should_stop = true;
        responder_for_new_call.reset();
    }

    void onNewCall(bool responder_started_ok)
    {
         std::lock_guard lock{mutex};
         if (should_stop)
             return;
         auto responder = std::move(responder_for_new_call);
         makeResponderForNewCall();
         if (responder_started_ok)
         {
             /// Connection established and the responder has been started.
             /// So we pass this responder to a Call and make another responder for next connection.
             auto new_call = std::make_unique<Call>(std::move(responder), owner.iserver, owner.log);
             auto new_call_ptr = new_call.get();
             current_calls[new_call_ptr] = std::move(new_call);
             new_call_ptr->start([this, new_call_ptr]() { onFinishCall(new_call_ptr); });
         }
    }

    void onFinishCall(Call * call)
    {
        /// Called on call_thread. That's why we can't destroy the `call` right now
        /// (thread can't join to itself). Thus here we only move the `call` from
        /// `current_call` to `finished_calls` and run() will actually destroy the `call`.
        std::lock_guard lock{mutex};
        auto it = current_calls.find(call);
        finished_calls.push_back(std::move(it->second));
        current_calls.erase(it);
    }

    void run()
    {
        while (true)
        {
            {
                std::lock_guard lock{mutex};
                finished_calls.clear(); /// Destroy finished calls.

                /// Continue processing until there is no unfinished calls.
                if (should_stop && current_calls.empty())
                    break;
            }

            bool ok = false;
            void * tag = nullptr;
            if (!owner.queue->Next(&tag, &ok))
            {
                /// Queue shutted down.
                break;
            }

            auto & callback = *static_cast<CompletionCallback *>(tag);
            callback(ok);
        }
    }

    GRPCServer & owner;
    ThreadFromGlobalPool queue_thread;
    std::unique_ptr<Responder> responder_for_new_call;
    std::map<Call *, std::unique_ptr<Call>> current_calls;
    std::vector<std::unique_ptr<Call>> finished_calls;
    bool should_stop = false;
    mutable std::mutex mutex;
};


GRPCServer::GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_)
    : iserver(iserver_), address_to_listen(address_to_listen_), log(&Poco::Logger::get("GRPCServer"))
{}

GRPCServer::~GRPCServer()
{
    runner.reset();

    /// Server should be shutdown before CompletionQueue.
    if (grpc_server)
        grpc_server->Shutdown();

    if (queue)
        queue->Shutdown();
}

void GRPCServer::start()
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address_to_listen.toString(), grpc::InsecureServerCredentials());
    builder.RegisterService(&grpc_service);
    builder.SetMaxReceiveMessageSize(INT_MAX);

    queue = builder.AddCompletionQueue();
    grpc_server = builder.BuildAndStart();
    runner = std::make_unique<Runner>(*this);
    runner->start();
}


void GRPCServer::stop()
{
    /// Stop receiving new calls.
    runner->stop();
}

size_t GRPCServer::currentConnections() const
{
    return runner->getNumCurrentCalls();
}

}
#endif
