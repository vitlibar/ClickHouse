#include <gtest/gtest.h>
#include <Common/Coroutines/Task.h>
#include <Common/Coroutines/parallel.h>
#include <Common/Coroutines/sequential.h>
#include <base/sleep.h>
#include <boost/algorithm/string.hpp>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int COROUTINE_TASK_CANCELLED;
}

using namespace DB;

namespace
{
    class ThreadSafeOutput
    {
    public:
        void append(char c)
        {
            std::lock_guard lock{mutex};
            str += c;
        }

        void append(const std::string & chars)
        {
            std::lock_guard lock{mutex};
            str += chars;
        }

        std::string get()
        {
            std::lock_guard lock{mutex};
            return std::exchange(str, {});
        }

    private:
        std::string TSA_GUARDED_BY(mutex) str;
        mutable std::mutex mutex;
    };

    constexpr size_t SLEEP_TIME = 50;

    /// A simple coroutine. Normally the first argument's type should be rather `std::shared_ptr<ThreadSafeOutput>`, but I wanted to make the test simple.
    /// Because in these tests any instance of ThreadSafeOutput passed to this coroutine won't go out of scope while the coroutine is running.
    Coroutine::Task<> appendChars(ThreadSafeOutput * output, char first_char, size_t num_chars, bool throw_exception = false)
    {
        for (size_t i = 0; i != num_chars; ++i)
        {
            if (throw_exception && (i == num_chars / 2))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Test error");
            char c = static_cast<char>(i + first_char);
            output->append(c);
            sleepForMilliseconds(SLEEP_TIME);
        }
        co_return;
    }

    /// Similar to appendChars(), but this coroutine returns a string.
    Coroutine::Task<std::string> appendCharsWithResult(ThreadSafeOutput * output, char first_char, size_t num_chars, bool throw_exception = false)
    {
        std::string result;
        for (size_t i = 0; i != num_chars; ++i)
        {
            if (throw_exception && (i == num_chars / 2))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Test error");
            char c = static_cast<char>(i + first_char);
            output->append(c);
            result += c;
            sleepForMilliseconds(SLEEP_TIME);
        }
        co_return result;
    }

    /// Calls a few `append_chars()` tasks either sequentially or in parallel.
    Coroutine::Task<> appendCharsMulti(ThreadSafeOutput * output, bool parallel, size_t num_tasks = 3, bool throw_exception = false)
    {
        output->append(parallel ? "par_" : "seq_");

        std::vector<Coroutine::Task<>> tasks;
        if (num_tasks >= 1)
            tasks.push_back(appendChars(output, 'a', 3));
        if (num_tasks >= 2)
            tasks.push_back(appendChars(output, 'A', 10, throw_exception));
        if (num_tasks >= 3)
            tasks.push_back(appendChars(output, 'a', 7));

        Coroutine::Task<> main_task;
        if (parallel)
            main_task = Coroutine::parallel(std::move(tasks));
        else
            main_task = Coroutine::sequential(std::move(tasks));

        output->append("run_");

        co_await std::move(main_task); /// That's how we call another coroutine.

        output->append("_end");
    }

    /// Similar to appendCharsMulti(), but this coroutine returns a string.
    Coroutine::Task<std::string> appendCharsWithResultMulti(ThreadSafeOutput * output, bool parallel, size_t num_tasks = 3, bool throw_exception = false)
    {
        output->append(parallel ? "par_" : "seq_");
        
        std::vector<Coroutine::Task<std::string>> tasks;
        if (num_tasks >= 1)
            tasks.push_back(appendCharsWithResult(output, 'a', 3));
        if (num_tasks >= 2)
            tasks.push_back(appendCharsWithResult(output, 'A', 10, throw_exception));
        if (num_tasks >= 3)
            tasks.push_back(appendCharsWithResult(output, 'a', 7));

        Coroutine::Task<std::vector<std::string>> main_task;
        if (parallel)
            main_task = Coroutine::parallel(std::move(tasks));
        else
            main_task = Coroutine::sequential(std::move(tasks));

        output->append("run_");
        auto results = co_await std::move(main_task);

        output->append("_end");
        co_return boost::join(results, "|");
    }

    Coroutine::Task<std::string> concatTwoStrings(std::string s1, std::string s2)
    {
        co_return s1 + s2;
    }

    Coroutine::Task<std::string> concatTwoStringsWithDelay(std::string s1, std::string s2)
    {
        sleepForMilliseconds(SLEEP_TIME);
        co_return s1 + s2;
    }

    Coroutine::Task<int> getValueFromSharedPtr(std::shared_ptr<int> ptr)
    {
        sleepForMilliseconds(SLEEP_TIME);
        co_await Coroutine::StopIfCancelled{};
        co_return *ptr;
    }

    Coroutine::Task<int> getValueFromSharedPtrConstRef(const std::shared_ptr<int> & ptr)
    {
        co_return *ptr;
    }

    Coroutine::Task<int> getValueFromSharedPtrRef(std::shared_ptr<int> & ptr)
    {
        co_return *ptr;
    }

#if 0
    Coroutine::Task<std::pair<int, int>> getValueFromSharedPtrRValue(std::shared_ptr<int> && ptr)
    {
        co_return {*ptr, ptr.use_count()};
    }
#endif

    void checkThrowsExpectedException(std::function<void()> && f, const Exception & expected_exception)
    {
        try
        {
            f();
            EXPECT_TRUE(false && "The expected exception wasn't thrown");
        }
        catch(const Exception & e)
        {
            EXPECT_EQ(e.code(), expected_exception.code());
            EXPECT_EQ(e.what(), String{expected_exception.what()});
            if ((e.code() == expected_exception.code()) && (strcmp(e.what(), expected_exception.what()) == 0))
                return;
            tryLogCurrentException("Unexpected exception");
        }
        catch(...)
        {
            EXPECT_TRUE(false);
            tryLogCurrentException("Unexpected exception");
        }
    }

}

/// Actual tests.

TEST(Coroutines, Sequential)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ false).syncRun(thread_pool);
    EXPECT_EQ(output.get(), "seq_run_abcABCDEFGHIJabcdefg_end");
}

TEST(Coroutines, Parallel)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ true).syncRun(thread_pool);
    {
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabBbcCcDdEeFfGgHIJ_end"));
    }
}

TEST(Coroutines, ParallelOnSmallerNumThreads)
{
    ThreadPool thread_pool{1, 0, 0};
    ThreadSafeOutput output;
    appendCharsMulti(&output, /* parallel= */ true).syncRun(thread_pool);
    {
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_abcABCDEFGHIJabcdefg_end"));
    }
}

TEST(Coroutines, ParallelOnSingleThread)
{
    ThreadSafeOutput output;

    /// A single thread cannot execute tasks in parallel, so we expect the same result as for just sequential tasks.
    {
        appendCharsMulti(&output, /* parallel= */ true).syncRun();
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
    }

    {
        ThreadPool thread_pool{1, 0, 0};
        appendCharsMulti(&output, /* parallel= */ true).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
    }
}

TEST(Coroutines, WithResult)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ false).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "seq_run_abcABCDEFGHIJabcdefg_end");
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabBbcCcDdEeFfGgHIJ_end"));
        }
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }

    {
        auto result_string = appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun();
        EXPECT_EQ(output.get(), "par_run_abcABCDEFGHIJabcdefg_end");
        EXPECT_EQ(result_string, "abc|ABCDEFGHIJ|abcdefg");
    }
}

TEST(Coroutines, SequentialNoTasks)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        appendCharsMulti(&output, /* parallel= */ false, /* num_tasks= */ 0).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "seq_run__end");
    }

    {
        appendCharsMulti(&output, /* parallel= */ false, /* num_tasks= */ 0).syncRun();
        EXPECT_EQ(output.get(), "seq_run__end");
    }

    {
        appendCharsWithResultMulti(&output, /* parallel= */ false, /* num_tasks= */ 0).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "seq_run__end"));
        }
    }
}

TEST(Coroutines, SequentialSingleTask)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        appendCharsMulti(&output, /* parallel= */ false, /* num_tasks= */ 1).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "seq_run_abc_end");
    }

    {
        appendCharsMulti(&output, /* parallel= */ false, /* num_tasks= */ 1).syncRun();
        EXPECT_EQ(output.get(), "seq_run_abc_end");
    }

    {
        appendCharsWithResultMulti(&output, /* parallel= */ false, /* num_tasks= */ 1).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "seq_run_abc_end"));
        }
    }
}

TEST(Coroutines, ParallelNoTasks)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 0).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "par_run__end");
    }

    {
        appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 0).syncRun();
        EXPECT_EQ(output.get(), "par_run__end");
    }

    {
        appendCharsWithResultMulti(&output, /* parallel= */ true, /* num_tasks= */ 0).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run__end"));
        }
    }
}

TEST(Coroutines, ParallelSingleTask)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    {
        appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 1).syncRun(thread_pool);
        EXPECT_EQ(output.get(), "par_run_abc_end");
    }

    {
        appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 1).syncRun();
        EXPECT_EQ(output.get(), "par_run_abc_end");
    }

    {
        appendCharsWithResultMulti(&output, /* parallel= */ true, /* num_tasks= */ 1).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_abc_end"));
        }
    }
}

TEST(Coroutines, ExceptionInTask)
{
    ThreadPool thread_pool{10};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::BAD_ARGUMENTS, "Test error")); };

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ false, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(thread_pool); });
        EXPECT_EQ(output.get(), "seq_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ false, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(thread_pool); });
        EXPECT_EQ(output.get(), "seq_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(thread_pool); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabbBccCdDeEfg"));
        }
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(thread_pool); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_aAabbBccCdDeEfg"));
        }
    }

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(); });
        EXPECT_EQ(output.get(), "par_run_abcABCDE");
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true, /* num_tasks= */ 3, /* throw_exception= */ true).syncRun(); });
        EXPECT_EQ(output.get(), "par_run_abcABCDE");
    }
}

TEST(Coroutines, ExceptionInScheduler)
{
    ThreadPool thread_pool{1, 0, 1};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot schedule a task: no free thread (timeout=0) (threads=1, jobs=1)")); };

    {
        check_throws([&]{ appendCharsMulti(&output, /* parallel= */ true).syncRun(threadPoolCallbackRunner<void>(thread_pool, "")); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_"));
        }
    }

    {
        check_throws([&]{ appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(threadPoolCallbackRunner<void>(thread_pool, "")); });
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_"));
        }
    }
}

TEST(Coroutines, FallbackInScheduler)
{
    ThreadPool thread_pool{1, 0, 1};
    ThreadSafeOutput output;

    {
        appendCharsMulti(&output, /* parallel= */ true).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_abcABCDEFGHIJabcdefg_end"));
        }
    }

    {
        appendCharsWithResultMulti(&output, /* parallel= */ true).syncRun(thread_pool);
        {
            auto output_string = output.get();
            SCOPED_TRACE(output_string);
            EXPECT_TRUE(boost::iequals(output_string, "par_run_abcABCDEFGHIJabcdefg_end"));
        }
    }
}

TEST(Coroutines, CancelTask)
{
    ThreadPool thread_pool{2, 0, 0};
    ThreadSafeOutput output;

    auto check_throws = [](std::function<void()> && f)
    { checkThrowsExpectedException(std::move(f), Exception(ErrorCodes::COROUTINE_TASK_CANCELLED, "Task cancelled")); };

    {
        auto awaiter = appendCharsMulti(&output, /* parallel= */ false).run(thread_pool);
        sleepForMilliseconds(SLEEP_TIME / 2);
        awaiter.tryCancelTask();
        check_throws([&] { awaiter.syncWait(); });
        EXPECT_EQ(output.get(), "seq_run_abc");
    }

    {
        auto awaiter = appendCharsMulti(&output, /* parallel= */ true).run(thread_pool);
        sleepForMilliseconds(SLEEP_TIME / 2);
        awaiter.tryCancelTask();
        check_throws([&] { awaiter.syncWait(); });
        auto output_string = output.get();
        SCOPED_TRACE(output_string);
        EXPECT_TRUE(boost::iequals(output_string, "par_run_aAbBcCDEFGHIJ"));
    }
}

TEST(Coroutines, OwnershipArgument)
{
    auto ptr = std::make_shared<int>(5);
    EXPECT_EQ(ptr.use_count(), 1); /// No references on `ptr` except `ptr`.

    /// Run a task normally.
    {
        ThreadPool thread_pool{10};
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_GE(ptr.use_count(), 2); /// Now `awaiter` should own `ptr` instead of `task`.
        EXPECT_FALSE(awaiter.isReady());
        auto result = awaiter.syncWait();
        EXPECT_TRUE(awaiter.isReady());
        EXPECT_EQ(result, 5);
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Now `awaiter` went out of scope, so no references on `ptr` except `ptr` again.

    /// Run a task after waiting in thread pool.
    {
        ThreadPool thread_pool{0 /* no threads */, 0, 1 /* one place in queue */};
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_GE(ptr.use_count(), 2); /// Now `awaiter` should own `ptr` instead of `task`.
        EXPECT_FALSE(awaiter.isReady());
        sleepForMilliseconds(SLEEP_TIME * 2);
        EXPECT_GE(ptr.use_count(), 2); /// Nothing changed.
        EXPECT_FALSE(awaiter.isReady());
        thread_pool.setMaxThreads(1); /// We add one thread to the pool so now task can be executed.
        sleepForMilliseconds(SLEEP_TIME * 2);
        EXPECT_TRUE(awaiter.isReady());
        EXPECT_EQ(awaiter.syncWait(), 5);
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Now `awaiter` went out of scope, so no references on `ptr` except `ptr` again.

    /// Create a task but never run it.
    {
        [[maybe_unused]] auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Now `task` went out of scope, so no references on `ptr` except `ptr` again.

    /// Run a task but cancel it.
    {
        ThreadPool thread_pool{10};
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        auto awaiter = std::move(task).run(thread_pool);
        awaiter.tryCancelTask();
        checkThrowsExpectedException([&] { awaiter.syncWait(); }, Exception(ErrorCodes::COROUTINE_TASK_CANCELLED, "Task cancelled"));
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Now `awaiter` went out of scope, so no references on `ptr` except `ptr` again.

    /// Cancel a task before it even starts.
    {
        ThreadPool thread_pool{0 /* no threads */, 0, 1 /* one place in queue */};
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_GE(ptr.use_count(), 2); /// Now `awaiter` should own `ptr` instead of `task`.
        EXPECT_FALSE(awaiter.isReady());
        awaiter.tryCancelTask();
        sleepForMilliseconds(SLEEP_TIME * 2);
        EXPECT_GE(ptr.use_count(), 2); /// Nothing changed, the task cannot be cancelled yet because there are still no threads in the thread pool.
        EXPECT_FALSE(awaiter.isReady());
        thread_pool.setMaxThreads(1); /// We add one thread to the pool so now task can now be executed (and cancelled).
        checkThrowsExpectedException([&] { awaiter.syncWait(); }, Exception(ErrorCodes::COROUTINE_TASK_CANCELLED, "Task cancelled"));
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Now `awaiter` went out of scope, so no references on `ptr` except `ptr` again.

    /// Exception in scheduler.
    {
        ThreadPool thread_pool{0 /* no threads */, 0, 1 /* no place in queue */};
        thread_pool.scheduleOrThrow([] {}); /// Just to fill the queue in the thread pool.
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        checkThrowsExpectedException(
            [&] { [[maybe_unused]] auto awaiter = std::move(task).run(thread_pool); },
            Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Cannot schedule a task: no free thread (timeout=0) (threads=0, jobs=1)"));
    }
    EXPECT_EQ(ptr.use_count(), 1); /// An awaiter was never created, so no references on `ptr` except `ptr` again.

    /// Exception in scheduler.
    {
        ThreadPool thread_pool{0 /* no threads */, 0, 1 /* no place in queue */};
        auto task = getValueFromSharedPtr(ptr);
        EXPECT_GE(ptr.use_count(), 2); /// Now `task` should also own `ptr`.
        [[maybe_unused]] auto awaiter = std::move(task).run(thread_pool);
    }
    EXPECT_EQ(ptr.use_count(), 1); /// Thread pool was destroyed and the task never ran.
}

/// Ownership when we pass an argument by reference, not by value.
TEST(Coroutines, OwnershipArgumentRef)
{
    auto ptr = std::make_shared<int>(5);
    EXPECT_EQ(ptr.use_count(), 1); /// No references on `ptr` except `ptr`.

    /// Pass `ptr` as `const std::shared_ptr<int> &`
    {
        ThreadPool thread_pool{10};
        auto task = getValueFromSharedPtrConstRef(ptr);
        EXPECT_EQ(ptr.use_count(), 1);
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_EQ(ptr.use_count(), 1);
        EXPECT_FALSE(awaiter.isReady());
        auto result = awaiter.syncWait();
        EXPECT_EQ(result, 5);
    }
    EXPECT_EQ(ptr.use_count(), 1);

    /// Pass `ptr` as `std::shared_ptr<int> &`
    {
        ThreadPool thread_pool{10};
        auto task = getValueFromSharedPtrRef(ptr);
        EXPECT_EQ(ptr.use_count(), 1);
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_EQ(ptr.use_count(), 1);
        EXPECT_FALSE(awaiter.isReady());
        auto result = awaiter.syncWait();
        EXPECT_EQ(result, 5);
    }
    EXPECT_EQ(ptr.use_count(), 1);
}

#if 0
/// Ownership when we pass a move-only argument.
/// TODO: Arguments of coroutines must not be rvalues, so we need to check argument types at compile time and make a compiler fail with an error.
TEST(Coroutines, OwnershipArgumentMoveOnly)
{
    /// Pass `ptr` as `std::shared_ptr<int> &&`
    {
        ThreadPool thread_pool{10};
        auto task = getValueFromSharedPtrRValue(std::exchange(ptr, nullptr));
        auto awaiter = std::move(task).run(thread_pool);
        EXPECT_FALSE(awaiter.isReady());
        auto result = awaiter.syncWait();
        EXPECT_EQ(result.first, 5);
        EXPECT_EQ(result.second, 1);
    }
    EXPECT_FALSE(ptr);
}
#endif

TEST(Coroutines, MultipleAwaiters)
{
    ThreadPool thread_pool{10};

    {
        auto awaiter1 = concatTwoStrings("first", "second").run(thread_pool);
        auto awaiter2 = awaiter1;
        auto awaiter3 = awaiter2;
        auto result1 = awaiter1.syncWait();
        auto result2 = awaiter2.syncWait();
        auto result3 = awaiter3.syncWait();
        EXPECT_EQ(result1, "firstsecond");
        EXPECT_EQ(result2, "firstsecond");
        EXPECT_EQ(result3, "firstsecond");
    }

    {
        auto awaiter1 = concatTwoStringsWithDelay("first", "second").run(thread_pool);
        auto awaiter2 = awaiter1;
        auto awaiter3 = awaiter2;
        EXPECT_FALSE(awaiter2.isReady());
        auto result1 = awaiter1.syncWait();
        EXPECT_TRUE(awaiter2.isReady());
        auto result2 = awaiter2.syncWait();
        auto result3 = awaiter3.syncWait();
        EXPECT_EQ(result1, "firstsecond");
        EXPECT_EQ(result2, "firstsecond");
        EXPECT_EQ(result3, "firstsecond");
    }
}
