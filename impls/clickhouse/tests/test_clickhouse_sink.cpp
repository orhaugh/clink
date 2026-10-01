// Tests for the text-format ClickHouse sink. None stands up a server: the INSERT
// text, the token rule and the metrics are observed through a subclass that
// stands in for the server, and the lifecycle and timeout paths run against a
// dead port or a local socket that never answers.

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <initializer_list>
#include <regex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/core/types.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::ClickHouseSink;

namespace {

// Observes the barrier hook's dispatch without a server: the sink's flush() is
// virtual, so a subclass can count the calls the barrier makes.
class FlushCountingSink : public ClickHouseSink {
public:
    using ClickHouseSink::ClickHouseSink;
    void flush() override { ++flushes; }
    int flushes{0};
};

// Stands in for the server: connect() opens nothing, and send_insert() keeps
// each statement it is handed, or throws while `fail_next` is set, as a server
// that refused the INSERT would.
class ServerlessClickHouseSink : public ClickHouseSink {
public:
    using ClickHouseSink::ClickHouseSink;

    std::vector<std::string> statements;
    int connects{0};
    bool fail_next{false};

protected:
    void connect() override { ++connects; }
    void send_insert(const std::string& statement) override {
        if (fail_next) {
            fail_next = false;
            throw std::runtime_error("insert refused");
        }
        statements.push_back(statement);
    }
};

ClickHouseSink::Options serverless_sink_options() {
    ClickHouseSink::Options opts;
    opts.database = "analytics";
    opts.table = "events";
    opts.format = ClickHouseSink::Format::JSONEachRow;
    // Only the triggers a test asks for: no interval flush unless it sets one.
    opts.batch_interval = std::chrono::hours{1};
    return opts;
}

clink::Batch<std::string> sink_rows(std::initializer_list<const char*> rows) {
    clink::Batch<std::string> batch;
    for (const char* r : rows) {
        batch.emplace(std::string{r});
    }
    return batch;
}

// The global connector metrics this sink writes, read before and after the
// step under test so that other tests in the binary do not matter.
struct SinkMetricsReading {
    std::uint64_t records{0};
    std::uint64_t bytes{0};
    std::uint64_t errors{0};
    std::uint64_t commits{0};

    static SinkMetricsReading now() {
        auto& reg = clink::MetricsRegistry::global();
        using clink::metrics::connector_metric_name;
        SinkMetricsReading r;
        r.records =
            reg.counter(connector_metric_name("records_total", "clickhouse", "sink")).value();
        r.bytes = reg.counter(connector_metric_name("bytes_total", "clickhouse", "sink")).value();
        r.errors = reg.counter(connector_metric_name("errors_total", "clickhouse", "sink")).value();
        r.commits = reg.histogram(connector_metric_name("commit_latency_ns", "clickhouse", "sink"))
                        .snapshot()
                        .count;
        return r;
    }

    SinkMetricsReading since(const SinkMetricsReading& before) const {
        return {records - before.records,
                bytes - before.bytes,
                errors - before.errors,
                commits - before.commits};
    }
};

// The parts of one legacy INSERT statement.
struct ParsedSinkInsert {
    std::string target;
    std::string nonce;
    std::uint64_t seq{0};
    std::string format;
    std::string body;
};

ParsedSinkInsert parse_sink_insert(const std::string& statement) {
    static const std::regex kShape(
        "INSERT INTO (.+) SETTINGS async_insert=0, wait_for_async_insert=1, "
        "insert_deduplication_token='clink1-([0-9a-f]{32})-([0-9]+)' FORMAT ([A-Za-z]+)\n"
        "([\\s\\S]*)");
    std::smatch m;
    if (!std::regex_match(statement, m, kShape)) {
        ADD_FAILURE() << "not a legacy INSERT: " << statement;
        return {};
    }
    return {m[1].str(), m[2].str(), std::stoull(m[3].str()), m[4].str(), m[5].str()};
}

// A TCP listener on loopback that never accepts. The kernel completes the
// handshake into its backlog, so a client connects and then waits for a server
// greeting that never comes.
class SilentListener {
public:
    SilentListener() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), "socket");
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        socklen_t len = sizeof(addr);
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), len) != 0 || ::listen(fd_, 4) != 0 ||
            ::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            const int err = errno;
            ::close(fd_);
            throw std::system_error(err, std::generic_category(), "listen");
        }
        port_ = static_cast<std::uint16_t>(ntohs(addr.sin_port));
    }
    ~SilentListener() { close(); }
    SilentListener(const SilentListener&) = delete;
    SilentListener& operator=(const SilentListener&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    // Resets the connections still in the backlog, which unblocks a client
    // waiting on one.
    void close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_{-1};
    std::uint16_t port_{0};
};

}  // namespace

// The barrier is the moment buffered rows have to be durable: the runner
// snapshots and acks the checkpoint right after on_barrier returns, and a
// recovery resumes the source past the records already consumed. A sink that
// kept rows buffered across the barrier lost them on the next crash - the
// tutorial's Kafka -> ClickHouse pipeline would have shown missing windows
// after a Worker kill, against a connector documented as at-least-once.
TEST(ClickHouseSink, ABarrierFlushesTheBufferedRows) {
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.batch_interval = std::chrono::hours{1};
    FlushCountingSink sink(std::move(opts));
    clink::Batch<std::string> batch;
    batch.emplace(std::string{R"({"sensor_id":"sensor-01","readings":10})"});
    sink.on_data(batch);  // buffered; never opened, so nothing reaches a client
    EXPECT_EQ(sink.flushes, 0) << "on_data alone must not flush a one-row buffer";
    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{7}});
    EXPECT_EQ(sink.flushes, 1) << "the barrier must flush before the checkpoint is acked";
    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{8}, /*terminal=*/true});
    EXPECT_EQ(sink.flushes, 2) << "a terminal barrier flushes too";
}

// Every statement names its settings, so a profile with async_insert=1 cannot
// turn the barrier flush into a fire-and-forget, and carries a token of its own.
TEST(ClickHouseSink, EveryInsertCarriesTheSettingsAndItsOwnToken) {
    ServerlessClickHouseSink sink(serverless_sink_options());
    sink.open();
    EXPECT_EQ(sink.connects, 1);
    sink.on_data(sink_rows({R"({"id":1})", R"({"id":2})"}));
    sink.flush();
    ASSERT_EQ(sink.statements.size(), 1U);
    const auto first = parse_sink_insert(sink.statements[0]);
    EXPECT_EQ(sink.statements[0],
              "INSERT INTO `analytics`.`events` SETTINGS async_insert=0, wait_for_async_insert=1, "
              "insert_deduplication_token='clink1-" +
                  first.nonce +
                  "-1' FORMAT JSONEachRow\n"
                  "{\"id\":1}\n"
                  "{\"id\":2}\n");
}

// The defect the token removes: without one, a ReplicatedMergeTree target drops
// a batch whose content matches an earlier separate batch. Identical batches
// now carry distinct tokens, from one nonce drawn at open and a sequence that
// moves on every flush.
TEST(ClickHouseSink, IdenticalSeparateBatchesCarryDistinctTokens) {
    ServerlessClickHouseSink sink(serverless_sink_options());
    sink.open();
    for (int i = 0; i < 3; ++i) {
        sink.on_data(sink_rows({R"({"id":1})"}));
        sink.on_barrier(
            clink::CheckpointBarrier{clink::CheckpointId{static_cast<std::uint64_t>(i + 1)}});
    }
    ASSERT_EQ(sink.statements.size(), 3U);
    const auto a = parse_sink_insert(sink.statements[0]);
    const auto b = parse_sink_insert(sink.statements[1]);
    const auto c = parse_sink_insert(sink.statements[2]);
    EXPECT_EQ(a.body, b.body);
    EXPECT_EQ(b.body, c.body);
    EXPECT_EQ(a.nonce, b.nonce);
    EXPECT_EQ(b.nonce, c.nonce);
    EXPECT_EQ(a.seq, 1U);
    EXPECT_EQ(b.seq, 2U);
    EXPECT_EQ(c.seq, 3U);
}

// A restart builds a new sink and opens it, so its tokens must not repeat the
// ones the previous run already put in the deduplication log.
TEST(ClickHouseSink, EachOpenDrawsAFreshNonce) {
    ServerlessClickHouseSink sink(serverless_sink_options());
    sink.open();
    sink.on_data(sink_rows({"{}"}));
    sink.close();
    sink.open();
    sink.on_data(sink_rows({"{}"}));
    sink.close();
    ServerlessClickHouseSink other(serverless_sink_options());
    other.open();
    other.on_data(sink_rows({"{}"}));
    other.close();
    ASSERT_EQ(sink.statements.size(), 2U);
    ASSERT_EQ(other.statements.size(), 1U);
    const auto first = parse_sink_insert(sink.statements[0]);
    const auto reopened = parse_sink_insert(sink.statements[1]);
    const auto second_sink = parse_sink_insert(other.statements[0]);
    EXPECT_NE(first.nonce, reopened.nonce);
    EXPECT_NE(first.nonce, second_sink.nonce);
    EXPECT_EQ(reopened.seq, 1U) << "the sequence starts again with the new nonce";
}

// The names were spliced in bare, so a database or table name with a backtick,
// a dot or a reserved word broke the statement, or changed its target.
TEST(ClickHouseSink, TheTargetIsQuoted) {
    auto opts = serverless_sink_options();
    opts.database = "my`db";
    opts.table = R"(t.x\y)";
    ServerlessClickHouseSink sink(std::move(opts));
    sink.open();
    sink.on_data(sink_rows({"{}"}));
    sink.flush();
    ASSERT_EQ(sink.statements.size(), 1U);
    EXPECT_EQ(parse_sink_insert(sink.statements[0]).target, R"(`my\`db`.`t.x\\y`)");
}

TEST(ClickHouseSink, TsvRowsAreSentAsTsv) {
    auto opts = serverless_sink_options();
    opts.format = ClickHouseSink::Format::TSV;
    ServerlessClickHouseSink sink(std::move(opts));
    sink.open();
    sink.on_data(sink_rows({"1\ta", "2\tb"}));
    sink.flush();
    ASSERT_EQ(sink.statements.size(), 1U);
    const auto insert = parse_sink_insert(sink.statements[0]);
    EXPECT_EQ(insert.format, "TSV");
    EXPECT_EQ(insert.body, "1\ta\n2\tb\n");
}

// records_out and bytes_out used to count every record on arrival, so a batch
// that was still buffered, or whose INSERT then failed, showed as written.
TEST(ClickHouseSink, RowsAndBytesCountOnlyOnceTheServerAcknowledges) {
    auto opts = serverless_sink_options();
    opts.batch_rows = 10;
    ServerlessClickHouseSink sink(std::move(opts));
    sink.open();

    const auto start = SinkMetricsReading::now();
    sink.on_data(sink_rows({"abc", "de"}));
    auto d = SinkMetricsReading::now().since(start);
    EXPECT_EQ(d.records, 0U) << "buffered rows are not written rows";
    EXPECT_EQ(d.bytes, 0U);

    sink.fail_next = true;
    EXPECT_THROW(sink.flush(), std::runtime_error);
    d = SinkMetricsReading::now().since(start);
    EXPECT_EQ(d.records, 0U) << "a refused INSERT wrote nothing";
    EXPECT_EQ(d.bytes, 0U);
    EXPECT_EQ(d.errors, 1U);
    EXPECT_EQ(d.commits, 0U);

    sink.flush();
    d = SinkMetricsReading::now().since(start);
    EXPECT_EQ(d.records, 2U);
    EXPECT_EQ(d.bytes, 5U) << "the bytes of the records themselves";
    EXPECT_EQ(d.errors, 1U);
    EXPECT_EQ(d.commits, 1U);
    ASSERT_EQ(sink.statements.size(), 1U);
    const auto resent = parse_sink_insert(sink.statements[0]);
    EXPECT_EQ(resent.body, "abc\nde\n") << "a failed flush keeps its rows for the next attempt";
    EXPECT_EQ(resent.seq, 2U) << "the resend carries a token of its own";
}

// The barrier and close flushes used to record nothing, so a job flushing only
// at barriers, the common case at low volume, showed no commits at all.
TEST(ClickHouseSink, EveryFlushTriggerRecordsTheSameMetrics) {
    auto opts = serverless_sink_options();
    opts.batch_rows = 2;
    ServerlessClickHouseSink by_count(opts);
    by_count.open();
    auto before = SinkMetricsReading::now();
    by_count.on_data(sink_rows({"a", "b", "c"}));
    auto d = SinkMetricsReading::now().since(before);
    ASSERT_EQ(by_count.statements.size(), 1U) << "row-count trigger";
    EXPECT_EQ(parse_sink_insert(by_count.statements[0]).body, "a\nb\n");
    EXPECT_EQ(d.records, 2U);
    EXPECT_EQ(d.bytes, 2U);
    EXPECT_EQ(d.commits, 1U);

    opts.batch_rows = 1000;
    opts.batch_interval = std::chrono::milliseconds{1};
    ServerlessClickHouseSink by_interval(opts);
    by_interval.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    before = SinkMetricsReading::now();
    by_interval.on_data(sink_rows({"ccc"}));
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(by_interval.statements.size(), 1U) << "interval trigger";
    EXPECT_EQ(d.records, 1U);
    EXPECT_EQ(d.bytes, 3U);
    EXPECT_EQ(d.commits, 1U);

    opts.batch_interval = std::chrono::hours{1};
    ServerlessClickHouseSink by_barrier(opts);
    by_barrier.open();
    by_barrier.on_data(sink_rows({"dd"}));
    EXPECT_TRUE(by_barrier.statements.empty());
    before = SinkMetricsReading::now();
    by_barrier.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(by_barrier.statements.size(), 1U) << "barrier trigger";
    EXPECT_EQ(d.records, 1U);
    EXPECT_EQ(d.bytes, 2U);
    EXPECT_EQ(d.commits, 1U);

    ServerlessClickHouseSink by_close(opts);
    by_close.open();
    by_close.on_data(sink_rows({"e"}));
    before = SinkMetricsReading::now();
    by_close.close();
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(by_close.statements.size(), 1U) << "close trigger";
    EXPECT_EQ(d.records, 1U);
    EXPECT_EQ(d.bytes, 1U);
    EXPECT_EQ(d.commits, 1U);
    EXPECT_EQ(d.errors, 0U);
}

TEST(ClickHouseSink, AFailedFlushCountsAnErrorOnEveryTrigger) {
    auto opts = serverless_sink_options();
    opts.batch_rows = 1;
    ServerlessClickHouseSink by_count(opts);
    by_count.open();
    by_count.fail_next = true;
    auto before = SinkMetricsReading::now();
    EXPECT_THROW(by_count.on_data(sink_rows({"a"})), std::runtime_error);
    auto d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(d.errors, 1U) << "row-count trigger";
    EXPECT_EQ(d.records, 0U);

    opts.batch_rows = 1000;
    opts.batch_interval = std::chrono::milliseconds{1};
    ServerlessClickHouseSink by_interval(opts);
    by_interval.open();
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
    by_interval.fail_next = true;
    before = SinkMetricsReading::now();
    EXPECT_THROW(by_interval.on_data(sink_rows({"a"})), std::runtime_error);
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(d.errors, 1U) << "interval trigger";
    EXPECT_EQ(d.records, 0U);

    opts.batch_interval = std::chrono::hours{1};
    ServerlessClickHouseSink by_barrier(opts);
    by_barrier.open();
    by_barrier.on_data(sink_rows({"a"}));
    by_barrier.fail_next = true;
    before = SinkMetricsReading::now();
    EXPECT_THROW(by_barrier.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{3}}),
                 std::runtime_error);
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(d.errors, 1U) << "barrier trigger";
    EXPECT_EQ(d.records, 0U);

    ServerlessClickHouseSink by_close(opts);
    by_close.open();
    by_close.on_data(sink_rows({"a"}));
    by_close.fail_next = true;
    before = SinkMetricsReading::now();
    EXPECT_THROW(by_close.close(), std::runtime_error);
    d = SinkMetricsReading::now().since(before);
    EXPECT_EQ(d.errors, 1U) << "close trigger";
    EXPECT_EQ(d.records, 0U);
    EXPECT_EQ(d.commits, 0U);
}

// An empty interval sends no statement: an INSERT with no rows is either an
// error or an empty part on the server.
TEST(ClickHouseSink, AnEmptyBufferSendsNothing) {
    ServerlessClickHouseSink sink(serverless_sink_options());
    sink.open();
    const auto before = SinkMetricsReading::now();
    sink.on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    sink.flush();
    sink.close();
    EXPECT_TRUE(sink.statements.empty());
    EXPECT_EQ(SinkMetricsReading::now().since(before).commits, 0U);
}

// Rows can only reach a flush before open() through a caller that skipped it.
// That used to dereference a null client; now it is a named error.
TEST(ClickHouseSink, AFlushOfRowsBeforeOpenIsRefused) {
    auto opts = serverless_sink_options();
    opts.batch_rows = 1;
    ServerlessClickHouseSink sink(std::move(opts));
    try {
        sink.on_data(sink_rows({"a"}));
        FAIL() << "expected a refusal";
    } catch (const std::logic_error& e) {
        EXPECT_STREQ(e.what(), "clickhouse_sink: rows are buffered but open() has not completed");
    }
    EXPECT_EQ(sink.connects, 0);
    EXPECT_TRUE(sink.statements.empty());
}

// The builder's format() takes any string, so an unknown value keeps meaning
// TSV, as it always has, but no longer silently: open names it once.
TEST(ClickHouseSink, AnUnrecognisedFormatIsNamedOnceAtOpen) {
    auto opts = serverless_sink_options();
    opts.format = ClickHouseSink::Format::TSV;
    opts.unrecognised_format = "csv";
    ServerlessClickHouseSink sink(std::move(opts));
    const std::string op_name = "clickhouse_sink_unrecognised_format_test";
    clink::RuntimeContext rt{clink::OperatorId{41}, op_name, nullptr, nullptr};
    sink.attach_runtime(&rt);
    sink.open();
    sink.on_data(sink_rows({"1\ta"}));
    sink.flush();
    const auto records = clink::LogBuffer::global().tail(1024, "warn", 0, op_name);
    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0].level, "warn");
    EXPECT_EQ(records[0].message,
              "format 'csv' is not one of tsv, json or jsoneachrow; the rows are sent as TSV");
    ASSERT_EQ(sink.statements.size(), 1U);
    EXPECT_EQ(parse_sink_insert(sink.statements[0]).format, "TSV");
    sink.attach_runtime(nullptr);
}

TEST(ClickHouseSink, ARecognisedFormatLogsNoWarning) {
    ServerlessClickHouseSink sink(serverless_sink_options());
    const std::string op_name = "clickhouse_sink_recognised_format_test";
    clink::RuntimeContext rt{clink::OperatorId{42}, op_name, nullptr, nullptr};
    sink.attach_runtime(&rt);
    sink.open();
    EXPECT_TRUE(clink::LogBuffer::global().tail(1024, "debug", 0, op_name).empty());
    sink.attach_runtime(nullptr);
}

TEST(ClickHouseSink, TheTimeoutsDefaultToBoundedValues) {
    const ClickHouseSink::Options opts;
    EXPECT_EQ(opts.connect_timeout, std::chrono::milliseconds{5000});
    EXPECT_EQ(opts.send_timeout, std::chrono::milliseconds{30000});
    EXPECT_EQ(opts.receive_timeout, std::chrono::milliseconds{30000});
    EXPECT_TRUE(opts.unrecognised_format.empty());
}

TEST(ClickHouseSinkReal, ConstructorIsClean) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP() << "Built without clickhouse-cpp; real-impl path not exercised";
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.host = "127.0.0.1";
    opts.port = 1;
    ClickHouseSink sink(std::move(opts));
    SUCCEED();
}

TEST(ClickHouseSinkReal, OpenAgainstDeadEndpointFailsCleanly) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.host = "127.0.0.1";
    opts.port = 1;
    ClickHouseSink sink(std::move(opts));
    EXPECT_THROW(sink.open(), std::exception);
    EXPECT_NO_THROW(sink.close());
}

TEST(ClickHouseSinkReal, FlushAndCloseBeforeOpenAreSafe) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP();
    }
    ClickHouseSink::Options opts;
    opts.table = "events";
    ClickHouseSink sink(std::move(opts));
    EXPECT_NO_THROW(sink.flush());
    EXPECT_NO_THROW(sink.close());
}

// The client's default receive timeout is none, so a server that accepted the
// connection and then went quiet held open(), or a barrier flush, for ever. With
// the timeout set, the wait for the server's greeting ends in the client's own
// timeout error.
TEST(ClickHouseSinkReal, TheReceiveTimeoutBoundsAServerThatNeverAnswers) {
    if (!ClickHouseSink::is_real_implementation()) {
        GTEST_SKIP();
    }
    SilentListener listener;
    ClickHouseSink::Options opts;
    opts.table = "events";
    opts.host = "127.0.0.1";
    opts.port = listener.port();
    opts.receive_timeout = std::chrono::milliseconds{200};
    ClickHouseSink sink(std::move(opts));

    const auto start = std::chrono::steady_clock::now();
    auto opened = std::async(std::launch::async, [&sink]() -> int {
        try {
            sink.open();
            return 0;
        } catch (const std::system_error& e) {
            return e.code().value();
        } catch (...) {
            return -1;
        }
    });
    const auto status = opened.wait_for(std::chrono::seconds{10});
    const auto elapsed = std::chrono::steady_clock::now() - start;
    listener.close();  // unblocks a client that never timed out
    const int code = opened.get();
    ASSERT_EQ(status, std::future_status::ready) << "open() was still waiting after 10 s";
    EXPECT_TRUE(code == EAGAIN || code == EWOULDBLOCK)
        << "expected the client's receive timeout, got error code " << code;
    EXPECT_GE(elapsed, std::chrono::milliseconds{150}) << "it failed before waiting at all";
}
