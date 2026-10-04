// The native ClickHouse sink against real servers: the sink itself, through
// the real transport, with the content of every target checked by the
// server's own count(), uniqExact(id) and sums. Three profiles, each skipped
// unless its variables are set (scripts/clickhouse-live.sh sets them and
// starts the servers):
//
//   ClickHouseNativeLive.*            one server of a supported line
//     CLINK_CLICKHOUSE_TEST_HOST, CLINK_CLICKHOUSE_TEST_PORT (default 9000),
//     CLINK_CLICKHOUSE_TEST_USER, CLINK_CLICKHOUSE_TEST_PASSWORD; the server
//     must have the one-shard cluster clink_pins_local
//     (docker/clickhouse/pins/cluster.xml). The refusal of a server-wide
//     async_insert default also needs CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT,
//     a server of the same line whose <merge_tree> sets it.
//   ClickHouseNativeLiveReplicated.*  two replicas of one shard sharing a Keeper
//     CLINK_CLICKHOUSE_TEST_HOST and CLINK_CLICKHOUSE_TEST_REPLICA_PORTS
//     ("<r1 port>,<r2 port>"). The failover case shuts the first replica
//     down, so it runs last and the profile is not reused after it.
//   ClickHouseNativeLiveTls.*         a server with the native port over TLS
//     CLINK_CLICKHOUSE_TEST_HOST, CLINK_CLICKHOUSE_TEST_PORT (plain, for
//     setup), CLINK_CLICKHOUSE_TEST_TLS_PORT, CLINK_CLICKHOUSE_TEST_TLS_CA (the
//     CA that signed the server's certificate) and
//     CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA (one that did not).
//
// Every fault-free case asserts that count() and uniqExact(id) equal the rows
// produced, that the numeric sums equal what was sent, and that there are no
// duplicates. A faulted case reports its duplicates and asserts only what the
// sink promises for that fault.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <clickhouse/client.h>
#include <clickhouse/columns/numeric.h>
#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/clickhouse/native_sink.hpp"
#include "clink/config/json.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "fake_transport.hpp"
#include "native/clickhouse_transport.hpp"
#include "native/errors.hpp"
#include "native/insert_transport.hpp"
#include "native/metrics.hpp"
#include "native/native_sink.hpp"
#include "native/sink_options.hpp"

// A struct with every leaf CLINK_FIELDS emits, unsigned included, and each
// composite, for the typed helper's round trip. CLINK_FIELDS specialises a
// clink template, so these live at namespace scope.
struct LiveTypedInner {
    std::int32_t n;
    std::string label;
};
CLINK_FIELDS(LiveTypedInner, n, label);

struct LiveTypedEvery {
    std::int64_t id;
    std::int8_t i8;
    std::int16_t i16;
    std::int32_t i32;
    std::uint8_t u8;
    std::uint16_t u16;
    std::uint32_t u32;
    std::uint64_t u64;
    float f32;
    double f64;
    bool flag;
    std::string text;
    std::optional<std::int64_t> maybe;
    std::vector<std::uint16_t> list;
    std::map<std::string, std::uint64_t> counts;
    LiveTypedInner inner;
};
CLINK_FIELDS(LiveTypedEvery,
             id,
             i8,
             i16,
             i32,
             u8,
             u16,
             u32,
             u64,
             f32,
             f64,
             flag,
             text,
             maybe,
             list,
             counts,
             inner);

namespace clink::clickhouse::native {
namespace {

using namespace std::chrono_literals;
namespace fake = clink::clickhouse::native::testing;
using LiveClock = std::chrono::steady_clock;

// --- The environment ---------------------------------------------------------

std::string live_env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string(v);
}

std::uint16_t live_port(const std::string& text, std::uint16_t fallback) {
    return text.empty() ? fallback : static_cast<std::uint16_t>(std::stoi(text));
}

std::string live_nonce() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    return std::to_string(gen()).substr(0, 8);
}

std::unique_ptr<::clickhouse::Client> live_client(const std::string& host, std::uint16_t port) {
    ::clickhouse::ClientOptions o;
    o.SetHost(host);
    o.SetPort(port);
    if (const auto user = live_env("CLINK_CLICKHOUSE_TEST_USER"); !user.empty()) {
        o.SetUser(user);
    }
    if (const auto pw = live_env("CLINK_CLICKHOUSE_TEST_PASSWORD"); !pw.empty()) {
        o.SetPassword(pw);
    }
    o.SetRethrowException(true);
    return std::make_unique<::clickhouse::Client>(o);
}

// Every cell of a SELECT as text. The SELECTs here put each expression in
// toString(), so a result is String, or LowCardinality(String) for a
// LowCardinality column.
std::vector<std::vector<std::string>> live_rows(::clickhouse::Client& client,
                                                const std::string& sql) {
    ResultSet rs;
    client.Select(sql, [&rs](const ::clickhouse::Block& b) { append_result_block(b, rs); });
    return rs.rows;
}

std::string live_scalar(::clickhouse::Client& client, const std::string& sql) {
    const auto rows = live_rows(client, sql);
    return rows.empty() || rows.front().empty() ? std::string{} : rows.front().front();
}

template <typename Pred>
bool live_eventually(Pred pred, std::chrono::milliseconds timeout = 10s) {
    const auto end = LiveClock::now() + timeout;
    while (LiveClock::now() < end) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return pred();
}

template <typename F>
std::optional<NativeSinkError> live_error(F&& f) {
    try {
        f();
    } catch (const NativeSinkError& e) {
        return e;
    }
    return std::nullopt;
}

bool live_has(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

// --- Rows --------------------------------------------------------------------

sql::Row live_row(const std::string& json) {
    auto object = clink::config::parse_object(json);
    if (!object) {
        throw std::invalid_argument("not a JSON object: " + json);
    }
    sql::Row row;
    row.values = sql::row_columns_from_json(std::move(*object));
    return row;
}

Batch<sql::Row> live_lines(const std::vector<std::string>& lines) {
    Batch<sql::Row> batch;
    for (const auto& line : lines) {
        batch.emplace(live_row(line));
    }
    return batch;
}

// Rows [from, to) of (id BIGINT, v DOUBLE, s VARCHAR): v is id / 2, exact in a
// double at these sizes, and s is `length` bytes of one letter.
Batch<sql::Row> live_ids(std::int64_t from, std::int64_t to, std::size_t length = 8) {
    Batch<sql::Row> batch;
    for (std::int64_t id = from; id < to; ++id) {
        clink::config::JsonObject object;
        object["id"] = clink::config::JsonValue{id};
        object["v"] = clink::config::JsonValue{static_cast<double>(id) / 2.0};
        object["s"] =
            clink::config::JsonValue{std::string(length, static_cast<char>('a' + id % 26))};
        sql::Row row;
        row.values = sql::row_columns_from_json(std::move(object));
        batch.emplace(std::move(row));
    }
    return batch;
}

constexpr const char* kIdsTypes = "id:BIGINT;v:DOUBLE;s:VARCHAR";
constexpr const char* kIdsColumns = "(id Int64, v Float64, s String)";

// --- One sink, driven directly -----------------------------------------------

std::atomic<std::uint64_t> g_live_op_ids{9000};

// The sink as a task drives it, with its own metrics and cancel flag. Built
// from options the factory's parser accepts, through the process-wide
// transport (the real one) unless a factory is given.
class LiveSink {
public:
    explicit LiveSink(const std::map<std::string, std::string>& params,
                      TransportFactory factory = {},
                      std::string op_name = "clickhouse_native_live")
        : cancel_(std::make_shared<std::atomic<bool>>(false)),
          ctx_(OperatorId{g_live_op_ids.fetch_add(1)}, std::move(op_name), nullptr, &metrics_) {
        ctx_.set_cancel_signal(CancelSignal(nullptr, cancel_));
        sink_ = std::make_unique<NativeSink>(parse_sink_options(params, 0, 1), std::move(factory));
        sink_->attach_runtime(&ctx_);
    }

    ~LiveSink() {
        if (!closed_) {
            sink_->close_cancelled();
        }
        sink_->attach_runtime(nullptr);
    }

    LiveSink(const LiveSink&) = delete;
    LiveSink& operator=(const LiveSink&) = delete;
    LiveSink(LiveSink&&) = delete;
    LiveSink& operator=(LiveSink&&) = delete;

    void open() { sink_->open(); }
    void push(const Batch<sql::Row>& batch) { sink_->on_data(batch); }
    void barrier(std::uint64_t id) { sink_->on_barrier(CheckpointBarrier{CheckpointId{id}}); }
    void flush() { sink_->flush(); }
    void close() {
        closed_ = true;
        sink_->close();
    }
    void cancel() { cancel_->store(true); }

    [[nodiscard]] std::uint64_t counter(const char* metric) {
        return metrics_
            .counter(std::string(metric) + "{op_id=\"" +
                     std::to_string(ctx_.operator_id().value()) + "\"}")
            .value();
    }

private:
    std::shared_ptr<std::atomic<bool>> cancel_;
    MetricsRegistry metrics_;
    RuntimeContext ctx_;
    std::unique_ptr<NativeSink> sink_;
    bool closed_{false};
};

// A factory whose transports run the real one behind the fake's fault script.
TransportFactory live_faulty(std::shared_ptr<std::deque<fake::Fault>> faults) {
    return [faults = std::move(faults)](const SinkOptions& opts) {
        return fake::faulty(make_clickhouse_transport(opts), faults);
    };
}

fake::Fault live_fault(fake::Step step, fake::Fault::Kind kind) {
    fake::Fault f;
    f.step = step;
    f.kind = kind;
    f.message = "injected by the live suite";
    return f;
}

// A point in the log ring after which only what the case does next appears.
std::int64_t live_log_mark() {
    const std::int64_t mark = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    std::this_thread::sleep_for(3ms);
    return mark;
}

std::vector<std::string> live_logs_with(std::int64_t since, const std::string& text) {
    std::vector<std::string> out;
    for (const auto& r : LogBuffer::global().tail(4096, "", since, "sink.clickhouse")) {
        if (live_has(r.message, text)) {
            out.push_back(r.message);
        }
    }
    return out;
}

// --- The fixture -------------------------------------------------------------

class LiveBase : public ::testing::Test {
protected:
    // Connects the setup client to host:port and makes a database for the case.
    void start(std::uint16_t port) {
        port_ = port;
        client_ = live_client(host_, port_);
        const auto& info = client_->GetServerInfo();
        line_ = std::to_string(info.version_major) + "." + std::to_string(info.version_minor);
        line_number_ = static_cast<int>((info.version_major * 100) + info.version_minor);
        RecordProperty("clickhouse_line", line_);
        db_ = "clink_live_" + std::to_string(::getpid()) + "_" + live_nonce();
        client_->Execute("CREATE DATABASE " + db_);
    }

    void TearDown() override {
        if (client_ == nullptr) {
            return;
        }
        try {
            client_->Execute("DROP DATABASE IF EXISTS " + db_ + " SYNC");
            for (const auto& user : users_) {
                client_->Execute("DROP USER IF EXISTS " + user);
            }
        } catch (const std::exception& e) {
            // A server the case shut down cannot be cleaned; the runner removes it.
            std::cout << "[live] cleanup skipped: " << e.what() << "\n";
        }
    }

    // The options every case starts from: one endpoint, the case's database,
    // INSERTs that close only on a flush, and a retry window short enough
    // that a case which should not retry fails fast.
    [[nodiscard]] std::map<std::string, std::string> params(const std::string& table,
                                                            const std::string& types) const {
        std::map<std::string, std::string> p{{"host", host_},
                                             {"port", std::to_string(port_)},
                                             {"database", db_},
                                             {"table", table},
                                             {"sql_column_types", types},
                                             {"batch_interval_ms", "3600000"},
                                             {"retry_window_ms", "30000"},
                                             {"connect_timeout_ms", "2000"}};
        if (const auto user = live_env("CLINK_CLICKHOUSE_TEST_USER"); !user.empty()) {
            p["user"] = user;
        }
        if (const auto pw = live_env("CLINK_CLICKHOUSE_TEST_PASSWORD"); !pw.empty()) {
            p["password"] = pw;
        }
        return p;
    }

    std::string q(const std::string& table) const { return db_ + "." + table; }

    std::uint64_t u64(const std::string& sql) { return std::stoull(live_scalar(*client_, sql)); }

    std::uint64_t rows(const std::string& table) {
        return u64("SELECT toString(count()) FROM " + q(table));
    }

    // The content gates: count() and uniqExact(id) equal the rows produced,
    // and sum(id) equals the sum sent. Duplicates are reported in every case
    // and must be `duplicates`, 0 for a fault-free run.
    void expect_content(const std::string& from,
                        std::uint64_t produced,
                        std::int64_t id_sum,
                        std::uint64_t duplicates = 0) {
        const auto r = live_rows(*client_,
                                 "SELECT toString(count()), toString(uniqExact(id)), "
                                 "toString(sum(id)) FROM " +
                                     from);
        ASSERT_EQ(r.size(), 1U);
        const std::uint64_t count = std::stoull(r[0][0]);
        const std::uint64_t uniq = std::stoull(r[0][1]);
        RecordProperty("duplicates", std::to_string(count - uniq));
        std::cout << "[live] " << line_ << " " << from << ": count=" << count << " uniq=" << uniq
                  << " duplicates=" << count - uniq << "\n";
        EXPECT_EQ(uniq, produced) << "rows lost or invented in " << from << " on " << line_;
        EXPECT_EQ(count, produced + duplicates) << "in " << from << " on " << line_;
        if (duplicates == 0) {
            EXPECT_EQ(std::stoll(r[0][2]), id_sum) << "sum(id) in " << from << " on " << line_;
        }
    }

    // For the (id, v, s) rows of live_ids: the gates, plus sum(v).
    void expect_ids(const std::string& table, std::int64_t from, std::int64_t to) {
        std::int64_t id_sum = 0;
        double v_sum = 0;
        for (std::int64_t id = from; id < to; ++id) {
            id_sum += id;
            v_sum += static_cast<double>(id) / 2.0;
        }
        expect_content(q(table), static_cast<std::uint64_t>(to - from), id_sum);
        EXPECT_EQ(std::stod(live_scalar(*client_, "SELECT toString(sum(v)) FROM " + q(table))),
                  v_sum)
            << "sum(v) in " << table << " on " << line_;
    }

    // A user whose settings profile carries `settings`, for the server-side
    // settings a sink INSERT does not send itself. Dropped at teardown.
    std::string user_with(const std::string& settings) {
        const std::string name = "clink_live_u_" + std::to_string(::getpid()) + "_" + live_nonce();
        client_->Execute("CREATE USER " + name + " IDENTIFIED WITH no_password SETTINGS " +
                         settings);
        users_.push_back(name);
        client_->Execute("GRANT CURRENT GRANTS ON *.* TO " + name);
        return name;
    }

    std::string host_ = live_env("CLINK_CLICKHOUSE_TEST_HOST");
    std::uint16_t port_{9000};
    std::unique_ptr<::clickhouse::Client> client_;
    std::string db_;
    std::string line_;
    int line_number_{0};
    std::vector<std::string> users_;
};

class ClickHouseNativeLive : public LiveBase {
protected:
    void SetUp() override {
        if (host_.empty()) {
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST to run the native sink's live suite";
        }
        start(live_port(live_env("CLINK_CLICKHOUSE_TEST_PORT"), 9000));
    }

    void create_ids(const std::string& table, const std::string& extra = {}) {
        client_->Execute("CREATE TABLE " + q(table) + " " + kIdsColumns +
                         " ENGINE = MergeTree ORDER BY id" + extra);
    }
};

// --- Every type pair ---------------------------------------------------------

struct TypePair {
    std::string sql;                    // declared clink type
    std::string target;                 // ClickHouse column type
    std::vector<std::string> cells;     // JSON text of each row's value
    std::vector<std::string> expected;  // what `read` returns for each row
    std::string read = "toString(c)";
};

std::vector<TypePair> every_type_pair() {
    const std::vector<std::string> i64{"0", "-9223372036854775808", "9223372036854775807"};
    const std::vector<std::string> i32{"0", "-2147483648", "2147483647"};
    const std::vector<std::string> i16{"0", "-32768", "32767"};
    const std::vector<std::string> i8{"0", "-128", "127"};
    return {
        // BIGINT
        {"BIGINT", "Int64", i64, i64},
        {"BIGINT", "Int128", i64, i64},
        {"BIGINT", "Int32", i32, i32},
        {"BIGINT", "Int16", i16, i16},
        {"BIGINT", "Int8", i8, i8},
        {"BIGINT", "UInt8", {"0", "255"}, {"0", "255"}},
        {"BIGINT", "UInt16", {"0", "65535"}, {"0", "65535"}},
        {"BIGINT", "UInt32", {"0", "4294967295"}, {"0", "4294967295"}},
        {"BIGINT", "UInt64", {"0", "9223372036854775807"}, {"0", "9223372036854775807"}},
        {"BIGINT", "Nullable(Int64)", {"1", "null"}, {"1", "NULL"}, "ifNull(toString(c), 'NULL')"},
        // INTEGER
        {"INTEGER", "Int32", i32, i32},
        {"INTEGER", "Int64", i32, i32},
        {"INTEGER", "Int128", i32, i32},
        {"INTEGER", "Int16", i16, i16},
        {"INTEGER", "Int8", i8, i8},
        {"INTEGER", "UInt8", {"0", "255"}, {"0", "255"}},
        {"INTEGER", "UInt16", {"0", "65535"}, {"0", "65535"}},
        {"INTEGER", "UInt32", {"0", "2147483647"}, {"0", "2147483647"}},
        {"INTEGER", "UInt64", {"0", "2147483647"}, {"0", "2147483647"}},
        // SMALLINT, TINYINT
        {"SMALLINT", "Int16", i16, i16},
        {"SMALLINT", "Int32", i16, i16},
        {"SMALLINT", "Int64", i16, i16},
        {"SMALLINT", "Int8", i8, i8},
        {"SMALLINT", "UInt16", {"0", "32767"}, {"0", "32767"}},
        {"SMALLINT", "UInt8", {"0", "255"}, {"0", "255"}},
        {"TINYINT", "Int8", i8, i8},
        {"TINYINT", "Int16", i8, i8},
        {"TINYINT", "Int64", i8, i8},
        {"TINYINT", "UInt8", {"0", "127"}, {"0", "127"}},
        // Floating point
        {"DOUBLE", "Float64", {"1.5", "-2.25", "1e300"}, {"1.5", "-2.25", "1e300"}},
        {"REAL", "Float32", {"1.5", "-2.25"}, {"1.5", "-2.25"}},
        {"REAL", "Float64", {"1.5", "-2.25"}, {"1.5", "-2.25"}},
        // BOOLEAN
        {"BOOLEAN", "Bool", {"true", "false"}, {"true", "false"}},
        {"BOOLEAN", "UInt8", {"true", "false"}, {"1", "0"}},
        // VARCHAR
        {"VARCHAR",
         "String",
         {R"("")", R"("héllo")", R"("a'b\\c")"},
         {"", "h\xc3\xa9llo", "a'b\\c"}},
        {"VARCHAR", "FixedString(4)", {R"("ab")", R"("abcd")"}, {"61620000", "61626364"}, "hex(c)"},
        {"VARCHAR", "Enum8('a' = 1, 'b' = 2)", {R"("a")", R"("b")"}, {"a", "b"}},
        {"VARCHAR", "Enum16('x' = 1000, 'y' = -1000)", {R"("x")", R"("y")"}, {"x", "y"}},
        {"VARCHAR",
         "UUID",
         {R"("123e4567-e89b-12d3-a456-426614174000")"},
         {"123e4567-e89b-12d3-a456-426614174000"}},
        {"VARCHAR",
         "IPv4",
         {R"("192.168.1.1")", R"("0.0.0.0")", R"("255.255.255.255")"},
         {"192.168.1.1", "0.0.0.0", "255.255.255.255"}},
        {"VARCHAR",
         "IPv6",
         {R"("2001:db8::1")", R"("192.168.1.1")"},
         {"2001:db8::1", "::ffff:192.168.1.1"}},
        {"VARCHAR", "LowCardinality(String)", {R"("a")", R"("b")"}, {"a", "b"}},
        {"VARCHAR",
         "LowCardinality(Nullable(String))",
         {R"("a")", "null"},
         {"a", "NULL"},
         "ifNull(toString(c), 'NULL')"},
        {"VARCHAR",
         "LowCardinality(FixedString(2))",
         {R"("ab")", R"("c")"},
         {"6162", "6300"},
         "hex(c)"},
        {"VARCHAR",
         "Nullable(String)",
         {R"("a")", "null"},
         {"a", "NULL"},
         "ifNull(toString(c), 'NULL')"},
        // DECIMAL, in the Row form the engine gives it: the canonical decimal text
        // behind a 0x01 tag byte, or an integral number.
        {"DECIMAL(10, 2)",
         "Decimal(10, 2)",
         {R"("\u00010.50")", R"("\u0001-12.25")", R"("\u000199999999.99")", "3"},
         {"0.5", "-12.25", "99999999.99", "3"}},
        {"DECIMAL(10, 2)",
         "Decimal(12, 4)",
         {R"("\u00010.50")", R"("\u0001-12.25")"},
         {"0.5", "-12.25"}},
        {"DECIMAL(18, 4)", "Decimal(38, 10)", {R"("\u00011.0625")"}, {"1.0625"}},
        // TIMESTAMP, as epoch milliseconds whatever the declared precision
        {"TIMESTAMP(3)",
         "DateTime64(3)",
         {"1700000000123", "-1", "0"},
         {"2023-11-14 22:13:20.123", "1969-12-31 23:59:59.999", "1970-01-01 00:00:00.000"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(3)",
         "DateTime64(6)",
         {"1700000000123"},
         {"2023-11-14 22:13:20.123000"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(9)",
         "DateTime64(9)",
         {"1700000000123", "-1000"},
         {"2023-11-14 22:13:20.123000000", "1969-12-31 23:59:59.000000000"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(0)",
         "DateTime64(0)",
         {"1700000000000"},
         {"2023-11-14 22:13:20"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(6)",
         "DateTime64(3, 'Asia/Tokyo')",
         {"1700000000123"},
         {"2023-11-14 22:13:20.123"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(3) WITH TIME ZONE",
         "DateTime64(3, 'UTC')",
         {"1700000000123"},
         {"2023-11-14 22:13:20.123"},
         "toString(c, 'UTC')"},
        {"TIMESTAMP(0)",
         "DateTime",
         {"1700000000000", "0"},
         {"2023-11-14 22:13:20", "1970-01-01 00:00:00"},
         "toString(c, 'UTC')"},
        // DATE
        {"DATE",
         "Date32",
         {"-25567", "0", R"("2299-12-31")", R"("1969-07-20")"},
         {"1900-01-01", "1970-01-01", "2299-12-31", "1969-07-20"}},
        {"DATE", "Date", {"0", R"("2149-06-06")"}, {"1970-01-01", "2149-06-06"}},
        // Composites
        {"BIGINT ARRAY", "Array(Int64)", {"[1, 2, 3]", "[]"}, {"[1,2,3]", "[]"}},
        {"INTEGER ARRAY", "Array(Int64)", {"[1, -2]"}, {"[1,-2]"}},
        {"BIGINT ARRAY", "Array(Nullable(Int64))", {"[1, null]"}, {"[1,NULL]"}},
        {"REAL ARRAY", "Array(Float64)", {"[1.5]"}, {"[1.5]"}},
        {"VARCHAR ARRAY", "Array(String)", {R"(["a", "b"])"}, {"['a','b']"}},
        {"VARCHAR ARRAY", "Array(LowCardinality(String))", {R"(["a"])"}, {"['a']"}},
        {"MAP<VARCHAR, BIGINT>", "Map(String, Int64)", {R"({"a": 1, "b": 2})"}, {"{'a':1,'b':2}"}},
        {"MAP<BIGINT, VARCHAR>", "Map(Int64, String)", {R"({"1": "x"})"}, {"{1:'x'}"}},
        {"ROW<a BIGINT, b VARCHAR>",
         "Tuple(a Int64, b String)",
         {R"({"a": 1, "b": "x"})"},
         {"(1,'x')"}},
    };
}

// A round trip of every pair the converter accepts, one table per pair, with
// the values read back as the server renders them.
TEST_F(ClickHouseNativeLive, EveryTypePairRoundTrips) {
    const auto pairs = every_type_pair();
    for (std::size_t i = 0; i < pairs.size(); ++i) {
        const auto& p = pairs[i];
        SCOPED_TRACE(p.sql + " into " + p.target);
        const std::string table = "pair_" + std::to_string(i);
        client_->Execute("CREATE TABLE " + q(table) + " (id Int64, c " + p.target +
                         ") ENGINE = MergeTree ORDER BY id");
        std::vector<std::string> lines;
        std::int64_t id_sum = 0;
        for (std::size_t r = 0; r < p.cells.size(); ++r) {
            lines.push_back("{\"id\": " + std::to_string(r) + ", \"c\": " + p.cells[r] + "}");
            id_sum += static_cast<std::int64_t>(r);
        }
        LiveSink sink(params(table, "id:BIGINT;c:" + p.sql));
        const auto error = live_error([&] {
            sink.open();
            sink.push(live_lines(lines));
            sink.flush();
            sink.close();
        });
        ASSERT_FALSE(error) << error->what();
        expect_content(q(table), p.cells.size(), id_sum);
        const auto got =
            live_rows(*client_, "SELECT " + p.read + " FROM " + q(table) + " ORDER BY id");
        ASSERT_EQ(got.size(), p.expected.size());
        for (std::size_t r = 0; r < got.size(); ++r) {
            EXPECT_EQ(got[r].at(0), p.expected[r]) << "row " << r << " on " << line_;
        }
    }
}

// Each compression value carries the same rows intact.
TEST_F(ClickHouseNativeLive, EachCompressionValueRoundTrips) {
    for (const std::string compression : {"none", "lz4", "zstd"}) {
        SCOPED_TRACE(compression);
        const std::string table = "c_" + compression;
        create_ids(table);
        auto p = params(table, kIdsTypes);
        p["compression"] = compression;
        LiveSink sink(p);
        sink.open();
        sink.push(live_ids(0, 10000, 32));
        // A barrier returns only once the rows before it are in the table.
        sink.barrier(1);
        EXPECT_EQ(rows(table), 10000U);
        sink.push(live_ids(10000, 20000, 32));
        sink.flush();
        sink.close();
        expect_ids(table, 0, 20000);
    }
}

// The INSERT's SETTINGS reach the server, and log_comment names the sink, its
// subtask and the token's sequence, so system.query_log can find an INSERT by
// its token: the method pin P1 uses.
TEST_F(ClickHouseNativeLive, SettingsAndLogCommentAreVisibleInTheQueryLog) {
    create_ids("t");
    {
        LiveSink sink(params("t", kIdsTypes), {}, "live sink/1");
        sink.open();
        sink.push(live_ids(0, 10));
        sink.flush();
        sink.push(live_ids(10, 20));
        sink.flush();
        sink.close();
    }
    expect_ids("t", 0, 20);
    client_->Execute("SYSTEM FLUSH LOGS");
    const auto logged = live_rows(
        *client_,
        "SELECT log_comment, Settings['insert_deduplication_token'], Settings['async_insert'], "
        "Settings['wait_for_async_insert'], "
        "Settings['input_format_native_allow_types_conversion'], "
        "Settings['input_format_null_as_default'], "
        "Settings['throw_on_max_partitions_per_insert_block'], "
        "Settings['distributed_foreground_insert'], Settings['min_insert_block_size_rows'], "
        "Settings['min_insert_block_size_bytes'], Settings['deduplicate_insert'], "
        "Settings['insert_deduplicate'], Settings['use_strict_insert_block_limits'] "
        "FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND "
        "has(databases, '" +
            db_ +
            "') AND log_comment LIKE 'clink:live_sink_1:sub0:%' ORDER BY event_time_microseconds");
    ASSERT_EQ(logged.size(), 2U) << "expected the sink's two INSERTs in system.query_log";
    const auto server_value = [this](const std::string& name) {
        return live_scalar(
            *client_, "SELECT toString(value) FROM system.settings WHERE name = '" + name + "'");
    };
    const bool has_dedup_insert = !server_value("deduplicate_insert").empty();
    const bool has_strict = !server_value("use_strict_insert_block_limits").empty();
    for (std::size_t i = 0; i < logged.size(); ++i) {
        const auto& r = logged[i];
        const std::string seq = std::to_string(i + 1);
        EXPECT_EQ(r[0], "clink:live_sink_1:sub0:" + seq);
        // clink1-<32 hex digits>-<seq>: the token and log_comment correlate.
        EXPECT_EQ(r[1].size(), std::string("clink1-").size() + 32 + 1 + seq.size()) << r[1];
        EXPECT_TRUE(r[1].starts_with("clink1-")) << r[1];
        EXPECT_TRUE(r[1].ends_with("-" + seq)) << r[1];
        EXPECT_EQ(r[2], "0");
        EXPECT_EQ(r[3], "1");
        EXPECT_EQ(r[4], "0");
        EXPECT_EQ(r[5], "0");
        EXPECT_EQ(r[6], "1");
        EXPECT_EQ(r[7], "1");
        EXPECT_EQ(r[8], server_value("min_insert_block_size_rows"));
        EXPECT_EQ(r[9], server_value("min_insert_block_size_bytes"));
        if (has_dedup_insert) {
            EXPECT_EQ(r[10], "enable");
        } else {
            EXPECT_EQ(r[11], "1");
        }
        EXPECT_EQ(r[12], has_strict ? "0" : "") << "use_strict_insert_block_limits on " << line_;
    }
    // Both INSERTs carry the same nonce: one per open.
    EXPECT_EQ(logged[0][1].substr(0, 39), logged[1][1].substr(0, 39));
}

// --- Refusals at open --------------------------------------------------------

TEST_F(ClickHouseNativeLive, ATableWithAsyncInsertIsRefusedAtOpen) {
    create_ids("t", " SETTINGS async_insert = 1");
    LiveSink sink(params("t", kIdsTypes));
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTargetAsyncInsert) << error->what();
    EXPECT_TRUE(live_has(error->what(), "MODIFY SETTING async_insert = 0")) << error->what();
    EXPECT_EQ(rows("t"), 0U);
}

TEST_F(ClickHouseNativeLive, AServerWideAsyncInsertDefaultIsRefusedAtOpen) {
    const auto async_port = live_env("CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT");
    if (async_port.empty()) {
        GTEST_SKIP() << "needs CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT, a server whose "
                        "<merge_tree> sets async_insert=1";
    }
    const auto port = live_port(async_port, 9000);
    auto other = live_client(host_, port);
    const std::string db = db_ + "_async";
    other->Execute("CREATE DATABASE " + db);
    other->Execute("CREATE TABLE " + db + ".t " + kIdsColumns + " ENGINE = MergeTree ORDER BY id");
    auto p = params("t", kIdsTypes);
    p["port"] = std::to_string(port);
    p["database"] = db;
    std::optional<NativeSinkError> error;
    {
        LiveSink sink(p);
        error = live_error([&] { sink.open(); });
    }
    const auto landed = live_scalar(*other, "SELECT toString(count()) FROM " + db + ".t");
    other->Execute("DROP DATABASE " + db + " SYNC");
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTargetAsyncInsert) << error->what();
    EXPECT_TRUE(live_has(error->what(), "<merge_tree>")) << error->what();
    EXPECT_EQ(landed, "0");
}

TEST_F(ClickHouseNativeLive, ADistributedTableOverAnAsyncLocalTableIsRefusedAtOpen) {
    create_ids("local", " SETTINGS async_insert = 1");
    client_->Execute("CREATE TABLE " + q("dist") + " " + kIdsColumns +
                     " ENGINE = Distributed(clink_pins_local, " + db_ + ", local)");
    LiveSink sink(params("dist", kIdsTypes));
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTargetAsyncInsert) << error->what();
    EXPECT_EQ(rows("local"), 0U);
}

TEST_F(ClickHouseNativeLive, AColumnPlanProblemIsRefusedAtOpenWithEveryProblemListed) {
    client_->Execute("CREATE TABLE " + q("t") +
                     " (id Int64, tenant String, f Float32) ENGINE = MergeTree ORDER BY id");
    LiveSink sink(params("t", "id:BIGINT;f:DOUBLE;extra:BIGINT"));
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kColumnPlan) << error->what();
    for (const char* column : {"`tenant`", "`f`", "`extra`"}) {
        EXPECT_TRUE(live_has(error->what(), column))
            << column << " missing from: " << error->what();
    }
    EXPECT_EQ(rows("t"), 0U);
}

// Authentication fails with 516, which no retry can cure: refused at once.
TEST_F(ClickHouseNativeLive, AWrongPasswordIsRefusedAtOpenWithoutRetrying) {
    create_ids("t");
    auto p = params("t", kIdsTypes);
    p["user"] = user_with("max_threads = 1");
    client_->Execute("ALTER USER " + p["user"] + " IDENTIFIED WITH plaintext_password BY 'right'");
    p["password"] = "wrong";
    LiveSink sink(p);
    const auto t0 = LiveClock::now();
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kAccessDenied) << error->what();
    EXPECT_LT(LiveClock::now() - t0, 10s) << "an authentication failure was retried";
}

// The open report names the server that answered, whether its line is
// tested, and the endpoint, as the sink saw them.
TEST_F(ClickHouseNativeLive, TheOpenReportNamesTheServerItsLineAndTheEndpoint) {
    create_ids("t");
    const std::int64_t since = live_log_mark();
    {
        LiveSink sink(params("t", kIdsTypes));
        sink.open();
        sink.close();
    }
    const auto& info = client_->GetServerInfo();
    const std::string version = std::to_string(info.version_major) + "." +
                                std::to_string(info.version_minor) + "." +
                                std::to_string(info.version_patch);
    const auto opened = live_logs_with(since, "clickhouse native sink open:");
    ASSERT_EQ(opened.size(), 1U);
    EXPECT_TRUE(live_has(opened.front(), " server=" + version + " (tested) ")) << opened.front();
    EXPECT_TRUE(live_has(opened.front(), " endpoint=" + host_ + ":" + std::to_string(port_) + " "))
        << opened.front();
    EXPECT_TRUE(live_has(opened.front(), " tls=off ")) << opened.front();
}

// --- Failures after open -----------------------------------------------------

// A column whose type changes while the sink runs: the next INSERT's header no
// longer matches the plan, and the sink fails rather than write into it.
TEST_F(ClickHouseNativeLive, HeaderDriftAfterAnAlterMidRunFailsTheTask) {
    client_->Execute("CREATE TABLE " + q("t") +
                     " (id Int64, v Int64) ENGINE = MergeTree ORDER BY id");
    LiveSink sink(params("t", "id:BIGINT;v:BIGINT"));
    sink.open();
    sink.push(live_lines({R"({"id": 1, "v": 10})", R"({"id": 2, "v": 20})"}));
    sink.flush();
    ASSERT_EQ(rows("t"), 2U);
    client_->Execute("ALTER TABLE " + q("t") + " MODIFY COLUMN v Nullable(Int64)");
    const auto error = live_error([&] {
        sink.push(live_lines({R"({"id": 3, "v": 30})"}));
        sink.flush();
    });
    ASSERT_TRUE(error) << "an INSERT went ahead against a changed header on " << line_;
    EXPECT_EQ(error->code(), code::kHeaderDrift) << error->what();
    EXPECT_TRUE(live_has(error->what(), "`v`")) << error->what();
    EXPECT_EQ(rows("t"), 2U);
}

// More partitions in one INSERT than max_partitions_per_insert_block (100 by
// default): 252, permanent, and nothing of the INSERT lands.
TEST_F(ClickHouseNativeLive, TooManyPartitionsIsPermanentAndLandsNothing) {
    create_ids("t", " PARTITION BY id");
    const auto limit =
        u64("SELECT toString(value) FROM system.settings WHERE name = "
            "'max_partitions_per_insert_block'");
    LiveSink sink(params("t", kIdsTypes));
    sink.open();
    const auto t0 = LiveClock::now();
    const auto error = live_error([&] {
        sink.push(live_ids(0, static_cast<std::int64_t>(limit) + 1));
        sink.flush();
    });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTooManyPartitions) << error->what();
    EXPECT_TRUE(live_has(error->what(), "max_partitions_per_insert_block")) << error->what();
    EXPECT_LT(LiveClock::now() - t0, 10s) << "a permanent failure was retried";
    EXPECT_EQ(rows("t"), 0U);
}

// Behind a Distributed table, the rows are on the shard when flush() returns,
// because flush() returns only once EndInsert has, and every INSERT carries
// distributed_foreground_insert=1. With prefer_localhost_replica=1 the shard
// (this server) is written in-process; with 0, over a connection, as a
// remote shard is. The sink does not set it, so the user's profile does.
TEST_F(ClickHouseNativeLive, DistributedRowsAreOnTheShardWhenFlushReturns) {
    for (const int prefer_local : {0, 1}) {
        SCOPED_TRACE("prefer_localhost_replica=" + std::to_string(prefer_local));
        const std::string suffix = std::to_string(prefer_local);
        create_ids("local" + suffix);
        client_->Execute("CREATE TABLE " + q("dist" + suffix) + " " + kIdsColumns +
                         " ENGINE = Distributed(clink_pins_local, " + db_ + ", local" + suffix +
                         ")");
        auto p = params("dist" + suffix, kIdsTypes);
        p["user"] = user_with("prefer_localhost_replica = " + std::to_string(prefer_local));
        p.erase("password");
        LiveSink sink(p);
        sink.open();
        for (std::int64_t round = 0; round < 3; ++round) {
            sink.push(live_ids(round * 1000, (round + 1) * 1000));
            sink.flush();
            EXPECT_EQ(rows("local" + suffix), static_cast<std::uint64_t>((round + 1) * 1000))
                << "rows were not on the shard when flush() returned";
        }
        sink.close();
        expect_ids("local" + suffix, 0, 3000);
        // The setting took effect: with 0 the shard took each INSERT as a
        // query of its own, over a connection; with 1 it never did.
        client_->Execute("SYSTEM FLUSH LOGS");
        const auto remote =
            u64("SELECT toString(count()) FROM system.query_log WHERE type = 'QueryFinish' AND "
                "query_kind = 'Insert' AND NOT is_initial_query AND has(tables, '" +
                q("local" + suffix) + "')");
        if (prefer_local == 0) {
            EXPECT_EQ(remote, 3U);
        } else {
            EXPECT_EQ(remote, 0U);
        }
    }
}

// --- The abandoned INSERT ----------------------------------------------------
//
// Every case stays far below the server's squash thresholds, where an INSERT
// that never sends its end-of-data marker lands nothing.

std::uint64_t live_inserts_seen(::clickhouse::Client& client,
                                const std::string& db,
                                const std::string& comment_like) {
    client.Execute("SYSTEM FLUSH LOGS");
    return std::stoull(live_scalar(client,
                                   "SELECT toString(count()) FROM system.query_log WHERE "
                                   "type = 'QueryStart' AND query_kind = 'Insert' AND "
                                   "has(databases, '" +
                                       db + "') AND log_comment LIKE '" + comment_like + "'"));
}

// Case 1: the blocks are sent and End fails without the rows landing; the
// resend is cancelled before its End returns. Neither attempt lands a row.
TEST_F(ClickHouseNativeLive, AnEndFaultThenACancelledResendLandsNothing) {
    create_ids("t", " SETTINGS non_replicated_deduplication_window = 100");
    auto faults = std::make_shared<std::deque<fake::Fault>>();
    faults->push_back(live_fault(fake::Step::End, fake::Fault::Kind::SystemError));
    std::atomic<bool> resend_held{false};
    auto hang = live_fault(fake::Step::End, fake::Fault::Kind::Hang);
    hang.on_fire = [&resend_held] { resend_held.store(true); };
    faults->push_back(hang);
    LiveSink sink(params("t", kIdsTypes), live_faulty(faults), "abandon_case_1");
    sink.open();
    sink.push(live_ids(0, 100));
    std::optional<NativeSinkError> error;
    std::thread flusher([&] { error = live_error([&] { sink.flush(); }); });
    const bool held = live_eventually([&] { return resend_held.load(); }, 20s);
    sink.cancel();
    flusher.join();
    ASSERT_TRUE(held) << "the resend never reached its End";
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled) << error->what();
    EXPECT_EQ(live_inserts_seen(*client_, db_, "clink:abandon_case_1:sub0:%"), 2U)
        << "both attempts should have reached the server";
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(rows("t"), 0U) << "an abandoned INSERT landed rows on " << line_;
}

// Case 2: the first block of an INSERT has gone to the server when a later
// chunk fails to convert. The INSERT is abandoned and lands nothing.
TEST_F(ClickHouseNativeLive, ABlockSentThenAConversionFailureLandsNothing) {
    create_ids("t");
    auto faults = std::make_shared<std::deque<fake::Fault>>();
    std::atomic<bool> sent{false};
    auto observe = live_fault(fake::Step::Send, fake::Fault::Kind::Delay);
    observe.on_fire = [&sent] { sent.store(true); };
    faults->push_back(observe);
    LiveSink sink(params("t", kIdsTypes), live_faulty(faults), "abandon_case_2");
    sink.open();
    // Twenty rows of 1 MiB: past the 16 MiB block cut, so a block is sent
    // while the INSERT stays open, and far below the squash thresholds.
    sink.push(live_ids(0, 20, 1U << 20));
    ASSERT_TRUE(live_eventually([&] { return sent.load(); }, 20s))
        << "no block was sent before the INSERT closed";
    const auto error = live_error([&] {
        sink.push(live_lines({R"({"id": 20, "v": 1.0, "s": null})"}));
        sink.flush();
    });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kConversionFailed) << error->what();
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(rows("t"), 0U) << "an INSERT abandoned after a sent block landed rows on " << line_;
}

::clickhouse::Block live_block(std::int64_t from, std::int64_t to) {
    auto id = std::make_shared<::clickhouse::ColumnInt64>();
    for (std::int64_t i = from; i < to; ++i) {
        id->Append(i);
    }
    ::clickhouse::Block b;
    b.AppendColumn("id", id);
    return b;
}

// Case 3: a client poisoned and then destroyed in the middle of an INSERT,
// with blocks sent, sends no end-of-data marker, so nothing lands. Only a real
// server reaches the Inserting state, which is why this is not a unit test.
TEST_F(ClickHouseNativeLive, APoisonedClientDestroyedMidInsertLandsNothing) {
    client_->Execute("CREATE TABLE " + q("t") + " (id Int64) ENGINE = MergeTree ORDER BY id");
    const std::string tag = "clink-live-abandon-" + live_nonce();
    auto opts = parse_sink_options(params("t", "id:BIGINT"), 0, 1);
    auto transport = make_clickhouse_transport(opts);
    transport->connect(opts.endpoints.front());
    (void)transport->begin_insert("INSERT INTO " + q("t") +
                                  " (id) SETTINGS async_insert=0, log_comment='" + tag +
                                  "' VALUES");
    transport->send_block(live_block(0, 100));
    transport->send_block(live_block(100, 200));
    // The server has the INSERT open before the client goes.
    ASSERT_TRUE(live_eventually([&] { return live_inserts_seen(*client_, db_, tag) == 1; }));
    transport->abandon();
    transport.reset();
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(rows("t"), 0U) << "a poisoned client's INSERT committed on " << line_;
}

// The control for case 3: the same INSERT from a raw client destroyed without
// a poison lands its rows, because the client's destructor sends the
// end-of-data marker. So the case above can see the hazard it guards against.
TEST_F(ClickHouseNativeLive, ControlARawClientDestroyedMidInsertCommitsItsRows) {
    client_->Execute("CREATE TABLE " + q("t") + " (id Int64) ENGINE = MergeTree ORDER BY id");
    {
        auto raw = live_client(host_, port_);
        raw->BeginInsert("INSERT INTO " + q("t") + " (id) SETTINGS async_insert=0 VALUES");
        raw->SendInsertBlock(live_block(0, 100));
        raw->SendInsertBlock(live_block(100, 200));
    }
    EXPECT_TRUE(live_eventually([&] { return rows("t") == 200; }))
        << "a raw client destroyed mid-INSERT did not commit on " << line_
        << "; the abandoned-INSERT case cannot see the hazard";
}

// --- The deduplication hashing regimes ---------------------------------------
//
// 26.3 hashes inserts both ways by default (compatible_double_hashes) and 26.8
// only the unified way (new_unified_hash). A resend under its own token must
// deduplicate under either, and a resend after its token has left the window
// lands again and is counted, under either.

TEST_F(ClickHouseNativeLive, TheLinesDeduplicationHashingIsItsDefault) {
    const auto version =
        live_scalar(*client_,
                    "SELECT toString(value) FROM system.server_settings WHERE name = "
                    "'insert_deduplication_version'");
    RecordProperty("insert_deduplication_version", version);
    std::cout << "[live] " << line_ << ": insert_deduplication_version=" << version << "\n";
    if (line_number_ == 2603) {
        EXPECT_EQ(version, "compatible_double_hashes");
    } else if (line_number_ >= 2608) {
        EXPECT_EQ(version, "new_unified_hash");
    }
}

// An INSERT that landed but whose acknowledgement was lost is resent under its
// own token to the same server, and deduplicates.
TEST_F(ClickHouseNativeLive, AnInDoubtResendUnderItsTokenDeduplicates) {
    create_ids("t", " SETTINGS non_replicated_deduplication_window = 100");
    auto faults = std::make_shared<std::deque<fake::Fault>>();
    auto lost_ack = live_fault(fake::Step::End, fake::Fault::Kind::SystemError);
    lost_ack.landing = fake::Fault::Landing::Everything;
    faults->push_back(lost_ack);
    const std::int64_t since = live_log_mark();
    {
        LiveSink sink(params("t", kIdsTypes), live_faulty(faults));
        sink.open();
        sink.push(live_ids(0, 1000));
        sink.flush();
        EXPECT_EQ(sink.counter(metric::kInDoubtTotal), 1U);
        sink.push(live_ids(1000, 2000));
        sink.flush();
        sink.close();
    }
    expect_ids("t", 0, 2000);
    const auto closed = live_logs_with(since, "clickhouse native sink closed:");
    ASSERT_EQ(closed.size(), 1U);
    EXPECT_TRUE(live_has(closed.front(), " rows_resent_with_token=1000 ")) << closed.front();
    EXPECT_TRUE(live_has(closed.front(), " rows_maybe_duplicated=0 ")) << closed.front();
}

// The same resend after other writers' INSERTs have pushed its token out of a
// window of two: it lands again. The rows are counted as resent with their
// token, and the summary states the window rather than claiming no duplicates.
TEST_F(ClickHouseNativeLive, AResendAfterItsTokenLeftTheWindowLandsAgainAndIsCounted) {
    create_ids("t", " SETTINGS non_replicated_deduplication_window = 2");
    const std::string host = host_;
    const std::uint16_t port = port_;
    const std::string table = q("t");
    auto faults = std::make_shared<std::deque<fake::Fault>>();
    auto lost_ack = live_fault(fake::Step::End, fake::Fault::Kind::SystemError);
    lost_ack.landing = fake::Fault::Landing::Everything;
    lost_ack.on_fire = [host, port, table] {
        auto other = live_client(host, port);
        for (int i = 0; i < 3; ++i) {
            other->Execute("INSERT INTO " + table +
                           " SETTINGS insert_deduplication_token = 'foreign-" + std::to_string(i) +
                           "' VALUES (" + std::to_string(-1 - i) + ", 0, '')");
        }
    };
    faults->push_back(lost_ack);
    const std::int64_t since = live_log_mark();
    {
        LiveSink sink(params("t", kIdsTypes), live_faulty(faults));
        sink.open();
        sink.push(live_ids(0, 1000));
        sink.flush();
        sink.close();
    }
    std::int64_t id_sum = 0;
    for (std::int64_t id = 0; id < 1000; ++id) {
        id_sum += id;
    }
    // The hazard is real on this line: the resend landed a second time.
    expect_content(q("t") + " WHERE id >= 0", 1000, id_sum, 1000);
    const auto closed = live_logs_with(since, "clickhouse native sink closed:");
    ASSERT_EQ(closed.size(), 1U);
    EXPECT_TRUE(live_has(closed.front(), " rows_resent_with_token=1000 ")) << closed.front();
}

// --- Two replicas sharing a Keeper -------------------------------------------

// --- The typed helper --------------------------------------------------------

// A CLINK_FIELDS struct through make_clickhouse_native_sink, with the column
// types taken from its batcher's schema: every leaf at its extremes, the
// unsigned ones past the signed range, a null, empty and filled composites,
// and a nested struct into a named Tuple. Each value read back as the server
// renders it.
TEST_F(ClickHouseNativeLive, ATypedStructWithEveryLeafRoundTrips) {
    client_->Execute("CREATE TABLE " + q("typed") +
                     " (id Int64, i8 Int8, i16 Int16, i32 Int32, u8 UInt8, u16 UInt16, "
                     "u32 UInt32, u64 UInt64, f32 Float32, f64 Float64, flag Bool, text String, "
                     "maybe Nullable(Int64), list Array(UInt16), counts Map(String, UInt64), "
                     "inner Tuple(n Int32, label String)) ENGINE = MergeTree ORDER BY id");
    Batch<LiveTypedEvery> batch;
    batch.emplace(LiveTypedEvery{0,
                                 -128,
                                 -32768,
                                 -2147483648,
                                 255,
                                 65535,
                                 4294967295U,
                                 18446744073709551615ULL,
                                 0.5F,
                                 -1.25,
                                 true,
                                 "plain",
                                 std::nullopt,
                                 {1, 65535},
                                 {{"a", 18446744073709551615ULL}},
                                 {7, "x"}});
    batch.emplace(LiveTypedEvery{
        1, 127, 32767, 2147483647, 0, 0, 0, 0, -2.5F, 1e300, false, "", -9, {}, {}, {-1, ""}});
    batch.emplace(LiveTypedEvery{2,
                                 0,
                                 0,
                                 0,
                                 1,
                                 2,
                                 3,
                                 9223372036854775808ULL,
                                 0.0F,
                                 0.0,
                                 true,
                                 "it's",
                                 9223372036854775807LL,
                                 {0},
                                 {{"b", 1}, {"c", 2}},
                                 {2147483647, "nested"}});

    auto p = params("typed", "");
    p.erase("sql_column_types");
    MetricsRegistry metrics;
    RuntimeContext ctx(
        OperatorId{g_live_op_ids.fetch_add(1)}, "clickhouse_native_live_typed", nullptr, &metrics);
    const auto sink = make_clickhouse_native_sink<LiveTypedEvery>(p);
    sink->attach_runtime(&ctx);
    const auto error = live_error([&] {
        sink->open();
        sink->on_data(batch);
        // A barrier returns only once the rows before it are in the table.
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        EXPECT_EQ(rows("typed"), 3U);
        sink->flush();
        sink->close();
    });
    sink->attach_runtime(nullptr);
    ASSERT_FALSE(error) << error->what();
    expect_content(q("typed"), 3, 3);

    const auto got =
        live_rows(*client_,
                  "SELECT toString(i8), toString(i16), toString(i32), toString(u8), "
                  "toString(u16), toString(u32), toString(u64), toString(f32), toString(f64), "
                  "toString(flag), text, ifNull(toString(maybe), 'NULL'), toString(list), "
                  "toString(counts), toString(inner) FROM " +
                      q("typed") + " ORDER BY id");
    const std::vector<std::vector<std::string>> expected = {
        {"-128",
         "-32768",
         "-2147483648",
         "255",
         "65535",
         "4294967295",
         "18446744073709551615",
         "0.5",
         "-1.25",
         "true",
         "plain",
         "NULL",
         "[1,65535]",
         "{'a':18446744073709551615}",
         "(7,'x')"},
        {"127",
         "32767",
         "2147483647",
         "0",
         "0",
         "0",
         "0",
         "-2.5",
         "1e300",
         "false",
         "",
         "-9",
         "[]",
         "{}",
         "(-1,'')"},
        {"0",
         "0",
         "0",
         "1",
         "2",
         "3",
         "9223372036854775808",
         "0",
         "0",
         "true",
         "it's",
         "9223372036854775807",
         "[0]",
         "{'b':1,'c':2}",
         "(2147483647,'nested')"},
    };
    ASSERT_EQ(got.size(), expected.size());
    for (std::size_t r = 0; r < got.size(); ++r) {
        EXPECT_EQ(got[r], expected[r]) << "row " << r << " on " << line_;
    }
}

class ClickHouseNativeLiveReplicated : public LiveBase {
protected:
    void SetUp() override {
        const auto ports = live_env("CLINK_CLICKHOUSE_TEST_REPLICA_PORTS");
        if (host_.empty() || ports.empty()) {
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST and "
                            "CLINK_CLICKHOUSE_TEST_REPLICA_PORTS to run the replicated suite";
        }
        const auto comma = ports.find(',');
        ASSERT_NE(comma, std::string::npos) << "CLINK_CLICKHOUSE_TEST_REPLICA_PORTS=" << ports;
        r1_ = live_port(ports.substr(0, comma), 0);
        r2_ = live_port(ports.substr(comma + 1), 0);
        start(r1_);
        replica2_ = live_client(host_, r2_);
        replica2_->Execute("CREATE DATABASE IF NOT EXISTS " + db_);
    }

    void TearDown() override {
        if (replica2_ != nullptr) {
            try {
                replica2_->Execute("DROP DATABASE IF EXISTS " + db_ + " SYNC");
            } catch (const std::exception&) {
            }
        }
        LiveBase::TearDown();
    }

    // One ReplicatedMergeTree table on both replicas, with `settings`.
    void create_replicated(const std::string& table, const std::string& settings) {
        for (auto* c : {client_.get(), replica2_.get()}) {
            c->Execute("CREATE TABLE " + q(table) + " " + kIdsColumns +
                       " ENGINE = ReplicatedMergeTree('/clickhouse/tables/" + db_ + "/" + table +
                       "', '{replica}') ORDER BY id" + settings);
        }
    }

    std::uint16_t r1_{0};
    std::uint16_t r2_{0};
    std::unique_ptr<::clickhouse::Client> replica2_;
};

// These replicas take Replicated tables asynchronously by default (their
// <replicated_merge_tree> section, pin P18), and the sink reads that default
// where it applies.
TEST_F(ClickHouseNativeLiveReplicated, TheReplicatedMergeTreeDefaultIsReadAndRefused) {
    create_replicated("t", "");
    LiveSink sink(params("t", kIdsTypes));
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTargetAsyncInsert) << error->what();
    EXPECT_TRUE(live_has(error->what(), "<replicated_merge_tree>")) << error->what();
}

// With both replicas as endpoints, stopping the first in the middle of an
// INSERT moves the inserts to the second: no exception reaches the task, and
// every row produced is on the surviving replica. The INSERT in flight is
// resent there under a fresh token, because the server changed, and counted
// as possibly duplicated. Runs last: the first replica does not come back.
TEST_F(ClickHouseNativeLiveReplicated, StoppingOneReplicaMidRunFailsOverWithoutLoss) {
    create_replicated("t", " SETTINGS async_insert = 0");
    auto p = params("t", kIdsTypes);
    p.erase("host");
    p.erase("port");
    p["endpoints"] = host_ + ":" + std::to_string(r1_) + "," + host_ + ":" + std::to_string(r2_);
    p["retry_window_ms"] = "60000";
    // Watches the second block send: the first block of the INSERT that the
    // shutdown interrupts.
    auto faults = std::make_shared<std::deque<fake::Fault>>();
    std::atomic<bool> sent{false};
    auto observe = live_fault(fake::Step::Send, fake::Fault::Kind::Delay);
    observe.nth = 2;
    observe.on_fire = [&sent] { sent.store(true); };
    faults->push_back(observe);
    const std::int64_t since = live_log_mark();
    LiveSink sink(p, live_faulty(faults));
    sink.open();
    sink.push(live_ids(0, 1000));
    sink.flush();
    // Every acknowledged part is on the second replica before the first goes,
    // so what the test reads there is what the sink delivered.
    replica2_->Execute("SYSTEM SYNC REPLICA " + q("t"));
    ASSERT_EQ(live_scalar(*replica2_, "SELECT toString(count()) FROM " + q("t")), "1000");
    // Twenty rows of 1 MiB pass the 16 MiB block cut, so the next INSERT is
    // open on the first replica, with a block sent, when it shuts down.
    sink.push(live_ids(1000, 1020, 1U << 20));
    ASSERT_TRUE(live_eventually([&] { return sent.load(); }, 20s))
        << "no block of the second INSERT was sent";
    try {
        client_->Execute("SYSTEM SHUTDOWN");
    } catch (const std::exception&) {
        // The server may close the connection before it replies.
    }
    client_.reset();
    ASSERT_TRUE(live_eventually(
        [&] {
            try {
                (void)live_client(host_, r1_);
                return false;
            } catch (const std::exception&) {
                return true;
            }
        },
        60s))
        << "the first replica did not stop";
    const auto error = live_error([&] {
        sink.push(live_ids(1020, 2000));
        sink.flush();
        sink.push(live_ids(2000, 3000));
        sink.flush();
        sink.close();
    });
    ASSERT_FALSE(error) << error->what();
    EXPECT_GE(sink.counter(metric::kReconnectsTotal), 1U);
    EXPECT_GE(sink.counter(metric::kInDoubtTotal), 1U)
        << "the shutdown did not catch an INSERT after its first block";
    EXPECT_EQ(live_logs_with(since, "now writes to").size(), 1U);
    const auto closed = live_logs_with(since, "clickhouse native sink closed:");
    ASSERT_EQ(closed.size(), 1U);
    EXPECT_FALSE(live_has(closed.front(), " rows_maybe_duplicated=0 ")) << closed.front();
    client_ = live_client(host_, r2_);  // the setup client now reads the survivor
    std::int64_t id_sum = 0;
    for (std::int64_t id = 0; id < 3000; ++id) {
        id_sum += id;
    }
    const auto r =
        live_rows(*client_, "SELECT toString(count()), toString(uniqExact(id)) FROM " + q("t"));
    const std::uint64_t count = std::stoull(r.at(0).at(0));
    const std::uint64_t uniq = std::stoull(r.at(0).at(1));
    std::cout << "[live] " << line_ << " failover: count=" << count << " uniq=" << uniq
              << " duplicates=" << count - uniq << "\n";
    RecordProperty("duplicates", std::to_string(count - uniq));
    EXPECT_EQ(uniq, 3000U) << "rows were lost across the failover on " << line_;
    if (count == uniq) {
        EXPECT_EQ(std::stoll(live_scalar(*client_, "SELECT toString(sum(id)) FROM " + q("t"))),
                  id_sum);
    }
}

// --- TLS ---------------------------------------------------------------------

class ClickHouseNativeLiveTls : public LiveBase {
protected:
    void SetUp() override {
        tls_port_ = live_env("CLINK_CLICKHOUSE_TEST_TLS_PORT");
        ca_ = live_env("CLINK_CLICKHOUSE_TEST_TLS_CA");
        wrong_ca_ = live_env("CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA");
        if (host_.empty() || tls_port_.empty() || ca_.empty() || wrong_ca_.empty()) {
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_TLS_PORT, CLINK_CLICKHOUSE_TEST_TLS_CA and "
                            "CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA to run the TLS suite";
        }
        start(live_port(live_env("CLINK_CLICKHOUSE_TEST_PORT"), 9000));
        client_->Execute("CREATE TABLE " + q("t") + " " + kIdsColumns +
                         " ENGINE = MergeTree ORDER BY id");
    }

    [[nodiscard]] std::map<std::string, std::string> tls_params(const std::string& ca) const {
        auto p = params("t", kIdsTypes);
        p["port"] = tls_port_;
        p["secure"] = "true";
        p["tls_verify"] = "true";
        p["tls_ca_file"] = ca;
        return p;
    }

    std::string tls_port_, ca_, wrong_ca_;
};

TEST_F(ClickHouseNativeLiveTls, ARoundTripWithVerificationAgainstTheNamedCa) {
    const std::int64_t since = live_log_mark();
    {
        LiveSink sink(tls_params(ca_));
        sink.open();
        sink.push(live_ids(0, 5000));
        sink.flush();
        sink.close();
    }
    expect_ids("t", 0, 5000);
    const auto opened = live_logs_with(since, "clickhouse native sink open:");
    ASSERT_EQ(opened.size(), 1U);
    EXPECT_TRUE(live_has(opened.front(), " tls=on ")) << opened.front();
}

TEST_F(ClickHouseNativeLiveTls, AServerTheNamedCaDidNotSignIsRefusedAtOpen) {
    LiveSink sink(tls_params(wrong_ca_));
    const auto t0 = LiveClock::now();
    const auto error = live_error([&] { sink.open(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTlsVerifyFailed) << error->what();
    EXPECT_LT(LiveClock::now() - t0, 10s) << "a verify failure was retried";
    EXPECT_EQ(rows("t"), 0U);
}

TEST_F(ClickHouseNativeLiveTls, AMalformedCaFileIsRefusedAtOpen) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("clink-live-bad-ca-" + std::to_string(::getpid()) + ".pem");
    {
        std::ofstream out(path);
        out << "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----\n";
    }
    LiveSink sink(tls_params(path.string()));
    const auto error = live_error([&] { sink.open(); });
    std::filesystem::remove(path);
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kOptionInvalid) << error->what();
    EXPECT_TRUE(live_has(error->what(), "tls_ca_file")) << error->what();
}

}  // namespace
}  // namespace clink::clickhouse::native
