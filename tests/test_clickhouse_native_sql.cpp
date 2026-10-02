// The native ClickHouse sink driven through the SQL frontend, against the
// in-process fake server rather than a real one:
//   - the values a script lands through insert_format='native' equal what the
//     same script hands connector='collect', under the sink's type mapping,
//     on a row plan and on plans whose rows were rebuilt from a columnar batch;
//   - every key the planner puts on a native sink op is one the sink owns or
//     tolerates, and the factory accepts the op;
//   - a barrier held by a slow INSERT stalls the periodic checkpoint without
//     failing it, and on a bounded job costs one restart and loses no rows;
//   - timestamps and dates leave the JSON decode exactly as written;
//   - what happens to a script that writes one source to two sinks.
//
// Two suites run against a real server instead, and skip unless
// CLINK_CLICKHOUSE_TEST_HOST names one (native port from
// CLINK_CLICKHOUSE_TEST_PORT, default 9000; CLINK_CLICKHOUSE_TEST_USER and
// CLINK_CLICKHOUSE_TEST_PASSWORD when set; CLINK_CLICKHOUSE_TEST_LINE, when
// set, is the line the server must report). scripts/clickhouse-live.sh starts
// the servers and runs them:
//   - ClickHouseNativeSqlLive: the temporal, wide-integer, decimal and nullable
//     values a script lands through insert_format='native' read back from the
//     server equal to what the same script hands connector='collect';
//   - ClickHouseLegacySqlLive: the text sink, configured as the tutorial
//     configures it, lands every row once, and the server logs every INSERT
//     with the settings the sink forces, for an ordinary user.

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <arrow/api.h>
#include <clickhouse/client.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include "clink/clickhouse/install.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/script_runner.hpp"
#include "clink/test/test_cluster.hpp"

#include "fake_transport.hpp"
#include "native/clickhouse_transport.hpp"
#include "native/column_plan.hpp"
#include "native/insert_transport.hpp"
#include "native/native_sink.hpp"
#include "native/sink_options.hpp"

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;
namespace fake = clink::clickhouse::native::testing;
namespace native = clink::clickhouse::native;
using clink::config::JsonArray;
using clink::config::JsonObject;
using clink::config::JsonValue;
using Clock = std::chrono::steady_clock;

// --- Installation ---------------------------------------------------------------

// Lines a test hands the in-memory string source, by the table's topic.
struct Feed {
    std::vector<std::string> lines;
    bool bounded{true};
};

std::mutex& feeds_mu() {
    static std::mutex mu;
    return mu;
}

std::map<std::string, Feed>& feeds() {
    static std::map<std::string, Feed> f;
    return f;
}

void set_feed(const std::string& topic, std::vector<std::string> lines, bool bounded) {
    const std::lock_guard<std::mutex> lock(feeds_mu());
    feeds()[topic] = Feed{std::move(lines), bounded};
}

// Plays its feed from the start each time it is built, so a restarted job
// reads every line again, then ends or idles until the job is cancelled.
class FeedSource final : public clink::Source<std::string> {
public:
    explicit FeedSource(Feed feed) : feed_(std::move(feed)) {}

    bool produce(clink::Emitter<std::string>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (next_ >= feed_.lines.size()) {
            if (feed_.bounded) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
            return true;
        }
        clink::Batch<std::string> batch;
        const std::size_t end = std::min(feed_.lines.size(), next_ + 64);
        for (; next_ < end; ++next_) {
            batch.emplace(feed_.lines[next_]);
        }
        return out.emit_data(std::move(batch));
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return feed_.bounded; }
    [[nodiscard]] std::string name() const override { return "native_sql_test.feed"; }

private:
    Feed feed_;
    std::size_t next_{0};
};

// The SQL frontend, the collect sink, the ClickHouse impl and the string
// source, once per process. EmbeddedEngine installs the SQL operators and the
// collect sink behind its own once-flag, and a second install of the SQL
// operators throws, so the first engine does it for every test, including
// the ones that submit to a TestCluster. clink::kafka is not linked into this
// binary, so the planner's Kafka arm finds the in-memory source under the
// Kafka source's factory name: that gives a String-channel source in front of
// json_string_to_row (or its columnar twin), and an unbounded one when asked.
void ensure_installed() {
    static const bool done = [] {
        {
            clink::embed::EngineOptions opts;
            std::ostringstream sink;
            opts.out = &sink;
            opts.err = &sink;
            const clink::embed::EmbeddedEngine first{std::move(opts)};
        }
        clink::plugin::PluginRegistry reg;
        clink::clickhouse::install(reg);
        reg.register_source<std::string>(
            "kafka_source_string",
            [](const clink::plugin::BuildContext& ctx)
                -> std::shared_ptr<clink::Source<std::string>> {
                const std::string topic = ctx.param_or("topic");
                const std::lock_guard<std::mutex> lock(feeds_mu());
                const auto it = feeds().find(topic);
                if (it == feeds().end()) {
                    throw std::runtime_error("native_sql_test: no feed for topic '" + topic + "'");
                }
                return std::make_shared<FeedSource>(it->second);
            });
        return true;
    }();
    (void)done;
}

// Points the native factory at one fake server for the life of a test, and
// checks on the way out that no client was ever dropped mid-INSERT.
class FakeServerScope {
public:
    explicit FakeServerScope(fake::FakeTable table)
        : server_(std::make_shared<fake::FakeServer>()) {
        server_->add_table(std::move(table));
        native::set_transport_factory_for_testing(fake::fake_factory(server_));
    }
    ~FakeServerScope() {
        EXPECT_EQ(server_->destroyed_mid_insert(), 0U) << "a client was destroyed mid-INSERT";
        native::set_transport_factory_for_testing(nullptr);
    }
    FakeServerScope(const FakeServerScope&) = delete;
    FakeServerScope& operator=(const FakeServerScope&) = delete;
    FakeServerScope(FakeServerScope&&) = delete;
    FakeServerScope& operator=(FakeServerScope&&) = delete;

    [[nodiscard]] fake::FakeServer& server() const { return *server_; }

private:
    std::shared_ptr<fake::FakeServer> server_;
};

// A scratch directory unique to this process and test, removed afterwards.
class ScratchDir {
public:
    explicit ScratchDir(const std::string& tag)
        : path_(fs::temp_directory_path() /
                ("clink_native_sql_" + tag + "_" + std::to_string(::getpid()))) {
        fs::remove_all(path_);
        fs::create_directories(path_);
    }
    ~ScratchDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    ScratchDir(const ScratchDir&) = delete;
    ScratchDir& operator=(const ScratchDir&) = delete;
    ScratchDir(ScratchDir&&) = delete;
    ScratchDir& operator=(ScratchDir&&) = delete;

    [[nodiscard]] const fs::path& path() const { return path_; }

private:
    fs::path path_;
};

void write_lines(const fs::path& path, const std::vector<std::string>& lines) {
    std::ofstream out(path);
    for (const auto& line : lines) {
        out << line << "\n";
    }
}

fake::FakeTable fake_table(const std::string& name,
                           const std::vector<std::pair<std::string, std::string>>& columns) {
    fake::FakeTable t;
    t.database = "db";
    t.name = name;
    t.engine = "MergeTree";
    t.engine_full = "MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 100";
    t.dedup_window = 100;
    std::uint32_t position = 1;
    for (const auto& [column, type] : columns) {
        t.columns.push_back({column, type, native::DefaultKind::None, position++});
    }
    return t;
}

// Every spec a script compiles, as the script runner hands them to a
// submitter. `ddl` runs first, then `prepare` sees the catalog, then `sql`.
std::vector<clink::cluster::JobGraphSpec> compile_script(
    const std::string& ddl,
    const std::string& sql,
    std::uint32_t parallelism = 1,
    const std::function<void(clink::sql::Catalog&)>& prepare = {}) {
    clink::sql::Catalog catalog;
    clink::sql::ScriptRunOptions opts;
    opts.parallelism = parallelism;
    std::ostringstream out;
    std::ostringstream err;
    const clink::sql::ScriptIO io{&out, &err};
    std::vector<clink::cluster::JobGraphSpec> specs;
    const auto submit = [&specs](const clink::cluster::JobGraphSpec& spec,
                                 const std::string& /*name*/) {
        specs.push_back(spec);
        return 0;
    };
    if (clink::sql::run_script(ddl, catalog, opts, io, submit) != 0) {
        throw std::runtime_error("the DDL did not compile: " + err.str());
    }
    if (prepare) {
        prepare(catalog);
    }
    if (!sql.empty() && clink::sql::run_script(sql, catalog, opts, io, submit) != 0) {
        throw std::runtime_error("the script did not compile: " + err.str());
    }
    return specs;
}

std::vector<const clink::cluster::OperatorSpec*> ops_of_type(
    const clink::cluster::JobGraphSpec& spec, const std::string& type) {
    std::vector<const clink::cluster::OperatorSpec*> out;
    for (const auto& op : spec.ops) {
        if (op.type == type) {
            out.push_back(&op);
        }
    }
    return out;
}

bool has_op(const clink::cluster::JobGraphSpec& spec, const std::string& type) {
    return !ops_of_type(spec, type).empty();
}

// --- The type mapping, restated for the comparison --------------------------------

// The comma-separated arguments of a type, at the top level only.
std::vector<std::string> split_args(std::string_view args) {
    std::vector<std::string> out;
    int depth = 0;
    bool quoted = false;
    std::string current;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const char c = args[i];
        if (quoted) {
            current += c;
            if (c == '\\' && i + 1 < args.size()) {
                current += args[++i];
            } else if (c == '\'') {
                quoted = false;
            }
            continue;
        }
        if (c == '\'') {
            quoted = true;
        } else if (c == '(' || c == '<') {
            ++depth;
        } else if (c == ')' || c == '>') {
            --depth;
        } else if (c == ',' && depth == 0) {
            out.push_back(current);
            current.clear();
            continue;
        }
        if (!(current.empty() && c == ' ')) {
            current += c;
        }
    }
    if (!current.empty()) {
        out.push_back(current);
    }
    return out;
}

// "Name(args)" -> args, when `type` has that wrapper.
std::optional<std::string> inner_of(const std::string& type, std::string_view wrapper) {
    const std::string open = std::string(wrapper) + "(";
    if (type.starts_with(open) && type.ends_with(")")) {
        return type.substr(open.size(), type.size() - open.size() - 1);
    }
    return std::nullopt;
}

std::string quoted_text(std::string_view text) {
    std::string out = "'";
    for (const char c : text) {
        if (c == '\'' || c == '\\') {
            out += '\\';
        }
        out += c;
    }
    return out + "'";
}

std::string as_text(std::string_view text, bool nested) {
    return nested ? quoted_text(text) : std::string(text);
}

template <typename T>
std::string shortest(T value) {
    std::array<char, 64> buf{};
    const auto res = std::to_chars(buf.data(), buf.data() + buf.size(), value);
    return {buf.data(), res.ptr};
}

std::int64_t floor_div(std::int64_t a, std::int64_t b) {
    std::int64_t q = a / b;
    if (a % b != 0 && ((a < 0) != (b < 0))) {
        --q;
    }
    return q;
}

std::int64_t pow10(int n) {
    std::int64_t v = 1;
    for (int i = 0; i < n; ++i) {
        v *= 10;
    }
    return v;
}

std::string civil_text(std::int64_t days) {
    const std::chrono::year_month_day ymd{
        std::chrono::sys_days{std::chrono::days{static_cast<int>(days)}}};
    std::array<char, 32> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  "%04d-%02u-%02u",
                  static_cast<int>(ymd.year()),
                  static_cast<unsigned>(ymd.month()),
                  static_cast<unsigned>(ymd.day()));
    return buf.data();
}

std::string civil_time_text(std::int64_t seconds) {
    const std::int64_t days = floor_div(seconds, 86400);
    const std::int64_t of_day = seconds - (days * 86400);
    std::array<char, 16> buf{};
    std::snprintf(buf.data(),
                  buf.size(),
                  " %02d:%02d:%02d",
                  static_cast<int>(of_day / 3600),
                  static_cast<int>(of_day / 60 % 60),
                  static_cast<int>(of_day % 60));
    return civil_text(days) + buf.data();
}

std::int64_t days_of(int y, unsigned m, unsigned d) {
    return std::chrono::sys_days{std::chrono::year_month_day{std::chrono::year{y},
                                                             std::chrono::month{m},
                                                             std::chrono::day{d}}}
        .time_since_epoch()
        .count();
}

// An integer cell: a JSON integer, or the decimal text the collect sink
// renders for a column its batcher carries as text.
std::int64_t integer_of(const JsonValue& v) {
    if (v.is_integral_number()) {
        return v.as_int();
    }
    if (v.is_string()) {
        std::int64_t out = 0;
        const std::string& s = v.as_string();
        const auto res = std::from_chars(s.data(), s.data() + s.size(), out);
        if (res.ec == std::errc{} && res.ptr == s.data() + s.size()) {
            return out;
        }
    }
    throw std::invalid_argument("not an integer cell: " + v.serialize(0));
}

// A DATE cell: days since 1970-01-01, or text YYYY-MM-DD.
std::int64_t days_value(const JsonValue& v) {
    if (v.is_string() && v.as_string().size() >= 10 &&
        v.as_string()[v.as_string().size() - 3] == '-') {
        const std::string& s = v.as_string();
        const std::size_t dash = s.find('-', 1);
        return days_of(std::stoi(s.substr(0, dash)),
                       static_cast<unsigned>(std::stoi(s.substr(dash + 1, 2))),
                       static_cast<unsigned>(std::stoi(s.substr(dash + 4, 2))));
    }
    return integer_of(v);
}

// A composite cell: the collect sink carries ARRAY (other than REAL ARRAY),
// MAP and ROW columns as their JSON text.
JsonValue composite_of(const JsonValue& v) {
    return v.is_string() ? clink::config::parse(v.as_string()) : v;
}

// What the fake renders for `cell`, a value of SQL type `sql`, written to a
// column of ClickHouse type `target`, under the sink's conversion rules.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::string render(const std::string& sql,
                   const std::string& target,
                   const JsonValue& cell,
                   bool nested) {
    if (const auto inner = inner_of(target, "Nullable")) {
        return cell.is_null() ? "NULL" : render(sql, *inner, cell, nested);
    }
    if (const auto inner = inner_of(target, "LowCardinality")) {
        return render(sql, *inner, cell, nested);
    }
    if (cell.is_null()) {
        return "<null into " + target + ">";
    }
    if (const auto inner = inner_of(target, "Array")) {
        const std::string element_sql = sql.substr(0, sql.size() - 2);  // "T[]"
        std::string out = "[";
        bool first = true;
        // Held in a local: a range-for over a member of a temporary dangles
        // before the loop body runs, unless the compiler extends the
        // temporary's life, which some do and others do not.
        const JsonValue elements = composite_of(cell);
        for (const auto& e : elements.as_array()) {
            out += (first ? "" : ",") + render(element_sql, *inner, e, true);
            first = false;
        }
        return out + "]";
    }
    if (const auto inner = inner_of(target, "Map")) {
        const auto kv = split_args(*inner);
        const auto sql_kv = split_args(sql.substr(4, sql.size() - 5));  // "MAP<K, V>"
        std::string out = "{";
        bool first = true;
        const JsonValue entries = composite_of(cell);
        for (const auto& [k, v] : entries.as_object()) {
            out += (first ? "" : ",") + render(sql_kv[0], kv[0], JsonValue{k}, true) + ":" +
                   render(sql_kv[1], kv[1], v, true);
            first = false;
        }
        return out + "}";
    }
    if (const auto inner = inner_of(target, "Tuple")) {
        const auto fields = split_args(*inner);
        const auto sql_fields = split_args(sql.substr(4, sql.size() - 5));  // "ROW<f T, ...>"
        const JsonValue object = composite_of(cell);
        std::string out = "(";
        for (std::size_t i = 0; i < fields.size(); ++i) {
            const std::size_t space = fields[i].find(' ');
            const std::string name = fields[i].substr(0, space);
            const std::string type = fields[i].substr(space + 1);
            const std::string field_sql = sql_fields[i].substr(sql_fields[i].find(' ') + 1);
            const auto& members = object.as_object();
            const auto it = members.find(name);
            out += (i == 0 ? "" : ",") +
                   render(field_sql, type, it == members.end() ? JsonValue{} : it->second, true);
        }
        return out + ")";
    }
    if (const auto inner = inner_of(target, "DateTime64")) {
        // Every TIMESTAMP(p) reaches the sink as epoch milliseconds.
        const int precision = std::stoi(split_args(*inner)[0]);
        const std::int64_t ms = integer_of(cell);
        const std::int64_t ticks =
            precision >= 3 ? ms * pow10(precision - 3) : ms / pow10(3 - precision);
        const std::int64_t scale = pow10(precision);
        const std::int64_t seconds = floor_div(ticks, scale);
        std::string out = civil_time_text(seconds);
        if (precision > 0) {
            std::string fraction = std::to_string(ticks - (seconds * scale));
            fraction.insert(0, static_cast<std::size_t>(precision) - fraction.size(), '0');
            out += "." + fraction;
        }
        return as_text(out, nested);
    }
    if (target.starts_with("DateTime")) {
        return as_text(civil_time_text(integer_of(cell) / 1000), nested);
    }
    if (target == "Date" || target == "Date32") {
        return as_text(civil_text(days_value(cell)), nested);
    }
    if (const auto inner = inner_of(target, "Decimal")) {
        // The collect sink hands the declared scale's text; the target's scale
        // is wider, so the value gains trailing zeros and nothing else.
        const int target_scale = std::stoi(split_args(*inner)[1]);
        std::string text = cell.as_string();
        const std::size_t dot = text.find('.');
        const int have = dot == std::string::npos ? 0 : static_cast<int>(text.size() - dot - 1);
        if (dot == std::string::npos && target_scale > 0) {
            text += '.';
        }
        text.append(static_cast<std::size_t>(target_scale - have), '0');
        return text;
    }
    if (const auto inner = inner_of(target, "FixedString")) {
        std::string text = cell.as_string();
        text.resize(static_cast<std::size_t>(std::stoi(*inner)), '\0');
        return as_text(text, nested);
    }
    if (target == "IPv6") {
        std::string text = cell.as_string();
        if (text.find(':') == std::string::npos) {
            text = "::ffff:" + text;
        }
        in6_addr address{};
        if (::inet_pton(AF_INET6, text.c_str(), &address) != 1) {
            return "<not an IPv6 address: " + text + ">";
        }
        std::array<char, INET6_ADDRSTRLEN> buf{};
        ::inet_ntop(AF_INET6, &address, buf.data(), buf.size());
        return as_text(buf.data(), nested);
    }
    if (target == "String" || target == "UUID" || target == "IPv4" || target.starts_with("Enum")) {
        return as_text(cell.as_string(), nested);
    }
    if (target == "Bool") {
        return cell.as_bool() ? "true" : "false";
    }
    if (target == "UInt8" && sql == "BOOLEAN") {
        return cell.as_bool() ? "1" : "0";
    }
    if (target == "Float32") {
        return shortest(static_cast<float>(cell.as_number()));
    }
    if (target == "Float64") {
        // A REAL arrives as the float it is, widened exactly.
        return sql == "REAL" ? shortest(static_cast<double>(static_cast<float>(cell.as_number())))
                             : shortest(cell.as_number());
    }
    if (target.starts_with("Int") || target.starts_with("UInt")) {
        return std::to_string(integer_of(cell));
    }
    return "<no mapping for " + sql + " into " + target + ">";
}

// One collected cell as a JSON value: integers exact, a REAL as its float
// widened, DECIMAL as its text at the declared scale, text as text.
JsonValue collected_cell(const arrow::Array& column, std::int64_t row) {
    if (column.IsNull(row)) {
        return JsonValue{};
    }
    switch (column.type_id()) {
        case arrow::Type::INT64:
            return JsonValue{static_cast<const arrow::Int64Array&>(column).Value(row)};
        case arrow::Type::INT32:
            return JsonValue{static_cast<std::int64_t>(
                static_cast<const arrow::Int32Array&>(column).Value(row))};
        case arrow::Type::FLOAT:
            return JsonValue{
                static_cast<double>(static_cast<const arrow::FloatArray&>(column).Value(row))};
        case arrow::Type::DOUBLE:
            return JsonValue{static_cast<const arrow::DoubleArray&>(column).Value(row)};
        case arrow::Type::BOOL:
            return JsonValue{static_cast<const arrow::BooleanArray&>(column).Value(row)};
        case arrow::Type::STRING:
            return JsonValue{
                std::string(static_cast<const arrow::StringArray&>(column).GetView(row))};
        case arrow::Type::DECIMAL128:
            return JsonValue{static_cast<const arrow::Decimal128Array&>(column).FormatValue(row)};
        case arrow::Type::LIST: {
            const auto& list = static_cast<const arrow::ListArray&>(column);
            const auto values = list.value_slice(row);
            JsonArray out;
            for (std::int64_t i = 0; i < values->length(); ++i) {
                out.push_back(collected_cell(*values, i));
            }
            return JsonValue{std::move(out)};
        }
        default:
            throw std::invalid_argument("the collect sink handed an unexpected " +
                                        column.type()->ToString());
    }
}

// --- The differential's table -----------------------------------------------------

struct Column {
    std::string name;
    std::string sql;
    std::string target;
};

// Every V1 SQL type, most of them into more than one target. TINYINT has no
// DDL spelling, so no SQL table can declare it.
const std::vector<Column>& differential_columns() {
    static const std::vector<Column> columns = {
        {"id", "BIGINT", "Int64"},
        {"big", "BIGINT", "Nullable(Int64)"},
        {"wide", "BIGINT", "Int128"},
        {"i", "INTEGER", "Nullable(Int32)"},
        {"iu", "INTEGER", "UInt32"},
        {"sm", "SMALLINT", "Int16"},
        {"smw", "SMALLINT", "Int64"},
        {"flag", "BOOLEAN", "Bool"},
        {"flag8", "BOOLEAN", "Nullable(UInt8)"},
        {"r", "REAL", "Float32"},
        {"rw", "REAL", "Float64"},
        {"d", "DOUBLE", "Nullable(Float64)"},
        {"v", "VARCHAR", "String"},
        {"vn", "VARCHAR", "Nullable(String)"},
        {"lc", "VARCHAR", "LowCardinality(String)"},
        {"fs", "VARCHAR", "FixedString(12)"},
        {"en", "VARCHAR", "Enum8('red' = 1, 'green' = 2, 'blue' = -3)"},
        {"u", "VARCHAR", "UUID"},
        {"ip4", "VARCHAR", "IPv4"},
        {"ip6", "VARCHAR", "IPv6"},
        {"dt", "DATE", "Date32"},
        {"dd", "DATE", "Date"},
        {"t0", "TIMESTAMP(0)", "DateTime64(0)"},
        {"t3", "TIMESTAMP(3)", "Nullable(DateTime64(3))"},
        {"t6", "TIMESTAMP(6)", "DateTime64(6, 'UTC')"},
        {"t9", "TIMESTAMP(9)", "DateTime64(9)"},
        {"tz", "TIMESTAMP(3) WITH TIME ZONE", "DateTime64(3, 'Asia/Tokyo')"},
        {"tdt", "TIMESTAMP(3)", "DateTime"},
        {"dec", "DECIMAL(10,2)", "Nullable(Decimal(12, 4))"},
        {"arr", "BIGINT[]", "Array(Int64)"},
        {"arrn", "BIGINT[]", "Array(Nullable(Int64))"},
        {"farr", "REAL[]", "Array(Float32)"},
        {"sarr", "VARCHAR[]", "Array(String)"},
        {"m", "MAP<VARCHAR, BIGINT>", "Map(String, Int64)"},
        {"rw2", "ROW<a BIGINT, b VARCHAR>", "Tuple(a Int64, b String)"},
    };
    return columns;
}

std::string column_ddl(const std::vector<Column>& columns = differential_columns()) {
    std::string out = "(";
    bool first = true;
    for (const auto& c : columns) {
        out += (first ? "" : ", ") + c.name + " " + c.sql;
        first = false;
    }
    return out + ")";
}

std::string column_list(const std::string& alias = "",
                        const std::vector<Column>& columns = differential_columns()) {
    std::string out;
    bool first = true;
    for (const auto& c : columns) {
        out += (first ? "" : ", ") + (alias.empty() ? "" : alias + ".") + c.name;
        first = false;
    }
    return out;
}

// The SQL type the mapping keys on: "T[]" for arrays, the spelling otherwise,
// with the precision of a TIMESTAMP and the time zone dropped.
std::string mapping_sql(const Column& c) {
    if (c.sql.starts_with("TIMESTAMP")) {
        return "TIMESTAMP";
    }
    if (c.sql.starts_with("DECIMAL")) {
        return "DECIMAL";
    }
    return c.sql;
}

constexpr std::int64_t kTwo53Plus1 = 9007199254740993;  // 2^53 + 1, not a double
constexpr std::int64_t k1900Ms = -2208988800000;        // 1900-01-01 00:00:00
constexpr std::int64_t k2299Ms = 10413791999999;        // 2299-12-31 23:59:59.999
constexpr std::int64_t kNanosMaxMs = 9223372036854;     // the last ms DateTime64(9) holds

const std::vector<std::string>& unicode_texts() {
    static const std::vector<std::string> texts = {
        "plain",
        "",
        "caf\xC3\xA9 cr\xC3\xA8me",
        "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E",
        "emoji \xF0\x9F\x9A\x80\xF0\x9F\x8C\x8D",
        "it's a \"quote\" and a \\ backslash",
        "tab\tnew\nline",
        "\xD0\x9F\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82",
    };
    return texts;
}

// Short enough for FixedString(12) in bytes.
const std::vector<std::string>& fixed_texts() {
    static const std::vector<std::string> texts = {
        "", "a", "abc", "caf\xC3\xA9", "\xE6\x97\xA5\xE6\x9C\xAC", "twelve bytes"};
    return texts;
}

// One generated row. Every value fits every target its column feeds; the
// nullable columns take NULL on their own stride.
JsonObject generated_row(std::int64_t id, std::mt19937_64& rng) {
    const auto uniform = [&rng](std::int64_t lo, std::int64_t hi) {
        return std::uniform_int_distribution<std::int64_t>(lo, hi)(rng);
    };
    const auto pick = [&](const std::vector<std::string>& from) {
        return from[static_cast<std::size_t>(
            uniform(0, static_cast<std::int64_t>(from.size()) - 1))];
    };
    const auto date_cell = [&](std::int64_t lo, std::int64_t hi) {
        const std::int64_t days = uniform(lo, hi);
        // Half as days since the epoch, half as text, the two forms DATE takes.
        return (id % 2 == 0) ? JsonValue{days} : JsonValue{civil_text(days)};
    };
    const auto ms_cell = [&](std::int64_t lo, std::int64_t hi, std::int64_t unit) {
        const std::int64_t ms = uniform(lo / unit, hi / unit) * unit;
        // A digit string is the other form the engine's convention allows.
        return (id % 3 == 0) ? JsonValue{std::to_string(ms)} : JsonValue{ms};
    };
    const auto quarter = [&] { return static_cast<double>(uniform(-4'000'000, 4'000'000)) / 4.0; };
    const auto max64 = std::numeric_limits<std::int64_t>::max();
    const auto min64 = std::numeric_limits<std::int64_t>::min();

    JsonObject row;
    row["id"] = JsonValue{id};
    row["big"] = id % 5 == 1 ? JsonValue{} : JsonValue{uniform(min64, max64)};
    row["wide"] = JsonValue{uniform(min64, max64)};
    row["i"] = id % 6 == 2 ? JsonValue{} : JsonValue{uniform(-2147483648LL, 2147483647LL)};
    row["iu"] = JsonValue{uniform(0, 2147483647LL)};
    row["sm"] = JsonValue{uniform(-32768, 32767)};
    row["smw"] = JsonValue{uniform(-32768, 32767)};
    row["flag"] = JsonValue{uniform(0, 1) == 1};
    row["flag8"] = id % 7 == 3 ? JsonValue{} : JsonValue{uniform(0, 1) == 1};
    row["r"] = JsonValue{quarter()};
    row["rw"] = JsonValue{quarter()};
    row["d"] = id % 8 == 4 ? JsonValue{}
                           : JsonValue{std::uniform_real_distribution<double>(-1e12, 1e12)(rng)};
    row["v"] = JsonValue{pick(unicode_texts()) + " #" + std::to_string(id)};
    row["vn"] = id % 4 == 0 ? JsonValue{} : JsonValue{pick(unicode_texts())};
    row["lc"] = JsonValue{pick({"alpha", "beta", "gamma"})};
    row["fs"] = JsonValue{pick(fixed_texts())};
    row["en"] = JsonValue{pick({"red", "green", "blue"})};
    {
        std::array<char, 40> buf{};
        const auto hi = static_cast<std::uint64_t>(uniform(min64, max64));
        const auto lo = static_cast<std::uint64_t>(uniform(min64, max64));
        std::snprintf(buf.data(),
                      buf.size(),
                      "%08llx-%04llx-%04llx-%04llx-%012llx",
                      static_cast<unsigned long long>(hi >> 32),
                      static_cast<unsigned long long>((hi >> 16) & 0xffffULL),
                      static_cast<unsigned long long>(hi & 0xffffULL),
                      static_cast<unsigned long long>(lo >> 48),
                      static_cast<unsigned long long>(lo & 0xffffffffffffULL));
        row["u"] = JsonValue{std::string(buf.data())};
    }
    row["ip4"] =
        JsonValue{std::to_string(uniform(1, 255)) + "." + std::to_string(uniform(0, 255)) + "." +
                  std::to_string(uniform(0, 255)) + "." + std::to_string(uniform(0, 255))};
    {
        std::array<char, 64> buf{};
        std::snprintf(buf.data(),
                      buf.size(),
                      "2001:db8:%llx:%llx::%llx",
                      static_cast<unsigned long long>(uniform(0, 0xffff)),
                      static_cast<unsigned long long>(uniform(0, 0xffff)),
                      static_cast<unsigned long long>(uniform(1, 0xffff)));
        row["ip6"] = JsonValue{std::string(buf.data())};
    }
    row["dt"] = date_cell(-25567, 120529);
    row["dd"] = date_cell(0, 65535);
    row["t0"] = ms_cell(k1900Ms, k2299Ms, 1000);
    row["t3"] = id % 9 == 5 ? JsonValue{} : ms_cell(k1900Ms, k2299Ms, 1);
    row["t6"] = ms_cell(k1900Ms, k2299Ms, 1);
    row["t9"] = ms_cell(k1900Ms, kNanosMaxMs, 1);
    row["tz"] = ms_cell(k1900Ms, k2299Ms, 1);
    row["tdt"] = ms_cell(0, 4294967295000LL, 1000);
    row["dec"] = id % 10 == 6
                     ? JsonValue{}
                     : JsonValue{static_cast<double>(uniform(-9999999999LL, 9999999999LL)) / 100.0};
    {
        JsonArray arr;
        JsonArray arrn;
        JsonArray farr;
        JsonArray sarr;
        const auto n = uniform(0, 4);
        for (std::int64_t k = 0; k < n; ++k) {
            arr.emplace_back(uniform(min64, max64));
            arrn.push_back(k % 2 == 1 ? JsonValue{} : JsonValue{uniform(min64, max64)});
            farr.emplace_back(quarter());
            sarr.emplace_back(pick(unicode_texts()));
        }
        row["arr"] = JsonValue{std::move(arr)};
        row["arrn"] = JsonValue{std::move(arrn)};
        row["farr"] = JsonValue{std::move(farr)};
        row["sarr"] = JsonValue{std::move(sarr)};
    }
    {
        JsonObject m;
        const auto n = uniform(0, 3);
        for (std::int64_t k = 0; k < n; ++k) {
            m["k" + std::to_string(k)] = JsonValue{uniform(min64, max64)};
        }
        row["m"] = JsonValue{std::move(m)};
    }
    {
        JsonObject r;
        r["a"] = JsonValue{uniform(min64, max64)};
        r["b"] = JsonValue{pick(unicode_texts())};
        row["rw2"] = JsonValue{std::move(r)};
    }
    return row;
}

// The edges, each on a row of its own over a generated base: 2^53+1 where it
// fits, the extremes of every range, negative epochs, nulls and unicode.
std::vector<JsonObject> edge_rows(std::mt19937_64& rng, std::int64_t& next_id) {
    std::vector<JsonObject> rows;
    const auto base = [&] { return generated_row(next_id++, rng); };
    {
        JsonObject r = base();
        r["big"] = JsonValue{kTwo53Plus1};
        r["wide"] = JsonValue{-kTwo53Plus1};
        r["i"] = JsonValue{static_cast<std::int64_t>(2147483647)};
        r["sm"] = JsonValue{static_cast<std::int64_t>(32767)};
        r["smw"] = JsonValue{static_cast<std::int64_t>(-32768)};
        r["d"] = JsonValue{1e-7};
        r["r"] = JsonValue{0.1};
        r["rw"] = JsonValue{1.23456789};
        r["dt"] = JsonValue{std::string("1900-01-01")};
        r["dd"] = JsonValue{static_cast<std::int64_t>(0)};
        r["t0"] = JsonValue{k1900Ms};
        r["t3"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["t6"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["t9"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["tz"] = JsonValue{std::string("-86400001")};
        r["tdt"] = JsonValue{static_cast<std::int64_t>(0)};
        r["dec"] = JsonValue{-0.05};
        r["arr"] = JsonValue{JsonArray{JsonValue{kTwo53Plus1}, JsonValue{-kTwo53Plus1}}};
        r["ip6"] = JsonValue{std::string("10.1.2.3")};
        rows.push_back(std::move(r));
    }
    {
        JsonObject r = base();
        r["big"] = JsonValue{std::numeric_limits<std::int64_t>::max()};
        r["wide"] = JsonValue{std::numeric_limits<std::int64_t>::min()};
        r["i"] = JsonValue{static_cast<std::int64_t>(-2147483648LL)};
        r["iu"] = JsonValue{static_cast<std::int64_t>(2147483647)};
        r["sm"] = JsonValue{static_cast<std::int64_t>(-32768)};
        r["d"] = JsonValue{1e300};
        r["r"] = JsonValue{1e-7};
        r["dt"] = JsonValue{static_cast<std::int64_t>(120529)};
        r["dd"] = JsonValue{std::string("2149-06-06")};
        r["t0"] = JsonValue{std::string("10413791999000")};
        r["t3"] = JsonValue{k2299Ms};
        r["t6"] = JsonValue{k1900Ms};
        r["t9"] = JsonValue{kNanosMaxMs};
        r["tdt"] = JsonValue{static_cast<std::int64_t>(4294967295000LL)};
        r["dec"] = JsonValue{99999999.99};
        r["fs"] = JsonValue{std::string("twelve bytes")};
        r["v"] = JsonValue{std::string("")};
        r["arr"] = JsonValue{JsonArray{}};
        r["m"] = JsonValue{JsonObject{}};
        rows.push_back(std::move(r));
    }
    {
        JsonObject r = base();
        r["big"] = JsonValue{};
        r["i"] = JsonValue{};
        r["flag8"] = JsonValue{};
        r["d"] = JsonValue{};
        r["vn"] = JsonValue{};
        r["t3"] = JsonValue{};
        r["dec"] = JsonValue{};
        r["v"] = JsonValue{std::string("\xF0\x9F\x8E\x89 f\xC3\xAAte \xE6\x97\xA5\xE6\x9C\xAC")};
        r["dt"] = JsonValue{std::string("1969-07-20")};
        r["t0"] = JsonValue{static_cast<std::int64_t>(-1000)};
        r["arrn"] = JsonValue{JsonArray{JsonValue{}, JsonValue{kTwo53Plus1}, JsonValue{}}};
        r["sarr"] =
            JsonValue{JsonArray{JsonValue{std::string("a'b")}, JsonValue{std::string("c\\d")}}};
        JsonObject m;
        m["\xC3\xA9t\xC3\xA9"] = JsonValue{kTwo53Plus1};
        m["it's"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["m"] = JsonValue{std::move(m)};
        JsonObject rw;
        rw["a"] = JsonValue{kTwo53Plus1};
        rw["b"] = JsonValue{std::string("\xF0\x9F\x9A\x80 'q'")};
        r["rw2"] = JsonValue{std::move(rw)};
        rows.push_back(std::move(r));
    }
    return rows;
}

// Keeps only `columns` of a generated row. With `temporal_as_text`, every
// DATE becomes its YYYY-MM-DD text and every TIMESTAMP its digit string: the
// two forms that a text-carried column decodes columnar.
std::string line_of(JsonObject row, const std::vector<Column>& columns, bool temporal_as_text) {
    JsonObject kept;
    for (const auto& c : columns) {
        JsonValue v = row[c.name];
        if (temporal_as_text && !v.is_null() && !v.is_string()) {
            if (c.sql == "DATE") {
                v = JsonValue{civil_text(v.as_int())};
            } else if (c.sql.starts_with("TIMESTAMP")) {
                v = JsonValue{std::to_string(v.as_int())};
            }
        }
        kept[c.name] = std::move(v);
    }
    return JsonValue{std::move(kept)}.serialize(0);
}

std::vector<std::string> differential_lines(
    std::size_t generated,
    const std::vector<Column>& columns = differential_columns(),
    bool temporal_as_text = false) {
    std::mt19937_64 rng(20261001);
    std::int64_t next_id = 1;
    std::vector<std::string> lines;
    for (auto& r : edge_rows(rng, next_id)) {
        lines.push_back(line_of(std::move(r), columns, temporal_as_text));
    }
    for (std::size_t k = 0; k < generated; ++k) {
        lines.push_back(line_of(generated_row(next_id++, rng), columns, temporal_as_text));
    }
    return lines;
}

fake::FakeTable differential_target(const std::vector<Column>& cols = differential_columns()) {
    std::vector<std::pair<std::string, std::string>> columns;
    for (const auto& c : cols) {
        columns.emplace_back(c.name, c.target);
    }
    return fake_table("events", columns);
}

const std::string kNativeWith =
    "connector='clickhouse', insert_format='native', host='fake', database='db', table='events'";

// Collected rows by id: one JSON cell per declared column.
using CollectedRows = std::map<std::int64_t, std::map<std::string, JsonValue>>;

// Runs `ddl` plus a collect table `out` and `insert` (which writes to `out`)
// through an EmbeddedEngine, and returns what the collect reader saw.
CollectedRows run_into_collect(const std::string& ddl,
                               const std::string& insert,
                               const std::vector<Column>& columns = differential_columns()) {
    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    if (engine.execute_script(ddl + "CREATE TABLE out " + column_ddl(columns) +
                              " WITH (connector='collect');") != 0) {
        throw std::runtime_error("collect DDL: " + err.str());
    }
    auto reader = engine.collect_reader("out").ValueOrDie();
    if (engine.execute_script(insert) != 0) {
        throw std::runtime_error("collect INSERT: " + err.str());
    }
    CollectedRows rows;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto st = reader->ReadNext(&batch);
        if (!st.ok()) {
            throw std::runtime_error("collect read: " + st.ToString());
        }
        if (!batch) {
            break;
        }
        const auto& schema = *batch->schema();
        for (std::int64_t r = 0; r < batch->num_rows(); ++r) {
            std::map<std::string, JsonValue> cells;
            for (int c = 0; c < batch->num_columns(); ++c) {
                cells[schema.field(c)->name()] = collected_cell(*batch->column(c), r);
            }
            const std::int64_t id = cells.at("id").as_int();
            if (!rows.emplace(id, std::move(cells)).second) {
                throw std::runtime_error("the collect sink saw id " + std::to_string(id) +
                                         " twice");
            }
        }
    }
    if (!engine.await_all()) {
        throw std::runtime_error("collect job: " + err.str());
    }
    return rows;
}

// The INSERT's column list, in order, from its statement text.
std::vector<std::string> insert_columns(const std::string& sql) {
    const std::size_t open = sql.find('(');
    const std::size_t close = sql.find(')', open);
    std::vector<std::string> out;
    for (auto name : split_args(sql.substr(open + 1, close - open - 1))) {
        std::erase(name, '`');
        std::erase(name, '"');
        out.push_back(name);
    }
    return out;
}

// Runs `ddl` plus the native table `ch` and `insert` through an
// EmbeddedEngine into the fake, and returns the landed rows by id, each
// column's rendering keyed by name.
std::map<std::int64_t, std::map<std::string, std::string>> run_into_native(
    const FakeServerScope& fake,
    const std::string& ddl,
    const std::string& insert,
    const std::vector<Column>& columns = differential_columns()) {
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        if (engine.execute_script(ddl + "CREATE TABLE ch " + column_ddl(columns) + " WITH (" +
                                  kNativeWith + ");") != 0) {
            throw std::runtime_error("native DDL: " + err.str());
        }
        if (engine.execute_script(insert) != 0) {
            throw std::runtime_error("native INSERT: " + err.str());
        }
        if (!engine.await_all()) {
            std::string joined;
            for (const auto& e : engine.errors()) {
                joined += e + "\n";
            }
            throw std::runtime_error("native job failed:\n" + joined + err.str());
        }
    }
    const auto inserts = fake.server().inserts("db.events");
    if (inserts.empty()) {
        throw std::runtime_error("the native sink sent no INSERT");
    }
    const auto names = insert_columns(inserts.front().sql);
    std::map<std::int64_t, std::map<std::string, std::string>> rows;
    for (const auto& block : fake.server().landed("db.events")) {
        for (const auto& values : block.values) {
            std::map<std::string, std::string> cells;
            for (std::size_t c = 0; c < names.size() && c < values.size(); ++c) {
                cells[names[c]] = values[c];
            }
            const std::int64_t id = std::stoll(cells.at("id"));
            if (!rows.emplace(id, std::move(cells)).second) {
                throw std::runtime_error("the fake landed id " + std::to_string(id) + " twice");
            }
        }
    }
    return rows;
}

// The differential itself: the same rows, each column's landed text against
// the mapping applied to what the collect sink saw.
void expect_same_values(const CollectedRows& collected,
                        const std::map<std::int64_t, std::map<std::string, std::string>>& landed,
                        std::size_t expected_rows,
                        const std::vector<Column>& columns = differential_columns()) {
    ASSERT_EQ(collected.size(), expected_rows);
    ASSERT_EQ(landed.size(), expected_rows);
    std::size_t mismatches = 0;
    for (const auto& [id, cells] : collected) {
        const auto it = landed.find(id);
        ASSERT_NE(it, landed.end()) << "id " << id << " never landed";
        for (const auto& c : columns) {
            const auto got = it->second.find(c.name);
            ASSERT_NE(got, it->second.end()) << "the INSERT has no column " << c.name;
            const std::string want = render(mapping_sql(c), c.target, cells.at(c.name), false);
            if (got->second != want && ++mismatches <= 20) {
                ADD_FAILURE() << "id " << id << ", column " << c.name << " (" << c.sql << " into "
                              << c.target << "): landed '" << got->second << "', collected "
                              << cells.at(c.name).serialize(0) << " maps to '" << want << "'";
            }
        }
    }
    EXPECT_EQ(mismatches, 0U);
}

constexpr std::size_t kGeneratedRows = 400;
constexpr std::size_t kEdgeRows = 3;

// --- The Row-form differential ------------------------------------------------

// A file source straight into each sink: the Row values the native sink
// receives are the ones json decoding produced.
TEST(ClickHouseNativeSql, LandsWhatTheCollectSinkSeesForEveryType) {
    ensure_installed();
    const ScratchDir dir("diff_row");
    write_lines(dir.path() / "in.ndjson", differential_lines(kGeneratedRows));
    const std::string ddl = "CREATE TABLE src " + column_ddl() +
                            " WITH (connector='file', format='json', path='" +
                            (dir.path() / "in.ndjson").string() + "');";

    const auto plan =
        compile_script(ddl + "CREATE TABLE ch " + column_ddl() + " WITH (" + kNativeWith + ");",
                       "INSERT INTO ch SELECT " + column_list() + " FROM src;",
                       1);
    ASSERT_EQ(plan.size(), 1U);
    ASSERT_TRUE(has_op(plan[0], "clickhouse_native_sink"));

    const auto collected =
        run_into_collect(ddl, "INSERT INTO out SELECT " + column_list() + " FROM src;");
    const FakeServerScope fake(differential_target());
    const auto landed =
        run_into_native(fake, ddl, "INSERT INTO ch SELECT " + column_list() + " FROM src;");
    expect_same_values(collected, landed, kGeneratedRows + kEdgeRows);
}

// An inner join whose output is born columnar, then the projection: the
// planner promotes the join's columnar output because the projection ingests
// it, and the sink boundary's row_bind_columns then rebuilds rows from that
// batch, so the native sink receives rows decoded from Arrow rather than from
// JSON. The planner always puts row_bind_columns last, so the projection
// before it is the last columnar producer a SQL plan can have.
TEST(ClickHouseNativeSql, LandsWhatTheCollectSinkSeesBehindAColumnarJoin) {
    ensure_installed();
    const ScratchDir dir("diff_join");
    const auto lines = differential_lines(kGeneratedRows);
    write_lines(dir.path() / "in.ndjson", lines);
    std::vector<std::string> keys;
    for (std::size_t k = 1; k <= lines.size(); ++k) {
        keys.push_back(R"({"k":)" + std::to_string(k) + "}");
    }
    write_lines(dir.path() / "keys.ndjson", keys);
    const std::string ddl =
        "CREATE TABLE src " + column_ddl() + " WITH (connector='file', format='json', path='" +
        (dir.path() / "in.ndjson").string() +
        "'); CREATE TABLE keys (k BIGINT) WITH (connector='file', format='json', "
        "path='" +
        (dir.path() / "keys.ndjson").string() + "');";
    const std::string select =
        "SELECT " + column_list("a") + " FROM src a JOIN keys b ON a.id = b.k;";

    const auto plan =
        compile_script(ddl + "CREATE TABLE ch " + column_ddl() + " WITH (" + kNativeWith + ");",
                       "INSERT INTO ch " + select,
                       1);
    ASSERT_EQ(plan.size(), 1U);
    const auto joins = ops_of_type(plan[0], "equi_join_row");
    ASSERT_EQ(joins.size(), 1U);
    EXPECT_EQ(joins[0]->params.count("columnar_output"), 1U)
        << "the join no longer emits columnar output, so this case tests the row path";
    ASSERT_TRUE(has_op(plan[0], "project_row"));

    const auto collected = run_into_collect(ddl, "INSERT INTO out " + select);
    const FakeServerScope fake(differential_target());
    const auto decoded_before = clink::detail::batch_materialize_counter().load();
    const auto landed = run_into_native(fake, ddl, "INSERT INTO ch " + select);
    EXPECT_GT(clink::detail::batch_materialize_counter().load(), decoded_before)
        << "no batch was decoded from its columnar sidecar, so the rows took the row path";
    expect_same_values(collected, landed, kGeneratedRows + kEdgeRows);
}

// The columns the columnar JSON decode carries as Arrow. It decodes a batch
// columnar only when every value round-trips through its column, and takes
// the row path for REAL and DECIMAL columns and for any value that is not a
// string in a column it carries as text: SMALLINT numbers, arrays, maps and
// rows.
const std::vector<Column>& columnar_decode_columns() {
    static const std::vector<Column> columns = [] {
        std::vector<Column> out;
        for (const auto& c : differential_columns()) {
            const bool row_only = c.sql == "REAL" || c.sql.starts_with("DECIMAL") ||
                                  c.sql == "SMALLINT" || c.sql.ends_with("[]") ||
                                  c.sql.starts_with("MAP") || c.sql.starts_with("ROW");
            if (!row_only) {
                out.push_back(c);
            }
        }
        return out;
    }();
    return columns;
}

// The columnar JSON decode, the default for a Kafka table: the bridge builds
// an Arrow sidecar at decode and the projection passes it on, so here too the
// sink's rows come out of a columnar batch. DATE and TIMESTAMP values are
// written as text, the form that keeps the batch columnar.
TEST(ClickHouseNativeSql, LandsWhatTheCollectSinkSeesBehindTheColumnarDecode) {
    ensure_installed();
    const auto& columns = columnar_decode_columns();
    set_feed("diff_columnar", differential_lines(kGeneratedRows, columns, true), true);
    const std::string ddl = "CREATE TABLE src " + column_ddl(columns) +
                            " WITH (connector='kafka', format='json', topic='diff_columnar');";
    const std::string select = "SELECT " + column_list("", columns) + " FROM src;";

    const auto plan = compile_script(
        ddl + "CREATE TABLE ch " + column_ddl(columns) + " WITH (" + kNativeWith + ");",
        "INSERT INTO ch " + select,
        1);
    ASSERT_EQ(plan.size(), 1U);
    ASSERT_TRUE(has_op(plan[0], "json_string_to_row_columnar"));

    const auto collected = run_into_collect(ddl, "INSERT INTO out " + select, columns);
    const FakeServerScope fake(differential_target(columns));
    const auto decoded_before = clink::detail::batch_materialize_counter().load();
    const auto landed = run_into_native(fake, ddl, "INSERT INTO ch " + select, columns);
    EXPECT_GT(clink::detail::batch_materialize_counter().load(), decoded_before)
        << "no batch was decoded from its columnar sidecar, so the rows took the row path";
    expect_same_values(collected, landed, kGeneratedRows + kEdgeRows, columns);
}

// --- The keys the planner puts on the op ------------------------------------

struct PlannerCase {
    std::string name;
    std::string sql;
    // What makes the case the case, on the native sink op.
    std::function<void(const clink::cluster::OperatorSpec&)> check;
};

// Every param the planner, the materialised-view code and the script runner
// put on a native sink op is one the sink owns or tolerates, and the factory
// builds the op as compiled. Each script runs at parallelism 4, as a fanned
// out job would. A new planner key fails here before it refuses a user's
// table as an unknown option.
TEST(ClickHouseNativeSql, EveryPlannerKeyIsOwnedOrToleratedAndTheFactoryAcceptsTheOp) {
    ensure_installed();
    const FakeServerScope fake(fake_table("events", {{"id", "Int64"}, {"v", "Int64"}}));
    const std::string src =
        "CREATE TABLE src (id BIGINT, v BIGINT) WITH (connector='file', format='json', "
        "path='/nonexistent/src.ndjson');"
        "CREATE TABLE other (id BIGINT) WITH (connector='file', format='json', "
        "path='/nonexistent/other.ndjson');";
    const std::string native_with =
        "connector='clickhouse', format='json', insert_format='native', host='fake', database='db'";
    const auto has = [](const clink::cluster::OperatorSpec& op, const std::string& key) {
        return op.params.count(key) == 1;
    };
    const std::vector<PlannerCase> cases = {
        {"append table",
         src + "CREATE TABLE ch (id BIGINT, v BIGINT) WITH (" + native_with +
             ", table='events', mode='append', delivery_guarantee='at_least_once', "
             "batch_rows='5000');"
             "INSERT INTO ch SELECT id, v FROM src;",
         [&](const clink::cluster::OperatorSpec& op) {
             EXPECT_TRUE(has(op, "sql_column_types"));
             EXPECT_TRUE(has(op, "schema_columns"));
         }},
        {"forced singleton",
         src + "CREATE TABLE ch (id BIGINT, v BIGINT) WITH (" + native_with +
             ", table='events');"
             "INSERT INTO ch SELECT id, v FROM src WHERE id NOT IN (SELECT id FROM other);",
         [&](const clink::cluster::OperatorSpec& op) {
             EXPECT_TRUE(has(op, std::string{clink::cluster::kForcedSingletonParam}));
             EXPECT_EQ(op.parallelism, 1U);
         }},
        {"continuous materialised view",
         src + "CREATE MATERIALIZED VIEW mv WITH (freshness='0', " + native_with +
             ", mode='append', table='events') AS SELECT id, v FROM src;",
         [&](const clink::cluster::OperatorSpec& op) {
             EXPECT_TRUE(has(op, "view_kind"));
             EXPECT_TRUE(has(op, "definition_sql"));
         }},
        {"table read and written",
         src + "CREATE TABLE ch (id BIGINT, v BIGINT) WITH (" + native_with +
             ", table='events', query='SELECT id, v FROM db.events', batch_size='100');"
             "INSERT INTO ch SELECT id, v FROM src;"
             "CREATE TABLE back_out (id BIGINT, v BIGINT) WITH (connector='file', format='json', "
             "path='/nonexistent/back.ndjson');"
             "INSERT INTO back_out SELECT id, v FROM ch;",
         [&](const clink::cluster::OperatorSpec& op) {
             EXPECT_TRUE(has(op, "query"));
             EXPECT_TRUE(has(op, "batch_size"));
         }},
        {"continuous view whose backing table sets query",
         src + "CREATE MATERIALIZED VIEW mv WITH (freshness='0', " + native_with +
             ", mode='append', table='events', query='SELECT id, v FROM db.events') AS SELECT "
             "id, v FROM src;",
         [&](const clink::cluster::OperatorSpec& op) {
             EXPECT_TRUE(has(op, "view_kind"));
             EXPECT_TRUE(has(op, "query"));
         }},
    };

    const auto& own = native::own_option_keys();
    const auto& tolerated = native::pass_through_keys();
    const auto* factory = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "clickhouse_native_sink", "row");
    ASSERT_NE(factory, nullptr);
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        const auto specs = compile_script(c.sql, "", 4);
        std::size_t native_ops = 0;
        for (const auto& spec : specs) {
            for (const auto* op : ops_of_type(spec, "clickhouse_native_sink")) {
                ++native_ops;
                for (const auto& [key, value] : op->params) {
                    const bool known =
                        std::find(own.begin(), own.end(), key) != own.end() ||
                        std::find(tolerated.begin(), tolerated.end(), key) != tolerated.end();
                    EXPECT_TRUE(known)
                        << "the planner put '" << key << "' (value '" << value.substr(0, 80)
                        << "') on the op, and the sink neither owns nor tolerates it";
                }
                c.check(*op);
                for (std::uint32_t subtask = 0; subtask < op->parallelism; ++subtask) {
                    clink::cluster::OperatorBuildContext ctx;
                    ctx.params = op->params;
                    ctx.subtask_idx = subtask;
                    ctx.parallelism = op->parallelism;
                    std::shared_ptr<void> built;
                    try {
                        built = factory->build(ctx);
                    } catch (const std::exception& e) {
                        ADD_FAILURE() << "the factory refused the op as compiled: " << e.what();
                        continue;
                    }
                    const auto sink = std::static_pointer_cast<clink::Sink<clink::sql::Row>>(built);
                    EXPECT_NE(std::dynamic_pointer_cast<native::NativeSink>(sink), nullptr);
                }
            }
        }
        EXPECT_EQ(native_ops, 1U) << "the case did not compile to one native sink op";
    }
}

// --- A held barrier on the periodic path ------------------------------------

// The table every timing case writes: two columns, rows by id.
fake::FakeTable small_target() {
    return fake_table("events", {{"id", "Int64"}, {"s", "String"}});
}

std::vector<std::string> small_lines(std::int64_t count) {
    std::vector<std::string> lines;
    for (std::int64_t id = 1; id <= count; ++id) {
        lines.push_back(R"({"id":)" + std::to_string(id) + R"(,"s":"row )" + std::to_string(id) +
                        R"("})");
    }
    return lines;
}

clink::test::TestCluster::Options checkpointing_cluster(const fs::path& dir,
                                                        std::int64_t interval_ms) {
    clink::test::TestCluster::Options options;
    options.slots_per_worker = 16;
    options.checkpoint.checkpoint_dir = dir.string();
    options.checkpoint.interval_ms = interval_ms;
    return options;
}

// Polls `done` until it holds or `limit` passes.
bool wait_for(const std::function<bool()>& done, std::chrono::milliseconds limit) {
    const auto until = Clock::now() + limit;
    while (Clock::now() < until) {
        if (done()) {
            return true;
        }
        std::this_thread::sleep_for(50ms);
    }
    return done();
}

std::string joined(const std::vector<std::string>& lines) {
    std::string out;
    for (const auto& line : lines) {
        out += line + "\n";
    }
    return out;
}

// On the periodic path nothing bounds how long the sink may hold a barrier:
// one EndInsert held for 40 s, past the 30 s that bounds a bounded job's
// final checkpoint, stalls the checkpoint and fails nothing, and the
// checkpoint completes once the INSERT is acknowledged. The INSERT closes
// only at a barrier (a one-hour batch interval), so the held call is a
// barrier flush, and the receive timeout is raised past the hold, as it would
// have to be for a real server to take that long without the client giving up.
TEST(ClickHouseNativeSql, AHeldBarrierStallsThePeriodicCheckpointWithoutFailingIt) {
    ensure_installed();
    const ScratchDir dir("held_barrier");
    const FakeServerScope fake(small_target());
    std::atomic<bool> fired{false};
    std::atomic<std::int64_t> fired_at_ms{0};
    const auto start = Clock::now();
    fake::Fault hold;
    hold.step = fake::Step::End;
    hold.kind = fake::Fault::Kind::Delay;
    hold.delay = 40s;
    hold.on_fire = [&] {
        fired_at_ms.store(
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
        fired.store(true);
    };
    fake.server().inject(hold);

    constexpr std::int64_t kRows = 200;
    set_feed("held_barrier", small_lines(kRows), /*bounded=*/false);
    const auto specs = compile_script(
        "CREATE TABLE src (id BIGINT, s VARCHAR) WITH (connector='kafka', format='json', "
        "topic='held_barrier', columnar_decode='false');"
        "CREATE TABLE ch (id BIGINT, s VARCHAR) WITH (" +
            kNativeWith +
            ", batch_interval_ms='3600000', receive_timeout_ms='120000');"
            "INSERT INTO ch SELECT id, s FROM src;",
        "");
    ASSERT_EQ(specs.size(), 1U);

    clink::test::TestCluster cluster(checkpointing_cluster(dir.path(), 500));
    auto& coordinator = cluster.coordinator();
    const auto job = cluster.submit(specs[0]);

    // The hold starts when the INSERT's block has gone and its EndInsert is
    // under way.
    ASSERT_TRUE(wait_for(
        [&] {
            const auto inserts = fake.server().inserts("db.events");
            return !inserts.empty() && !inserts.front().blocks.empty();
        },
        60s))
        << "no INSERT reached the fake: " << joined(cluster.errors(job));
    const auto hold_seen = Clock::now();
    std::this_thread::sleep_for(1s);
    const std::uint64_t held = coordinator.latest_completed_checkpoint(job);

    // Past the 30 s bound, with a margin, and still well short of the 40 s
    // hold: the checkpoint has not completed, and nothing has failed.
    while (Clock::now() < hold_seen + 34s) {
        ASSERT_TRUE(cluster.errors(job).empty()) << joined(cluster.errors(job));
        ASSERT_FALSE(fired.load()) << "the hold ended early";
        ASSERT_EQ(coordinator.latest_completed_checkpoint(job), held)
            << "a checkpoint completed while the sink held its barrier";
        std::this_thread::sleep_for(250ms);
    }
    EXPECT_EQ(fake.server().rows("db.events"), 0U);

    ASSERT_TRUE(wait_for([&] { return fired.load(); }, 30s)) << "the hold never ended";
    ASSERT_TRUE(wait_for([&] { return coordinator.latest_completed_checkpoint(job) > held; }, 30s))
        << "the held checkpoint never completed: " << joined(cluster.errors(job));
    // The checkpoint waited out the whole hold, which began no later than the
    // INSERT was seen.
    EXPECT_GE(fired_at_ms.load() -
                  std::chrono::duration_cast<std::chrono::milliseconds>(hold_seen - start).count(),
              39'000);
    EXPECT_TRUE(cluster.errors(job).empty()) << joined(cluster.errors(job));
    EXPECT_EQ(fake.server().rows("db.events"), static_cast<std::uint64_t>(kRows));

    coordinator.cancel_job(job);
    ASSERT_TRUE(cluster.await_completion(job, 60s));
    const auto record = coordinator.job_history(job);
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->restart_attempts, 0U);
}

// --- A held barrier at a bounded job's end ---------------------------------

// The CTest environment sets the final-checkpoint bound to 3 s for this
// binary. Setting the same value here, without overwriting, keeps a direct
// run of the binary equivalent; the engine reads it once, on first use.
const bool kFinalCheckpointBound = [] {
    ::setenv("CLINK_EOS_FINAL_CKPT_TIMEOUT_MS", "3000", 0);
    return true;
}();

// A bounded source's end of input waits a bounded time for its final
// checkpoint, and the sink cannot tell that barrier from any other. An
// EndInsert held past the bound fails that wait: the job spends one restart,
// replays from the start, completes, and every row is in the table.
TEST(ClickHouseNativeSql, AFinalCheckpointHeldPastItsBoundCostsOneRestartAndNoRows) {
    (void)kFinalCheckpointBound;
    ensure_installed();
    ASSERT_EQ(clink::eos_final_checkpoint_timeout(), 3000ms)
        << "the case is built on a 3 s bound; CLINK_EOS_FINAL_CKPT_TIMEOUT_MS says otherwise";
    const ScratchDir dir("final_ckpt");
    constexpr std::int64_t kRows = 300;
    write_lines(dir.path() / "in.ndjson", small_lines(kRows));
    const FakeServerScope fake(small_target());
    fake::Fault hold;
    hold.step = fake::Step::End;
    hold.kind = fake::Fault::Kind::Delay;
    hold.delay = 6s;  // twice the bound
    fake.server().inject(hold);

    const auto specs = compile_script(
        "CREATE TABLE src (id BIGINT, s VARCHAR) WITH (connector='file', format='json', path='" +
            (dir.path() / "in.ndjson").string() +
            "');"
            "CREATE TABLE ch (id BIGINT, s VARCHAR) WITH (" +
            kNativeWith +
            ", batch_interval_ms='3600000');"
            "INSERT INTO ch SELECT id, s FROM src;",
        "");
    ASSERT_EQ(specs.size(), 1U);

    // A periodic interval far beyond the job's life, so the only barrier with
    // rows behind it is the final checkpoint's.
    clink::test::TestCluster cluster(checkpointing_cluster(dir.path() / "ckpt", 600'000));
    const auto job = cluster.submit(specs[0]);
    ASSERT_TRUE(cluster.await_completion(job, 120s)) << joined(cluster.errors(job));
    const auto record = cluster.coordinator().job_history(job);
    ASSERT_TRUE(record.has_value());
    EXPECT_EQ(record->status, "ok") << joined(record->errors);
    EXPECT_EQ(record->restart_attempts, 1U) << joined(record->errors);

    std::set<std::int64_t> ids;
    for (const auto& block : fake.server().landed("db.events")) {
        for (const auto& values : block.values) {
            ids.insert(std::stoll(values.at(0)));
        }
    }
    EXPECT_EQ(ids.size(), static_cast<std::size_t>(kRows));
    EXPECT_EQ(*ids.begin(), 1);
    EXPECT_EQ(*ids.rbegin(), kRows);
    // The held attempt was cut off by the restart, so it landed nothing.
    const auto inserts = fake.server().inserts("db.events");
    ASSERT_GE(inserts.size(), 2U);
    EXPECT_NE(inserts.front().outcome, fake::ReceivedInsert::Outcome::Committed);
}

// --- Timestamps and dates leave the JSON decode as written ------------------

// TIMESTAMP(p) and TIMESTAMPTZ carry epoch milliseconds whatever p, and DATE
// carries whatever the source wrote, so the decode must hand each value on
// unscaled. Both JSON bridges are pinned: json_string_to_row, and the
// columnar decode that is the Kafka default. The collect sink carries these
// columns as the text of the value, so a rescale shows as different digits.
// The row decode is fed both forms the engine's convention allows, integers
// and digit strings. The columnar decode keeps a batch columnar only when
// every value of a column it carries as text is a string, so it is fed the
// text forms, which is what it decodes columnar in practice.
TEST(ClickHouseNativeSql, TheJsonDecodeHandsTimestampsAndDatesOnAsWritten) {
    ensure_installed();
    const std::vector<std::string> columns = {"t0", "t3", "t6", "t9", "tz", "dd"};
    const std::vector<std::string> mixed = {
        R"({"id":1,"t0":1700000000000,"t3":1700000000123,"t6":1700000000123,"t9":1700000000123,"tz":1700000000123,"dd":19675})",
        R"({"id":2,"t0":-86400000,"t3":-1,"t6":-1,"t9":-1,"tz":-1,"dd":-1})",
        R"({"id":3,"t0":"1700000000000","t3":"-2208988800000","t6":"1","t9":"9223372036854","tz":"0","dd":"1969-07-20"})",
        R"({"id":4,"t0":0,"t3":0,"t6":0,"t9":0,"tz":0,"dd":"2299-12-31"})",
    };
    const std::vector<std::string> text_only = {
        R"({"id":1,"t0":"1700000000000","t3":"1700000000123","t6":"-1","t9":"9223372036854","tz":"-2208988800000","dd":"1969-07-20"})",
        R"({"id":2,"t0":"-86400000","t3":"-1","t6":"1700000000123","t9":"-1","tz":"0","dd":"2299-12-31"})",
        R"({"id":3,"t0":"0","t3":"0","t6":"0","t9":"0","tz":"1","dd":"1970-01-01"})",
    };
    const std::string table =
        "(id BIGINT, t0 TIMESTAMP(0), t3 TIMESTAMP(3), t6 TIMESTAMP(6), t9 TIMESTAMP(9), tz "
        "TIMESTAMPTZ, "
        "dd DATE)";
    for (const bool row_form : {true, false}) {
        SCOPED_TRACE(row_form ? "json_string_to_row" : "json_string_to_row_columnar");
        const std::string topic = row_form ? "decode_row" : "decode_columnar";
        const auto& lines = row_form ? mixed : text_only;
        set_feed(topic, lines, true);
        const std::string ddl = "CREATE TABLE src " + table +
                                " WITH (connector='kafka', format='json', topic='" + topic + "'" +
                                (row_form ? ", columnar_decode='false'" : "") +
                                "); CREATE TABLE out " + table + " WITH (connector='collect');";
        const std::string insert = "INSERT INTO out SELECT id, t0, t3, t6, t9, tz, dd FROM src;";
        const auto plan = compile_script(ddl, insert, 1);
        ASSERT_EQ(plan.size(), 1U);
        ASSERT_TRUE(
            has_op(plan[0], row_form ? "json_string_to_row" : "json_string_to_row_columnar"));

        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(ddl), 0) << err.str();
        auto reader = engine.collect_reader("out").ValueOrDie();
        const auto decoded_before = clink::detail::batch_materialize_counter().load();
        ASSERT_EQ(engine.execute_script(insert), 0) << err.str();
        std::map<std::int64_t, std::map<std::string, JsonValue>> got;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            ASSERT_TRUE(reader->ReadNext(&batch).ok());
            if (!batch) {
                break;
            }
            for (std::int64_t r = 0; r < batch->num_rows(); ++r) {
                std::map<std::string, JsonValue> cells;
                for (int c = 0; c < batch->num_columns(); ++c) {
                    cells[batch->schema()->field(c)->name()] = collected_cell(*batch->column(c), r);
                }
                got[cells.at("id").as_int()] = std::move(cells);
            }
        }
        ASSERT_TRUE(engine.await_all()) << err.str();
        if (!row_form) {
            EXPECT_GT(clink::detail::batch_materialize_counter().load(), decoded_before)
                << "the columnar decode took the row path for every batch";
        }
        ASSERT_EQ(got.size(), lines.size());
        for (const auto& line : lines) {
            const JsonValue written = clink::config::parse(line);
            const std::int64_t id = written.at("id").as_int();
            for (const auto& column : columns) {
                const JsonValue& in = written.at(column);
                const JsonValue& out = got.at(id).at(column);
                const std::string want =
                    in.is_string() ? in.as_string() : std::to_string(in.as_int());
                const std::string have =
                    out.is_string() ? out.as_string()
                                    : (out.is_integral_number() ? std::to_string(out.as_int())
                                                                : out.serialize(0));
                EXPECT_EQ(have, want) << "id " << id << ", column " << column;
            }
        }
    }
}

// --- One source, two sinks ---------------------------------------------------

// A script with two INSERTs from one source, one into native ClickHouse and
// one into collect. The native sink must be the only sink on its chain, so
// the script either plans the two sinks apart and runs, or is refused at
// deploy with the chain rule's message. The case records which.
TEST(ClickHouseNativeSql, TwoInsertsFromOneSourceArePlannedApartOrRefused) {
    ensure_installed();
    const ScratchDir dir("two_sinks");
    constexpr std::int64_t kRows = 50;
    write_lines(dir.path() / "in.ndjson", small_lines(kRows));
    const FakeServerScope fake(small_target());
    const std::string ddl =
        "CREATE TABLE src (id BIGINT, s VARCHAR) WITH (connector='file', format='json', path='" +
        (dir.path() / "in.ndjson").string() +
        "');"
        "CREATE TABLE ch (id BIGINT, s VARCHAR) WITH (" +
        kNativeWith +
        ");"
        "CREATE TABLE out (id BIGINT, s VARCHAR) WITH (connector='collect');";
    const std::string inserts =
        "INSERT INTO ch SELECT id, s FROM src;"
        "INSERT INTO out SELECT id, s FROM src;";

    const auto specs = compile_script(ddl, inserts);
    std::size_t native_jobs = 0;
    for (const auto& spec : specs) {
        const bool native_here = has_op(spec, "clickhouse_native_sink");
        native_jobs += native_here ? 1 : 0;
        if (native_here) {
            EXPECT_FALSE(has_op(spec, "collect_sink_row"))
                << "the planner put both sinks in one job";
        }
    }
    EXPECT_EQ(native_jobs, 1U);

    clink::embed::EngineOptions opts;
    std::ostringstream err;
    opts.err = &err;
    opts.out = &err;
    clink::embed::EmbeddedEngine engine{std::move(opts)};
    ASSERT_EQ(engine.execute_script(ddl), 0) << err.str();
    auto reader = engine.collect_reader("out").ValueOrDie();
    const int rc = engine.execute_script(inserts);
    std::int64_t collected = 0;
    if (rc == 0) {
        while (true) {
            std::shared_ptr<arrow::RecordBatch> batch;
            ASSERT_TRUE(reader->ReadNext(&batch).ok());
            if (!batch) {
                break;
            }
            collected += batch->num_rows();
        }
    }
    const bool ran = rc == 0 && engine.await_all();
    std::string errors = err.str();
    for (const auto& e : engine.errors()) {
        errors += e + "\n";
    }
    const std::string chain_rule = "must be the only sink on its chain";
    if (ran) {
        RecordProperty("two_sinks_outcome", "planned on separate chains and ran");
        std::cout << "two INSERTs from one source: planned on separate chains (" << specs.size()
                  << " jobs) and ran\n";
        EXPECT_EQ(specs.size(), 2U);
        EXPECT_EQ(collected, kRows);
        EXPECT_EQ(fake.server().rows("db.events"), static_cast<std::uint64_t>(kRows));
    } else {
        RecordProperty("two_sinks_outcome", "refused at deploy");
        std::cout << "two INSERTs from one source: refused at deploy\n";
        EXPECT_NE(errors.find(chain_rule), std::string::npos)
            << "the script failed for a reason other than the chain rule:\n"
            << errors;
    }
}

// --- Against a real server ---------------------------------------------------

std::string live_env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string(v);
}

// A quoted SQL string literal, for values that go into a WITH clause or a
// ClickHouse statement.
std::string sql_string(std::string_view text) {
    std::string out = "'";
    for (const char c : text) {
        if (c == '\'' || c == '\\') {
            out += '\\';
        }
        out += c;
    }
    return out + "'";
}

// The server a live case writes to, from the variables the live runner sets.
// Each case gets a database of its own, dropped afterwards, and a client of
// its own for the DDL and for reading the target back.
class LiveSqlServer : public ::testing::Test {
protected:
    void SetUp() override {
        host_ = live_env("CLINK_CLICKHOUSE_TEST_HOST");
        if (host_.empty()) {
            GTEST_SKIP() << "set CLINK_CLICKHOUSE_TEST_HOST to run the live SQL cases";
        }
        const std::string port = live_env("CLINK_CLICKHOUSE_TEST_PORT");
        port_ = port.empty() ? 9000 : static_cast<std::uint16_t>(std::stoi(port));
        user_ = live_env("CLINK_CLICKHOUSE_TEST_USER");
        password_ = live_env("CLINK_CLICKHOUSE_TEST_PASSWORD");
        ensure_installed();
        // A case before this one may have left the fake installed; these
        // cases go through the real transport.
        native::set_transport_factory_for_testing(nullptr);

        clickhouse::ClientOptions o;
        o.SetHost(host_);
        o.SetPort(port_);
        if (!user_.empty()) {
            o.SetUser(user_);
        }
        if (!password_.empty()) {
            o.SetPassword(password_);
        }
        o.SetRethrowException(true);
        client_ = std::make_unique<clickhouse::Client>(o);
        const auto& info = client_->GetServerInfo();
        line_ = std::to_string(info.version_major) + "." + std::to_string(info.version_minor);
        RecordProperty("clickhouse_line", line_);
        // The runner names the line it started, so a case pointed at the
        // wrong server says so instead of passing for the wrong line.
        if (const std::string want = live_env("CLINK_CLICKHOUSE_TEST_LINE"); !want.empty()) {
            ASSERT_EQ(line_, want) << "the server on port " << port_ << " is not the line named";
        }
        std::random_device rd;
        db_ = "clink_sql_live_" + std::to_string(::getpid()) + "_" +
              std::to_string(std::mt19937_64(rd())() % 100000000);
        client_->Execute("CREATE DATABASE " + db_);
    }

    void TearDown() override {
        if (client_ == nullptr) {
            return;
        }
        if (!db_.empty()) {
            client_->Execute("DROP DATABASE IF EXISTS " + db_ + " SYNC");
        }
        for (const auto& user : users_) {
            client_->Execute("DROP USER IF EXISTS " + user);
        }
    }

    // Every cell of a SELECT as text: each expression must yield a String
    // (a NULL in a Nullable(String) reads as empty text).
    std::vector<std::vector<std::string>> rows(const std::string& sql) {
        native::ResultSet rs;
        client_->Select(sql,
                        [&rs](const clickhouse::Block& b) { native::append_result_block(b, rs); });
        return rs.rows;
    }

    std::string scalar(const std::string& sql) {
        const auto r = rows(sql);
        return r.empty() || r.front().empty() ? std::string{} : r.front().front();
    }

    // The WITH options that reach this server as `user` (the configured one
    // when empty).
    [[nodiscard]] std::string server_with(const std::string& user = {},
                                          const std::string& password = {}) const {
        std::string out = "connector='clickhouse', host=" + sql_string(host_) +
                          ", port=" + sql_string(std::to_string(port_)) +
                          ", database=" + sql_string(db_);
        const std::string& u = user.empty() ? user_ : user;
        const std::string& p = user.empty() ? password_ : password;
        if (!u.empty()) {
            out += ", user=" + sql_string(u);
        }
        if (!p.empty()) {
            out += ", password=" + sql_string(p);
        }
        return out;
    }

    // count(), uniqExact(id) and the duplicates of `table`, as text.
    void expect_content(const std::string& table, std::size_t produced) {
        const auto gate = rows(
            "SELECT toString(count()), toString(uniqExact(id)), "
            "toString(count() - uniqExact(id)) FROM " +
            db_ + "." + table);
        ASSERT_EQ(gate.size(), 1U);
        EXPECT_EQ(gate[0][0], std::to_string(produced)) << "count() on " << line_;
        EXPECT_EQ(gate[0][1], std::to_string(produced)) << "uniqExact(id) on " << line_;
        EXPECT_EQ(gate[0][2], "0") << "duplicates on " << line_;
    }

    std::string host_;
    std::uint16_t port_{9000};
    std::string user_;
    std::string password_;
    std::string line_;
    std::string db_;
    std::vector<std::string> users_;
    std::unique_ptr<clickhouse::Client> client_;
};

class ClickHouseNativeSqlLive : public LiveSqlServer {};
class ClickHouseLegacySqlLive : public LiveSqlServer {};

// The temporal columns, every precision and both time-zoned spellings, beside
// an integer past 2^53, two decimals and nullable columns. Each target is the
// type a user would pick for the declared one.
const std::vector<Column>& live_columns() {
    static const std::vector<Column> columns = {
        {"id", "BIGINT", "Int64"},
        {"big", "BIGINT", "Int64"},
        {"n", "BIGINT", "Nullable(Int64)"},
        {"d", "DATE", "Date32"},
        {"dd", "DATE", "Date"},
        {"t0", "TIMESTAMP(0)", "DateTime64(0)"},
        {"t3", "TIMESTAMP(3)", "DateTime64(3)"},
        {"t6", "TIMESTAMP(6)", "DateTime64(6, 'UTC')"},
        {"t9", "TIMESTAMP(9)", "DateTime64(9)"},
        {"tz", "TIMESTAMPTZ", "DateTime64(6, 'Asia/Tokyo')"},
        {"tz0", "TIMESTAMPTZ(0)", "DateTime64(0, 'America/New_York')"},
        {"tz3", "TIMESTAMP(3) WITH TIME ZONE", "Nullable(DateTime64(3, 'Europe/London'))"},
        {"tdt", "TIMESTAMP(0)", "DateTime"},
        {"dec", "DECIMAL(10,2)", "Nullable(Decimal(12, 4))"},
        {"dec0", "DECIMAL(18,0)", "Decimal(18, 0)"},
    };
    return columns;
}

// One generated row: every value inside the range of the column's target,
// negative epochs and dates before 1970 included, each temporal value in
// either form the engine's convention allows.
JsonObject live_row(std::int64_t id, std::mt19937_64& rng) {
    const auto uniform = [&rng](std::int64_t lo, std::int64_t hi) {
        return std::uniform_int_distribution<std::int64_t>(lo, hi)(rng);
    };
    const auto date_cell = [&](std::int64_t lo, std::int64_t hi) {
        const std::int64_t days = uniform(lo, hi);
        return (id % 2 == 0) ? JsonValue{days} : JsonValue{civil_text(days)};
    };
    const auto ms_cell = [&](std::int64_t lo, std::int64_t hi, std::int64_t unit) {
        const std::int64_t ms = uniform(lo / unit, hi / unit) * unit;
        return (id % 3 == 0) ? JsonValue{std::to_string(ms)} : JsonValue{ms};
    };
    const auto max64 = std::numeric_limits<std::int64_t>::max();
    const auto min64 = std::numeric_limits<std::int64_t>::min();
    JsonObject row;
    row["id"] = JsonValue{id};
    row["big"] = JsonValue{uniform(min64, max64)};
    row["n"] = id % 5 == 1 ? JsonValue{} : JsonValue{uniform(min64, max64)};
    row["d"] = date_cell(-25567, 120529);
    row["dd"] = date_cell(0, 65535);
    row["t0"] = ms_cell(k1900Ms, k2299Ms, 1000);
    row["t3"] = ms_cell(k1900Ms, k2299Ms, 1);
    row["t6"] = ms_cell(k1900Ms, k2299Ms, 1);
    row["t9"] = ms_cell(k1900Ms, kNanosMaxMs, 1);
    row["tz"] = ms_cell(k1900Ms, k2299Ms, 1);
    row["tz0"] = ms_cell(k1900Ms, k2299Ms, 1000);
    row["tz3"] = id % 7 == 3 ? JsonValue{} : ms_cell(k1900Ms, k2299Ms, 1);
    row["tdt"] = ms_cell(0, 4294967295000LL, 1000);
    row["dec"] = id % 6 == 2
                     ? JsonValue{}
                     : JsonValue{static_cast<double>(uniform(-9999999999LL, 9999999999LL)) / 100.0};
    row["dec0"] = JsonValue{uniform(-999999999999999999LL, 999999999999999999LL)};
    return row;
}

// The edges, each on a row of its own: a millisecond and a second before the
// epoch, the first and last instant of every range, 2^53 + 1, and NULLs.
std::vector<JsonObject> live_edge_rows(std::mt19937_64& rng, std::int64_t& next_id) {
    std::vector<JsonObject> rows;
    const auto base = [&] { return live_row(next_id++, rng); };
    {
        JsonObject r = base();
        r["big"] = JsonValue{kTwo53Plus1};
        r["n"] = JsonValue{-kTwo53Plus1};
        r["d"] = JsonValue{std::string("1969-12-31")};
        r["dd"] = JsonValue{static_cast<std::int64_t>(0)};
        r["t0"] = JsonValue{static_cast<std::int64_t>(-1000)};
        r["t3"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["t6"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["t9"] = JsonValue{static_cast<std::int64_t>(-1)};
        r["tz"] = JsonValue{std::string("-1")};
        r["tz0"] = JsonValue{std::string("-1000")};
        r["tz3"] = JsonValue{static_cast<std::int64_t>(-86400001)};
        r["tdt"] = JsonValue{static_cast<std::int64_t>(0)};
        r["dec"] = JsonValue{-0.05};
        r["dec0"] = JsonValue{kTwo53Plus1};
        rows.push_back(std::move(r));
    }
    {
        JsonObject r = base();
        r["big"] = JsonValue{std::numeric_limits<std::int64_t>::max()};
        r["n"] = JsonValue{std::numeric_limits<std::int64_t>::min()};
        r["d"] = JsonValue{std::string("1900-01-01")};
        r["dd"] = JsonValue{std::string("2149-06-06")};
        r["t0"] = JsonValue{k1900Ms};
        r["t3"] = JsonValue{k2299Ms};
        r["t6"] = JsonValue{k1900Ms};
        r["t9"] = JsonValue{kNanosMaxMs};
        r["tz"] = JsonValue{k2299Ms};
        r["tz0"] = JsonValue{std::string("10413791999000")};
        r["tz3"] = JsonValue{k1900Ms};
        r["tdt"] = JsonValue{static_cast<std::int64_t>(4294967295000LL)};
        r["dec"] = JsonValue{99999999.99};
        r["dec0"] = JsonValue{static_cast<std::int64_t>(-999999999999999999LL)};
        rows.push_back(std::move(r));
    }
    {
        JsonObject r = base();
        r["big"] = JsonValue{std::numeric_limits<std::int64_t>::min()};
        r["n"] = JsonValue{};
        r["d"] = JsonValue{static_cast<std::int64_t>(120529)};
        r["dd"] = JsonValue{static_cast<std::int64_t>(65535)};
        r["t0"] = JsonValue{std::string("10413791999000")};
        r["t3"] = JsonValue{std::string("-2208988800000")};
        r["t6"] = JsonValue{k2299Ms};
        r["t9"] = JsonValue{k1900Ms};
        r["tz"] = JsonValue{k1900Ms};
        r["tz0"] = JsonValue{k1900Ms};
        r["tz3"] = JsonValue{};
        r["tdt"] = JsonValue{static_cast<std::int64_t>(0)};
        r["dec"] = JsonValue{};
        r["dec0"] = JsonValue{static_cast<std::int64_t>(0)};
        rows.push_back(std::move(r));
    }
    {
        JsonObject r = base();
        r["d"] = JsonValue{static_cast<std::int64_t>(-25567)};
        r["t0"] = JsonValue{static_cast<std::int64_t>(-14182940000)};  // 1969-07-20 20:17:40
        r["t3"] = JsonValue{static_cast<std::int64_t>(-14182939999)};
        r["t6"] = JsonValue{std::string("-14182940001")};
        r["t9"] = JsonValue{static_cast<std::int64_t>(-2208988799999)};
        r["dec"] = JsonValue{-99999999.99};
        rows.push_back(std::move(r));
    }
    return rows;
}

constexpr std::size_t kLiveGeneratedRows = 2000;
constexpr std::size_t kLiveEdgeRows = 4;

std::vector<std::string> live_lines() {
    std::mt19937_64 rng(20261002);
    std::int64_t next_id = 1;
    std::vector<std::string> lines;
    for (auto& r : live_edge_rows(rng, next_id)) {
        lines.push_back(line_of(std::move(r), live_columns(), false));
    }
    for (std::size_t k = 0; k < kLiveGeneratedRows; ++k) {
        lines.push_back(line_of(live_row(next_id++, rng), live_columns(), false));
    }
    return lines;
}

// The read-back expression of a column, as text the mapping's rendering can
// be compared with: an instant in UTC whatever the column's display zone, and
// a NULL as the word.
std::string read_back(const Column& c) {
    std::string type = c.target;
    const bool nullable = inner_of(type, "Nullable").has_value();
    if (nullable) {
        type = *inner_of(type, "Nullable");
    }
    const std::string text = type.starts_with("DateTime") ? "toString(" + c.name + ", 'UTC')"
                                                          : "toString(" + c.name + ")";
    return nullable ? "ifNull(" + text + ", 'NULL')" : text;
}

// The server prints a decimal without trailing zeros, and the rendering pads
// it to the target's scale, so both sides drop them before the comparison.
std::string decimal_text(std::string text) {
    if (text.find('.') == std::string::npos) {
        return text;
    }
    while (text.ends_with('0')) {
        text.pop_back();
    }
    if (text.ends_with('.')) {
        text.pop_back();
    }
    return text == "-0" ? "0" : text;
}

// Through EmbeddedEngine and the real transport, a script lands DATE, every
// TIMESTAMP precision and both TIMESTAMPTZ spellings, with dates before 1970
// and negative epochs, plus an integer past 2^53, two decimals and nullable
// columns, into a real MergeTree with insert_format='native'. Every value
// read back from the server must be what the same script hands
// connector='collect', under the sink's type mapping, and the content gates
// hold. A handful of edge values are also checked against their literal
// spelling, so that the comparison does not rest on the collect path alone.
TEST_F(ClickHouseNativeSqlLive, LandsWhatTheCollectSinkSeesForTemporalAndWideValues) {
    const auto& columns = live_columns();
    const ScratchDir dir("live_native");
    const auto lines = live_lines();
    write_lines(dir.path() / "in.ndjson", lines);
    std::string target = "CREATE TABLE " + db_ + ".events (";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        target += (i == 0 ? "" : ", ") + columns[i].name + " " + columns[i].target;
    }
    client_->Execute(target + ") ENGINE = MergeTree ORDER BY id");

    const std::string ddl = "CREATE TABLE src " + column_ddl(columns) +
                            " WITH (connector='file', format='json', path='" +
                            (dir.path() / "in.ndjson").string() + "');";
    const std::string select = "SELECT " + column_list("", columns) + " FROM src;";

    const auto collected = run_into_collect(ddl, "INSERT INTO out " + select, columns);
    ASSERT_EQ(collected.size(), lines.size());
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(
            engine.execute_script(ddl + "CREATE TABLE ch " + column_ddl(columns) + " WITH (" +
                                  server_with() + ", insert_format='native', table='events');"),
            0)
            << err.str();
        ASSERT_EQ(engine.execute_script("INSERT INTO ch " + select), 0) << err.str();
        const bool ok = engine.await_all();
        std::string errors;
        for (const auto& e : engine.errors()) {
            errors += e + "\n";
        }
        ASSERT_TRUE(ok) << "the native job failed on " << line_ << ":\n" << errors << err.str();
    }

    expect_content("events", lines.size());

    std::string exprs;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        exprs += (i == 0 ? "" : ", ") + read_back(columns[i]);
    }
    std::map<std::int64_t, std::map<std::string, std::string>> landed;
    for (auto& row : rows("SELECT " + exprs + " FROM " + db_ + ".events")) {
        std::map<std::string, std::string> cells;
        for (std::size_t i = 0; i < columns.size(); ++i) {
            cells[columns[i].name] = std::move(row[i]);
        }
        const std::int64_t id = std::stoll(cells.at("id"));
        ASSERT_TRUE(landed.emplace(id, std::move(cells)).second) << "id " << id << " twice";
    }
    ASSERT_EQ(landed.size(), lines.size());

    std::size_t mismatches = 0;
    for (const auto& [id, cells] : collected) {
        const auto it = landed.find(id);
        ASSERT_NE(it, landed.end()) << "id " << id << " never landed";
        for (const auto& c : columns) {
            const bool decimal = c.target.find("Decimal") != std::string::npos;
            std::string want = render(mapping_sql(c), c.target, cells.at(c.name), false);
            std::string got = it->second.at(c.name);
            if (decimal) {
                want = decimal_text(want);
                got = decimal_text(got);
            }
            if (got != want && ++mismatches <= 20) {
                ADD_FAILURE() << line_ << ", id " << id << ", column " << c.name << " (" << c.sql
                              << " into " << c.target << "): the server holds '" << got
                              << "', collected " << cells.at(c.name).serialize(0) << " maps to '"
                              << want << "'";
            }
        }
    }
    EXPECT_EQ(mismatches, 0U);

    // The edge rows, by what the script wrote rather than by the collect sink.
    const std::map<std::pair<std::int64_t, std::string>, std::string> literal = {
        {{1, "big"}, "9007199254740993"},
        {{1, "n"}, "-9007199254740993"},
        {{1, "dec0"}, "9007199254740993"},
        {{1, "d"}, "1969-12-31"},
        {{1, "t0"}, "1969-12-31 23:59:59"},
        {{1, "t3"}, "1969-12-31 23:59:59.999"},
        {{1, "t6"}, "1969-12-31 23:59:59.999000"},
        {{1, "t9"}, "1969-12-31 23:59:59.999000000"},
        {{1, "tz"}, "1969-12-31 23:59:59.999000"},
        {{1, "tz0"}, "1969-12-31 23:59:59"},
        {{1, "tz3"}, "1969-12-30 23:59:59.999"},
        {{1, "dec"}, "-0.05"},
        {{2, "big"}, "9223372036854775807"},
        {{2, "n"}, "-9223372036854775808"},
        {{2, "d"}, "1900-01-01"},
        {{2, "dd"}, "2149-06-06"},
        {{2, "t0"}, "1900-01-01 00:00:00"},
        {{2, "t3"}, "2299-12-31 23:59:59.999"},
        {{2, "t9"}, "2262-04-11 23:47:16.854000000"},
        {{2, "tdt"}, "2106-02-07 06:28:15"},
        {{2, "dec"}, "99999999.99"},
        {{2, "dec0"}, "-999999999999999999"},
        {{3, "n"}, "NULL"},
        {{3, "tz3"}, "NULL"},
        {{3, "dec"}, "NULL"},
        {{3, "d"}, "2299-12-31"},
        {{4, "d"}, "1900-01-01"},
        {{4, "t0"}, "1969-07-20 20:17:40"},
        {{4, "t3"}, "1969-07-20 20:17:40.001"},
        {{4, "t6"}, "1969-07-20 20:17:39.999000"},
        {{4, "t9"}, "1900-01-01 00:00:00.001000000"},
    };
    for (const auto& [key, want] : literal) {
        const auto& [id, column] = key;
        std::string got = landed.at(id).at(column);
        if (column.starts_with("dec")) {
            got = decimal_text(got);
        }
        EXPECT_EQ(got, want) << line_ << ", edge row " << id << ", column " << column;
    }
}

// The text sink with format='json' and batch_rows='1', as the Kafka to
// ClickHouse tutorial's pipeline configures it, writing as an ordinary user
// that holds INSERT on the one table and nothing else. Every row lands once,
// with its values, and system.query_log shows that every INSERT carried
// async_insert=0, wait_for_async_insert=1 and a deduplication token of its
// own: the server accepted the forced settings from that user and ran them.
TEST_F(ClickHouseLegacySqlLive, TheTutorialsTextSinkLandsEveryRowOnceWithItsForcedSettings) {
    constexpr std::int64_t kRows = 60;
    const ScratchDir dir("live_legacy");
    std::vector<std::string> lines;
    std::map<std::int64_t, std::vector<std::string>> sent;
    for (std::int64_t id = 1; id <= kRows; ++id) {
        const std::string name =
            unicode_texts()[static_cast<std::size_t>(id) % unicode_texts().size()] + " #" +
            std::to_string(id);
        const std::int64_t big = kTwo53Plus1 + id;
        const double v = static_cast<double>(id) / 4.0;
        JsonObject row;
        row["id"] = JsonValue{id};
        row["name"] = JsonValue{name};
        row["big"] = JsonValue{big};
        row["v"] = JsonValue{v};
        lines.push_back(JsonValue{std::move(row)}.serialize(0));
        sent[id] = {std::to_string(id), name, std::to_string(big), shortest(v)};
    }
    write_lines(dir.path() / "in.ndjson", lines);
    client_->Execute("CREATE TABLE " + db_ +
                     ".events (id Int64, name String, big Int64, v Float64) "
                     "ENGINE = MergeTree ORDER BY id");

    std::random_device rd;
    const std::string user =
        "clink_sql_live_writer_" + std::to_string(std::mt19937_64(rd())() % 100000000);
    const std::string password = "pw-" + std::to_string(std::mt19937_64(rd())());
    client_->Execute("CREATE USER " + user + " IDENTIFIED WITH sha256_password BY " +
                     sql_string(password));
    users_.push_back(user);
    client_->Execute("GRANT INSERT ON " + db_ + ".events TO " + user);

    const std::string script =
        "CREATE TABLE src (id BIGINT, name VARCHAR, big BIGINT, v DOUBLE) WITH ("
        "connector='file', format='json', path='" +
        (dir.path() / "in.ndjson").string() +
        "');"
        "CREATE TABLE out (id BIGINT, name VARCHAR, big BIGINT, v DOUBLE) WITH (" +
        server_with(user, password) +
        ", table='events', format='json', batch_rows='1');"
        "INSERT INTO out SELECT id, name, big, v FROM src;";
    {
        clink::embed::EngineOptions opts;
        std::ostringstream err;
        opts.err = &err;
        opts.out = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        ASSERT_EQ(engine.execute_script(script), 0) << err.str();
        const bool ok = engine.await_all();
        std::string errors;
        for (const auto& e : engine.errors()) {
            errors += e + "\n";
        }
        ASSERT_TRUE(ok) << "the text sink's job failed on " << line_ << ":\n"
                        << errors << err.str();
    }

    expect_content("events", static_cast<std::size_t>(kRows));
    const auto landed = rows("SELECT toString(id), name, toString(big), toString(v) FROM " + db_ +
                             ".events ORDER BY id");
    ASSERT_EQ(landed.size(), static_cast<std::size_t>(kRows));
    for (const auto& row : landed) {
        const std::int64_t id = std::stoll(row[0]);
        EXPECT_EQ(row, sent.at(id)) << line_ << ", id " << id;
    }

    client_->Execute("SYSTEM FLUSH LOGS");
    const std::string mine = " FROM system.query_log WHERE user = " + sql_string(user) +
                             " AND query_kind = 'Insert' AND has(databases, " + sql_string(db_) +
                             ")";
    EXPECT_EQ(scalar("SELECT toString(count())" + mine + " AND type != 'QueryFinish'" +
                     " AND type != 'QueryStart'"),
              "0")
        << "an INSERT failed on " << line_;
    const auto logged = rows(
        "SELECT ifNull(Settings['async_insert'], ''), "
        "ifNull(Settings['wait_for_async_insert'], ''), "
        "ifNull(Settings['insert_deduplication_token'], ''), query" +
        mine + " AND type = 'QueryFinish'");
    // batch_rows='1' flushes after every row, so every row is an INSERT.
    EXPECT_EQ(logged.size(), static_cast<std::size_t>(kRows)) << "INSERTs logged on " << line_;
    std::set<std::string> tokens;
    for (const auto& entry : logged) {
        SCOPED_TRACE(entry[3]);
        EXPECT_EQ(entry[0], "0") << "async_insert on " << line_;
        EXPECT_EQ(entry[1], "1") << "wait_for_async_insert on " << line_;
        EXPECT_FALSE(entry[2].empty()) << "insert_deduplication_token on " << line_;
        tokens.insert(entry[2]);
    }
    EXPECT_EQ(tokens.size(), logged.size()) << "two INSERTs shared a token on " << line_;
}

}  // namespace
