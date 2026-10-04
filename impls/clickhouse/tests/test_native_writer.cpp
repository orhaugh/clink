// The insert thread, driven against the fake server: when an INSERT closes,
// what a failure freezes, how a resend picks its token, resource splits,
// merge back-pressure, the attempt deadline, backpressure, cancel and the
// detach, and what the writer counts and logs. Every case ends by checking
// that no client was destroyed mid-INSERT and that every memory reservation
// came back.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <clickhouse/block.h>
#include <gtest/gtest.h>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "fake_transport.hpp"
#include "native/arrow_to_block.hpp"
#include "native/column_plan.hpp"
#include "native/errors.hpp"
#include "native/insert_transport.hpp"
#include "native/metrics.hpp"
#include "native/sql_text.hpp"
#include "native/statements.hpp"
#include "native/target_table.hpp"
#include "native/types.hpp"
#include "native/writer.hpp"
#include "test_helpers/sanitizer_slack.hpp"

namespace clink::clickhouse::native {
namespace {

using namespace std::chrono_literals;
namespace fake = clink::clickhouse::native::testing;
using WrClock = std::chrono::steady_clock;
using WrOutcome = fake::ReceivedInsert::Outcome;
using clink::test_support::scale_slack;

constexpr std::uint64_t kWrOpId = 7;
constexpr std::size_t kWrKiB = 1024;
constexpr std::size_t kWrMiB = 1024 * kWrKiB;
const Endpoint kWrEp1{"ch-1", 9000};
const Endpoint kWrEp2{"ch-2", 9000};
const std::string kWrTable = "events";

// The 26.8 texts of the three error 252 messages.
const std::string kWrTooManyPartitions =
    "Too many partitions for single INSERT block (more than 100). The limit is controlled by "
    "'max_partitions_per_insert_block' setting. Large number of partitions is a common "
    "misconception.";
const std::string kWrPartsInTotal =
    "Too many parts (100000) in all partitions in total in table 'db.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. This indicates wrong choice of partition key. "
    "The threshold can be modified with 'max_parts_in_total' setting in <merge_tree> element "
    "in config.xml or with per-table setting.";
const std::string kWrPartsInPartition =
    "Too many parts (3000 with average size of 1.20 MiB) in table 'db.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. Merges are processing significantly slower "
    "than inserts";

void wr_ok(const arrow::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error("arrow: " + status.ToString());
    }
}

// A chunk laid out as the plan expects "id:BIGINT;s:VARCHAR".
std::shared_ptr<arrow::RecordBatch> wr_batch(const std::vector<std::optional<std::int64_t>>& ids,
                                             const std::vector<std::string>& texts) {
    arrow::Int64Builder id_builder;
    arrow::StringBuilder text_builder;
    std::int64_t text_bytes = 0;
    for (const auto& t : texts) {
        text_bytes += static_cast<std::int64_t>(t.size());
    }
    wr_ok(text_builder.ReserveData(text_bytes));
    for (const auto& id : ids) {
        wr_ok(id ? id_builder.Append(*id) : id_builder.AppendNull());
    }
    for (const auto& t : texts) {
        wr_ok(text_builder.Append(t));
    }
    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> text_array;
    wr_ok(id_builder.Finish(&id_array));
    wr_ok(text_builder.Finish(&text_array));
    const auto schema =
        arrow::schema({arrow::field("id", arrow::int64()), arrow::field("s", arrow::utf8())});
    return arrow::RecordBatch::Make(
        schema, static_cast<std::int64_t>(ids.size()), {id_array, text_array});
}

// Rows [from, to) of a chunk laid out as "id:BIGINT".
std::shared_ptr<arrow::RecordBatch> wr_id_rows(std::int64_t from, std::int64_t to) {
    arrow::Int64Builder builder;
    for (std::int64_t id = from; id < to; ++id) {
        wr_ok(builder.Append(id));
    }
    std::shared_ptr<arrow::Array> ids;
    wr_ok(builder.Finish(&ids));
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int64())}), to - from, {ids});
}

// Rows [from, to), each text `length` bytes of one letter.
std::shared_ptr<arrow::RecordBatch> wr_rows(std::int64_t from,
                                            std::int64_t to,
                                            std::size_t length) {
    std::vector<std::optional<std::int64_t>> ids;
    std::vector<std::string> texts;
    for (std::int64_t id = from; id < to; ++id) {
        ids.emplace_back(id);
        texts.emplace_back(length, static_cast<char>('a' + id % 26));
    }
    return wr_batch(ids, texts);
}

fake::FakeTable wr_table(std::size_t window = 100) {
    fake::FakeTable t;
    t.database = "db";
    t.name = kWrTable;
    t.engine = "MergeTree";
    t.engine_full = "MergeTree ORDER BY id";
    t.columns = {{"id", "Int64", DefaultKind::None, 1}, {"s", "String", DefaultKind::None, 2}};
    t.dedup_window = window;
    return t;
}

ColumnPlan wr_plan(const fake::FakeTable& table, const std::string& spec) {
    PlanResult r = compile_column_plan(parse_sql_column_types(spec), table.columns);
    if (!r.plan) {
        throw std::runtime_error("the column plan refused the test table");
    }
    return std::move(*r.plan);
}

// What the opener's probe would have found about the table itself.
TargetInfo wr_base(const fake::FakeTable& table) {
    TargetInfo info;
    info.engine = table.engine;
    info.family = EngineFamily::MergeTree;
    info.columns = table.columns;
    info.dedup_window = table.dedup_window;
    info.keeps_dedup_log = table.dedup_window > 0;
    return info;
}

// A stand-in for the open-time probe that reads, from whichever fake server
// the client reached, what the writer's re-probe rules depend on: the line,
// the switches the server lists, the squash thresholds and the MergeTree
// async_insert default.
TargetInfo wr_probe(InsertTransport& transport, const TargetInfo& base) {
    TargetInfo out = base;
    const ServerIdentity& id = transport.server();
    out.server = id;
    out.tested_line = is_tested_line(id.major, id.minor);
    out.keep_token_on_resend = out.tested_line;
    std::map<std::string, std::string> settings;
    for (const auto& row :
         transport.select(MetaQuery::ServerSettings, select_server_settings(1s)).rows) {
        settings[row.at(0)] = row.at(1);
    }
    out.caps.deduplicate_insert = settings.contains("deduplicate_insert");
    out.caps.use_strict_insert_block_limits = settings.contains("use_strict_insert_block_limits");
    out.caps.quorum = settings.contains("insert_quorum") && settings.at("insert_quorum") != "0";
    out.caps.min_insert_block_size_rows = std::stoull(settings.at("min_insert_block_size_rows"));
    out.caps.min_insert_block_size_bytes = std::stoull(settings.at("min_insert_block_size_bytes"));
    out.caps.max_partitions_per_insert_block =
        std::stoull(settings.at("max_partitions_per_insert_block"));
    for (const auto& row :
         transport.select(MetaQuery::MergeTreeSettings, select_merge_tree_settings(1s)).rows) {
        if (row.at(0) == "async_insert" && row.at(1) != "0") {
            throw NativeSinkError(code::kTargetAsyncInsert,
                                  "`db`.`events` takes inserts asynchronously (server default "
                                  "async_insert=" +
                                      row.at(1) + ")");
        }
    }
    return out;
}

// Passes every call through, and says when it is destroyed: the moment a
// detached writer thread lets go of its state.
class WrWatched final : public InsertTransport {
public:
    WrWatched(std::unique_ptr<InsertTransport> inner, std::shared_ptr<std::atomic<bool>> gone)
        : inner_(std::move(inner)), gone_(std::move(gone)) {}
    ~WrWatched() override {
        inner_.reset();
        gone_->store(true);
    }
    WrWatched(const WrWatched&) = delete;
    WrWatched& operator=(const WrWatched&) = delete;
    WrWatched(WrWatched&&) = delete;
    WrWatched& operator=(WrWatched&&) = delete;

    void connect(const Endpoint& endpoint) override { inner_->connect(endpoint); }
    [[nodiscard]] bool connected() const noexcept override { return inner_->connected(); }
    [[nodiscard]] const ServerIdentity& server() const override { return inner_->server(); }
    ResultSet select(MetaQuery kind, const std::string& sql) override {
        return inner_->select(kind, sql);
    }
    std::vector<HeaderColumn> begin_insert(const std::string& sql) override {
        return inner_->begin_insert(sql);
    }
    void send_block(const ::clickhouse::Block& block) override { inner_->send_block(block); }
    void end_insert() override { inner_->end_insert(); }
    void abandon() noexcept override { inner_->abandon(); }
    void interrupt() noexcept override { inner_->interrupt(); }
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept override {
        inner_->set_deadline(deadline);
    }
    [[nodiscard]] TransportCounters counters() const noexcept override {
        return inner_->counters();
    }

private:
    std::unique_ptr<InsertTransport> inner_;
    std::shared_ptr<std::atomic<bool>> gone_;
};

// The executor's flag and the external token, both owned by the test.
struct WrFlags {
    std::shared_ptr<std::atomic<bool>> executor = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<std::atomic<bool>> external = std::make_shared<std::atomic<bool>>(false);
    [[nodiscard]] CancelSignal signal() const { return CancelSignal{executor, external}; }
    void cancel() const { external->store(true); }
};

// One writer over one or more fake servers, built as the sink would build it
// but with whatever options a case needs. Tear-down checks the two
// invariants every case shares.
struct WrRig {
    explicit WrRig(fake::FakeTable t = wr_table(), std::string declared = "id:BIGINT;s:VARCHAR")
        : table(std::move(t)), spec(std::move(declared)), plan(wr_plan(table, spec)) {
        server = add_server(kWrEp1);
        options.endpoints = {kWrEp1};
        options.database = "db";
        options.table = kWrTable;
        options.batch_rows = 1'048'449;
        options.batch_bytes = 64 * kWrMiB;
        options.batch_interval = 60s;
        options.retry_window = 10s;
        options.sql_column_types = spec;
    }

    ~WrRig() {
        writer.reset();
        for (const auto& [endpoint, s] : servers) {
            EXPECT_EQ(s->destroyed_mid_insert(), 0U)
                << "a client was destroyed mid-INSERT on " << endpoint.host;
        }
        EXPECT_EQ(budget->usage().used, 0U) << "a memory reservation was never returned";
    }

    WrRig(const WrRig&) = delete;
    WrRig& operator=(const WrRig&) = delete;
    WrRig(WrRig&&) = delete;
    WrRig& operator=(WrRig&&) = delete;

    std::shared_ptr<fake::FakeServer> add_server(const Endpoint& endpoint) {
        auto s = std::make_shared<fake::FakeServer>();
        s->add_table(table);
        servers[endpoint] = s;
        return s;
    }

    // Connects to the first endpoint and probes it, as the opener does, then
    // starts the writer over that transport.
    void start(bool watched = false, bool client_gone = false) {
        std::unique_ptr<InsertTransport> transport = fake::fake_factory(servers)(options);
        transport->connect(options.endpoints.front());
        TargetInfo base = wr_base(table);
        if (target_tweak) {
            target_tweak(base);
        }
        WriterConfig config;
        config.options = options;
        config.plan = plan;
        config.target = wr_probe(*transport, base);
        if (client_gone) {
            transport->abandon();
        }
        config.sink_id = "events-sink";
        config.budget = budget;
        config.cancel = flags.signal();
        config.metrics = &metrics;
        config.op_id = kWrOpId;
        config.reprobe = [counter = probes, base](InsertTransport& t) {
            counter->fetch_add(1);
            return wr_probe(t, base);
        };
        config.part_rate_span = part_rate_span;
        config.part_rate_quiet = part_rate_quiet;
        if (watched) {
            transport = std::make_unique<WrWatched>(std::move(transport), gone);
        }
        handed = transport.get();
        writer = std::make_unique<Writer>(
            std::move(config), std::move(transport), TokenSource::random());
    }

    Chunk chunk(std::int64_t from, std::int64_t to, std::size_t length) const {
        return wrap(wr_rows(from, to, length));
    }

    Chunk wrap(std::shared_ptr<arrow::RecordBatch> batch) const {
        Chunk c;
        c.bytes = chunk_bytes(*batch);
        c.reservation = MemoryReservation(budget, MemoryCategory::Queue, c.bytes);
        c.batch = std::move(batch);
        return c;
    }

    [[nodiscard]] std::vector<fake::ReceivedInsert> inserts() const {
        return server->inserts(kWrTable);
    }

    [[nodiscard]] std::vector<fake::ReceivedInsert> committed() const {
        std::vector<fake::ReceivedInsert> out;
        for (auto& insert : inserts()) {
            if (insert.outcome == WrOutcome::Committed) {
                out.push_back(std::move(insert));
            }
        }
        return out;
    }

    fake::FakeTable table;
    std::string spec;
    ColumnPlan plan;
    SinkOptions options;
    std::map<Endpoint, std::shared_ptr<fake::FakeServer>> servers;
    std::shared_ptr<fake::FakeServer> server;  // at kWrEp1
    MetricsRegistry metrics;
    WrFlags flags;
    std::shared_ptr<MemoryBudget> budget = std::make_shared<MemoryBudget>(0, "writer-test");
    std::shared_ptr<std::atomic<int>> probes = std::make_shared<std::atomic<int>>(0);
    std::shared_ptr<std::atomic<bool>> gone = std::make_shared<std::atomic<bool>>(false);
    // Applied to what the probe reads from the table, on open and on every
    // new client, for targets the fake has no table engine for.
    std::function<void(TargetInfo&)> target_tweak;
    std::chrono::milliseconds part_rate_span{60s};
    std::chrono::milliseconds part_rate_quiet{10min};
    // Owned by the writer. Only interrupt() may be called through it, as the
    // task thread would, and counters(), whose fields are atomic.
    InsertTransport* handed{nullptr};
    std::unique_ptr<Writer> writer;
};

// Waits up to `timeout` for `pred`, scaled for a sanitizer build.
template <typename Pred>
bool wr_eventually(Pred pred, std::chrono::milliseconds timeout = 5s) {
    const auto end = WrClock::now() + scale_slack(timeout);
    while (WrClock::now() < end) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

template <typename F>
std::optional<NativeSinkError> wr_error(F&& f) {
    try {
        f();
    } catch (const NativeSinkError& e) {
        return e;
    }
    return std::nullopt;
}

fake::Fault wr_fault(fake::Step step,
                     fake::Fault::Kind kind,
                     int code = 0,
                     std::string message = "injected") {
    fake::Fault f;
    f.step = step;
    f.kind = kind;
    f.code = code;
    f.message = std::move(message);
    return f;
}

fake::Fault wr_delay(fake::Step step, std::chrono::milliseconds delay, std::size_t nth = 1) {
    fake::Fault f = wr_fault(step, fake::Fault::Kind::Delay);
    f.delay = delay;
    f.nth = nth;
    return f;
}

fake::Fault wr_reset_at_end(fake::Fault::Landing landing = fake::Fault::Landing::Nothing) {
    fake::Fault f = wr_fault(fake::Step::End, fake::Fault::Kind::SystemError);
    f.landing = landing;
    return f;
}

// What the submit that threw said, and how long that one call took.
struct WrHeld {
    std::optional<NativeSinkError> error;
    WrClock::duration took{};
};

// Submits chunks of four 1 MiB rows, so that four of them fill the queue,
// until one throws. With batch_rows=4 the first one is the INSERT the writer
// is busy with, and every one after the fifth has to wait for room.
WrHeld wr_submit_until_one_throws(WrRig& rig) {
    WrHeld out;
    for (std::int64_t from = 0; from < 160 && !out.error; from += 4) {
        Chunk c = rig.chunk(from, from + 4, kWrMiB);
        const auto start = WrClock::now();
        out.error = wr_error([&] { rig.writer->submit(std::move(c)); });
        out.took = WrClock::now() - start;
    }
    return out;
}

std::vector<std::string> wr_tokens(const fake::FakeServer& s) {
    std::vector<std::string> out;
    for (const auto& insert : s.inserts(kWrTable)) {
        out.push_back(insert.token);
    }
    return out;
}

std::size_t wr_rows_of(const fake::ReceivedInsert& insert) {
    std::size_t rows = 0;
    for (const auto& block : insert.blocks) {
        rows += block.rows;
    }
    return rows;
}

// The payload measure of a block of (id Int64, s String): 8 bytes for the id,
// the text, and one byte for its length.
std::size_t wr_block_payload(const fake::ReceivedBlock& block) {
    std::size_t bytes = 0;
    for (const auto& row : block.values) {
        bytes += 8 + row.at(1).size() + 1;
    }
    return bytes;
}

std::size_t wr_payload_of(const fake::ReceivedInsert& insert) {
    std::size_t bytes = 0;
    for (const auto& block : insert.blocks) {
        bytes += wr_block_payload(block);
    }
    return bytes;
}

std::multiset<std::int64_t> wr_ids(const std::vector<std::vector<std::string>>& values) {
    std::multiset<std::int64_t> out;
    for (const auto& row : values) {
        out.insert(std::stoll(row.at(0)));
    }
    return out;
}

std::multiset<std::int64_t> wr_landed_ids(const fake::FakeServer& s) {
    std::multiset<std::int64_t> out;
    for (const auto& block : s.landed(kWrTable)) {
        out.merge(wr_ids(block.values));
    }
    return out;
}

std::multiset<std::int64_t> wr_ids_of(const fake::ReceivedInsert& insert) {
    std::multiset<std::int64_t> out;
    for (const auto& block : insert.blocks) {
        out.merge(wr_ids(block.values));
    }
    return out;
}

std::multiset<std::int64_t> wr_range(std::int64_t from, std::int64_t to) {
    std::multiset<std::int64_t> out;
    for (std::int64_t id = from; id < to; ++id) {
        out.insert(id);
    }
    return out;
}

// Every id in [from, to) twice: what at least once looks like when a resend
// was not deduplicated.
std::multiset<std::int64_t> wr_twice(std::int64_t from, std::int64_t to) {
    std::multiset<std::int64_t> out = wr_range(from, to);
    out.merge(wr_range(from, to));
    return out;
}

std::string wr_name(const char* metric) {
    return std::string(metric) + "{op_id=\"" + std::to_string(kWrOpId) + "\"}";
}

std::string wr_name(const char* metric, const char* key, const std::string& value) {
    return std::string(metric) + "{op_id=\"" + std::to_string(kWrOpId) + "\"," + key + "=\"" +
           value + "\"}";
}

std::uint64_t wr_counter(MetricsRegistry& registry, const std::string& name) {
    return registry.counter(name).value();
}

std::uint64_t wr_connector_counter(const char* metric) {
    return MetricsRegistry::global()
        .counter(clink::metrics::connector_metric_name(metric, metric::kConnector, "sink"))
        .value();
}

std::uint64_t wr_retries(const WriterStats& stats, FailureClass cls) {
    return stats.retries.at(static_cast<std::size_t>(cls));
}

std::int64_t wr_wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// A point in the log ring after which only what the case does next appears:
// the ring keeps milliseconds, and the filter takes records strictly after
// the mark, so the case waits out the mark's own millisecond, which may hold
// the previous case's last line.
std::int64_t wr_log_mark() {
    const std::int64_t mark = wr_wall_ms();
    std::this_thread::sleep_for(3ms);
    return mark;
}

// The writer's log lines since `since_ms`; with no logger they go to the
// process ring.
std::vector<LogRecord> wr_logs_since(std::int64_t since_ms) {
    return LogBuffer::global().tail(1024, "", since_ms, "sink.clickhouse");
}

std::size_t wr_logs_containing(const std::vector<LogRecord>& logs, const std::string& text) {
    return static_cast<std::size_t>(
        std::count_if(logs.begin(), logs.end(), [&](const LogRecord& r) {
            return r.message.find(text) != std::string::npos;
        }));
}

// Every counter, gauge and histogram count in `registry`, for comparing
// before and after.
std::map<std::string, double> wr_values(const MetricsRegistry& registry) {
    std::map<std::string, double> out;
    const MetricsRegistry::Snapshot snap = registry.snapshot();
    for (const auto& [name, value] : snap.counters) {
        out[name] = static_cast<double>(value);
    }
    for (const auto& [name, value] : snap.gauges) {
        out[name] = static_cast<double>(value);
    }
    for (const auto& h : snap.histograms) {
        out[h.name + " count"] = static_cast<double>(h.data.count);
        out[h.name + " sum"] = h.data.sum;
    }
    return out;
}

// --- When an INSERT closes ----------------------------------------------------

TEST(NativeWriterClose, AQuietStreamLandsWithinTheBatchIntervalWithoutAFlush) {
    WrRig rig;
    rig.options.batch_interval = 300ms;
    rig.start();
    const auto submitted = WrClock::now();
    rig.writer->submit(rig.chunk(0, 10, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->rows(kWrTable) == 10; }));
    const auto landed = WrClock::now() - submitted;
    // Nothing else can close this INSERT, so it cannot land before the
    // interval has run from its first row.
    EXPECT_GE(landed, 300ms);
    EXPECT_LE(landed, 300ms + scale_slack(200ms));
    EXPECT_EQ(rig.committed().size(), 1U);
    rig.writer->finish();
}

TEST(NativeWriterClose, TheBatchIntervalAlsoClosesAnInsertOnABusyStream) {
    WrRig rig;
    rig.options.batch_interval = 200ms;
    rig.start();
    // A chunk every 20 ms for a second: the stream never goes quiet, so only
    // the interval, timed from each INSERT's first row, can close them.
    for (std::int64_t from = 0; from < 50; ++from) {
        rig.writer->submit(rig.chunk(from * 10, from * 10 + 10, 8));
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_GE(rig.committed().size(), 3U);
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 500));
    rig.writer->finish();
}

TEST(NativeWriterClose, BatchRowsClosesEachInsertAtExactlyThatManyRows) {
    WrRig rig;
    rig.options.batch_rows = 100;
    rig.start();
    rig.writer->submit(rig.chunk(0, 250, 8));
    // Two INSERTs fill and close on their own; the last 50 rows wait for the
    // flush.
    ASSERT_TRUE(wr_eventually([&] { return rig.committed().size() == 2; }));
    for (const auto& insert : rig.committed()) {
        EXPECT_EQ(wr_rows_of(insert), 100U);
    }
    rig.writer->flush(1);
    const auto all = rig.committed();
    ASSERT_EQ(all.size(), 3U);
    EXPECT_EQ(wr_rows_of(all[2]), 50U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 250));
    rig.writer->finish();
}

TEST(NativeWriterClose, BatchBytesClosesAnInsertOnceTheNextRowWouldOverrunIt) {
    WrRig rig;
    rig.options.batch_bytes = kWrMiB;
    rig.start();
    // Small chunks, so that what the INSERT retains of them stays well below
    // the charged-memory limit and only the payload can close it.
    for (std::int64_t from = 0; from < 3000; from += 100) {
        rig.writer->submit(rig.chunk(from, from + 100, 1000));
    }
    ASSERT_TRUE(wr_eventually([&] { return rig.committed().size() == 2; }));
    constexpr std::size_t kRow = 8 + 1000 + 1;
    for (const auto& insert : rig.committed()) {
        const std::size_t payload = wr_payload_of(insert);
        EXPECT_LE(payload, kWrMiB);
        EXPECT_GT(payload + kRow, kWrMiB);
        EXPECT_EQ(wr_rows_of(insert), kWrMiB / kRow);
    }
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 3000));
    rig.writer->finish();
}

TEST(NativeWriterClose, ChargedMemoryClosesAnInsertOfShortStringsLongBeforeItsPayloadFills) {
    WrRig rig;
    rig.options.batch_bytes = kWrMiB;
    rig.start();
    // Ten bytes of payload a row, so all 100000 rows fit under batch_bytes,
    // but each also holds a 16-byte view, its id and its share of the chunk.
    for (std::int64_t from = 0; from < 100'000; from += 1000) {
        rig.writer->submit(rig.chunk(from, from + 1000, 1));
    }
    // No row limit, interval or flush can close an INSERT here, and the
    // payload never reaches batch_bytes: only the charge is left.
    ASSERT_TRUE(wr_eventually([&] { return !rig.committed().empty(); }));
    for (const auto& insert : rig.committed()) {
        EXPECT_LT(wr_payload_of(insert), kWrMiB * 3 / 4);
        EXPECT_LT(wr_rows_of(insert), 100'000U);
    }
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 100'000));
    rig.writer->finish();
}

TEST(NativeWriterClose, AnInsertLargerThanABlockGoesAsSeveralBlocksNoneAbove16MiB) {
    WrRig rig;
    rig.start();
    // 524297 bytes of payload a row: 31 rows fit in a block, 32 do not.
    for (std::int64_t from = 0; from < 40; from += 5) {
        rig.writer->submit(rig.chunk(from, from + 5, 512 * kWrKiB));
    }
    rig.writer->flush(1);
    const auto all = rig.committed();
    ASSERT_EQ(all.size(), 1U);
    ASSERT_EQ(all[0].blocks.size(), 2U);
    EXPECT_EQ(all[0].blocks[0].rows, 31U);
    EXPECT_EQ(all[0].blocks[1].rows, 9U);
    for (const auto& block : all[0].blocks) {
        EXPECT_LE(wr_block_payload(block), kMaxBlockBytes);
    }
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 40));
    rig.writer->finish();
}

TEST(NativeWriterClose, AChunkLargerThanTheQueueCapIsTakenByAnEmptyQueue) {
    WrRig rig;
    rig.start();
    Chunk big = rig.chunk(0, 17, kWrMiB);
    ASSERT_GT(big.bytes, kQueueBytes);
    const auto start = WrClock::now();
    rig.writer->submit(std::move(big));
    EXPECT_LT(WrClock::now() - start, scale_slack(1s));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 17));
    rig.writer->finish();
}

// --- What a failure freezes -------------------------------------------------

TEST(NativeWriterFrozen, AFailedInsertResendsTheBlocksItCutAndLaterRowsGoToTheNextInsert) {
    WrRig rig;
    // The failure lands rows on the queue while the INSERT is failing, from
    // the writer's own thread, so they arrive during the retry by
    // construction.
    fake::Fault fault = wr_reset_at_end();
    fault.on_fire = [&rig] { rig.writer->submit(rig.chunk(100, 105, 8)); };
    rig.server->inject(std::move(fault));
    rig.start();
    // 31 rows go out as the first block while the INSERT is open; the other
    // nine are in the builder when the flush closes it.
    for (std::int64_t from = 0; from < 40; from += 5) {
        rig.writer->submit(rig.chunk(from, from + 5, 512 * kWrKiB));
    }
    rig.writer->flush(1);
    rig.writer->flush(2);

    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 3U);
    const auto& first = all[0];
    const auto& resend = all[1];
    const auto& next = all[2];
    EXPECT_EQ(first.outcome, WrOutcome::Failed);
    EXPECT_EQ(resend.outcome, WrOutcome::Committed);
    EXPECT_EQ(next.outcome, WrOutcome::Committed);
    EXPECT_EQ(resend.token, first.token);
    ASSERT_EQ(first.blocks.size(), 2U);
    ASSERT_EQ(resend.blocks.size(), 2U);
    EXPECT_EQ(first.blocks[0].rows, 31U);
    EXPECT_EQ(first.blocks[1].rows, 9U);
    for (std::size_t i = 0; i < first.blocks.size(); ++i) {
        EXPECT_EQ(resend.blocks[i].rows, first.blocks[i].rows) << i;
        EXPECT_TRUE(resend.blocks[i].bytes == first.blocks[i].bytes)
            << "block " << i << " was not resent byte for byte";
    }
    EXPECT_NE(next.token, first.token);
    EXPECT_EQ(wr_ids_of(next), wr_range(100, 105));
    std::multiset<std::int64_t> expected = wr_range(0, 40);
    expected.merge(wr_range(100, 105));
    EXPECT_EQ(wr_landed_ids(*rig.server), expected);
    rig.writer->finish();
}

TEST(NativeWriterFrozen, AConversionFailureAfterABlockWasSentLandsNothingAndShowsNoText) {
    WrRig rig;
    rig.start();
    const std::uint64_t written_before = rig.handed->counters().bytes_written;
    const std::uint64_t bytes_before = wr_connector_counter("bytes_total");
    const std::uint64_t errors_before = wr_connector_counter("errors_total");
    for (std::int64_t from = 0; from < 40; from += 5) {
        rig.writer->submit(rig.chunk(from, from + 5, 512 * kWrKiB));
    }
    ASSERT_TRUE(wr_eventually([&] {
        const auto all = rig.inserts();
        return !all.empty() && !all[0].blocks.empty();
    }));
    rig.writer->submit(rig.wrap(wr_batch({std::nullopt}, {"TOPSECRET-VALUE"})));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kConversionFailed);
    const std::string text = error->what();
    EXPECT_NE(text.find("null in non-Nullable column `id`"), std::string::npos) << text;
    EXPECT_NE(text.find("row 0"), std::string::npos) << text;
    EXPECT_NE(text.find("id=Int64(NULL)"), std::string::npos) << text;
    EXPECT_NE(text.find("s=String"), std::string::npos) << text;
    EXPECT_EQ(text.find("TOPSECRET"), std::string::npos) << text;
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 1U);
    EXPECT_EQ(all[0].outcome, WrOutcome::Abandoned);
    EXPECT_EQ(rig.server->abandoned_mid_insert(), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    const WriterStats stats = rig.writer->stats();
    // The INSERT's 40 rows, and the row that could not be converted.
    EXPECT_EQ(stats.abandoned_rows, 41U);
    // The failed INSERT is a failed attempt like any other: the 16 MiB block
    // it sent counts in wire_bytes and bytes_out, and it counts once as a
    // connector error. The failure is stored last, so all of it is in place.
    const std::uint64_t written = rig.handed->counters().bytes_written - written_before;
    EXPECT_GT(written, all[0].blocks.at(0).bytes.size());
    EXPECT_EQ(stats.wire_bytes, written);
    EXPECT_EQ(wr_connector_counter("bytes_total") - bytes_before, written);
    EXPECT_EQ(wr_connector_counter("errors_total") - errors_before, 1U);
    // The failure stays: every later call on the task thread throws it.
    const auto again = wr_error([&] { rig.writer->submit(rig.chunk(50, 51, 8)); });
    ASSERT_TRUE(again);
    EXPECT_EQ(again->code(), code::kConversionFailed);
    const auto at_finish = wr_error([&] { rig.writer->finish(); });
    ASSERT_TRUE(at_finish);
    EXPECT_EQ(at_finish->code(), code::kConversionFailed);
}

// --- The barrier contract ---------------------------------------------------

TEST(NativeWriterBarrier, FlushReturnsOnlyAfterTheServerAcknowledgedEveryInsert) {
    WrRig rig;
    rig.options.batch_rows = 50;
    // The third End is the one the flush itself closes.
    rig.server->inject(wr_delay(fake::Step::End, 2s, 3));
    rig.start();
    rig.writer->submit(rig.chunk(0, 120, 8));
    const auto start = WrClock::now();
    rig.writer->flush(1);
    const auto took = WrClock::now() - start;
    EXPECT_GE(took, 2s);
    EXPECT_LT(took, 2s + scale_slack(2s));
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 3U);
    for (const auto& insert : all) {
        EXPECT_EQ(insert.outcome, WrOutcome::Committed);
    }
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 120));
    const auto flushes = rig.metrics.histogram(wr_name(metric::kBarrierFlushNs)).snapshot();
    EXPECT_EQ(flushes.count, 1U);
    EXPECT_GE(flushes.sum, 2e9);
    rig.writer->finish();
}

TEST(NativeWriterBarrier, AnEmptyIntervalSendsNothing) {
    WrRig rig;
    rig.start();
    rig.writer->flush(1);
    rig.writer->flush(2);
    EXPECT_TRUE(rig.inserts().empty());
    rig.writer->finish();
    for (const auto& sql : rig.server->statements()) {
        EXPECT_FALSE(sql.starts_with("INSERT")) << sql;
    }
}

// --- Retries and tokens -----------------------------------------------------

TEST(NativeWriterRetry, ABeginRefusedBeforeAnySendIsRetriedUnderTheSameToken) {
    WrRig rig;
    rig.server->inject(wr_fault(fake::Step::Begin,
                                fake::Fault::Kind::ServerError,
                                202,
                                "Too many simultaneous queries. Maximum: 100"));
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    std::vector<std::string> statements;
    for (const auto& sql : rig.server->statements()) {
        if (sql.starts_with("INSERT")) {
            statements.push_back(sql);
        }
    }
    ASSERT_EQ(statements.size(), 2U);
    EXPECT_EQ(statements[0], statements[1]);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 1U);
    EXPECT_EQ(all[0].outcome, WrOutcome::Committed);
    EXPECT_EQ(rig.server->landed(kWrTable).size(), 1U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 5));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(wr_retries(stats, FailureClass::TransientNotWritten), 1U);
    EXPECT_EQ(stats.in_doubt, 0U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(stats.rows_maybe_duplicated, 0U);
    // The reconnect is a new client, so the server was checked again.
    EXPECT_EQ(rig.probes->load(), 1);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kReconnectsTotal)), 1U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "retried_ok")), 1U);
    rig.writer->finish();
}

TEST(NativeWriterRetry, AClientThatWentBeforeTheWriterStartedIsRebuiltAndCheckedFirst) {
    WrRig rig;
    rig.start(false, true);
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 5));
    EXPECT_EQ(rig.probes->load(), 1);
    // Building it was not a failure, so nothing was retried.
    const WriterStats stats = rig.writer->stats();
    for (const auto retries : stats.retries) {
        EXPECT_EQ(retries, 0U);
    }
    rig.writer->finish();
}

TEST(NativeWriterRetry, AnOutageShorterThanTheWindowSpendsNothing) {
    WrRig rig;
    rig.options.retry_window = 5s;
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    rig.server->set_down(true, true);
    std::thread revive([&rig] {
        std::this_thread::sleep_for(1s);
        rig.server->set_down(false);
    });
    rig.writer->submit(rig.chunk(10, 20, 8));
    rig.writer->flush(2);
    revive.join();
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 20));
    EXPECT_GE(wr_counter(rig.metrics, wr_name(metric::kReconnectsTotal)), 1U);
    rig.writer->finish();
}

TEST(NativeWriterRetry, AnOutageLongerThanTheWindowFailsTheFlushAndKeepsWhatWasAcknowledged) {
    WrRig rig;
    rig.options.retry_window = 500ms;
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    rig.server->set_down(true, true);
    rig.writer->submit(rig.chunk(10, 20, 8));
    const auto start = WrClock::now();
    const auto error = wr_error([&] { rig.writer->flush(2); });
    const auto took = WrClock::now() - start;
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kRetryWindowExhausted);
    EXPECT_NE(std::string(error->what()).find("retry_window_ms=500"), std::string::npos)
        << error->what();
    EXPECT_LT(took, 500ms + scale_slack(1500ms));
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 10));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_acknowledged, 10U);
    EXPECT_EQ(stats.abandoned_rows, 10U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "failed")), 1U);
    const auto at_finish = wr_error([&] { rig.writer->finish(); });
    ASSERT_TRUE(at_finish);
    EXPECT_EQ(at_finish->code(), code::kRetryWindowExhausted);
}

TEST(NativeWriterRetry, TooManyPartsIsRetriedAsMergeBackPressure) {
    WrRig rig;
    rig.server->inject(
        wr_fault(fake::Step::End, fake::Fault::Kind::ServerError, 252, kWrPartsInPartition));
    rig.server->inject(
        wr_fault(fake::Step::End, fake::Fault::Kind::ServerError, 252, kWrPartsInTotal));
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 10));
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kPartsBackoffTotal)), 2U);
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::MergeBackpressure), 2U);
    EXPECT_EQ(
        wr_counter(rig.metrics, wr_name(metric::kRetriesTotal, "class", "merge_backpressure")), 2U);
    rig.writer->finish();
}

TEST(NativeWriterRetry, TooManyPartitionsFailsAtOnceNamingPartitionBy) {
    WrRig rig;
    rig.server->inject(
        wr_fault(fake::Step::End, fake::Fault::Kind::ServerError, 252, kWrTooManyPartitions));
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTooManyPartitions);
    const std::string text = error->what();
    EXPECT_NE(text.find("PARTITION BY"), std::string::npos) << text;
    EXPECT_NE(text.find("batch_rows"), std::string::npos) << text;
    EXPECT_NE(text.find("max_partitions_per_insert_block=100"), std::string::npos) << text;
    const WriterStats stats = rig.writer->stats();
    for (const auto retries : stats.retries) {
        EXPECT_EQ(retries, 0U);
    }
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterRetry, ABadOptionalAccessAtSendRebuildsTheClientAndLands) {
    WrRig rig;
    rig.server->inject(wr_fault(fake::Step::Send, fake::Fault::Kind::BadOptionalAccess));
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 10));
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::ClientDefect), 1U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kReconnectsTotal)), 1U);
    rig.writer->finish();
}

TEST(NativeWriterRetry, AValidationErrorIsRetriedOnceAndThenIsPermanent) {
    WrRig rig;
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::ValidationError));
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::ValidationError));
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kInsertFailed);
    EXPECT_NE(std::string(error->what()).find("client state error"), std::string::npos)
        << error->what();
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::ClientDefect), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterRetry, AnUnknownServerCodeFailsAfterItsCapNotAtTheEndOfTheWindow) {
    WrRig rig;
    rig.options.retry_window = 4s;
    for (int i = 0; i < 100; ++i) {
        rig.server->inject(wr_fault(
            fake::Step::Begin, fake::Fault::Kind::ServerError, 12345, "a code nobody listed"));
    }
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    const auto start = WrClock::now();
    const auto error = wr_error([&] { rig.writer->flush(1); });
    const auto took = WrClock::now() - start;
    ASSERT_TRUE(error);
    // The writer feeds each INSERT's earlier unknown failures to the policy,
    // so a permanent code nobody listed fails on its fourth appearance,
    // after three retries, instead of holding the barrier for the window.
    EXPECT_EQ(error->code(), code::kInsertFailed) << error->what();
    EXPECT_NE(std::string(error->what()).find("a code nobody listed"), std::string::npos)
        << error->what();
    // Not scaled for a sanitizer build: ten times the margin would pass the
    // 4 s window this tells apart.
    EXPECT_LT(took, 3s);
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::Unclassified), 3U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

// --- In-doubt resends -------------------------------------------------------

TEST(NativeWriterInDoubt, ALandedInsertResentToTheSameServerKeepsItsTokenAndLandsOnce) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 2U);
    EXPECT_EQ(all[0].token, all[1].token);
    EXPECT_EQ(rig.server->landed(kWrTable).size(), 1U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 20));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.in_doubt, 1U);
    EXPECT_EQ(stats.rows_resent_with_token, 20U);
    EXPECT_EQ(stats.rows_maybe_duplicated, 0U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInDoubtTotal)), 1U);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, ALandedInsertResentToAnUntestedLineTakesAFreshTokenAndMayDuplicate) {
    WrRig rig;
    rig.server->set_version(25, 8, 4);
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 2U);
    EXPECT_NE(all[0].token, all[1].token);
    // At least once: every row is there, and the first attempt's rows twice.
    std::multiset<std::int64_t> twice = wr_range(0, 20);
    twice.merge(wr_range(0, 20));
    EXPECT_EQ(wr_landed_ids(*rig.server), twice);
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsMaybeDuplicatedTotal)), 20U);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AResendThatReachesAnotherServerTakesAFreshTokenAndCountsOnce) {
    WrRig rig;
    const auto first = rig.server;
    const auto second = rig.add_server(kWrEp2);
    rig.options.endpoints = {kWrEp1, kWrEp2};
    fake::Fault fault = wr_reset_at_end();
    fault.on_fire = [first] { first->set_down(true, false); };
    first->inject(std::move(fault));
    // A second in-doubt failure, on the server the resend reached.
    second->inject(wr_reset_at_end());
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);

    const auto on_first = first->inserts(kWrTable);
    const auto on_second = second->inserts(kWrTable);
    ASSERT_EQ(on_first.size(), 1U);
    ASSERT_EQ(on_second.size(), 2U);
    EXPECT_NE(on_second[0].token, on_first[0].token);
    EXPECT_EQ(on_second[1].token, on_second[0].token);
    EXPECT_EQ(first->rows(kWrTable), 0U);
    EXPECT_EQ(wr_landed_ids(*second), wr_range(0, 20));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(stats.in_doubt, 2U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsMaybeDuplicatedTotal)), 20U);

    // The writer stays on the endpoint that works: the next rebuild starts
    // there, not at the first endpoint.
    const std::size_t tried_first = first->connects();
    second->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::SystemError));
    rig.writer->submit(rig.chunk(20, 30, 8));
    rig.writer->flush(2);
    EXPECT_EQ(first->connects(), tried_first);
    EXPECT_EQ(wr_landed_ids(*second), wr_range(0, 30));
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AnInsertThatLandedNothingIsResentByteForByteAndLandsOnce) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end());
    rig.start();
    rig.writer->submit(rig.chunk(0, 30, 64));
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 2U);
    EXPECT_EQ(all[0].outcome, WrOutcome::Failed);
    EXPECT_EQ(all[1].outcome, WrOutcome::Committed);
    EXPECT_EQ(all[0].token, all[1].token);
    ASSERT_EQ(all[0].blocks.size(), all[1].blocks.size());
    for (std::size_t i = 0; i < all[0].blocks.size(); ++i) {
        EXPECT_TRUE(all[0].blocks[i].bytes == all[1].blocks[i].bytes) << i;
    }
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 30));
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, APartialLandingAcrossPartitionsCompletesWithEachPartitionOnce) {
    fake::FakeTable table = wr_table();
    table.partition_of = [](std::int64_t id) { return id / 10; };
    WrRig rig(table);
    fake::Fault fault = wr_reset_at_end(fake::Fault::Landing::FirstPartitions);
    fault.landed_partitions = 2;
    rig.server->inject(std::move(fault));
    rig.start();
    rig.writer->submit(rig.chunk(0, 40, 8));
    rig.writer->flush(1);
    EXPECT_EQ(rig.server->landed(kWrTable).size(), 4U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 40));
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AnExpiredDeduplicationWindowLetsTheResendLandAgain) {
    WrRig rig(wr_table(2));
    fake::Fault fault = wr_reset_at_end(fake::Fault::Landing::Everything);
    const auto server = rig.server;
    fault.on_fire = [server] { server->land_foreign(kWrTable, 3); };
    server->inject(std::move(fault));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    // The token fell out of the window, so the resend lands again: a
    // duplicate, never a loss.
    std::multiset<std::int64_t> twice = wr_range(0, 20);
    twice.merge(wr_range(0, 20));
    EXPECT_EQ(wr_landed_ids(*server), twice);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, TheSameHostRestartedIntoAnotherPatchTakesAFreshToken) {
    WrRig rig;
    fake::Fault fault = wr_reset_at_end(fake::Fault::Landing::Everything);
    const auto server = rig.server;
    // The connection drops as the server restarts into another patch of the
    // same tested line, at the same endpoint and under the same name.
    fault.on_fire = [server] { server->set_version(26, 8, 99); };
    server->inject(std::move(fault));
    rig.start();
    const std::int64_t since = wr_log_mark();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    // Another version may squash or hash the resend differently, so the
    // token is not kept: the resend lands again, a duplicate and no loss.
    const auto tokens = wr_tokens(*server);
    ASSERT_EQ(tokens.size(), 2U);
    EXPECT_NE(tokens[0], tokens[1]);
    EXPECT_EQ(wr_landed_ids(*server), wr_twice(0, 20));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsMaybeDuplicatedTotal)), 20U);
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since),
                                 "now writes to " + server->display_name() + " 26.8.99"),
              1U);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AKeptTokenIntoATableWithoutALogCountsAsMaybeDuplicated) {
    // A plain MergeTree keeps no deduplication log by default, so even the
    // same token, to the same server, lands again.
    WrRig rig(wr_table(0));
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    const auto tokens = wr_tokens(*rig.server);
    ASSERT_EQ(tokens.size(), 2U);
    EXPECT_EQ(tokens[0], tokens[1]);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_twice(0, 20));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AKeptTokenIntoDistributedCountsAsMaybeDuplicated) {
    WrRig rig;
    // Distributed deduplicates nothing itself, whatever the local tables keep.
    rig.target_tweak = [](TargetInfo& t) { t.family = EngineFamily::Distributed; };
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    const auto tokens = wr_tokens(*rig.server);
    ASSERT_EQ(tokens.size(), 2U);
    EXPECT_EQ(tokens[0], tokens[1]);
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    rig.writer->finish();
}

TEST(NativeWriterInDoubt, AKeptTokenThenAFreshOneMovesTheRowsFromResentToMaybeDuplicated) {
    WrRig rig;
    const auto first = rig.server;
    const auto second = rig.add_server(kWrEp2);
    rig.options.endpoints = {kWrEp1, kWrEp2};
    // In doubt on the first server, which then takes the resend under the
    // same token; that resend fails in doubt too, as the server goes down.
    first->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    fake::Fault again = wr_reset_at_end();
    again.on_fire = [first] { first->set_down(true, false); };
    first->inject(std::move(again));
    rig.start();
    rig.writer->submit(rig.chunk(0, 20, 8));
    rig.writer->flush(1);
    const auto on_first = wr_tokens(*first);
    const auto on_second = wr_tokens(*second);
    ASSERT_EQ(on_first.size(), 2U);
    ASSERT_EQ(on_second.size(), 1U);
    EXPECT_EQ(on_first[1], on_first[0]);
    EXPECT_NE(on_second[0], on_first[0]);
    EXPECT_EQ(wr_landed_ids(*first), wr_range(0, 20));
    EXPECT_EQ(wr_landed_ids(*second), wr_range(0, 20));
    // Counted once, in one of the two: the fresh token on the second server
    // means the rows may be twice in the cluster.
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 20U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsMaybeDuplicatedTotal)), 20U);
    rig.writer->finish();
}

// --- Re-probing every new client ----------------------------------------------

TEST(NativeWriterReprobe, ANewClientOnAServerThatNowInsertsAsynchronouslyIsRefused) {
    WrRig rig;
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    rig.server->set_merge_tree_setting("async_insert", "1");
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::SystemError));
    rig.writer->submit(rig.chunk(5, 10, 8));
    const auto error = wr_error([&] { rig.writer->flush(2); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kTargetAsyncInsert);
    EXPECT_EQ(wr_counter(rig.metrics,
                         wr_name(metric::kRefusalsTotal, "reason", code::kTargetAsyncInsert)),
              1U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 5));
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
}

TEST(NativeWriterReprobe,
     ANewClientOnA263ServerDropsTheStrictLimitsSettingButNotThePinnedThresholds) {
    WrRig rig;
    const auto older = rig.add_server(kWrEp2);
    older->set_version(26, 3, 7);
    older->remove_setting("use_strict_insert_block_limits");
    older->set_setting("min_insert_block_size_rows", "777");
    older->set_setting("min_insert_block_size_bytes", "888");
    rig.options.endpoints = {kWrEp1, kWrEp2};
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    const std::string newer_sql = rig.inserts().at(0).sql;
    EXPECT_NE(newer_sql.find(", use_strict_insert_block_limits=0"), std::string::npos) << newer_sql;
    EXPECT_NE(newer_sql.find("min_insert_block_size_rows=1048449,"), std::string::npos)
        << newer_sql;
    EXPECT_NE(newer_sql.find("min_insert_block_size_bytes=268402944,"), std::string::npos)
        << newer_sql;

    const std::int64_t since = wr_log_mark();
    rig.server->set_down(true, true);
    rig.writer->submit(rig.chunk(5, 10, 8));
    rig.writer->flush(2);
    const auto on_older = older->inserts(kWrTable);
    ASSERT_EQ(on_older.size(), 1U);
    EXPECT_EQ(on_older[0].sql.find("use_strict_insert_block_limits"), std::string::npos)
        << on_older[0].sql;
    EXPECT_NE(on_older[0].sql.find("min_insert_block_size_rows=1048449,"), std::string::npos)
        << on_older[0].sql;
    EXPECT_NE(on_older[0].sql.find("min_insert_block_size_bytes=268402944,"), std::string::npos)
        << on_older[0].sql;
    EXPECT_EQ(wr_landed_ids(*older), wr_range(5, 10));
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since),
                                 "now writes to " + older->display_name() + " 26.3.7"),
              1U);
    rig.writer->finish();
}

TEST(NativeWriterReprobe, AFailedProbeReadIsRetriedLikeAnyOtherTransientFailure) {
    WrRig rig;
    rig.start();
    // After start, so the opener's own probe does not meet them.
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::SystemError));
    rig.server->inject(wr_fault(fake::Step::Select, fake::Fault::Kind::SystemError));
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 5));
    // One probe failed at its first read and the next one passed.
    EXPECT_EQ(rig.probes->load(), 2);
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::TransientNotWritten), 2U);
    rig.writer->finish();
}

// --- Cancel, stop and the detach ----------------------------------------------

TEST(NativeWriterCancel, ACancelDuringABackoffAbandonsTheInsertAndLandsNothingOfIt) {
    WrRig rig;
    rig.options.retry_window = 30s;
    for (int i = 0; i < 50; ++i) {
        rig.server->inject(wr_reset_at_end());
    }
    rig.start();
    rig.writer->submit(rig.chunk(0, 30, 8));
    auto flushed = std::async(std::launch::async,
                              [&rig] { return wr_error([&rig] { rig.writer->flush(1); }); });
    ASSERT_TRUE(wr_eventually([&] { return rig.inserts().size() >= 2; }));
    const auto cancelled_at = WrClock::now();
    rig.flags.cancel();
    const auto error = flushed.get();
    EXPECT_LE(WrClock::now() - cancelled_at, scale_slack(200ms));
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    // Every attempt was abandoned, the one in flight included, and nothing of
    // the INSERT landed.
    EXPECT_EQ(rig.server->abandoned_mid_insert(), rig.inserts().size());
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 30U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "abandoned")), 1U);
}

TEST(NativeWriterCancel, AStopThatLandsWhileTheWriterConnectsEndsItWithoutStartingAnInsert) {
    WrRig rig;
    rig.options.batch_interval = 50ms;
    rig.options.retry_window = 30s;
    rig.server->inject(wr_reset_at_end());
    rig.start();
    // After start, so the opener's own connect does not meet it.
    rig.server->inject(wr_delay(fake::Step::Connect, 3s));
    rig.writer->submit(rig.chunk(0, 5, 8));
    // The rig's connect, then the writer's reconnect after the failed End.
    ASSERT_TRUE(wr_eventually([&] { return rig.server->connects() >= 2; }));
    const auto stopped_at = WrClock::now();
    rig.writer->abort();
    // Scaled, it stays below the 3 s connect delay it tells apart.
    EXPECT_LE(WrClock::now() - stopped_at, scale_slack(200ms));
    EXPECT_EQ(rig.inserts().size(), 1U);
    std::size_t insert_statements = 0;
    for (const auto& sql : rig.server->statements()) {
        if (sql.starts_with("INSERT")) {
            ++insert_statements;
        }
    }
    EXPECT_EQ(insert_statements, 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    const auto later = wr_error([&] { rig.writer->submit(rig.chunk(5, 6, 8)); });
    ASSERT_TRUE(later);
    EXPECT_EQ(later->code(), code::kCancelled);
}

TEST(NativeWriterCancel, AnInterruptThatLandsInsideTheConnectFailsItAndStartsNoInsert) {
    WrRig rig;
    rig.options.batch_interval = 50ms;
    rig.options.retry_window = 30s;
    rig.server->inject(wr_reset_at_end());
    rig.start();
    // The cancel and the interrupt land after the writer's own stop check
    // and after the transport's check of its flag, inside the connect, as a
    // task thread's abort() would. The interrupt is sticky, so the connect
    // still fails rather than completing on a fresh client.
    fake::Fault fault = wr_delay(fake::Step::Connect, 0ms);
    fault.on_fire = [&rig] {
        rig.flags.cancel();
        rig.handed->interrupt();
    };
    rig.server->inject(std::move(fault));
    ASSERT_EQ(rig.handed->counters().connects, 1U) << "the rig's own connect, as the opener's";
    rig.writer->submit(rig.chunk(0, 5, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->connects() >= 2; }));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    // The connect itself failed. The cancel alone would also end the writer,
    // at its check after the connect, so only this count shows that the
    // interrupt reached a connect already under way: one that was lost would
    // have let the connect complete on a fresh client.
    EXPECT_EQ(rig.handed->counters().connects, 1U);
    // No probe read followed the connect, and no INSERT began after the
    // first.
    EXPECT_EQ(rig.probes->load(), 0);
    EXPECT_EQ(rig.inserts().size(), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
}

TEST(NativeWriterCancel, AWriterStuckWhereNoInterruptReachesIsDetachedAndThenStaysSilent) {
    WrRig rig;
    rig.options.batch_interval = 50ms;
    rig.options.retry_window = 30s;
    rig.server->inject(wr_reset_at_end());
    rig.start(true);
    rig.server->inject(wr_fault(fake::Step::Connect, fake::Fault::Kind::Uninterruptible));
    rig.writer->submit(rig.chunk(0, 5, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->connects() >= 2; }));

    const std::int64_t aborting = wr_log_mark();
    const auto aborted_at = WrClock::now();
    rig.writer->abort();
    const auto took = WrClock::now() - aborted_at;
    EXPECT_GE(took, 4900ms);
    EXPECT_LE(took, 5s + scale_slack(200ms));
    // The summary is the last the writer says, so it counts the five rows of
    // the INSERT the stuck thread still holds: they were never acknowledged,
    // and the job's restart replays them.
    const auto logs = wr_logs_since(aborting);
    EXPECT_EQ(wr_logs_containing(logs, "clickhouse native sink cancelled:"), 1U);
    EXPECT_EQ(wr_logs_containing(logs, " abandoned_rows=5 "), 1U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "abandoned")), 1U);

    const auto metrics_before = wr_values(rig.metrics);
    const std::uint64_t bytes_before = wr_connector_counter("bytes_total");
    const std::uint64_t errors_before = wr_connector_counter("errors_total");
    const std::uint64_t records_before = wr_connector_counter("records_total");
    rig.writer.reset();
    ASSERT_FALSE(rig.gone->load()) << "the stuck thread should still hold its transport";
    const std::int64_t since = wr_log_mark();
    rig.server->release();
    ASSERT_TRUE(wr_eventually([&] { return rig.gone->load(); }, 3s))
        << "the detached thread never let go of its state";
    EXPECT_EQ(wr_values(rig.metrics), metrics_before);
    EXPECT_EQ(wr_connector_counter("bytes_total"), bytes_before);
    EXPECT_EQ(wr_connector_counter("errors_total"), errors_before);
    EXPECT_EQ(wr_connector_counter("records_total"), records_before);
    EXPECT_TRUE(wr_logs_since(since).empty());
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterCancel, ADetachedWritersRowsAreCountedOnceWhenItFinallyLetsGo) {
    WrRig rig;
    rig.options.batch_interval = 50ms;
    rig.options.retry_window = 30s;
    rig.server->inject(wr_reset_at_end());
    rig.start(true);
    rig.server->inject(wr_fault(fake::Step::Connect, fake::Fault::Kind::Uninterruptible));
    rig.writer->submit(rig.chunk(0, 5, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->connects() >= 2; }));
    rig.writer->abort();
    // Checked without returning early, so the stuck thread is always let go.
    const auto abandoned = wr_name(metric::kInsertsTotal, "outcome", "abandoned");
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
    EXPECT_EQ(wr_counter(rig.metrics, abandoned), 1U);
    EXPECT_GT(rig.budget->usage().used, 0U) << "the stuck thread still holds its INSERT";

    // The Writer stays, so its stats can be read after the thread goes. The
    // thread passes its own count of the INSERT before it lets go of it.
    rig.server->release();
    ASSERT_TRUE(wr_eventually([&] { return rig.budget->usage().used == 0; }, 3s))
        << "the detached thread never let go of its INSERT";
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
    EXPECT_EQ(wr_counter(rig.metrics, abandoned), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    rig.writer.reset();
    EXPECT_TRUE(wr_eventually([&] { return rig.gone->load(); }, 3s));
}

// The rows the writer never took from its queue, and the rest of the chunk
// it was taking in, are as unacknowledged as the INSERT it holds, and the
// job's restart replays them too, so abandoned_rows counts every one. Only
// the INSERT in flight counts as an abandoned INSERT.
TEST(NativeWriterCancel, AnAbortCountsTheQueuedRowsAndTheRestOfTheChunkAsAbandoned) {
    struct Case {
        std::string label;
        std::vector<std::pair<std::int64_t, std::int64_t>> chunks;  // [from, to)
    };
    const std::vector<Case> cases = {
        {"chunks waiting in the queue", {{0, 5}, {5, 10}, {10, 15}}},
        {"the rest of the chunk the writer was taking in", {{0, 15}}},
        {"both", {{0, 12}, {12, 15}}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.label);
        WrRig rig;
        rig.options.batch_rows = 5;
        auto hanging = std::make_shared<std::atomic<bool>>(false);
        fake::Fault hang = wr_fault(fake::Step::End, fake::Fault::Kind::Hang);
        hang.on_fire = [hanging] { hanging->store(true); };
        rig.server->inject(std::move(hang));
        rig.start();
        rig.writer->submit(rig.chunk(c.chunks.front().first, c.chunks.front().second, 8));
        ASSERT_TRUE(wr_eventually([&hanging] { return hanging->load(); }));
        // Held in the first INSERT's End, so these stay in the queue.
        for (std::size_t i = 1; i < c.chunks.size(); ++i) {
            rig.writer->submit(rig.chunk(c.chunks[i].first, c.chunks[i].second, 8));
        }
        const std::int64_t since = wr_log_mark();
        rig.writer->abort();
        EXPECT_EQ(rig.server->rows(kWrTable), 0U);
        EXPECT_EQ(rig.writer->stats().abandoned_rows, 15U);
        EXPECT_EQ(wr_logs_containing(wr_logs_since(since), " abandoned_rows=15 "), 1U);
        EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "abandoned")),
                  1U);
    }
}

// A writer that fails on its own drops what is queued behind the failed
// INSERT, and counts it, before the task thread can see it has exited.
TEST(NativeWriterCancel, AFailureCountsTheRowsItDropsFromTheQueueAsAbandoned) {
    WrRig rig;
    rig.options.batch_rows = 5;
    // The first chunk fills the first INSERT, which fails before the writer
    // takes anything more from the queue; its End answers late, so the
    // chunks behind it are queued by then.
    fake::Fault missing = wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 60, "Table db.events does not exist");
    missing.delay = 500ms;
    rig.server->inject(std::move(missing));
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->submit(rig.chunk(5, 10, 8));
    rig.writer->submit(rig.chunk(10, 15, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kInsertFailed);
    const std::int64_t since = wr_log_mark();
    rig.writer->abort();
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 15U);
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since), " abandoned_rows=15 "), 1U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "failed")), 1U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "abandoned")), 0U);
}

// A detached writer reports nothing more, so abort() counts what it holds
// for it: the rest of the chunk it was taking in as well as its INSERT.
TEST(NativeWriterCancel, ADetachedWriterHoldingPartOfAChunkHasAllOfItCountedAsAbandoned) {
    WrRig rig;
    rig.options.batch_rows = 5;
    rig.options.retry_window = 30s;
    rig.server->inject(wr_reset_at_end());
    rig.start(true);
    rig.server->inject(wr_fault(fake::Step::Connect, fake::Fault::Kind::Uninterruptible));
    // The first five rows make an INSERT whose End fails; the reconnect for
    // its resend sticks, with ten rows of the chunk not yet taken in.
    rig.writer->submit(rig.chunk(0, 15, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->connects() >= 2; }));
    const std::int64_t since = wr_log_mark();
    rig.writer->abort();
    const auto abandoned = wr_name(metric::kInsertsTotal, "outcome", "abandoned");
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 15U);
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since), " abandoned_rows=15 "), 1U);
    EXPECT_EQ(wr_counter(rig.metrics, abandoned), 1U);
    // Once it lets go, the thread finds its rows already counted.
    rig.server->release();
    ASSERT_TRUE(wr_eventually([&] { return rig.budget->usage().used == 0; }, 3s));
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 15U);
    EXPECT_EQ(wr_counter(rig.metrics, abandoned), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    rig.writer.reset();
    EXPECT_TRUE(wr_eventually([&] { return rig.gone->load(); }, 3s));
}

TEST(NativeWriterCancel, ACancelTheWriterSawFirstIsStillAbortedBeforeTheTaskThreadThrows) {
    WrRig rig;
    rig.start();
    Chunk c = rig.chunk(0, 5, 8);
    const std::size_t chunk_size = c.bytes;
    rig.writer->submit(std::move(c));
    // The INSERT is charged for its rows beside the chunk once they are in the
    // builder.
    ASSERT_TRUE(wr_eventually([&] { return rig.budget->usage().used > chunk_size; }));
    rig.flags.cancel();
    // The writer sees the cancel at its next slice, stores its own failure and
    // lets go of the INSERT, all before the task thread calls anything.
    ASSERT_TRUE(wr_eventually([&] { return rig.budget->usage().used == 0; }));

    const std::int64_t since = wr_log_mark();
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    EXPECT_NE(std::string(error->what()).find("the writer stopped on cancel"), std::string::npos)
        << error->what();
    // abort() ran before the throw, as on every cancel: the writer is joined
    // and its summary is out.
    const auto logs = wr_logs_since(since);
    ASSERT_EQ(wr_logs_containing(logs, "clickhouse native sink cancelled:"), 1U);
    EXPECT_EQ(wr_logs_containing(logs, " abandoned_rows=5 "), 1U);
    const auto later = wr_error([&] { rig.writer->submit(rig.chunk(5, 6, 8)); });
    ASSERT_TRUE(later);
    EXPECT_EQ(later->code(), code::kCancelled);
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since), "clickhouse native sink cancelled:"), 1U);
}

TEST(NativeWriterCancel, AbortIsIdempotentLogsOneSummaryAndLeavesTheTaskThreadACancel) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end());
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    const std::int64_t since = wr_log_mark();
    rig.writer->abort();
    rig.writer->abort();
    const auto logs = wr_logs_since(since);
    ASSERT_EQ(wr_logs_containing(logs, "clickhouse native sink cancelled:"), 1U);
    const auto summary = std::find_if(logs.begin(), logs.end(), [](const LogRecord& r) {
        return r.message.find("clickhouse native sink cancelled:") != std::string::npos;
    });
    EXPECT_EQ(summary->level, "warn");
    EXPECT_NE(summary->message.find("rows_acknowledged=10 inserts=1"), std::string::npos)
        << summary->message;
    EXPECT_NE(summary->message.find("in_doubt:1"), std::string::npos) << summary->message;
    EXPECT_NE(summary->message.find(" in_doubt=1 "), std::string::npos) << summary->message;
    const auto at_finish = wr_error([&] { rig.writer->finish(); });
    ASSERT_TRUE(at_finish);
    EXPECT_EQ(at_finish->code(), code::kCancelled);
}

TEST(NativeWriterCancel, AbortAfterACleanFinishDoesNothing) {
    WrRig rig;
    rig.start();
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->finish();
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 10));
    const std::int64_t since = wr_log_mark();
    rig.writer->abort();
    EXPECT_EQ(wr_logs_containing(wr_logs_since(since), "cancelled"), 0U);
}

// --- The attempt deadline -----------------------------------------------------

TEST(NativeWriterDeadline, AnEndKeptBusyPastTheWindowTimesOutInDoubtAndExhaustsWithinTwoWindows) {
    WrRig rig;
    rig.options.retry_window = 400ms;
    for (int i = 0; i < 10; ++i) {
        rig.server->inject(wr_delay(fake::Step::End, 30s));
    }
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    const auto start = WrClock::now();
    auto flushed = std::async(std::launch::async,
                              [&rig] { return wr_error([&rig] { rig.writer->flush(1); }); });
    ASSERT_TRUE(wr_eventually([&] {
        const auto all = rig.inserts();
        return !all.empty() && all[0].outcome != WrOutcome::Open;
    }));
    const auto first_failed = WrClock::now() - start;
    const auto error = flushed.get();
    const auto took = WrClock::now() - start;
    // The first End is cut one window after it began, not 30 s later.
    EXPECT_GE(first_failed, 400ms);
    EXPECT_LE(first_failed, 400ms + scale_slack(200ms));
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kRetryWindowExhausted);
    EXPECT_NE(std::string(error->what()).find("past the attempt deadline"), std::string::npos)
        << error->what();
    EXPECT_LE(took, 2 * 400ms + scale_slack(200ms));
    EXPECT_GE(rig.writer->stats().in_doubt, 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterDeadline, AnInsertHeldOpenByALongIntervalIsNotCutByTheDeadline) {
    WrRig rig;
    rig.options.retry_window = 300ms;
    rig.options.batch_interval = 1500ms;
    rig.start();
    // The first block goes out at once, so BeginInsert runs early and the
    // INSERT then stays open for the rest of the interval.
    for (std::int64_t from = 0; from < 40; from += 5) {
        rig.writer->submit(rig.chunk(from, from + 5, 512 * kWrKiB));
    }
    ASSERT_TRUE(wr_eventually([&] {
        const auto all = rig.inserts();
        return !all.empty() && !all[0].blocks.empty();
    }));
    std::this_thread::sleep_for(600ms);
    rig.writer->submit(rig.chunk(40, 45, 8));
    ASSERT_TRUE(wr_eventually([&] { return rig.server->rows(kWrTable) == 45; }, 5s));
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 1U);
    EXPECT_EQ(all[0].outcome, WrOutcome::Committed);
    ASSERT_EQ(all[0].blocks.size(), 2U);
    EXPECT_EQ(all[0].blocks[1].rows, 14U);
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.in_doubt, 0U);
    for (const auto retries : stats.retries) {
        EXPECT_EQ(retries, 0U);
    }
    rig.writer->finish();
}

// A BeginInsert the server keeps busy is cut one window after it began, on
// the first attempt too, wherever that attempt sends it: before the first
// block when one is due, or when the INSERT closes.
void wr_expect_a_busy_first_begin_is_cut(bool first_block_due) {
    WrRig rig;
    rig.options.retry_window = 400ms;
    for (int i = 0; i < 10; ++i) {
        rig.server->inject(wr_delay(fake::Step::Begin, 20s));
    }
    rig.start();
    const auto start = WrClock::now();
    const auto error = wr_error([&] {
        if (first_block_due) {
            // 31 rows fill a block, so its BeginInsert goes out before any
            // flush.
            for (std::int64_t from = 0; from < 40; from += 5) {
                rig.writer->submit(rig.chunk(from, from + 5, 512 * kWrKiB));
            }
        } else {
            rig.writer->submit(rig.chunk(0, 5, 8));
        }
        rig.writer->flush(1);
    });
    const auto took = WrClock::now() - start;
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kRetryWindowExhausted) << error->what();
    EXPECT_NE(std::string(error->what()).find("past the attempt deadline"), std::string::npos)
        << error->what();
    // The first BeginInsert is cut a window after it began, and the retry at
    // the end of the window its failure started, not after the 20 s the
    // server would have taken. Not scaled for a sanitizer build: ten times the
    // margin would pass those 20 s.
    EXPECT_LT(took, 3s);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterDeadline, AFirstBeginKeptBusyBeforeTheFirstBlockIsCutByTheDeadline) {
    wr_expect_a_busy_first_begin_is_cut(true);
}

TEST(NativeWriterDeadline, AFirstBeginKeptBusyWhenTheInsertClosesIsCutByTheDeadline) {
    wr_expect_a_busy_first_begin_is_cut(false);
}

// --- Backpressure -------------------------------------------------------------

TEST(NativeWriterQueue, SubmitBlocksWhileTheQueueIsFullAndTheQueueStaysWithinItsCap) {
    WrRig rig;
    rig.options.batch_rows = 4;
    for (int i = 0; i < 7; ++i) {
        rig.server->inject(wr_delay(fake::Step::End, 250ms));
    }
    rig.start();
    std::atomic<bool> sampling{true};
    std::atomic<std::size_t> highest{0};
    std::thread sampler([&] {
        while (sampling.load()) {
            highest.store(std::max(highest.load(), rig.writer->queue_bytes()));
            std::this_thread::sleep_for(1ms);
        }
    });
    std::size_t chunk_size = 0;
    const auto start = WrClock::now();
    for (std::int64_t from = 0; from < 28; from += 4) {
        Chunk c = rig.chunk(from, from + 4, kWrMiB);
        chunk_size = std::max(chunk_size, c.bytes);
        rig.writer->submit(std::move(c));
    }
    const auto submitting = WrClock::now() - start;
    rig.writer->flush(1);
    sampling.store(false);
    sampler.join();
    // Four chunks of just over 4 MiB fill the queue, so the sixth submit waits
    // for the writer to take the second.
    EXPECT_GE(submitting, 200ms);
    EXPECT_LE(highest.load(), kQueueBytes + chunk_size);
    const auto blocked = rig.metrics.histogram(wr_name(metric::kBackpressureBlockedNs)).snapshot();
    EXPECT_GE(blocked.count, 1U);
    EXPECT_GT(blocked.sum, 0.0);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 28));
    EXPECT_EQ(rig.writer->queue_bytes(), 0U);
    rig.writer->finish();
}

// A submit held by a full queue looks at the writer's failure, the task's
// cancel and the stop flag every slice, so each of them releases it.
TEST(NativeWriterQueue, ACancelReleasesASubmitHeldByAFullQueue) {
    WrRig rig;
    rig.options.batch_rows = 4;
    // The INSERT ahead of the queue never ends by itself.
    rig.server->inject(wr_fault(fake::Step::End, fake::Fault::Kind::Hang));
    rig.start();
    auto submitting =
        std::async(std::launch::async, [&rig] { return wr_submit_until_one_throws(rig); });
    ASSERT_TRUE(wr_eventually([&] { return rig.writer->queue_bytes() >= kQueueBytes; }));
    // Long enough that the next submit is waiting for room.
    std::this_thread::sleep_for(250ms);
    const auto cancelled_at = WrClock::now();
    rig.flags.cancel();
    const auto status = submitting.wait_for(scale_slack(2s));
    const auto released = WrClock::now() - cancelled_at;
    if (status != std::future_status::ready) {
        // Only the abort can free it now; the case fails rather than hangs.
        rig.writer->abort();
    }
    ASSERT_EQ(status, std::future_status::ready) << "the cancel did not release the submit";
    const WrHeld held = submitting.get();
    ASSERT_TRUE(held.error);
    EXPECT_EQ(held.error->code(), code::kCancelled);
    EXPECT_GE(held.took, 100ms) << "the submit that threw was not the one held";
    EXPECT_LE(released, scale_slack(200ms));
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterQueue, APermanentFailureReleasesASubmitHeldByAFullQueueWithThatFailure) {
    WrRig rig;
    rig.options.batch_rows = 4;
    // The INSERT's End answers late, with a failure that is never retried.
    fake::Fault missing = wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 60, "Table db.events does not exist");
    missing.delay = 1500ms;
    rig.server->inject(std::move(missing));
    rig.start();
    const WrHeld held = wr_submit_until_one_throws(rig);
    ASSERT_TRUE(held.error);
    EXPECT_EQ(held.error->code(), code::kInsertFailed);
    EXPECT_NE(std::string(held.error->what()).find("does not exist"), std::string::npos)
        << held.error->what();
    // The queue was full well before the End answered, so the submit that
    // threw had been held until the failure came.
    EXPECT_GE(held.took, 500ms);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterQueue, AnAbortFromAnotherThreadReleasesASubmitHeldByAFullQueue) {
    WrRig rig;
    rig.options.batch_rows = 4;
    rig.server->inject(wr_fault(fake::Step::End, fake::Fault::Kind::Hang));
    rig.start();
    auto submitting =
        std::async(std::launch::async, [&rig] { return wr_submit_until_one_throws(rig); });
    ASSERT_TRUE(wr_eventually([&] { return rig.writer->queue_bytes() >= kQueueBytes; }));
    std::this_thread::sleep_for(250ms);
    const auto aborted_at = WrClock::now();
    std::thread aborter([&rig] { rig.writer->abort(); });
    const auto status = submitting.wait_for(scale_slack(2s));
    const auto released = WrClock::now() - aborted_at;
    aborter.join();
    ASSERT_EQ(status, std::future_status::ready) << "the abort did not release the submit";
    const WrHeld held = submitting.get();
    ASSERT_TRUE(held.error);
    EXPECT_EQ(held.error->code(), code::kCancelled);
    EXPECT_GE(held.took, 100ms) << "the submit that threw was not the one held";
    EXPECT_LE(released, scale_slack(300ms));
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterQueue, ACancelReleasesAFlushQueuedBehindAFullQueue) {
    WrRig rig;
    rig.options.batch_rows = 4;
    rig.server->inject(wr_fault(fake::Step::End, fake::Fault::Kind::Hang));
    rig.start();
    // One INSERT hanging in End and four chunks behind it: the queue is full.
    for (std::int64_t from = 0; from < 20; from += 4) {
        rig.writer->submit(rig.chunk(from, from + 4, kWrMiB));
    }
    ASSERT_GE(rig.writer->queue_bytes(), kQueueBytes);
    auto flushed = std::async(std::launch::async,
                              [&rig] { return wr_error([&rig] { rig.writer->flush(1); }); });
    // Long enough that the flush is waiting.
    std::this_thread::sleep_for(100ms);
    const auto cancelled_at = WrClock::now();
    rig.flags.cancel();
    const auto status = flushed.wait_for(scale_slack(2s));
    const auto released = WrClock::now() - cancelled_at;
    if (status != std::future_status::ready) {
        // Only the abort can free it now; the case fails rather than hangs.
        rig.writer->abort();
    }
    ASSERT_EQ(status, std::future_status::ready) << "the cancel did not release the flush";
    const auto error = flushed.get();
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    EXPECT_LE(released, scale_slack(200ms));
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterQueue, TheQueueHoldsAtMost256ChunksHoweverSmallTheyAre) {
    WrRig rig;
    rig.options.batch_rows = 1;
    rig.server->inject(wr_delay(fake::Step::End, 1500ms));
    rig.start();
    rig.writer->submit(rig.chunk(0, 1, 8));
    // The writer is in that INSERT's End now, so what follows stays queued.
    ASSERT_TRUE(wr_eventually([&] { return !rig.inserts().empty(); }));
    const auto waits = [&] {
        return rig.metrics.histogram(wr_name(metric::kBackpressureBlockedNs)).snapshot().count;
    };
    const auto cap = static_cast<std::int64_t>(kQueueChunks);
    for (std::int64_t id = 1; id <= cap; ++id) {
        rig.writer->submit(rig.chunk(id, id + 1, 8));
    }
    // Far below the byte cap, and not one of them had to wait.
    EXPECT_LT(rig.writer->queue_bytes(), kQueueBytes / 100);
    EXPECT_EQ(waits(), 0U);
    const auto start = WrClock::now();
    rig.writer->submit(rig.chunk(cap + 1, cap + 2, 8));
    const auto waited = WrClock::now() - start;
    // The next one waits for the writer to take one, after the End.
    EXPECT_EQ(waits(), 1U);
    EXPECT_GE(waited, 500ms);
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, cap + 2));
    rig.writer->finish();
}

TEST(NativeWriterQueue, AFlushGoesPastAFullQueueAndNeverCountsAgainstItsCaps) {
    WrRig rig;
    rig.options.batch_rows = 4;
    for (int i = 0; i < 5; ++i) {
        rig.server->inject(wr_delay(fake::Step::End, 300ms));
    }
    rig.start();
    // One INSERT in flight and four chunks behind it: the queue is full.
    for (std::int64_t from = 0; from < 20; from += 4) {
        rig.writer->submit(rig.chunk(from, from + 4, kWrMiB));
    }
    ASSERT_GE(rig.writer->queue_bytes(), kQueueBytes);
    // The flush is queued at once, behind the chunks, and returns only once
    // every one of them has landed.
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 20));
    EXPECT_EQ(rig.writer->queue_bytes(), 0U);
    // Flushes take no room: after more of them than the queue holds chunks,
    // a submit still goes straight in.
    for (std::uint64_t id = 2; id < kQueueChunks + 10; ++id) {
        rig.writer->flush(id);
    }
    const auto waits = [&] {
        return rig.metrics.histogram(wr_name(metric::kBackpressureBlockedNs)).snapshot().count;
    };
    const auto waits_before = waits();
    auto submitted = std::async(std::launch::async, [&rig] {
        return wr_error([&rig] { rig.writer->submit(rig.chunk(20, 24, 8)); });
    });
    const auto status = submitted.wait_for(scale_slack(2s));
    if (status != std::future_status::ready) {
        // Only the abort can free it now; the case fails rather than hangs.
        rig.writer->abort();
    }
    ASSERT_EQ(status, std::future_status::ready) << "the flushes took the room of chunks";
    EXPECT_FALSE(submitted.get());
    EXPECT_EQ(waits(), waits_before);
    rig.writer->flush(kQueueChunks + 10);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 24));
    rig.writer->finish();
}

// --- The header check ---------------------------------------------------------

TEST(NativeWriterHeader, AColumnRetypedBetweenInsertsFailsTheNextWithTheDifference) {
    WrRig rig;
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    rig.writer->flush(1);
    rig.server->alter_column_type(kWrTable, "s", "Nullable(String)");
    rig.writer->submit(rig.chunk(5, 10, 8));
    const auto error = wr_error([&] { rig.writer->flush(2); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kHeaderDrift);
    EXPECT_NE(std::string(error->what()).find("column `s`: plan String, server Nullable(String)"),
              std::string::npos)
        << error->what();
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 5));
    EXPECT_EQ(rig.server->abandoned_mid_insert(), 1U);
}

TEST(NativeWriterHeader, AHeaderTypeTheClientCannotBuildFailsWithTheSameCode) {
    WrRig rig;
    rig.server->inject(wr_fault(
        fake::Step::Begin, fake::Fault::Kind::Unimplemented, 0, "unsupported column type: Int256"));
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kHeaderDrift);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

TEST(NativeWriterHeader, DriftFoundOnARetryFailsWithHeaderDriftToo) {
    WrRig rig;
    // The connection drops as the column is retyped, so the resend meets the
    // new header.
    fake::Fault fault = wr_reset_at_end();
    const auto server = rig.server;
    fault.on_fire = [server] { server->alter_column_type(kWrTable, "s", "Nullable(String)"); };
    server->inject(std::move(fault));
    rig.start();
    rig.writer->submit(rig.chunk(0, 5, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kHeaderDrift) << error->what();
    EXPECT_NE(std::string(error->what()).find("column `s`: plan String, server Nullable(String)"),
              std::string::npos)
        << error->what();
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 5U);
}

// --- Resource splits ----------------------------------------------------------

TEST(NativeWriterSplit, AMemoryLimitOnALargeInsertSplitsItIntoTwoUnderFreshTokens) {
    WrRig rig;
    rig.server->inject(wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    rig.start();
    rig.writer->submit(rig.chunk(0, 4000, 8));
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 3U);
    EXPECT_EQ(all[0].outcome, WrOutcome::Failed);
    EXPECT_EQ(wr_rows_of(all[0]), 4000U);
    const std::set<std::string> tokens{all[0].token, all[1].token, all[2].token};
    EXPECT_EQ(tokens.size(), 3U);
    EXPECT_EQ(wr_ids_of(all[1]), wr_range(0, 2000));
    EXPECT_EQ(wr_ids_of(all[2]), wr_range(2000, 4000));
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 4000));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.inserts, 2U);
    EXPECT_EQ(wr_retries(stats, FailureClass::Resource), 1U);
    // After a send, both halves may duplicate what the parent landed.
    EXPECT_EQ(stats.rows_maybe_duplicated, 4000U);
    rig.writer->finish();
}

TEST(NativeWriterSplit, AMultiBlockInsertIsSplitByRowsAcrossItsBlocks) {
    WrRig rig;
    rig.server->inject(wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    rig.start();
    // 16393 bytes of payload a row: 1023 rows fill the first block, so the
    // middle row of the 2000 falls inside it.
    for (std::int64_t from = 0; from < 2000; from += 100) {
        rig.writer->submit(rig.chunk(from, from + 100, 16 * kWrKiB));
    }
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 3U);
    ASSERT_EQ(all[0].blocks.size(), 2U);
    EXPECT_EQ(all[0].blocks[0].rows, 1023U);
    // The first half is a slice of the first block; the second is the rest
    // of that block followed by the second block whole.
    ASSERT_EQ(all[1].blocks.size(), 1U);
    EXPECT_EQ(wr_ids_of(all[1]), wr_range(0, 1000));
    ASSERT_EQ(all[2].blocks.size(), 2U);
    EXPECT_EQ(all[2].blocks[0].rows, 23U);
    EXPECT_TRUE(all[2].blocks[1].bytes == all[0].blocks[1].bytes);
    EXPECT_EQ(wr_ids_of(all[2]), wr_range(1000, 2000));
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 2000));
    rig.writer->finish();
}

TEST(NativeWriterSplit, AMemoryLimitOnASmallInsertIsRetriedWhole) {
    WrRig rig;
    rig.server->inject(wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    rig.start();
    rig.writer->submit(rig.chunk(0, 500, 8));
    rig.writer->flush(1);
    const auto all = rig.inserts();
    ASSERT_EQ(all.size(), 2U);
    EXPECT_EQ(all[0].token, all[1].token);
    EXPECT_EQ(wr_rows_of(all[1]), 500U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 500));
    EXPECT_EQ(wr_retries(rig.writer->stats(), FailureClass::Resource), 1U);
    rig.writer->finish();
}

TEST(NativeWriterSplit, RepeatedMemoryLimitsExhaustOneWindowHoweverManyHalvesAreMade) {
    WrRig rig;
    rig.options.retry_window = 2s;
    for (int i = 0; i < 200; ++i) {
        rig.server->inject(wr_fault(
            fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    }
    rig.start();
    rig.writer->submit(rig.chunk(0, 4000, 8));
    const auto start = WrClock::now();
    const auto error = wr_error([&] { rig.writer->flush(1); });
    const auto took = WrClock::now() - start;
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kRetryWindowExhausted);
    EXPECT_LE(took, 2s + scale_slack(200ms));
    std::set<std::size_t> sizes;
    for (const auto& insert : rig.inserts()) {
        sizes.insert(wr_rows_of(insert));
    }
    // The tree went down to the 1000-row floor, all inside one window.
    EXPECT_TRUE(sizes.contains(4000U));
    EXPECT_TRUE(sizes.contains(2000U));
    EXPECT_TRUE(sizes.contains(1000U));
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 4000U);
}

TEST(NativeWriterSplit, AHalfFailsAtItsParentsDeadlineNotAWindowAfterItsOwnStart) {
    WrRig rig;
    rig.options.retry_window = 2s;
    // The window starts at this failure; the resend's 241 comes back most
    // of a window later, and every half's End is slower than what is left.
    rig.server->inject(wr_reset_at_end());
    fake::Fault late = wr_fault(
        fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded");
    late.delay = 1200ms;
    rig.server->inject(std::move(late));
    for (int i = 0; i < 20; ++i) {
        fake::Fault slow = wr_fault(
            fake::Step::End, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded");
        slow.delay = 1500ms;
        rig.server->inject(std::move(slow));
    }
    rig.start();
    rig.writer->submit(rig.chunk(0, 4000, 8));
    const auto start = WrClock::now();
    const auto error = wr_error([&] { rig.writer->flush(1); });
    const auto took = WrClock::now() - start;
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kRetryWindowExhausted) << error->what();
    std::set<std::size_t> sizes;
    for (const auto& insert : rig.inserts()) {
        sizes.insert(wr_rows_of(insert));
    }
    EXPECT_TRUE(sizes.contains(2000U)) << "the INSERT was never split";
    // The first half's End is cut at the deadline the halves share. A half
    // with a window of its own would have gone on for another one. Not scaled
    // for a sanitizer build: the margin has to stay short of that one.
    EXPECT_GE(took, 1900ms);
    EXPECT_LT(took, 2700ms);
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
}

// The first attempt lands every row and is then in doubt; the resend is split
// by a 241 before it sends anything. The halves go under fresh tokens, so the
// first attempt's rows land a second time, and the counts must say so.
TEST(NativeWriterSplit, ASplitBeforeSendAfterAnInDoubtAttemptCountsItsRowsAsMaybeDuplicated) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    fake::Fault oom = wr_fault(
        fake::Step::Begin, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded");
    oom.nth = 2;
    rig.server->inject(std::move(oom));
    rig.start();
    rig.writer->submit(rig.chunk(0, 4000, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_twice(0, 4000));
    const auto tokens = wr_tokens(*rig.server);
    EXPECT_EQ(std::set<std::string>(tokens.begin(), tokens.end()).size(), 3U);
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 4000U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsMaybeDuplicatedTotal)), 4000U);
    rig.writer->finish();
}

TEST(NativeWriterSplit, ASplitByTheReprobeAfterAnInDoubtAttemptCountsItsRowsAsMaybeDuplicated) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end(fake::Fault::Landing::Everything));
    rig.start();
    // After start, so the opener's own probe does not meet it: the first
    // read after this is the re-probe of the client rebuilt for the resend.
    rig.server->inject(wr_fault(
        fake::Step::Select, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    rig.writer->submit(rig.chunk(0, 4000, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_twice(0, 4000));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 4000U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    rig.writer->finish();
}

TEST(NativeWriterSplit, ASplitWithNothingInDoubtCountsNoDuplicates) {
    WrRig rig;
    // The first attempt fails before it sends anything, so nothing can have
    // landed under its token.
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::SystemError));
    rig.server->inject(wr_fault(
        fake::Step::Begin, fake::Fault::Kind::ServerError, 241, "Memory limit (total) exceeded"));
    rig.start();
    rig.writer->submit(rig.chunk(0, 4000, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 4000));
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_maybe_duplicated, 0U);
    EXPECT_EQ(stats.rows_resent_with_token, 0U);
    rig.writer->finish();
}

// --- The memory charge ---------------------------------------------------------

TEST(NativeWriterMemory, AChargeTheBudgetRefusesFailsTheInsertAndEveryReservationComesBack) {
    WrRig rig;
    rig.budget = std::make_shared<MemoryBudget>(64 * kWrKiB, "writer-test-small");
    rig.start();
    const std::uint64_t errors_before = wr_connector_counter("errors_total");
    // The chunk fits the budget; the Native copy of it, with its 16-byte
    // string views and its ids, does not fit beside it.
    rig.writer->submit(rig.chunk(0, 2000, 8));
    const auto error = wr_error([&] { rig.writer->flush(1); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kInsertFailed);
    EXPECT_NE(std::string(error->what()).find("MEMORY_LIMIT_EXCEEDED"), std::string::npos)
        << error->what();
    EXPECT_EQ(rig.server->rows(kWrTable), 0U);
    EXPECT_EQ(rig.writer->stats().abandoned_rows, 2000U);
    // It failed outside any call to the server, and still counts once as a
    // connector error: it is the failure that ends the task.
    EXPECT_EQ(wr_connector_counter("errors_total") - errors_before, 1U);
    rig.writer.reset();
    EXPECT_EQ(rig.budget->usage().used, 0U);
    EXPECT_GE(rig.budget->usage().refused, 1U);
}

TEST(NativeWriterMemory, AChunkWithoutAZeroCopyColumnIsReleasedOnceConverted) {
    fake::FakeTable table = wr_table();
    table.columns = {{"id", "Int64", DefaultKind::None, 1}};
    WrRig rig(table, "id:BIGINT");
    ASSERT_FALSE(rig.plan.retains_chunks);
    rig.start();
    Chunk c = rig.wrap(wr_id_rows(0, 10'000));
    const std::size_t chunk_size = c.bytes;
    rig.writer->submit(std::move(c));
    // Once the writer has the rows in its builder, the INSERT holds their
    // Native copy, at least 8 bytes a row, and no longer the chunk.
    ASSERT_TRUE(wr_eventually([&] {
        const std::size_t used = rig.budget->usage().used;
        return rig.writer->queue_bytes() == 0 && used >= 8 * 10'000U &&
               used < chunk_size + 8 * 10'000U;
    })) << "used "
        << rig.budget->usage().used << " with a chunk of " << chunk_size;
    rig.writer->flush(1);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 10'000));
    rig.writer->finish();
}

TEST(NativeWriterMemory, OneLargeChunkOfShortStringsKeepsItsNativeCopiesWithinTheChargeLimit) {
    WrRig rig;
    rig.options.batch_bytes = 4 * kWrMiB;
    rig.start();
    // Ten bytes of payload a row, so all 400000 rows fit batch_bytes, but
    // each Native row also owns its id and a 16-byte view: about 10 MB of
    // copies, against a charge limit of 8 MiB.
    Chunk c = rig.chunk(0, 400'000, 1);
    const std::size_t chunk_size = c.bytes;
    rig.writer->submit(std::move(c));
    rig.writer->flush(1);
    // The chunk is charged from the queue until its last row is acknowledged,
    // and only one INSERT holds copies at a time, so the rest of the peak is
    // the most any INSERT's copies took. An INSERT may pass the limit by the
    // chunk it holds, never by the copies of its rows.
    const std::size_t copies_at_peak = rig.budget->usage().peak - chunk_size;
    EXPECT_LE(copies_at_peak, 2 * rig.options.batch_bytes);
    EXPECT_GT(rig.committed().size(), 1U);
    EXPECT_EQ(rig.server->rows(kWrTable), 400'000U);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 400'000));
    rig.writer->finish();
}

TEST(NativeWriterMemory, AChunkLargerThanTheChargeLimitIsStillTakenInInsertsOfManyRows) {
    WrRig rig;
    rig.options.batch_bytes = kWrMiB;
    rig.start();
    // The chunk alone is over the 2 MiB limit, so every INSERT holding any
    // of it is too. Each still takes copies up to the limit before it closes,
    // rather than one row.
    Chunk c = rig.chunk(0, 400'000, 1);
    const std::size_t chunk_size = c.bytes;
    ASSERT_GT(chunk_size, 2 * rig.options.batch_bytes);
    rig.writer->submit(std::move(c));
    rig.writer->flush(1);
    EXPECT_LE(rig.budget->usage().peak - chunk_size, 2 * rig.options.batch_bytes);
    const auto all = rig.committed();
    EXPECT_GT(all.size(), 1U);
    EXPECT_LT(all.size(), 50U);
    EXPECT_EQ(rig.server->rows(kWrTable), 400'000U);
    rig.writer->finish();
}

TEST(NativeWriterMemory, ManySmallChunksOfShortStringsKeepTheChargeWithinTheLimit) {
    WrRig rig;
    rig.options.batch_bytes = kWrMiB;
    rig.start();
    // Each chunk is small, but the INSERT's block grows its columns by half
    // again as it fills, and a growth near the limit could carry the charge
    // past it by half of what the block owns.
    std::size_t chunk_size = 0;
    for (std::int64_t from = 0; from < 200'000; from += 1000) {
        Chunk c = rig.chunk(from, from + 1000, 1);
        chunk_size = std::max(chunk_size, c.bytes);
        rig.writer->submit(std::move(c));
        // One chunk at a time, so at most one waits in the queue.
        ASSERT_TRUE(wr_eventually([&] { return rig.writer->queue_bytes() == 0; }));
    }
    rig.writer->flush(1);
    // The INSERT's chunks and copies stay within the limit, beside the chunk
    // in hand and the one queued behind it.
    EXPECT_LE(rig.budget->usage().peak, 2 * rig.options.batch_bytes + 2 * chunk_size);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 200'000));
    rig.writer->finish();
}

// --- What the writer counts ---------------------------------------------------

TEST(NativeWriterMetrics, RowsCountAsOutOnlyOnceTheServerAcknowledgesThem) {
    WrRig rig;
    rig.server->inject(wr_delay(fake::Step::End, 1s));
    rig.start();
    const std::uint64_t records_before = wr_connector_counter("records_total");
    const std::uint64_t bytes_before = wr_connector_counter("bytes_total");
    rig.writer->submit(rig.chunk(0, 10, 8));
    EXPECT_GT(rig.budget->usage().used, 0U);
    auto flushed = std::async(std::launch::async, [&rig] { rig.writer->flush(1); });
    ASSERT_TRUE(wr_eventually([&] {
        const auto all = rig.inserts();
        return !all.empty() && !all[0].blocks.empty();
    }));
    // Sent and buffered on the server, not yet acknowledged.
    EXPECT_EQ(wr_connector_counter("records_total"), records_before);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsTotal)), 0U);
    flushed.get();
    EXPECT_EQ(wr_connector_counter("records_total"), records_before + 10);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kRowsTotal)), 10U);
    EXPECT_EQ(wr_counter(rig.metrics, wr_name(metric::kInsertsTotal, "outcome", "ok")), 1U);
    const WriterStats stats = rig.writer->stats();
    EXPECT_EQ(stats.rows_acknowledged, 10U);
    EXPECT_EQ(stats.inserts, 1U);
    EXPECT_GT(stats.wire_bytes, 0U);
    EXPECT_EQ(wr_connector_counter("bytes_total"), bytes_before + stats.wire_bytes);
    const auto latency = rig.metrics.histogram(wr_name(metric::kInsertLatencyNs)).snapshot();
    EXPECT_EQ(latency.count, 1U);
    EXPECT_GE(latency.sum, 1e9);
    const auto blocks = rig.metrics.histogram(wr_name(metric::kBlockRows)).snapshot();
    EXPECT_EQ(blocks.count, 1U);
    EXPECT_EQ(blocks.sum, 10.0);
    EXPECT_EQ(blocks.upper_bounds.size(), metric::kBlockRowsBounds.size());
    rig.writer->finish();
}

TEST(NativeWriterMetrics, EveryFailedAttemptCountsAsAConnectorError) {
    WrRig rig;
    rig.server->inject(wr_reset_at_end());
    rig.server->inject(wr_fault(fake::Step::Begin, fake::Fault::Kind::SystemError));
    rig.start();
    const std::uint64_t errors_before = wr_connector_counter("errors_total");
    rig.writer->submit(rig.chunk(0, 10, 8));
    rig.writer->flush(1);
    EXPECT_EQ(wr_connector_counter("errors_total"), errors_before + 2);
    const auto waits = rig.metrics.histogram(wr_name(metric::kRetryWaitNs)).snapshot();
    EXPECT_EQ(waits.count, 2U);
    rig.writer->finish();
}

TEST(NativeWriterMetrics, TheQueueBytesGaugeFollowsEveryPushAndPop) {
    WrRig rig;
    rig.options.batch_rows = 4;
    rig.server->inject(wr_delay(fake::Step::End, 1500ms));
    rig.start();
    const auto gauge = [&] { return rig.metrics.gauge(wr_name(metric::kQueueBytes)).value(); };
    // This one closes an INSERT at once, whose End then takes its time.
    rig.writer->submit(rig.chunk(0, 4, 8));
    ASSERT_TRUE(wr_eventually([&] { return !rig.inserts().empty(); }));
    EXPECT_EQ(gauge(), 0) << "the writer has taken the only chunk";
    Chunk c = rig.chunk(4, 8, 8);
    const auto queued = static_cast<std::int64_t>(c.bytes);
    rig.writer->submit(std::move(c));
    EXPECT_EQ(gauge(), queued) << "a chunk waits behind the INSERT in End";
    rig.writer->flush(1);
    EXPECT_EQ(gauge(), 0);
    EXPECT_EQ(wr_landed_ids(*rig.server), wr_range(0, 8));
    rig.writer->finish();
}

TEST(NativeWriterMetrics, FrequentSmallInsertsLogThePartRateWarningOncePerQuietPeriod) {
    WrRig rig;
    rig.options.parallelism = 4;
    rig.part_rate_span = 300ms;
    rig.start();
    const std::int64_t since = wr_log_mark();
    const auto warnings = [&] {
        return wr_logs_containing(wr_logs_since(since), " INSERTs a second over the last ");
    };
    // One-row INSERTs as fast as flushes go: far more than one a second for
    // the job, and far fewer than 10000 rows each.
    std::int64_t id = 0;
    const auto insert_one = [&] {
        rig.writer->submit(rig.chunk(id, id + 1, 8));
        rig.writer->flush(static_cast<std::uint64_t>(id) + 1);
        ++id;
    };
    const auto give_up_at = WrClock::now() + scale_slack(3s);
    while (warnings() == 0 && WrClock::now() < give_up_at) {
        insert_one();
    }
    ASSERT_EQ(warnings(), 1U) << "no warning after " << id << " INSERTs";
    const auto logs = wr_logs_since(since);
    const auto warning = std::find_if(logs.begin(), logs.end(), [](const LogRecord& r) {
        return r.message.find(" INSERTs a second over the last ") != std::string::npos;
    });
    EXPECT_EQ(warning->level, "warn");
    EXPECT_NE(warning->message.find("subtask=0/4 "), std::string::npos) << warning->message;
    EXPECT_NE(warning->message.find("1 rows each on average"), std::string::npos)
        << warning->message;
    EXPECT_NE(warning->message.find("across parallelism 4"), std::string::npos) << warning->message;
    EXPECT_NE(warning->message.find("Raise batch_interval_ms (now 60000)"), std::string::npos)
        << warning->message;
    // Several more spans of the same, inside the quiet period: no repeat.
    const auto quiet_until = WrClock::now() + 1s;
    while (WrClock::now() < quiet_until) {
        insert_one();
    }
    EXPECT_EQ(warnings(), 1U);
    rig.writer->finish();
}

// --- Exit summaries and the part-rate rule --------------------------------------

TEST(NativeWriterSummary, TheSummaryLineCarriesEveryFieldInOrder) {
    SinkOptions options;
    options.subtask_idx = 3;
    options.parallelism = 8;
    WriterStats stats;
    stats.rows_acknowledged = 10'485'760;
    stats.inserts = 11;
    stats.in_doubt = 1;
    stats.rows_resent_with_token = 953'211;
    stats.wire_bytes = 391'002'113;
    stats.retries.at(static_cast<std::size_t>(FailureClass::TransientNotWritten)) = 2;
    stats.retries.at(static_cast<std::size_t>(FailureClass::InDoubt)) = 1;
    EXPECT_EQ(summary_line("closed", options, stats, 61'210ms),
              "clickhouse native sink closed: subtask=3/8 rows_acknowledged=10485760 inserts=11 "
              "retries=transient:2,in_doubt:1,merge_backpressure:0,resource:0,client_defect:0,"
              "unclassified:0 in_doubt=1 rows_resent_with_token=953211 rows_maybe_duplicated=0 "
              "abandoned_rows=0 wire_bytes=391002113 elapsed_ms=61210 columnar_batches=0 "
              "row_batches=0");
}

TEST(NativePartRate, FrequentSmallInsertsWarnAtMostOncePerQuietPeriod) {
    const auto t0 = WrClock::time_point{} + 1h;
    PartRateMonitor monitor(8, 1000ms, t0, 60s, 10min);
    // One 100-row INSERT a second on each of eight subtasks.
    std::optional<std::string> warning;
    for (int s = 1; s <= 60; ++s) {
        warning = monitor.on_insert(t0 + std::chrono::seconds{s}, 100);
        if (s < 60) {
            EXPECT_FALSE(warning) << s;
        }
    }
    ASSERT_TRUE(warning);
    EXPECT_NE(warning->find("1.0 INSERTs a second"), std::string::npos) << *warning;
    EXPECT_NE(warning->find("100 rows each"), std::string::npos) << *warning;
    EXPECT_NE(warning->find("parallelism 8"), std::string::npos) << *warning;
    EXPECT_NE(warning->find("batch_interval_ms (now 1000)"), std::string::npos) << *warning;
    // Still frequent and small, but within the quiet period.
    for (int s = 61; s <= 120; ++s) {
        EXPECT_FALSE(monitor.on_insert(t0 + std::chrono::seconds{s}, 100)) << s;
    }
    // Ten minutes after the first warning it may warn again.
    std::optional<std::string> again;
    for (int s = 121; s <= 720 && !again; ++s) {
        again = monitor.on_insert(t0 + std::chrono::seconds{s}, 100);
    }
    EXPECT_TRUE(again);
}

TEST(NativePartRate, LargeOrInfrequentInsertsNeverWarn) {
    const auto t0 = WrClock::time_point{} + 1h;
    PartRateMonitor large(8, 1000ms, t0);
    for (int s = 1; s <= 600; ++s) {
        EXPECT_FALSE(large.on_insert(t0 + std::chrono::seconds{s}, 60'000)) << s;
    }
    // One small INSERT every two seconds on a single subtask is half an
    // INSERT a second for the job.
    PartRateMonitor slow(1, 1000ms, t0);
    for (int s = 2; s <= 600; s += 2) {
        EXPECT_FALSE(slow.on_insert(t0 + std::chrono::seconds{s}, 100)) << s;
    }
}

}  // namespace
}  // namespace clink::clickhouse::native
