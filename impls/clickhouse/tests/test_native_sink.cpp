// The native sink end to end: built by its factory from options the factory
// accepts, run by a Dag-direct LocalExecutor behind a scripted source, and
// writing to the fake server through the transport seam. The cases cover the
// open sequence and its retry window, the barrier contract, cancel at every
// stage, the memory cap, the barrier-mode refusals, the chain rule, what the
// sink counts and logs, and the at-least-once contract across a crash. Every
// case ends by checking that no client was destroyed mid-INSERT, and puts the
// process-wide transport factory back.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/core/record.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_batcher.hpp"

#include "fake_transport.hpp"
#include "native/errors.hpp"
#include "native/fault_points.hpp"
#include "native/insert_transport.hpp"
#include "native/metrics.hpp"
#include "native/native_sink.hpp"
#include "native/native_sink_core.hpp"
#include "native/sink_options.hpp"
#include "native/writer.hpp"
#include "test_helpers/sanitizer_slack.hpp"
#include "thread_recording_pool.hpp"

namespace clink::clickhouse::native {
namespace {

using namespace std::chrono_literals;
namespace fake = clink::clickhouse::native::testing;
using NsClock = std::chrono::steady_clock;
using clink::config::JsonValue;
using clink::test_support::scale_slack;

const std::string kNsTable = "events";
constexpr std::size_t kNsMiB = 1024 * 1024;

// The 26.8 text of a too-many-parts error in one partition.
const std::string kNsTooManyParts =
    "Too many parts (3000 with average size of 1.20 MiB) in table 'db.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. Merges are processing significantly slower "
    "than inserts";

// --- Rows and the scripted source -------------------------------------------

sql::Row ns_row(std::int64_t id, std::size_t length) {
    clink::config::JsonObject object;
    object["id"] = JsonValue{id};
    object["s"] = JsonValue{std::string(length, static_cast<char>('a' + id % 26))};
    sql::Row row;
    row.values = sql::row_columns_from_json(std::move(object));
    return row;
}

// Rows [from, to), each text `length` bytes of one letter.
Batch<sql::Row> ns_rows(std::int64_t from, std::int64_t to, std::size_t length = 8) {
    Batch<sql::Row> batch;
    for (std::int64_t id = from; id < to; ++id) {
        batch.emplace(ns_row(id, length));
    }
    return batch;
}

// One record per NDJSON line, for rows a test spells out.
Batch<sql::Row> ns_json_rows(const std::vector<std::string>& lines) {
    Batch<sql::Row> batch;
    for (const auto& line : lines) {
        auto object = clink::config::parse_object(line);
        if (!object) {
            throw std::invalid_argument("not a JSON object: " + line);
        }
        sql::Row row;
        row.values = sql::row_columns_from_json(std::move(*object));
        batch.emplace(std::move(row));
    }
    return batch;
}

// One step of the source's script.
struct NsStep {
    enum class Kind : std::uint8_t { Rows, Barrier, Wait };
    Kind kind{Kind::Rows};
    std::function<Batch<sql::Row>()> rows;  // Rows
    std::uint64_t checkpoint{0};            // Barrier
    std::function<bool()> until;            // Wait: holds the script until true
};

NsStep ns_data(std::int64_t from, std::int64_t to, std::size_t length = 8) {
    NsStep s;
    s.rows = [from, to, length] { return ns_rows(from, to, length); };
    return s;
}

NsStep ns_lines(std::vector<std::string> lines) {
    NsStep s;
    s.rows = [lines = std::move(lines)] { return ns_json_rows(lines); };
    return s;
}

NsStep ns_barrier(std::uint64_t checkpoint) {
    NsStep s;
    s.kind = NsStep::Kind::Barrier;
    s.checkpoint = checkpoint;
    return s;
}

NsStep ns_wait(std::function<bool()> until) {
    NsStep s;
    s.kind = NsStep::Kind::Wait;
    s.until = std::move(until);
    return s;
}

// Plays its script, then ends (bounded) or idles until the job is cancelled.
// It polls every millisecond, so it never holds up a cancel.
class NsSource final : public Source<sql::Row> {
public:
    NsSource(std::vector<NsStep> steps, bool bounded)
        : steps_(std::move(steps)), bounded_(bounded) {}

    bool produce(Emitter<sql::Row>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (next_ >= steps_.size()) {
            played.store(true);
            if (bounded_) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
            return true;
        }
        const NsStep& step = steps_[next_];
        switch (step.kind) {
            case NsStep::Kind::Rows:
                ++next_;
                return out.emit_data(step.rows());
            case NsStep::Kind::Barrier:
                ++next_;
                return out.emit_barrier(CheckpointBarrier{CheckpointId{step.checkpoint}});
            case NsStep::Kind::Wait:
                if (step.until()) {
                    ++next_;
                } else {
                    std::this_thread::sleep_for(1ms);
                }
                return true;
        }
        return true;
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return bounded_; }
    [[nodiscard]] std::string name() const override { return "native_sink_test.source"; }

    std::atomic<bool> played{false};

private:
    std::vector<NsStep> steps_;
    std::size_t next_{0};
    bool bounded_{false};
};

// A sink that takes rows and does nothing with them, to sit beside the native
// sink on a chain.
class NsDiscardSink final : public Sink<sql::Row> {
public:
    void on_data(const Batch<sql::Row>& /*batch*/) override {}
    [[nodiscard]] std::string name() const override { return "native_sink_test.discard"; }
};

// Passes every call through, and says when it is destroyed: the moment an
// opener that was left behind lets go of its state. `linger` holds the
// destructor first, so that a caller that waits for it is told apart from
// one that does not.
class NsWatched final : public InsertTransport {
public:
    NsWatched(std::unique_ptr<InsertTransport> inner,
              std::shared_ptr<std::atomic<bool>> gone,
              std::chrono::milliseconds linger = std::chrono::milliseconds::zero())
        : inner_(std::move(inner)), gone_(std::move(gone)), linger_(linger) {}
    ~NsWatched() override {
        inner_.reset();
        std::this_thread::sleep_for(linger_);
        gone_->store(true);
    }
    NsWatched(const NsWatched&) = delete;
    NsWatched& operator=(const NsWatched&) = delete;
    NsWatched(NsWatched&&) = delete;
    NsWatched& operator=(NsWatched&&) = delete;

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
    std::chrono::milliseconds linger_;
};

// Refuses at connect with the sink's own error, before any socket, as the
// real transport does for a CA file OpenSSL cannot load.
class NsCaRefused final : public InsertTransport {
public:
    explicit NsCaRefused(std::shared_ptr<std::atomic<int>> connects)
        : connects_(std::move(connects)) {}

    void connect(const Endpoint& /*endpoint*/) override {
        connects_->fetch_add(1);
        throw NativeSinkError(code::kOptionInvalid,
                              "clickhouse_native_sink: option 'tls_ca_file' ('/etc/ch/ca.pem') "
                              "could not be loaded by OpenSSL: no start line");
    }
    [[nodiscard]] bool connected() const noexcept override { return false; }
    [[nodiscard]] const ServerIdentity& server() const override { return identity_; }
    ResultSet select(MetaQuery /*kind*/, const std::string& /*sql*/) override {
        throw std::logic_error("NsCaRefused never connects");
    }
    std::vector<HeaderColumn> begin_insert(const std::string& /*sql*/) override {
        throw std::logic_error("NsCaRefused never connects");
    }
    void send_block(const ::clickhouse::Block& /*block*/) override {
        throw std::logic_error("NsCaRefused never connects");
    }
    void end_insert() override { throw std::logic_error("NsCaRefused never connects"); }
    void abandon() noexcept override {}
    void interrupt() noexcept override {}
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> /*deadline*/) noexcept override {}
    [[nodiscard]] TransportCounters counters() const noexcept override { return {}; }

private:
    std::shared_ptr<std::atomic<int>> connects_;
    ServerIdentity identity_;
};

// --- The fake server's tables -----------------------------------------------

// (id Int64, s String) on MergeTree, with a deduplication log the probe can
// read from engine_full and the fake keeps itself.
fake::FakeTable ns_table(std::size_t window = 100) {
    fake::FakeTable t;
    t.database = "db";
    t.name = kNsTable;
    t.engine = "MergeTree";
    t.engine_full = "MergeTree ORDER BY id";
    if (window > 0) {
        t.engine_full +=
            " SETTINGS non_replicated_deduplication_window = " + std::to_string(window);
    }
    t.columns = {{"id", "Int64", DefaultKind::None, 1}, {"s", "String", DefaultKind::None, 2}};
    t.dedup_window = window;
    return t;
}

// --- The rig ------------------------------------------------------------------

// Through the factory, as a job graph reaches it.
std::shared_ptr<NativeSink> ns_build_sink(const std::map<std::string, std::string>& params,
                                          std::uint32_t subtask = 0,
                                          std::uint32_t parallelism = 1) {
    const auto* factory =
        cluster::OperatorRegistry::default_instance().find_sink("clickhouse_native_sink", "row");
    if (factory == nullptr) {
        throw std::logic_error("clickhouse_native_sink is not registered");
    }
    cluster::OperatorBuildContext ctx;
    ctx.params = params;
    ctx.subtask_idx = subtask;
    ctx.parallelism = parallelism;
    auto built = std::static_pointer_cast<Sink<sql::Row>>(factory->build(ctx));
    auto typed = std::dynamic_pointer_cast<NativeSink>(built);
    if (!typed) {
        throw std::logic_error("clickhouse_native_sink built something other than a NativeSink");
    }
    return typed;
}

using NsConfigure = std::function<void(JobConfig&, OperatorId source, OperatorId sink)>;

// One server, the factory pointed at it, and a job built from a script. The
// destructor checks the two invariants every case shares.
struct NsRig {
    explicit NsRig(fake::FakeTable t = ns_table()) : table(std::move(t)) {
        server = add_server();
        set_transport_factory_for_testing(fake::fake_factory(server));
        params = {{"database", "db"},
                  {"table", kNsTable},
                  {"sql_column_types", "id:BIGINT;s:VARCHAR"},
                  {"batch_interval_ms", "3600000"}};
        config.metrics = &metrics;
        config.on_checkpoint_ack = [this](CheckpointId id, bool ok, const std::string& /*why*/) {
            // What the table held at the moment of the ack: the barrier
            // contract says every row before the barrier is in it by then.
            const std::uint64_t landed = server->rows(kNsTable);
            const std::lock_guard<std::mutex> lock(acks_mu);
            acks.emplace_back(id.value(), ok);
            rows_at_ack.push_back(landed);
        };
    }

    ~NsRig() {
        exec.reset();
        sink.reset();
        source.reset();
        for (const auto& s : servers) {
            EXPECT_EQ(s->destroyed_mid_insert(), 0U)
                << "a client was destroyed mid-INSERT on " << s->display_name();
        }
        set_transport_factory_for_testing(nullptr);
    }

    NsRig(const NsRig&) = delete;
    NsRig& operator=(const NsRig&) = delete;
    NsRig(NsRig&&) = delete;
    NsRig& operator=(NsRig&&) = delete;

    std::shared_ptr<fake::FakeServer> add_server() {
        auto s = std::make_shared<fake::FakeServer>();
        s->add_table(table);
        servers.push_back(s);
        return s;
    }

    [[nodiscard]] std::shared_ptr<NativeSink> build(std::uint32_t subtask = 0,
                                                    std::uint32_t parallelism = 1) const {
        return ns_build_sink(params, subtask, parallelism);
    }

    void start(std::vector<NsStep> steps,
               bool bounded = true,
               const NsConfigure& configure = {},
               std::shared_ptr<NativeSink> given = nullptr,
               std::uint32_t subtask = 0,
               std::uint32_t parallelism = 1) {
        source = std::make_shared<NsSource>(std::move(steps), bounded);
        sink = given ? std::move(given) : build(subtask, parallelism);
        Dag dag;
        auto h = dag.add_source<sql::Row>(source);
        dag.add_sink<sql::Row>(h, sink);
        JobConfig job = config;
        if (configure) {
            configure(job, source->id(), sink->id());
        }
        exec = std::make_unique<LocalExecutor>(std::move(dag), std::move(job));
        exec->start();
    }

    // One bounded source per script, every one of them into a single sink
    // subtask, so the sink runs on the fan-in runner rather than the chain's.
    void start_fan_in(std::vector<std::vector<NsStep>> scripts) {
        sink = build();
        Dag dag;
        auto sources = std::make_shared<std::vector<std::vector<NsStep>>>(std::move(scripts));
        auto h = dag.add_parallel_source<sql::Row>(
            [sources](std::size_t subtask) -> std::shared_ptr<Source<sql::Row>> {
                return std::make_shared<NsSource>(sources->at(subtask), true);
            },
            sources->size());
        dag.add_parallel_sink<sql::Row>(
            h,
            [s = sink](std::size_t /*subtask*/) -> std::shared_ptr<Sink<sql::Row>> { return s; },
            1);
        exec = std::make_unique<LocalExecutor>(std::move(dag), config);
        exec->start();
    }

    void wait() const { exec->await_termination(); }

    // Each operator error that carries `code`.
    [[nodiscard]] std::vector<std::string> errors_with(const std::string& code) const {
        std::vector<std::string> out;
        for (const auto& [op, message] : exec->operator_errors()) {
            if (message.find("[" + code + "]") != std::string::npos) {
                out.push_back(message);
            }
        }
        return out;
    }

    [[nodiscard]] std::vector<std::uint64_t> acknowledged() {
        const std::lock_guard<std::mutex> lock(acks_mu);
        std::vector<std::uint64_t> out;
        for (const auto& [id, ok] : acks) {
            if (ok) {
                out.push_back(id);
            }
        }
        return out;
    }

    [[nodiscard]] std::uint64_t op_id() const { return sink->id().value(); }

    fake::FakeTable table;
    std::shared_ptr<fake::FakeServer> server;
    std::vector<std::shared_ptr<fake::FakeServer>> servers;
    std::map<std::string, std::string> params;
    MetricsRegistry metrics;
    JobConfig config;
    std::mutex acks_mu;
    std::vector<std::pair<std::uint64_t, bool>> acks;
    std::vector<std::uint64_t> rows_at_ack;
    std::shared_ptr<NsSource> source;
    std::shared_ptr<NativeSink> sink;
    std::unique_ptr<LocalExecutor> exec;
};

// The sink's own calls made from the case's thread, without an executor, for
// the orders of data, barrier and cancel a running job cannot pin down. The
// destructor checks what NsRig's does.
struct NsDirect {
    static constexpr std::uint64_t kOpId = 4242;

    explicit NsDirect(fake::FakeTable t = ns_table()) {
        server = std::make_shared<fake::FakeServer>();
        server->add_table(std::move(t));
        set_transport_factory_for_testing(fake::fake_factory(server));
        params = {{"database", "db"},
                  {"table", kNsTable},
                  {"sql_column_types", "id:BIGINT;s:VARCHAR"},
                  {"batch_interval_ms", "3600000"}};
        ctx = std::make_unique<RuntimeContext>(
            OperatorId{kOpId}, "clickhouse_native_sink", nullptr, &metrics);
        ctx->set_cancel_signal(CancelSignal{cancel, nullptr});
    }

    ~NsDirect() {
        sink.reset();
        ctx.reset();
        EXPECT_EQ(server->destroyed_mid_insert(), 0U) << "a client was destroyed mid-INSERT";
        set_transport_factory_for_testing(nullptr);
    }

    NsDirect(const NsDirect&) = delete;
    NsDirect& operator=(const NsDirect&) = delete;
    NsDirect(NsDirect&&) = delete;
    NsDirect& operator=(NsDirect&&) = delete;

    // Builds the sink from `params` and attaches it to the context.
    NativeSink& build() {
        sink = ns_build_sink(params);
        sink->attach_runtime(ctx.get());
        return *sink;
    }

    NativeSink& open() {
        NativeSink& s = build();
        s.open();
        return s;
    }

    std::shared_ptr<fake::FakeServer> server;
    std::map<std::string, std::string> params;
    MetricsRegistry metrics;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<RuntimeContext> ctx;
    std::shared_ptr<NativeSink> sink;
};

// --- Helpers --------------------------------------------------------------------

// Waits up to `timeout` for `pred`, scaled for a sanitizer build.
template <typename Pred>
bool ns_eventually(Pred pred, std::chrono::milliseconds timeout = 10s) {
    const auto end = NsClock::now() + scale_slack(timeout);
    while (NsClock::now() < end) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

template <typename F>
std::optional<NativeSinkError> ns_error(F&& f) {
    try {
        f();
    } catch (const NativeSinkError& e) {
        return e;
    }
    return std::nullopt;
}

fake::Fault ns_fault(fake::Step step,
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

std::map<std::int64_t, std::size_t> ns_id_counts(const fake::FakeServer& s) {
    std::map<std::int64_t, std::size_t> out;
    for (const auto& block : s.landed(kNsTable)) {
        for (const auto& row : block.values) {
            ++out[std::stoll(row.at(0))];
        }
    }
    return out;
}

// Every id in [from, to) landed exactly `times` times.
bool ns_each_landed(const fake::FakeServer& s,
                    std::int64_t from,
                    std::int64_t to,
                    std::size_t times = 1) {
    const auto counts = ns_id_counts(s);
    for (std::int64_t id = from; id < to; ++id) {
        const auto it = counts.find(id);
        if ((it == counts.end() ? 0 : it->second) != times) {
            return false;
        }
    }
    return true;
}

std::string ns_name(const char* metric, std::uint64_t op_id) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op_id) + "\"}";
}

std::string ns_name(const char* metric,
                    std::uint64_t op_id,
                    const char* key,
                    const std::string& value) {
    return std::string(metric) + "{op_id=\"" + std::to_string(op_id) + "\"," + key + "=\"" + value +
           "\"}";
}

std::uint64_t ns_count(MetricsRegistry& registry, const std::string& name) {
    return registry.counter(name).value();
}

std::uint64_t ns_observations(MetricsRegistry& registry, const std::string& name) {
    return registry.histogram(name).snapshot().count;
}

std::uint64_t ns_connector(const char* metric) {
    return MetricsRegistry::global()
        .counter(clink::metrics::connector_metric_name(metric, metric::kConnector, "sink"))
        .value();
}

std::uint64_t ns_global_refusals(const std::string& code) {
    return MetricsRegistry::global()
        .counter(std::string(metric::kRefusalsTotal) + "{reason=\"" + code + "\"}")
        .value();
}

std::int64_t ns_wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// A point in the log ring after which only what the case does next appears:
// the ring keeps milliseconds and the filter takes records strictly after the
// mark, so the mark's own millisecond, which may hold an earlier line, is
// waited out.
std::int64_t ns_log_mark() {
    const std::int64_t mark = ns_wall_ms();
    std::this_thread::sleep_for(3ms);
    return mark;
}

std::vector<LogRecord> ns_logs_since(std::int64_t since_ms) {
    return LogBuffer::global().tail(1024, "", since_ms, "sink.clickhouse");
}

std::vector<LogRecord> ns_logs_with(const std::vector<LogRecord>& logs, const std::string& text) {
    std::vector<LogRecord> out;
    for (const auto& r : logs) {
        if (r.message.find(text) != std::string::npos) {
            out.push_back(r);
        }
    }
    return out;
}

bool ns_has(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

// The number a summary line gives for ` field=`.
std::uint64_t ns_field(const std::string& line, const std::string& field) {
    const std::string key = " " + field + "=";
    const std::size_t at = line.find(key);
    if (at == std::string::npos) {
        throw std::invalid_argument("no " + field + " in: " + line);
    }
    return std::stoull(line.substr(at + key.size()));
}

// The one cancelled summary logged since `since`.
std::string ns_cancelled_summary(std::int64_t since) {
    const auto lines = ns_logs_with(ns_logs_since(since), "clickhouse native sink cancelled:");
    if (lines.size() != 1) {
        throw std::logic_error(std::to_string(lines.size()) + " cancelled summaries, not one");
    }
    return lines.front().message;
}

// --- The factory and the record -----------------------------------------------

TEST(NativeSinkFactory, TheFactoryIsReachableOnTheRowChannelAndBuildsASinkThatGatesTheAck) {
    NsRig rig;
    EXPECT_NE(
        cluster::RunnerRegistry::default_instance().find_sink("clickhouse_native_sink", "row"),
        nullptr);
    const auto sink = rig.build();
    EXPECT_TRUE(sink->gates_checkpoint_ack());
    EXPECT_TRUE(sink->supports_columnar());
    EXPECT_EQ(sink->name(), "clickhouse_native_sink");
}

TEST(NativeSinkFactory, TheRecordClaimsAtLeastOnceWithRetriesAndPassesItsOwnCheck) {
    const auto* rec = connectors::CapabilityRegistry::instance().find("clickhouse_native");
    ASSERT_NE(rec, nullptr);
    EXPECT_TRUE(rec->self_check().empty());
    EXPECT_EQ(rec->version, "1");
    EXPECT_TRUE(rec->is_sink);
    EXPECT_FALSE(rec->is_source);
    EXPECT_EQ(rec->delivery, connectors::DeliveryGuarantee::AtLeastOnce);
    EXPECT_TRUE(rec->checkpoint_integrated);
    EXPECT_FALSE(rec->transactional);
    EXPECT_TRUE(rec->retries);
    EXPECT_TRUE(rec->backpressure);
    EXPECT_TRUE(rec->available_in_sql);
    EXPECT_EQ(rec->boundedness, connectors::Boundedness::Either);
    EXPECT_EQ(rec->formats, std::vector<std::string>{"native"});
    EXPECT_EQ(rec->auth_methods, (std::vector<std::string>{"none", "password"}));
    EXPECT_EQ(rec->build_dependencies, std::vector<std::string>{"clickhouse-cpp 2.6.2"});
    EXPECT_EQ(rec->timeout_options,
              (std::vector<std::string>{"connect_timeout_ms",
                                        "send_timeout_ms",
                                        "receive_timeout_ms",
                                        "retry_window_ms",
                                        "batch_interval_ms"}));
#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    EXPECT_TRUE(rec->tls);
#else
    EXPECT_FALSE(rec->tls);
#endif
    EXPECT_EQ(rec->runtime_dependencies,
              std::vector<std::string>{"clickhouse server, native protocol (port 9000, or 9440 "
                                       "with TLS); 26.3 and 26.8 tested"});
    // Every limitation, word for word: the record is what the manifest and
    // the capability catalogue print, so a dropped or reworded one shows here.
    std::vector<std::string> limitations = {
        "at-least-once: rows after the last completed checkpoint are replayed and may appear "
        "twice",
        "a resent INSERT is deduplicated only on targets that keep a deduplication log, on 26.3 "
        "and 26.8",
        "exactly-once, mode='upsert' and changelog='true' are refused",
        "targets whose effective async_insert is not 0 are refused at open, on every replica "
        "behind a Distributed target",
        "SharedMergeTree targets are refused: their deduplication is untested",
        "must be the only sink on its chain: Dag::add_sink refuses another sink beside it",
        "unaligned and adaptive checkpoints are refused at open until the engine captures "
        "in-flight rows at a fan-in",
        "a retry that holds the barrier beyond CLINK_EOS_FINAL_CKPT_TIMEOUT_MS (default 30 s) "
        "at the end of a bounded job or at a hot cutover spends a restart; nothing is lost",
        "a columnar batch is taken without building rows when its event-time column is int64 "
        "and every value column has a type the Row sidecar carries; otherwise it goes through "
        "its row accessors, and clink_clickhouse_columnar_declined_total says why",
    };
#if !defined(CLINK_CLICKHOUSE_NATIVE)
    limitations.emplace_back(
        "not in this build: the clickhouse-cpp it found is older than 2.6.2, or of a version "
        "it could not read, so the factory refuses clickhouse.native_unavailable");
#endif
    EXPECT_EQ(rec->limitations, limitations);

    // The legacy sink keeps its own record, so its series and its claims stay
    // apart from these.
    const auto* legacy = connectors::CapabilityRegistry::instance().find("clickhouse");
    ASSERT_NE(legacy, nullptr);
    EXPECT_FALSE(legacy->retries);
}

TEST(NativeSinkFactory, ARefusalAtBuildCountsInTheProcessRegistryUnderItsReasonAlone) {
    NsRig rig;
    const std::uint64_t before = ns_global_refusals(code::kUnknownOption);
    rig.params["tls_server_name"] = "ch-1";
    const auto error = ns_error([&rig] { (void)rig.build(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kUnknownOption);
    EXPECT_EQ(ns_global_refusals(code::kUnknownOption), before + 1);
}

TEST(NativeSinkFactory, AMalformedColumnTypeListIsRefusedWhenTheSinkIsBuilt) {
    NsRig rig;
    const std::uint64_t before = ns_global_refusals(code::kOptionInvalid);
    rig.params["sql_column_types"] = "id:BIGINT;;s:VARCHAR";
    const auto error = ns_error([&rig] { (void)rig.build(); });
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kOptionInvalid);
    EXPECT_TRUE(ns_has(error->what(), "sql_column_types")) << error->what();
    EXPECT_EQ(ns_global_refusals(code::kOptionInvalid), before + 1);
    EXPECT_EQ(rig.server->connects(), 0U);
}

TEST(NativeSinkFactory, ADirectlyBuiltSinkWithoutColumnTypesOrEndpointsIsRefused) {
    SinkOptions options;
    options.endpoints = {Endpoint{"ch-1", 9000}};
    options.database = "db";
    options.table = kNsTable;
    const auto untyped =
        ns_error([&options] { (void)std::make_shared<NativeSink>(options, TransportFactory{}); });
    ASSERT_TRUE(untyped);
    EXPECT_EQ(untyped->code(), code::kOptionInvalid);
    EXPECT_TRUE(ns_has(untyped->what(), "must pass sql_column_types")) << untyped->what();

    options.sql_column_types = "id:BIGINT";
    options.endpoints.clear();
    const auto nowhere =
        ns_error([&options] { (void)std::make_shared<NativeSink>(options, TransportFactory{}); });
    ASSERT_TRUE(nowhere);
    EXPECT_EQ(nowhere->code(), code::kOptionInvalid);
}

// --- A clean run: the barrier contract, the reports and the token ------------

TEST(NativeSinkRun, EveryRowLandsOnceAndEachBarrierIsAcknowledgedAfterItsRows) {
    NsRig rig;
    rig.start(
        {ns_data(0, 100), ns_barrier(1), ns_data(100, 250), ns_barrier(2), ns_data(250, 300)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 300));
    EXPECT_EQ(rig.server->rows(kNsTable), 300U);
    EXPECT_EQ(rig.acknowledged(), (std::vector<std::uint64_t>{1, 2}));
    {
        const std::lock_guard<std::mutex> lock(rig.acks_mu);
        EXPECT_EQ(rig.rows_at_ack, (std::vector<std::uint64_t>{100, 250}));
    }
    // One INSERT per barrier and one for the end of input.
    EXPECT_EQ(rig.server->inserts(kNsTable).size(), 3U);
    // log_comment carries the sanitised operator name, the subtask and the
    // token's sequence, so system.query_log matches the open report.
    std::size_t tagged = 0;
    for (const auto& sql : rig.server->statements()) {
        tagged += ns_has(sql, "log_comment='clink:clickhouse_native_sink:sub0:") ? 1 : 0;
    }
    EXPECT_EQ(tagged, 3U);
}

TEST(NativeSinkLogs, TheOpenReportAndTheClosedSummaryCarryEveryField) {
    NsRig rig;
    // One End that lands and then fails in doubt: the resend keeps its token
    // on this tested line, and the log deduplicates it.
    fake::Fault doubt = ns_fault(fake::Step::End, fake::Fault::Kind::SystemError);
    doubt.landing = fake::Fault::Landing::Everything;
    rig.server->inject(std::move(doubt));
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 100), ns_barrier(1), ns_data(100, 200)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 200));
    const auto logs = ns_logs_since(since);

    const auto opens = ns_logs_with(logs, "clickhouse native sink open:");
    ASSERT_EQ(opens.size(), 1U);
    EXPECT_EQ(opens.front().level, "info");
    const std::string& open = opens.front().message;
    for (const char* field : {"subtask=0/1 ",
                              " factory=clickhouse_native_sink ",
                              " mode=append ",
                              " delivery=at_least_once ",
                              " table=`db`.`events` ",
                              " engine=MergeTree ",
                              " server=26.8.1 (tested) ",
                              " endpoint=localhost:9000 ",
                              " tls=off ",
                              " compression=lz4 ",
                              " dedup_setting=deduplicate_insert ",
                              " strict_limits=sent ",
                              " token_on_resend=kept ",
                              " dedup_window=non_replicated_deduplication_window=100 (table) ",
                              " async_insert=0 (table unset, server default 0) ",
                              " columns=2 omitted=0 ",
                              " batch_rows=1048449 ",
                              " batch_bytes=67108864 ",
                              " batch_interval_ms=3600000 ",
                              " retry_window_ms=600000 ",
                              " inserts_per_s_est=0.0+ckpt ",
                              " eos_bound_ms="}) {
        EXPECT_TRUE(ns_has(open, field)) << "missing '" << field << "' in: " << open;
    }
    // Nothing about this target or these options calls for a warning at open:
    // the only warning is the writer's own, for the attempt that failed.
    for (const auto& r : logs) {
        if (r.level == "warn") {
            EXPECT_TRUE(ns_has(r.message, "attempt 1 failed at end")) << r.message;
        }
    }

    // Subtask 0 reports the column plan and the options, without the full
    // planner spelling of the column types.
    const auto plan = ns_logs_with(logs, "clickhouse native sink column plan:");
    ASSERT_EQ(plan.size(), 1U);
    EXPECT_TRUE(ns_has(plan.front().message, "`id`: sql=BIGINT target=Int64"))
        << plan.front().message;
    const auto described = ns_logs_with(logs, "clickhouse native sink options:");
    ASSERT_EQ(described.size(), 1U);
    EXPECT_TRUE(ns_has(described.front().message, "\n  table=events")) << described.front().message;
    EXPECT_TRUE(ns_has(described.front().message, "sql_column_types=2 columns, in the column plan"))
        << described.front().message;
    EXPECT_FALSE(ns_has(described.front().message, "id:BIGINT")) << described.front().message;
    EXPECT_TRUE(ns_has(described.front().message, "password=unset")) << described.front().message;

    const auto closed = ns_logs_with(logs, "clickhouse native sink closed:");
    ASSERT_EQ(closed.size(), 1U);
    EXPECT_EQ(closed.front().level, "info");
    const std::string& summary = closed.front().message;
    for (const char* field :
         {"subtask=0/1 rows_acknowledged=200 inserts=2 ",
          " retries=transient:0,in_doubt:1,merge_backpressure:0,resource:0,client_defect:0,"
          "unclassified:0 ",
          " in_doubt=1 ",
          " rows_resent_with_token=100 ",
          " rows_maybe_duplicated=0 ",
          " abandoned_rows=0 ",
          " wire_bytes=",
          " elapsed_ms=",
          " columnar_batches=0 ",
          " row_batches=2"}) {
        EXPECT_TRUE(ns_has(summary, field)) << "missing '" << field << "' in: " << summary;
    }
    EXPECT_TRUE(ns_logs_with(logs, "clickhouse native sink cancelled:").empty());
}

TEST(NativeSinkLogs, OnlySubtaskZeroReportsTheColumnPlan) {
    NsRig rig;
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 10)}, true, {}, nullptr, 1, 2);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    const auto logs = ns_logs_since(since);
    const auto opens = ns_logs_with(logs, "clickhouse native sink open: subtask=1/2 ");
    ASSERT_EQ(opens.size(), 1U);
    // Twice the parallelism over an interval of an hour.
    EXPECT_TRUE(ns_has(opens.front().message, " inserts_per_s_est=0.0+ckpt "));
    EXPECT_TRUE(ns_logs_with(logs, "column plan:").empty());
    EXPECT_TRUE(ns_logs_with(logs, "clickhouse native sink options:").empty());
    EXPECT_EQ(ns_logs_with(logs, "clickhouse native sink closed: subtask=1/2 ").size(), 1U);
    // log_comment names the subtask.
    bool sub1 = false;
    for (const auto& sql : rig.server->statements()) {
        sub1 = sub1 || ns_has(sql, ":sub1:1'");
    }
    EXPECT_TRUE(sub1);
}

TEST(NativeSinkLogs, AnUntestedLineASmallBatchAndATableWithoutALogAreEachWarnedAbout) {
    NsRig rig(ns_table(0));
    rig.server->set_version(25, 8, 4);
    rig.params["batch_rows"] = "100";
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 10)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    const auto logs = ns_logs_since(since);
    const auto opens = ns_logs_with(logs, "clickhouse native sink open:");
    ASSERT_EQ(opens.size(), 1U);
    EXPECT_TRUE(ns_has(opens.front().message, " server=25.8.4 (accepted, untested) "))
        << opens.front().message;
    EXPECT_TRUE(ns_has(opens.front().message, " token_on_resend=fresh ")) << opens.front().message;
    std::vector<std::string> warnings;
    for (const auto& r : logs) {
        if (r.level == "warn") {
            warnings.push_back(r.message);
        }
    }
    ASSERT_EQ(warnings.size(), 3U);
    EXPECT_TRUE(ns_has(warnings[0],
                       "25.8.4 at localhost:9000 is on a line the native sink accepts "
                       "but has not been tested against"))
        << warnings[0];
    EXPECT_TRUE(ns_has(warnings[1], "`db`.`events` keeps no deduplication log")) << warnings[1];
    EXPECT_TRUE(ns_has(warnings[2], "batch_rows=100 is below 10000")) << warnings[2];
}

TEST(NativeSinkLogs, AFailureThatSkipsBothClosesStillLogsTheCancelledSummary) {
    NsRig rig;
    // The second INSERT's header names a type the client cannot build.
    fake::Fault unbuildable = ns_fault(fake::Step::Begin, fake::Fault::Kind::Unimplemented);
    unbuildable.nth = 2;
    rig.server->inject(std::move(unbuildable));
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 10), ns_barrier(1), ns_data(10, 30), ns_barrier(2)});
    rig.wait();
    EXPECT_EQ(rig.errors_with(code::kHeaderDrift).size(), 1U);
    EXPECT_EQ(rig.acknowledged(), std::vector<std::uint64_t>{1});
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 10));
    EXPECT_EQ(rig.server->rows(kNsTable), 10U);
    const auto logs = ns_logs_since(since);
    const auto cancelled = ns_logs_with(logs, "clickhouse native sink cancelled:");
    ASSERT_EQ(cancelled.size(), 1U);
    EXPECT_EQ(cancelled.front().level, "warn");
    EXPECT_TRUE(ns_has(cancelled.front().message, " rows_acknowledged=10 "))
        << cancelled.front().message;
    EXPECT_TRUE(ns_has(cancelled.front().message, " abandoned_rows=20 "))
        << cancelled.front().message;
    EXPECT_TRUE(ns_logs_with(logs, "clickhouse native sink closed:").empty());
    EXPECT_EQ(ns_logs_with(logs, "[clickhouse.header_drift]").size(), 1U);
}

// --- Metrics --------------------------------------------------------------------

TEST(NativeSinkMetrics, EverySeriesMovesAsDescribedAndRowsCountOutOnlyOnceAcknowledged) {
    NsRig rig;
    std::atomic<bool> first{false};
    std::atomic<bool> second{false};
    fake::Fault doubt = ns_fault(fake::Step::End, fake::Fault::Kind::SystemError);
    doubt.landing = fake::Fault::Landing::Everything;
    rig.server->inject(std::move(doubt));
    const std::uint64_t records_before = ns_connector("records_total");
    const std::uint64_t errors_before = ns_connector("errors_total");
    const std::uint64_t bytes_before = ns_connector("bytes_total");

    rig.start({ns_data(0, 100),
               ns_wait([&first] { return first.load(); }),
               ns_barrier(1),
               ns_wait([&second] { return second.load(); }),
               ns_data(100, 200),
               ns_barrier(2),
               ns_data(200, 300)});
    const std::uint64_t op = rig.op_id();
    // The rows reach the sink, which buffers them: nothing is out yet.
    ASSERT_TRUE(ns_eventually([&] {
        return ns_count(rig.metrics, clink::metrics::op_metric_name("records_in_total", op)) == 100;
    }));
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(ns_connector("records_total"), records_before);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
    EXPECT_EQ(ns_count(rig.metrics, ns_name(metric::kRowsTotal, op)), 0U);

    first.store(true);
    ASSERT_TRUE(ns_eventually([&] { return rig.acknowledged().size() == 1; }));
    EXPECT_EQ(ns_connector("records_total"), records_before + 100);
    // The second INSERT meets merge back-pressure once at its BeginInsert.
    rig.server->inject(
        ns_fault(fake::Step::Begin, fake::Fault::Kind::ServerError, 252, kNsTooManyParts));
    second.store(true);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 300));

    MetricsRegistry& m = rig.metrics;
    const auto outcome = [&](const char* value) {
        return ns_count(m, ns_name(metric::kInsertsTotal, op, "outcome", value));
    };
    EXPECT_EQ(outcome(metric::kOutcomeOk), 1U);
    EXPECT_EQ(outcome(metric::kOutcomeRetriedOk), 2U);
    EXPECT_EQ(outcome(metric::kOutcomeFailed), 0U);
    EXPECT_EQ(outcome(metric::kOutcomeAbandoned), 0U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kRowsTotal, op)), 300U);
    EXPECT_EQ(ns_observations(m, ns_name(metric::kInsertLatencyNs, op)), 3U);
    EXPECT_EQ(ns_observations(m, ns_name(metric::kBlockRows, op)), 3U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kRetriesTotal, op, "class", "in_doubt")), 1U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kRetriesTotal, op, "class", "merge_backpressure")), 1U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kRetriesTotal, op, "class", "transient")), 0U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kInDoubtTotal, op)), 1U);
    EXPECT_EQ(ns_observations(m, ns_name(metric::kRetryWaitNs, op)), 2U);
    EXPECT_EQ(m.gauge(ns_name(metric::kQueueBytes, op)).value(), 0);
    // Barriers 1 and 2, and the end of input.
    EXPECT_EQ(ns_observations(m, ns_name(metric::kBarrierFlushNs, op)), 3U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kPartsBackoffTotal, op)), 1U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kReconnectsTotal, op)), 2U);
    EXPECT_EQ(ns_count(m, ns_name(metric::kRowsMaybeDuplicatedTotal, op)), 0U);
    EXPECT_EQ(ns_connector("records_total"), records_before + 300);
    EXPECT_EQ(ns_connector("errors_total"), errors_before + 2);
    EXPECT_GT(ns_connector("bytes_total"), bytes_before);
}

TEST(NativeSinkMetrics, AFullQueueHoldsTheTaskThreadAndTheWaitIsTimed) {
    NsRig rig;
    rig.params["batch_bytes"] = std::to_string(kNsMiB);
    fake::Fault slow = ns_fault(fake::Step::End, fake::Fault::Kind::Delay);
    slow.delay = 1s;
    rig.server->inject(std::move(slow));
    // About 1 MiB a batch, 24 of them: the queue fills while the first
    // INSERT's End is held.
    std::vector<NsStep> steps;
    for (std::int64_t b = 0; b < 24; ++b) {
        steps.push_back(ns_data(b * 4, b * 4 + 4, 256 * 1024));
    }
    rig.start(std::move(steps));
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 96));
    EXPECT_GE(ns_observations(rig.metrics, ns_name(metric::kBackpressureBlockedNs, rig.op_id())),
              1U);
    EXPECT_EQ(rig.metrics.gauge(ns_name(metric::kQueueBytes, rig.op_id())).value(), 0);
}

// --- The memory cap -------------------------------------------------------------

TEST(NativeSinkMemory, ABudgetCapsBatchBytesTheReportSaysSoAndEveryReservationComesBack) {
    NsRig rig;
    auto budget = std::make_shared<MemoryBudget>(64 * kNsMiB, "native-sink-test");
    rig.config.memory_budget = budget;
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 100), ns_barrier(1), ns_data(100, 200)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 200));
    const auto logs = ns_logs_since(since);
    const auto opens = ns_logs_with(logs, "clickhouse native sink open:");
    ASSERT_EQ(opens.size(), 1U);
    // Half of 64 MiB, less the 16 MiB queue, halved again.
    EXPECT_TRUE(ns_has(opens.front().message,
                       " batch_bytes=8388608 (reduced from 67108864 for the memory budget) "))
        << opens.front().message;
    EXPECT_EQ(ns_logs_with(logs, "batch_bytes is reduced from 67108864 to 8388608").size(), 1U);
    rig.exec.reset();
    rig.sink.reset();
    EXPECT_EQ(budget->usage().used, 0U);
    EXPECT_GT(budget->usage().peak, 0U);
}

TEST(NativeSinkMemory, ABudgetTooSmallForTheQueueIsRefusedBeforeAnyConnect) {
    NsRig rig;
    rig.config.memory_budget = std::make_shared<MemoryBudget>(8 * kNsMiB, "native-sink-test");
    rig.start({ns_data(0, 10)});
    rig.wait();
    const auto refused = rig.errors_with(code::kMemoryBudgetTooSmall);
    ASSERT_EQ(refused.size(), 1U);
    EXPECT_TRUE(ns_has(refused.front(), "at least 37748736 bytes")) << refused.front();
    EXPECT_EQ(rig.server->connects(), 0U);
    EXPECT_EQ(
        ns_count(
            rig.metrics,
            ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kMemoryBudgetTooSmall)),
        1U);
}

TEST(NativeSinkMemory, TheCapHoldsForABatchBytesAtTheTopOfItsRange) {
    NsRig rig;
    SinkOptions options;
    options.endpoints = {Endpoint{"ch-1", 9000}};
    options.database = "db";
    options.table = kNsTable;
    options.sql_column_types = "id:BIGINT;s:VARCHAR";
    options.batch_interval = 3600000ms;
    // Twice this, with the queue added, wraps a 64-bit sum.
    options.batch_bytes = std::numeric_limits<std::uint64_t>::max();
    auto sink = std::make_shared<NativeSink>(options, fake::fake_factory(rig.server));
    rig.config.memory_budget = std::make_shared<MemoryBudget>(64 * kNsMiB, "native-sink-test");
    const std::int64_t since = ns_log_mark();
    rig.start({ns_data(0, 10)}, true, {}, sink);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 10));
    const auto opens = ns_logs_with(ns_logs_since(since), "clickhouse native sink open:");
    ASSERT_EQ(opens.size(), 1U);
    EXPECT_TRUE(ns_has(opens.front().message,
                       " batch_bytes=8388608 (reduced from 18446744073709551615 for the memory "
                       "budget) "))
        << opens.front().message;
}

// The cap is only worth something if the writer closes its INSERTs at it: 24
// MiB in one interval goes as several INSERTs, none above the capped 8 MiB by
// more than the one row that would have overrun it.
TEST(NativeSinkMemory, TheWriterClosesItsInsertsAtTheCappedBatchBytes) {
    NsRig rig;
    auto budget = std::make_shared<MemoryBudget>(64 * kNsMiB, "native-sink-test");
    rig.config.memory_budget = budget;
    constexpr std::size_t kRowBytes = 64 * 1024;
    std::vector<NsStep> steps;
    for (std::int64_t b = 0; b < 24; ++b) {
        steps.push_back(ns_data(b * 16, b * 16 + 16, kRowBytes));
    }
    steps.push_back(ns_barrier(1));
    rig.start(std::move(steps));
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 384));
    EXPECT_EQ(rig.acknowledged(), std::vector<std::uint64_t>{1});
    const auto inserts = rig.server->inserts(kNsTable);
    EXPECT_GE(inserts.size(), 3U);
    for (const auto& insert : inserts) {
        std::size_t bytes = 0;
        for (const auto& block : insert.blocks) {
            bytes += block.bytes.size();
        }
        EXPECT_LE(bytes, 8 * kNsMiB + 2 * kRowBytes) << "an INSERT ran past the capped batch_bytes";
    }
    rig.exec.reset();
    rig.sink.reset();
    EXPECT_EQ(budget->usage().used, 0U);
}

// --- Barrier modes and the chain rule ---------------------------------------------

TEST(NativeSinkBarrierMode, UnalignedOrAdaptiveCheckpointsAreRefusedAtOpenBeforeAnyConnect) {
    const std::vector<std::pair<std::string, NsConfigure>> modes = {
        {"unaligned",
         [](JobConfig& c, OperatorId /*source*/, OperatorId /*sink*/) {
             c.unaligned_checkpoints = true;
         }},
        {"adaptive",
         [](JobConfig& c, OperatorId /*source*/, OperatorId /*sink*/) {
             c.adaptive_barrier_mode = true;
         }},
        {"an unaligned override on the sink",
         [](JobConfig& c, OperatorId /*source*/, OperatorId sink) {
             c.barrier_mode_overrides_by_operator[sink] = CheckpointBarrier::Mode::Unaligned;
         }},
    };
    for (const auto& [label, configure] : modes) {
        SCOPED_TRACE(label);
        NsRig rig;
        rig.start({ns_data(0, 10), ns_barrier(1)}, true, configure);
        rig.wait();
        const auto refused = rig.errors_with(code::kBarrierModeUnsupported);
        ASSERT_EQ(refused.size(), 1U);
        EXPECT_TRUE(ns_has(refused.front(), "needs aligned checkpoint barriers"))
            << refused.front();
        EXPECT_EQ(rig.server->connects(), 0U);
        EXPECT_TRUE(rig.acknowledged().empty());
        EXPECT_EQ(
            ns_count(
                rig.metrics,
                ns_name(
                    metric::kRefusalsTotal, rig.op_id(), "reason", code::kBarrierModeUnsupported)),
            1U);
    }
}

TEST(NativeSinkBarrierMode, AnUnalignedBarrierOnAnAlignedJobIsRefusedAndNeverAcknowledged) {
    NsRig rig;
    // An override upstream is invisible from the sink's own context: only
    // the barrier carries it. The source is bounded, so a sink that let the
    // barrier through would end the job rather than leave the case waiting.
    rig.start({ns_data(0, 10), ns_barrier(1), ns_data(10, 20)},
              true,
              [](JobConfig& c, OperatorId source, OperatorId /*sink*/) {
                  c.barrier_mode_overrides_by_operator[source] = CheckpointBarrier::Mode::Unaligned;
              });
    rig.wait();
    const auto refused = rig.errors_with(code::kBarrierModeUnsupported);
    ASSERT_EQ(refused.size(), 1U);
    EXPECT_TRUE(ns_has(refused.front(), "checkpoint barrier 1 arrived unaligned"))
        << refused.front();
    EXPECT_TRUE(rig.acknowledged().empty());
    // It opened: the refusal came at the barrier, before anything was flushed.
    EXPECT_GE(rig.server->connects(), 1U);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
    EXPECT_EQ(
        ns_count(
            rig.metrics,
            ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kBarrierModeUnsupported)),
        1U);
}

// A terminal barrier ends a bounded stream: nothing can arrive after it, so
// there are no rows in flight for an unaligned one to lose, and it is
// flushed. Only a barrier with more to come is refused.
TEST(NativeSinkBarrierMode, ATerminalUnalignedBarrierIsFlushedAndOnlyANonTerminalOneIsRefused) {
    NsDirect d;
    NativeSink& sink = d.open();
    sink.on_data(ns_rows(0, 10));
    EXPECT_NO_THROW(
        sink.on_barrier(CheckpointBarrier{CheckpointId{std::numeric_limits<std::uint64_t>::max()},
                                          true,
                                          CheckpointBarrier::Mode::Unaligned}));
    EXPECT_EQ(d.server->rows(kNsTable), 10U);

    const std::int64_t since = ns_log_mark();
    sink.on_data(ns_rows(10, 20));
    const auto refused = ns_error([&sink] {
        sink.on_barrier(CheckpointBarrier{CheckpointId{5}, CheckpointBarrier::Mode::Unaligned});
    });
    ASSERT_TRUE(refused);
    EXPECT_EQ(refused->code(), code::kBarrierModeUnsupported);
    EXPECT_EQ(d.server->rows(kNsTable), 10U);
    EXPECT_EQ(
        ns_count(
            d.metrics,
            ns_name(
                metric::kRefusalsTotal, NsDirect::kOpId, "reason", code::kBarrierModeUnsupported)),
        1U);
    // The refusal stopped the writer, so nothing more is taken.
    EXPECT_THROW(sink.on_data(ns_rows(20, 30)), std::exception);
    d.sink.reset();
    EXPECT_TRUE(ns_has(ns_cancelled_summary(since), " abandoned_rows=10 "));
    EXPECT_EQ(d.server->rows(kNsTable), 10U);
}

TEST(NativeSinkChain, TheSinkMustBeTheOnlySinkOnItsChainWhicheverIsAddedFirst) {
    NsRig rig;
    const auto identity = [](const sql::Row& r) { return r; };
    {
        Dag dag;
        auto h = dag.add_source<sql::Row>(std::make_shared<NsSource>(std::vector<NsStep>{}, true));
        auto m = dag.add_operator<sql::Row, sql::Row>(
            h, std::make_shared<MapOperator<sql::Row, sql::Row>>(identity, "ns_chain_a"));
        dag.add_sink<sql::Row>(m, rig.build());
        try {
            dag.add_sink<sql::Row>(m, std::make_shared<NsDiscardSink>());
            ADD_FAILURE() << "a second sink joined the native sink's chain";
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(
                ns_has(e.what(), "clickhouse_native_sink must be the only sink on its chain"))
                << e.what();
        }
    }
    {
        Dag dag;
        auto h = dag.add_source<sql::Row>(std::make_shared<NsSource>(std::vector<NsStep>{}, true));
        auto m = dag.add_operator<sql::Row, sql::Row>(
            h, std::make_shared<MapOperator<sql::Row, sql::Row>>(identity, "ns_chain_b"));
        dag.add_sink<sql::Row>(m, std::make_shared<NsDiscardSink>());
        try {
            dag.add_sink<sql::Row>(m, rig.build());
            ADD_FAILURE() << "the native sink joined a chain that already had a sink";
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(
                ns_has(e.what(), "clickhouse_native_sink must be the only sink on its chain"))
                << e.what();
        }
    }
}

// --- Refusals at open -------------------------------------------------------------

// A refusal rests on what a server said, or on the options, so no retry could
// change it: the open fails at once, the reason is counted, and the line says
// why the subtask did not open.
TEST(NativeSinkOpen, ARefusalFromTheServerOrThePlanIsCountedByReasonAndNeverRetried) {
    struct Case {
        std::string label;
        std::string code;
        std::function<void(NsRig&)> arrange;
        fake::FakeTable table = ns_table();
    };
    fake::FakeTable async_table = ns_table();
    async_table.engine_full += ", async_insert = 1";
    const std::vector<Case> cases = {
        {"a table that takes inserts asynchronously",
         code::kTargetAsyncInsert,
         [](NsRig& /*rig*/) {},
         async_table},
        {"a table the server does not have",
         code::kTargetMissing,
         [](NsRig& rig) { rig.params["table"] = "absent"; }},
        {"a column the table does not have",
         code::kColumnPlan,
         [](NsRig& rig) { rig.params["sql_column_types"] = "id:BIGINT;s:VARCHAR;extra:BIGINT"; }},
        {"a server that lacks a setting every INSERT sends",
         code::kServerSettingsUnsupported,
         [](NsRig& rig) { rig.server->remove_setting("distributed_foreground_insert"); }},
        {"credentials the server does not accept",
         code::kAccessDenied,
         [](NsRig& rig) {
             rig.server->inject(ns_fault(fake::Step::Connect,
                                         fake::Fault::Kind::ServerError,
                                         516,
                                         "default: Authentication failed: password is incorrect, "
                                         "or there is no user with such name"));
         }},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.label);
        NsRig rig(c.table);
        rig.params["retry_window_ms"] = "60000";
        c.arrange(rig);
        const std::int64_t since = ns_log_mark();
        const auto started = NsClock::now();
        rig.start({ns_data(0, 10)});
        rig.wait();
        // A retry would have waited out at least one backoff and connected
        // again.
        EXPECT_LE(NsClock::now() - started, scale_slack(2s));
        const auto refused = rig.errors_with(c.code);
        ASSERT_EQ(refused.size(), 1U);
        EXPECT_EQ(rig.server->connects(), 1U);
        EXPECT_TRUE(rig.server->inserts(kNsTable).empty());
        EXPECT_EQ(rig.server->rows(kNsTable), 0U);
        EXPECT_EQ(
            ns_count(rig.metrics, ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", c.code)),
            1U);
        const auto lines = ns_logs_with(ns_logs_since(since), "did not open: [" + c.code + "]");
        ASSERT_EQ(lines.size(), 1U);
        EXPECT_EQ(lines.front().level, "error");
        EXPECT_TRUE(ns_logs_with(ns_logs_since(since), "clickhouse native sink open:").empty());
    }
}

// The opener compiles the plan with the core's input kind, so a typed struct's
// refusal names the struct's remedies and a SQL table's keeps its own.
TEST(NativeSinkOpen, APlanRefusalAtOpenIsWordedForTheInputKind) {
    auto server = std::make_shared<fake::FakeServer>();
    server->add_table(ns_table());
    const auto schema = arrow::schema({arrow::field("id", arrow::uint64()),
                                       arrow::field("s", arrow::utf8()),
                                       arrow::field("extra", arrow::int64())});
    const std::map<std::string, std::string> params = {{"database", "db"}, {"table", kNsTable}};
    const auto refusal = [&](InputKind kind) {
        SinkCore core(parse_typed_sink_options(params, 0, 1),
                      columns_from_arrow_schema(*schema),
                      kind,
                      fake::fake_factory(server));
        try {
            core.open(nullptr, OperatorId{4243}, "clickhouse_native_sink");
        } catch (const NativeSinkError& e) {
            EXPECT_EQ(e.code(), code::kColumnPlan);
            return std::string(e.what());
        }
        ADD_FAILURE() << "the plan was accepted";
        return std::string();
    };
    EXPECT_EQ(refusal(InputKind::TypedStruct),
              "[clickhouse.column_plan] `db`.`events` cannot take this struct's rows:\n"
              "  - column `extra` is not in the target table; remove the field from the "
              "CLINK_FIELDS declaration or add the column to the table");
    EXPECT_EQ(refusal(InputKind::SqlTable),
              "[clickhouse.column_plan] `db`.`events` cannot take this table's rows:\n"
              "  - column `extra` is not in the target table; drop it from the SELECT or add it "
              "to the table");
    EXPECT_TRUE(server->inserts(kNsTable).empty());
    EXPECT_EQ(server->destroyed_mid_insert(), 0U);
}

TEST(NativeSinkOpen, ARefusalFromTheTransportItselfIsNeverRetried) {
    NsRig rig;
    rig.params["retry_window_ms"] = "60000";
    auto connects = std::make_shared<std::atomic<int>>(0);
    set_transport_factory_for_testing([connects](const SinkOptions& /*options*/) {
        return std::unique_ptr<InsertTransport>(std::make_unique<NsCaRefused>(connects));
    });
    rig.start({ns_data(0, 10)});
    rig.wait();
    const auto refused = rig.errors_with(code::kOptionInvalid);
    ASSERT_EQ(refused.size(), 1U);
    EXPECT_TRUE(ns_has(refused.front(), "tls_ca_file")) << refused.front();
    EXPECT_EQ(connects->load(), 1);
    EXPECT_EQ(
        ns_count(rig.metrics,
                 ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kOptionInvalid)),
        1U);
}

// A refusal after a failure that was retried: both attempts failed, so both
// count as connector errors, as the writer counts a refusal a reconnect
// meets; only the first was retried, after one wait.
TEST(NativeSinkOpen, ARefusalAfterATransientFailureCountsBothAttemptsAndOneRetry) {
    NsRig rig;
    rig.params["retry_window_ms"] = "60000";
    rig.params["table"] = "absent";
    rig.server->inject(ns_fault(fake::Step::Connect, fake::Fault::Kind::SystemError));
    const std::uint64_t errors_before = ns_connector("errors_total");
    rig.start({ns_data(0, 10)});
    rig.wait();
    ASSERT_EQ(rig.errors_with(code::kTargetMissing).size(), 1U);
    EXPECT_EQ(rig.server->connects(), 2U);
    EXPECT_EQ(ns_connector("errors_total"), errors_before + 2);
    EXPECT_EQ(
        ns_count(rig.metrics, ns_name(metric::kRetriesTotal, rig.op_id(), "class", "transient")),
        1U);
    EXPECT_EQ(ns_observations(rig.metrics, ns_name(metric::kRetryWaitNs, rig.op_id())), 1U);
    EXPECT_EQ(ns_count(rig.metrics, ns_name(metric::kReconnectsTotal, rig.op_id())), 1U);
    EXPECT_EQ(
        ns_count(rig.metrics,
                 ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kTargetMissing)),
        1U);
}

// An unknown server code at open is retried three times, as it is at an
// INSERT, and then fails the open: it neither waits out the whole window nor
// counts as a refusal.
TEST(NativeSinkOpen, AnUnknownServerCodeIsRetriedThreeTimesAndThenFailsTheOpen) {
    NsRig rig;
    rig.params["retry_window_ms"] = "60000";
    for (int i = 0; i < 5; ++i) {
        rig.server->inject(ns_fault(fake::Step::Select, fake::Fault::Kind::ServerError, 99999));
    }
    const auto started = NsClock::now();
    rig.start({ns_data(0, 10)});
    rig.wait();
    // Three backoffs of at most 100, 200 and 400 ms.
    EXPECT_LE(NsClock::now() - started, 100ms + 200ms + 400ms + scale_slack(2300ms));
    ASSERT_EQ(rig.errors_with(code::kInsertFailed).size(), 1U);
    EXPECT_EQ(rig.server->connects(), 4U);
    EXPECT_EQ(
        ns_count(rig.metrics, ns_name(metric::kRetriesTotal, rig.op_id(), "class", "unclassified")),
        3U);
    EXPECT_EQ(ns_count(rig.metrics,
                       ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kInsertFailed)),
              0U);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
}

// --- Outages and the retry window -------------------------------------------------

// The outage cases give the window ten times the outage. With full jitter a
// backoff drawn late can end past a deadline the next attempt would have met,
// so a window only five times the outage gives up about once in two hundred
// runs; ten times makes that about once in forty thousand.
TEST(NativeSinkOutage, AnOutageAtOpenShorterThanTheWindowIsWaitedOut) {
    NsRig rig;
    rig.params["retry_window_ms"] = "10000";
    rig.server->set_down(true);
    rig.start({ns_data(0, 50)});
    ASSERT_TRUE(ns_eventually([&rig] { return rig.server->connects() >= 1; }));
    std::this_thread::sleep_for(1s);
    rig.server->set_down(false);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 50));
    EXPECT_GE(rig.server->connects(), 2U);
    // The client the opener built once the server was back is a reconnect.
    EXPECT_EQ(ns_count(rig.metrics, ns_name(metric::kReconnectsTotal, rig.op_id())), 1U);
}

// An outage that holds the open shows in the sink's own retry series while it
// lasts, as one at an INSERT does, so an alert on them does not wait for the
// server to come back. Every failed attempt counts once as a connector error.
TEST(NativeSinkOutage, AnOutageAtOpenMovesTheRetrySeriesWhileItLasts) {
    NsRig rig;
    rig.params["retry_window_ms"] = "10000";
    rig.server->set_down(true);
    const std::uint64_t errors_before = ns_connector("errors_total");
    rig.start({ns_data(0, 50)});
    const std::string transient = ns_name(metric::kRetriesTotal, rig.op_id(), "class", "transient");
    const std::string waits = ns_name(metric::kRetryWaitNs, rig.op_id());
    ASSERT_TRUE(ns_eventually([&] {
        return ns_count(rig.metrics, transient) >= 2 && ns_observations(rig.metrics, waits) >= 1;
    }));
    rig.server->set_down(false);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 50));
    // Every attempt but the last failed, and each was retried after one wait.
    ASSERT_GE(rig.server->connects(), 3U);
    const std::uint64_t failed = rig.server->connects() - 1;
    EXPECT_EQ(ns_count(rig.metrics, transient), failed);
    EXPECT_EQ(ns_observations(rig.metrics, waits), failed);
    EXPECT_EQ(ns_connector("errors_total"), errors_before + failed);
    EXPECT_EQ(ns_count(rig.metrics, ns_name(metric::kReconnectsTotal, rig.op_id())), 1U);
}

TEST(NativeSinkOutage, AnOutageShorterThanTheWindowSpendsNothingAndEveryRowLandsOnce) {
    NsRig rig;
    rig.params["retry_window_ms"] = "10000";
    std::atomic<bool> go{false};
    rig.start({ns_data(0, 100),
               ns_barrier(1),
               ns_data(100, 200),
               ns_wait([&go] { return go.load(); }),
               ns_barrier(2),
               ns_data(200, 300)});
    ASSERT_TRUE(ns_eventually([&rig] { return rig.acknowledged().size() == 1; }));
    // The writer's client breaks at its next call, and every reconnect is
    // refused until the server is back.
    rig.server->set_down(true, true);
    go.store(true);
    std::this_thread::sleep_for(1s);
    rig.server->set_down(false);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_EQ(rig.acknowledged(), (std::vector<std::uint64_t>{1, 2}));
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 300));
    EXPECT_GE(ns_count(rig.metrics, ns_name(metric::kReconnectsTotal, rig.op_id())), 1U);
}

TEST(NativeSinkOutage, AnOutageLongerThanTheWindowFailsTheBarrierAndKeepsWhatWasAcknowledged) {
    NsRig rig;
    rig.params["retry_window_ms"] = "1000";
    std::atomic<bool> go{false};
    rig.start({ns_data(0, 50),
               ns_barrier(1),
               ns_data(50, 100),
               ns_wait([&go] { return go.load(); }),
               ns_barrier(2)});
    ASSERT_TRUE(ns_eventually([&rig] { return rig.acknowledged().size() == 1; }));
    rig.server->set_down(true, true);
    go.store(true);
    rig.wait();
    rig.server->set_down(false);
    EXPECT_EQ(rig.errors_with(code::kRetryWindowExhausted).size(), 1U);
    EXPECT_EQ(rig.acknowledged(), std::vector<std::uint64_t>{1});
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 50));
    EXPECT_EQ(rig.server->rows(kNsTable), 50U);
}

// An initiator holding a Distributed table over cluster "c", whose two
// replicas each hold the local table.
struct NsCluster {
    explicit NsCluster(const std::string& window_ms) {
        fake::FakeTable distributed = ns_table(0);
        distributed.engine = "Distributed";
        distributed.engine_full = "Distributed('c', 'db', 'events_local', rand())";
        rig = std::make_unique<NsRig>(distributed);
        for (int i = 0; i < 2; ++i) {
            auto replica = std::make_shared<fake::FakeServer>();
            fake::FakeTable local = ns_table(0);
            local.name = "events_local";
            local.engine = "ReplicatedMergeTree";
            local.engine_full = "ReplicatedMergeTree('/t/events', '{replica}') ORDER BY id";
            replica->add_table(local);
            replicas.push_back(replica);
            rig->servers.push_back(replica);
        }
        rig->server->add_cluster("c", {replicas[0].get(), replicas[1].get()});
        rig->params["retry_window_ms"] = window_ms;
    }
    std::vector<std::shared_ptr<fake::FakeServer>> replicas;
    std::unique_ptr<NsRig> rig;
};

TEST(NativeSinkOutage, AReplicaUnreadableAtOpenForLessThanTheWindowIsWaitedOutNotRefused) {
    NsCluster c("10000");
    NsRig& rig = *c.rig;
    rig.server->set_unreadable_replica("c", 1);
    rig.start({ns_data(0, 20)});
    ASSERT_TRUE(ns_eventually([&rig] { return rig.server->connects() >= 1; }));
    std::this_thread::sleep_for(1s);
    rig.server->set_unreadable_replica("c", 1, false);
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 20));
    EXPECT_EQ(
        ns_count(rig.metrics,
                 ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kTargetAsyncInsert)),
        0U);
}

TEST(NativeSinkOutage, AReplicaUnreadableForLongerThanTheWindowExhaustsItRatherThanRefuses) {
    NsCluster c("5000");
    NsRig& rig = *c.rig;
    rig.server->set_unreadable_replica("c", 1);
    const auto started = NsClock::now();
    rig.start({ns_data(0, 20)});
    rig.wait();
    const auto took = NsClock::now() - started;
    EXPECT_EQ(rig.errors_with(code::kRetryWindowExhausted).size(), 1U);
    EXPECT_TRUE(rig.errors_with(code::kTargetAsyncInsert).empty());
    // The open gives up once the next backoff would end past the window's
    // deadline, which with full jitter can be well before it, and never
    // later than the window and one attempt.
    EXPECT_LE(took, 5s + scale_slack(1s));
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
    EXPECT_EQ(
        ns_count(rig.metrics,
                 ns_name(metric::kRefusalsTotal, rig.op_id(), "reason", code::kTargetAsyncInsert)),
        0U);
}

TEST(NativeSinkFailover, ARefusingFirstEndpointIsSkippedAndARebuildStaysOnTheOneThatWorks) {
    NsRig rig;
    const Endpoint first{"ch-1", 9000};
    const Endpoint second{"ch-2", 9000};
    auto down = rig.server;
    down->set_down(true);
    auto up = rig.add_server();
    set_transport_factory_for_testing(fake::fake_factory(
        std::map<Endpoint, std::shared_ptr<fake::FakeServer>>{{first, down}, {second, up}}));
    rig.params["endpoints"] = "ch-1:9000,ch-2:9000";
    // An End that fails in doubt makes the writer build a new client.
    up->inject(ns_fault(fake::Step::End, fake::Fault::Kind::SystemError));
    rig.start({ns_data(0, 100), ns_barrier(1), ns_data(100, 200)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*up, 0, 200));
    // The opener tried the first endpoint once, and the rebuild went straight
    // back to the second.
    EXPECT_EQ(down->connects(), 1U);
    EXPECT_GE(up->connects(), 2U);
    EXPECT_EQ(down->rows(kNsTable), 0U);
}

// The open moves on from an endpoint that refuses its connect, and stays on
// one that answered when a read of its metadata fails.
TEST(NativeSinkFailover, AMetadataFailureAtOpenStaysOnTheEndpointThatAnswered) {
    NsRig rig;
    const Endpoint first{"ch-1", 9000};
    const Endpoint second{"ch-2", 9000};
    auto other = rig.add_server();
    set_transport_factory_for_testing(
        fake::fake_factory(std::map<Endpoint, std::shared_ptr<fake::FakeServer>>{
            {first, rig.server}, {second, other}}));
    rig.params["endpoints"] = "ch-1:9000,ch-2:9000";
    rig.params["retry_window_ms"] = "60000";
    rig.server->inject(ns_fault(fake::Step::Select, fake::Fault::Kind::SystemError));
    rig.start({ns_data(0, 20)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_TRUE(ns_each_landed(*rig.server, 0, 20));
    EXPECT_EQ(rig.server->connects(), 2U);
    EXPECT_EQ(other->connects(), 0U);
}

// --- Cancel -------------------------------------------------------------------------

TEST(NativeSinkCancel, ACancelDuringABackoffFailsTheBarrierWithin200msAndLandsNothingOfIt) {
#if !defined(CLINK_FAULT_INJECTION)
    GTEST_SKIP() << "needs the fault points compiled in";
#else
    NsRig rig;
    rig.params["retry_window_ms"] = "60000";
    constexpr std::size_t kFailures = 3;
    for (std::size_t i = 0; i < kFailures; ++i) {
        rig.server->inject(ns_fault(fake::Step::End, fake::Fault::Kind::SystemError));
    }
    // Holds the writer just before its third backoff, so the cancel lands
    // there and nowhere else.
    const clink::fault::ScopedFault parked(clink::fault::Rule{
        .point = points::kBeforeRetryWait,
        .ordinal = kFailures,
        .action = clink::fault::Action::Block,
    });
    rig.start({ns_data(0, 40), ns_barrier(1)}, false);
    ASSERT_TRUE(ns_eventually([] {
        return clink::fault::Registry::instance().hits(points::kBeforeRetryWait) == kFailures;
    }));
    const std::int64_t since = ns_log_mark();
    const auto cancelled_at = NsClock::now();
    rig.exec->cancel();
    clink::fault::Registry::instance().release(points::kBeforeRetryWait);
    rig.wait();
    EXPECT_LE(NsClock::now() - cancelled_at, scale_slack(200ms));
    // on_barrier threw rather than let the checkpoint complete.
    EXPECT_EQ(rig.errors_with(code::kCancelled).size(), 1U);
    EXPECT_TRUE(rig.acknowledged().empty());
    EXPECT_EQ(rig.server->abandoned_mid_insert(), kFailures);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
    const auto cancelled = ns_logs_with(ns_logs_since(since), "clickhouse native sink cancelled:");
    ASSERT_EQ(cancelled.size(), 1U);
    EXPECT_TRUE(ns_has(cancelled.front().message, " abandoned_rows=40 "))
        << cancelled.front().message;
#endif
}

TEST(NativeSinkCancel, ACancelWhileTheServerHangsInEndClosesTheSinkWithin200ms) {
    NsRig rig;
    rig.params["batch_interval_ms"] = "50";
    auto hanging = std::make_shared<std::atomic<bool>>(false);
    fake::Fault hang = ns_fault(fake::Step::End, fake::Fault::Kind::Hang);
    hang.on_fire = [hanging] { hanging->store(true); };
    rig.server->inject(std::move(hang));
    rig.start({ns_data(0, 10)}, false);
    ASSERT_TRUE(ns_eventually([&hanging] { return hanging->load(); }));
    const std::int64_t since = ns_log_mark();
    const auto cancelled_at = NsClock::now();
    rig.exec->cancel();
    rig.wait();
    // close_cancelled() returned that fast only through interrupt().
    EXPECT_LE(NsClock::now() - cancelled_at, scale_slack(200ms));
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    EXPECT_EQ(rig.server->abandoned_mid_insert(), 1U);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
    const auto cancelled = ns_logs_with(ns_logs_since(since), "clickhouse native sink cancelled:");
    ASSERT_EQ(cancelled.size(), 1U);
    EXPECT_TRUE(ns_has(cancelled.front().message, " abandoned_rows=10 "))
        << cancelled.front().message;
}

TEST(NativeSinkCancel, ACancelWhileOpenHangsInConnectFailsTheOpenWithin200ms) {
    NsRig rig;
    auto hanging = std::make_shared<std::atomic<bool>>(false);
    fake::Fault hang = ns_fault(fake::Step::Connect, fake::Fault::Kind::Hang);
    hang.on_fire = [hanging] { hanging->store(true); };
    rig.server->inject(std::move(hang));
    rig.start({ns_data(0, 10)}, false);
    ASSERT_TRUE(ns_eventually([&hanging] { return hanging->load(); }));
    const auto cancelled_at = NsClock::now();
    rig.exec->cancel();
    rig.wait();
    EXPECT_LE(NsClock::now() - cancelled_at, scale_slack(200ms));
    EXPECT_EQ(rig.errors_with(code::kCancelled).size(), 1U);
    EXPECT_TRUE(rig.server->inserts(kNsTable).empty());
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
}

TEST(NativeSinkCancel, AnOpenStuckWhereNoInterruptReachesIsLeftBehindAndThenStaysSilent) {
    NsRig rig;
    auto gone = std::make_shared<std::atomic<bool>>(false);
    set_transport_factory_for_testing([inner = fake::fake_factory(rig.server),
                                       gone](const SinkOptions& options) {
        return std::unique_ptr<InsertTransport>(std::make_unique<NsWatched>(inner(options), gone));
    });
    auto stuck = std::make_shared<std::atomic<bool>>(false);
    fake::Fault fault = ns_fault(fake::Step::Connect, fake::Fault::Kind::Uninterruptible);
    fault.on_fire = [stuck] { stuck->store(true); };
    rig.server->inject(std::move(fault));
    rig.start({ns_data(0, 10)}, false);
    ASSERT_TRUE(ns_eventually([&stuck] { return stuck->load(); }));
    const auto cancelled_at = NsClock::now();
    rig.exec->cancel();
    rig.wait();
    const auto took = NsClock::now() - cancelled_at;
    EXPECT_GE(took, 4900ms);
    EXPECT_LE(took, 5s + scale_slack(200ms));
    EXPECT_EQ(rig.errors_with(code::kCancelled).size(), 1U);

    ASSERT_FALSE(gone->load()) << "the stuck opener should still hold its transport";
    const std::uint64_t errors_before = ns_connector("errors_total");
    const std::int64_t since = ns_log_mark();
    rig.server->release();
    ASSERT_TRUE(ns_eventually([&gone] { return gone->load(); }, 3s))
        << "the opener left behind never let go of its state";
    EXPECT_TRUE(ns_logs_since(since).empty());
    EXPECT_EQ(ns_connector("errors_total"), errors_before);
    EXPECT_EQ(rig.server->rows(kNsTable), 0U);
}

// The opener waits out its backoffs in slices that look at the cancel. By the
// fifth attempt a backoff may run to 1.6 s, so a wait that ignored the cancel
// would show in the time the open takes to end.
TEST(NativeSinkCancel, ACancelDuringTheOpenersBackoffEndsTheOpenWithoutAnotherConnect) {
    NsDirect d;
    d.params["retry_window_ms"] = "60000";
    d.server->set_down(true);
    NativeSink& sink = d.build();
    auto opened =
        std::async(std::launch::async, [&sink] { return ns_error([&sink] { sink.open(); }); });
    ASSERT_TRUE(ns_eventually([&d] { return d.server->connects() >= 5; }, 20s));
    const std::size_t before = d.server->connects();
    const auto cancelled_at = NsClock::now();
    d.cancel->store(true);
    ASSERT_EQ(opened.wait_for(scale_slack(2s)), std::future_status::ready);
    const auto took = NsClock::now() - cancelled_at;
    const auto error = opened.get();
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    // Not scaled for a sanitizer build: ten times this would pass the 1.6 s
    // backoff it tells apart.
    EXPECT_LE(took, 300ms);
    // At most the attempt the cancel raced.
    EXPECT_LE(d.server->connects(), before + 1);
}

// A probe blocked in a metadata read has a socket that interrupt() shuts
// down, so the opener is joined rather than left behind: its client is gone
// by the time open() returns.
TEST(NativeSinkCancel, ACancelWhileTheProbeHangsJoinsTheOpenerBeforeOpenReturns) {
    NsDirect d;
    auto gone = std::make_shared<std::atomic<bool>>(false);
    // The client takes 100 ms to go, so an open() that returned without
    // joining the opener would find it still there.
    set_transport_factory_for_testing(
        [inner = fake::fake_factory(d.server), gone](const SinkOptions& options) {
            return std::unique_ptr<InsertTransport>(
                std::make_unique<NsWatched>(inner(options), gone, 100ms));
        });
    auto hanging = std::make_shared<std::atomic<bool>>(false);
    fake::Fault hang = ns_fault(fake::Step::Select, fake::Fault::Kind::Hang);
    hang.on_fire = [hanging] { hanging->store(true); };
    d.server->inject(std::move(hang));
    NativeSink& sink = d.build();
    auto opened = std::async(std::launch::async, [&sink, gone] {
        auto error = ns_error([&sink] { sink.open(); });
        return std::make_pair(error, gone->load());
    });
    ASSERT_TRUE(ns_eventually([&hanging] { return hanging->load(); }));
    const auto cancelled_at = NsClock::now();
    d.cancel->store(true);
    ASSERT_EQ(opened.wait_for(scale_slack(2s)), std::future_status::ready);
    const auto took = NsClock::now() - cancelled_at;
    const auto [error, gone_at_return] = opened.get();
    ASSERT_TRUE(error);
    EXPECT_EQ(error->code(), code::kCancelled);
    // The client's 100 ms linger, which the join waits out, and a margin.
    EXPECT_LE(took, 100ms + scale_slack(200ms));
    EXPECT_TRUE(gone_at_return) << "the opener was left behind instead of joined";
}

// Rows the writer had not yet taken from its queue are as unacknowledged as
// the INSERT it holds, and the restart replays them too, so the cancelled
// summary counts them.
TEST(NativeSinkCancel, TheCancelledSummaryCountsTheQueuedRowsAsAbandonedToo) {
    NsDirect d;
    d.params["batch_rows"] = "5";
    auto hanging = std::make_shared<std::atomic<bool>>(false);
    fake::Fault hang = ns_fault(fake::Step::End, fake::Fault::Kind::Hang);
    hang.on_fire = [hanging] { hanging->store(true); };
    d.server->inject(std::move(hang));
    const std::int64_t since = ns_log_mark();
    NativeSink& sink = d.open();
    sink.on_data(ns_rows(0, 5));
    ASSERT_TRUE(ns_eventually([&hanging] { return hanging->load(); }));
    // The writer is held in the first INSERT's End, so these stay queued.
    sink.on_data(ns_rows(5, 15));
    sink.close_cancelled();
    EXPECT_EQ(d.server->rows(kNsTable), 0U);
    const std::string summary = ns_cancelled_summary(since);
    EXPECT_TRUE(ns_has(summary, " rows_acknowledged=0 ")) << summary;
    EXPECT_TRUE(ns_has(summary, " abandoned_rows=15 ")) << summary;
    // Only the INSERT in flight counts as an abandoned INSERT.
    EXPECT_EQ(ns_count(d.metrics,
                       ns_name(metric::kInsertsTotal, NsDirect::kOpId, "outcome", "abandoned")),
              1U);
}

// However soon the cancel comes, the summary accounts for every row the sink
// was given: acknowledged, or abandoned wherever the writer held it, in its
// queue, in the chunk it was taking in or in an INSERT.
TEST(NativeSinkCancel, EveryRowIsAcknowledgedOrAbandonedHoweverSoonTheCancelComes) {
    for (int run = 0; run < 25; ++run) {
        SCOPED_TRACE(run);
        NsDirect d;
        d.params["batch_rows"] = "4";
        const std::int64_t since = ns_log_mark();
        NativeSink& sink = d.open();
        sink.on_data(ns_rows(0, 10));
        sink.close_cancelled();
        const std::string summary = ns_cancelled_summary(since);
        EXPECT_EQ(ns_field(summary, "rows_acknowledged") + ns_field(summary, "abandoned_rows"), 10U)
            << summary;
    }
}

// --- Rows the target cannot take ----------------------------------------------------

TEST(NativeSinkConversion, AWrongKindCellFailsTheTaskInsteadOfLandingAsNull) {
    struct Case {
        std::string label;
        std::string bad_row;
        std::string column;
        std::string hidden;  // cell text the log must not show; empty when a number
    };
    const std::vector<Case> cases = {
        {"ISO-8601 text in a TIMESTAMP",
         R"({"id":3,"ts":"2024-01-01T00:00:00Z","n":1})",
         "ts",
         "2024-01-01T00:00:00Z"},
        {"SMALLINT out of range", R"({"id":3,"ts":1700000000000,"n":40000})", "n", ""},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.label);
        fake::FakeTable table = ns_table();
        table.columns = {{"id", "Int64", DefaultKind::None, 1},
                         {"ts", "Nullable(DateTime64(3))", DefaultKind::None, 2},
                         {"n", "Nullable(Int16)", DefaultKind::None, 3}};
        NsRig rig(table);
        rig.params["sql_column_types"] = "id:BIGINT;ts:TIMESTAMP(3);n:SMALLINT";
        const std::int64_t since = ns_log_mark();
        rig.start({ns_lines({R"({"id":1,"ts":1700000000000,"n":7})"}),
                   ns_barrier(1),
                   ns_lines({R"({"id":2,"ts":1700000000001,"n":8})", c.bad_row})});
        rig.wait();
        const auto failed = rig.errors_with(code::kConversionFailed);
        ASSERT_EQ(failed.size(), 1U);
        EXPECT_TRUE(ns_has(failed.front(), "column `" + c.column + "`, row 1")) << failed.front();
        // The batch with the bad cell sent nothing: only the first row is in
        // the table, and no cell of it is NULL.
        const auto landed = rig.server->landed(kNsTable);
        std::size_t rows = 0;
        for (const auto& block : landed) {
            for (const auto& row : block.values) {
                ++rows;
                EXPECT_EQ(row.at(0), "1");
                EXPECT_EQ(std::count(row.begin(), row.end(), "NULL"), 0);
            }
        }
        EXPECT_EQ(rows, 1U);
        const auto lines = ns_logs_with(ns_logs_since(since), "cannot write a row of this batch");
        ASSERT_EQ(lines.size(), 1U);
        EXPECT_EQ(lines.front().level, "error");
        EXPECT_TRUE(ns_has(lines.front().message, "column `" + c.column + "`"))
            << lines.front().message;
        if (!c.hidden.empty()) {
            EXPECT_FALSE(ns_has(lines.front().message, c.hidden)) << lines.front().message;
        }
    }
}

// --- The columnar intake ------------------------------------------------------------

// The rows ns_rows(from, to, length) holds, as the sidecar a columnar Row
// producer emits: a null event time, then id and s.
std::shared_ptr<arrow::RecordBatch> ns_sidecar(std::int64_t from,
                                               std::int64_t to,
                                               std::size_t length = 8) {
    arrow::Int64Builder times;
    arrow::Int64Builder ids;
    arrow::StringBuilder texts;
    for (std::int64_t id = from; id < to; ++id) {
        EXPECT_TRUE(times.AppendNull().ok());
        EXPECT_TRUE(ids.Append(id).ok());
        EXPECT_TRUE(texts.Append(std::string(length, static_cast<char>('a' + id % 26))).ok());
    }
    std::shared_ptr<arrow::Array> t;
    std::shared_ptr<arrow::Array> i;
    std::shared_ptr<arrow::Array> s;
    EXPECT_TRUE(times.Finish(&t).ok());
    EXPECT_TRUE(ids.Finish(&i).ok());
    EXPECT_TRUE(texts.Finish(&s).ok());
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64()),
                                                   arrow::field("id", arrow::int64()),
                                                   arrow::field("s", arrow::utf8())}),
                                    to - from,
                                    {t, i, s});
}

// A columnar batch whose closure counts its calls: any call is a Row built
// from the sidecar. It still answers with the rows, so a sink that calls it
// lands the right values and only the count tells.
Batch<sql::Row> ns_counted(std::shared_ptr<arrow::RecordBatch> sidecar,
                           std::shared_ptr<std::atomic<int>> calls) {
    const auto rows = static_cast<std::size_t>(sidecar->num_rows());
    return Batch<sql::Row>{
        std::move(sidecar), rows, [calls = std::move(calls)](const arrow::RecordBatch& b) {
            calls->fetch_add(1);
            auto out = sql::rows_from_record_batch(b);
            return out ? std::move(*out) : std::vector<Record<sql::Row>>{};
        }};
}

NsStep ns_columnar(std::int64_t from, std::int64_t to, std::shared_ptr<std::atomic<int>> calls) {
    NsStep s;
    s.rows = [from, to, calls = std::move(calls)] {
        return ns_counted(ns_sidecar(from, to), calls);
    };
    return s;
}

// Every block the table received, in order: the bytes of each column as the
// client wrote them, then its rows.
std::vector<std::pair<std::string, std::vector<std::vector<std::string>>>> ns_received(
    const fake::FakeServer& s) {
    std::vector<std::pair<std::string, std::vector<std::vector<std::string>>>> out;
    for (const auto& insert : s.inserts(kNsTable)) {
        for (const auto& block : insert.blocks) {
            out.emplace_back(block.bytes, block.values);
        }
    }
    return out;
}

std::vector<std::vector<std::string>> ns_sorted_rows(const fake::FakeServer& s) {
    std::vector<std::vector<std::string>> out;
    for (const auto& block : s.landed(kNsTable)) {
        out.insert(out.end(), block.values.begin(), block.values.end());
    }
    std::sort(out.begin(), out.end());
    return out;
}

// The sink on its chain, behind a source that emits columnar batches: no Row
// is built for them anywhere on the hop, and the server receives the same
// blocks, byte for byte, as from row batches holding the same rows.
TEST(NativeSinkColumnar, AChainSinkTakesAColumnarBatchWithoutCallingItsClosure) {
    const auto calls = std::make_shared<std::atomic<int>>(0);
    std::vector<std::pair<std::string, std::vector<std::vector<std::string>>>> columnar;
    {
        NsRig rig;
        rig.start({ns_columnar(0, 100, calls), ns_barrier(1), ns_columnar(100, 250, calls)});
        rig.wait();
        EXPECT_TRUE(rig.exec->operator_errors().empty());
        EXPECT_TRUE(ns_each_landed(*rig.server, 0, 250));
        columnar = ns_received(*rig.server);
    }
    EXPECT_EQ(calls->load(), 0) << "a Row was built from the sidecar on the sink's hop";
    NsRig rig;
    rig.start({ns_data(0, 100), ns_barrier(1), ns_data(100, 250)});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    const auto rows = ns_received(*rig.server);
    ASSERT_EQ(columnar.size(), 2U);
    EXPECT_EQ(columnar, rows);
}

// The same through the fan-in runner, a single sink subtask reading two
// upstream subtasks.
TEST(NativeSinkColumnar, AFanInSinkTakesAColumnarBatchWithoutCallingItsClosure) {
    const auto calls = std::make_shared<std::atomic<int>>(0);
    std::vector<std::vector<std::string>> columnar;
    {
        NsRig rig;
        rig.start_fan_in({{ns_columnar(0, 60, calls), ns_columnar(60, 100, calls)},
                          {ns_columnar(100, 180, calls)}});
        rig.wait();
        EXPECT_TRUE(rig.exec->operator_errors().empty());
        EXPECT_TRUE(ns_each_landed(*rig.server, 0, 180));
        columnar = ns_sorted_rows(*rig.server);
    }
    EXPECT_EQ(calls->load(), 0) << "a Row was built from the sidecar on the sink's hop";
    NsRig rig;
    rig.start_fan_in({{ns_data(0, 60), ns_data(60, 100)}, {ns_data(100, 180)}});
    rig.wait();
    EXPECT_TRUE(rig.exec->operator_errors().empty());
    ASSERT_EQ(columnar.size(), 180U);
    EXPECT_EQ(columnar, ns_sorted_rows(*rig.server));
}

// A sidecar the self-describing reader cannot read as the intake would is
// handed back before anything is reserved or sent, its reason is counted, and
// on_data then lands the batch through its rows. The closure here is not the
// reader, so each batch still has rows to give.
TEST(NativeSinkColumnar, ADeclinedBatchReservesAndSendsNothingAndOnDataLandsIt) {
    struct Case {
        std::string label;
        std::shared_ptr<arrow::RecordBatch> sidecar;
        const char* reason;
    };
    const auto base = ns_sidecar(0, 4);
    std::shared_ptr<arrow::Array> int32_times;
    {
        arrow::Int32Builder b;
        ASSERT_TRUE(b.AppendNulls(4).ok());
        ASSERT_TRUE(b.Finish(&int32_times).ok());
    }
    std::shared_ptr<arrow::Array> dates;
    {
        arrow::Date32Builder b;
        ASSERT_TRUE(b.AppendValues({1, 2, 3, 4}).ok());
        ASSERT_TRUE(b.Finish(&dates).ok());
    }
    const std::vector<Case> cases = {
        {"an int32 event-time column",
         arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int32()),
                                                 arrow::field("id", arrow::int64()),
                                                 arrow::field("s", arrow::utf8())}),
                                  4,
                                  {int32_times, base->column(1), base->column(2)}),
         "event_time"},
        {"a date32 value column",
         base->AddColumn(3, arrow::field("d", arrow::date32()), dates).ValueOrDie(),
         "unsupported_type"},
        {"a declared name carried twice",
         base->AddColumn(3, arrow::field("id", arrow::int64()), base->column(1)).ValueOrDie(),
         "duplicate_name"},
    };
    std::int64_t next = 0;
    for (const auto& c : cases) {
        SCOPED_TRACE(c.label);
        NsDirect direct;
        NativeSink& sink = direct.open();
        const std::int64_t since = ns_log_mark();
        const std::int64_t from = next;
        next += 4;
        const auto rows = ns_rows(from, from + 4);
        const Batch<sql::Row> batch{
            c.sidecar, 4, [&rows](const arrow::RecordBatch&) { return rows.records(); }};
        // The same schema twice: both are counted, one is logged.
        EXPECT_FALSE(sink.on_data_columnar(batch));
        EXPECT_FALSE(sink.on_data_columnar(batch));
        EXPECT_EQ(direct.metrics.gauge(ns_name(metric::kQueueBytes, NsDirect::kOpId)).value(), 0);
        sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
        EXPECT_TRUE(direct.server->inserts(kNsTable).empty())
            << "a declined batch reached the writer";
        EXPECT_EQ(ns_count(direct.metrics,
                           ns_name(metric::kColumnarDeclinedTotal,
                                   NsDirect::kOpId,
                                   "reason",
                                   std::string(c.reason))),
                  2U);
        const auto logged = ns_logs_with(ns_logs_since(since),
                                         "takes columnar batches of this schema through their "
                                         "rows (" +
                                             std::string(c.reason) + ")");
        ASSERT_EQ(logged.size(), 1U);
        EXPECT_EQ(logged.front().level, "info");

        sink.on_data(batch);
        sink.on_barrier(CheckpointBarrier{CheckpointId{2}});
        EXPECT_TRUE(ns_each_landed(*direct.server, from, from + 4));
        EXPECT_EQ(direct.server->rows(kNsTable), 4U);
        EXPECT_EQ(
            ns_count(
                direct.metrics,
                ns_name(
                    metric::kInputBatchesTotal, NsDirect::kOpId, "carrier", metric::kCarrierRow)),
            1U);
        EXPECT_EQ(ns_count(direct.metrics,
                           ns_name(metric::kInputBatchesTotal,
                                   NsDirect::kOpId,
                                   "carrier",
                                   metric::kCarrierColumnar)),
                  0U);
        sink.flush();
        sink.close();
    }
}

// What the sink counts and reports for each carrier: the batches it took
// columnar and through their rows, in the metric and in the closed summary.
TEST(NativeSinkColumnar, EachCarrierIsCountedAndTheSummaryReportsBoth) {
    NsDirect direct;
    NativeSink& sink = direct.open();
    const std::int64_t since = ns_log_mark();
    const auto calls = std::make_shared<std::atomic<int>>(0);
    EXPECT_TRUE(sink.on_data_columnar(ns_counted(ns_sidecar(0, 10), calls)));
    EXPECT_TRUE(sink.on_data_columnar(ns_counted(ns_sidecar(10, 30), calls)));
    // An empty columnar batch is taken, and is no batch to count.
    EXPECT_TRUE(sink.on_data_columnar(ns_counted(ns_sidecar(30, 30), calls)));
    sink.on_data(ns_rows(30, 40));
    // A row batch has no sidecar to take.
    EXPECT_FALSE(sink.on_data_columnar(ns_rows(40, 41)));
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.flush();
    sink.close();
    EXPECT_EQ(calls->load(), 0);
    EXPECT_TRUE(ns_each_landed(*direct.server, 0, 40));
    EXPECT_EQ(direct.server->rows(kNsTable), 40U);
    EXPECT_EQ(ns_count(direct.metrics,
                       ns_name(metric::kInputBatchesTotal, NsDirect::kOpId, "carrier", "columnar")),
              2U);
    EXPECT_EQ(ns_count(direct.metrics,
                       ns_name(metric::kInputBatchesTotal, NsDirect::kOpId, "carrier", "row")),
              1U);
    const auto closed = ns_logs_with(ns_logs_since(since), "clickhouse native sink closed:");
    ASSERT_EQ(closed.size(), 1U);
    EXPECT_TRUE(ns_has(closed.front().message, " elapsed_ms=")) << closed.front().message;
    EXPECT_TRUE(closed.front().message.ends_with(" columnar_batches=2 row_batches=1"))
        << closed.front().message;
}

// A cell the intake cannot convert fails the task as on_data does, with the
// same line, and sends nothing of its batch.
TEST(NativeSinkColumnar, AColumnarCellThatCannotConvertFailsTheTaskAsOnDataDoes) {
    fake::FakeTable table = ns_table();
    table.columns = {{"id", "Int64", DefaultKind::None, 1},
                     {"ts", "Nullable(DateTime64(3))", DefaultKind::None, 2}};
    NsDirect direct(table);
    direct.params["sql_column_types"] = "id:BIGINT;ts:TIMESTAMP(3)";
    NativeSink& sink = direct.open();
    const std::int64_t since = ns_log_mark();
    arrow::Int64Builder times;
    arrow::Int64Builder ids;
    arrow::StringBuilder stamps;
    ASSERT_TRUE(times.AppendNulls(2).ok());
    ASSERT_TRUE(ids.AppendValues({1, 2}).ok());
    ASSERT_TRUE(stamps.AppendValues({"1700000000000", "2024-01-01T00:00:00Z"}).ok());
    const auto sidecar = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("event_time", arrow::int64()),
                       arrow::field("id", arrow::int64()),
                       arrow::field("ts", arrow::utf8())}),
        2,
        {times.Finish().ValueOrDie(), ids.Finish().ValueOrDie(), stamps.Finish().ValueOrDie()});
    const auto calls = std::make_shared<std::atomic<int>>(0);
    try {
        (void)sink.on_data_columnar(ns_counted(sidecar, calls));
        ADD_FAILURE() << "the batch was taken";
    } catch (const ConversionError& e) {
        EXPECT_EQ(e.column(), "ts");
        EXPECT_EQ(e.row(), 1);
    }
    EXPECT_EQ(calls->load(), 0);
    sink.close_cancelled();
    EXPECT_TRUE(direct.server->inserts(kNsTable).empty());
    const auto lines = ns_logs_with(ns_logs_since(since), "cannot write a row of this batch");
    ASSERT_EQ(lines.size(), 1U);
    EXPECT_EQ(lines.front().level, "error");
    EXPECT_FALSE(ns_has(lines.front().message, "2024")) << lines.front().message;
}

// --- Shared chunks are freed on the task thread ---------------------------------------

// A sidecar of two builder-made int64 columns, id and v = id * 3, both of which
// the intake reuses for BIGINT columns, allocated from `pool`.
std::shared_ptr<arrow::RecordBatch> ns_int_sidecar(
    std::int64_t from, std::int64_t to, arrow::MemoryPool* pool = arrow::default_memory_pool()) {
    arrow::Int64Builder times(pool);
    arrow::Int64Builder ids(pool);
    arrow::Int64Builder values(pool);
    for (std::int64_t id = from; id < to; ++id) {
        EXPECT_TRUE(times.AppendNull().ok());
        EXPECT_TRUE(ids.Append(id).ok());
        EXPECT_TRUE(values.Append(id * 3).ok());
    }
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("event_time", arrow::int64()),
                       arrow::field("id", arrow::int64()),
                       arrow::field("v", arrow::int64())}),
        to - from,
        {times.Finish().ValueOrDie(), ids.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
}

// The writer is held at the fault point with every reused chunk while the task
// thread, after the sink has taken the batch, reads the batch's columns once
// more, as a sibling consumer of the same element does, and drops it. The
// writer's reference is then the last. The sidecar's arrays come from a pool
// that records which thread frees them, so on every build, with or without a
// sanitizer, a writer that freed a reused array itself turns this red: every
// free must run on the task thread, and none of a held chunk's before the
// writer lets go of it.
TEST(NativeSinkRelease, AChunkHeldOnTheWriterPastTheInputsDropIsFreedOnTheTaskThread) {
#if !defined(CLINK_FAULT_INJECTION)
    GTEST_SKIP() << "needs the fault points compiled in";
#else
    // Declared first, so it outlives every array it allocated.
    fake::ThreadRecordingPool pool;
    fake::FakeTable table = ns_table();
    table.columns = {{"id", "Int64", DefaultKind::None, 1}, {"v", "Int64", DefaultKind::None, 2}};
    NsDirect direct(table);
    direct.params["sql_column_types"] = "id:BIGINT;v:BIGINT";
    NativeSink& sink = direct.open();
    const clink::fault::ScopedFault held(clink::fault::Rule{
        .point = points::kBeforeSharedChunkRelease,
        .ordinal = 0,
        .action = clink::fault::Action::Block,
    });
    constexpr std::int64_t kBatches = 12;
    constexpr std::int64_t kRows = 50;
    std::int64_t sum = 0;
    for (std::int64_t b = 0; b < kBatches; ++b) {
        auto sidecar = ns_int_sidecar(b * kRows, (b + 1) * kRows, &pool);
        {
            const Batch<sql::Row> batch{
                sidecar, static_cast<std::size_t>(kRows), sql::row_materialize_fn()};
            ASSERT_TRUE(sink.on_data_columnar(batch));
        }
        // The writer is parked at the point with the chunk, so the drop below
        // comes first.
        ASSERT_TRUE(ns_eventually([b] {
            return clink::fault::Registry::instance().hits(points::kBeforeSharedChunkRelease) ==
                   static_cast<std::uint64_t>(b + 1);
        }));
        for (int c = 1; c <= 2; ++c) {
            const auto& column = static_cast<const arrow::Int64Array&>(*sidecar->column(c));
            for (std::int64_t i = 0; i < column.length(); ++i) {
                sum += column.Value(i);
            }
        }
        sidecar.reset();
        // The chunk still holds the two reused columns.
        EXPECT_GE(pool.bytes_allocated(), 2 * kRows * 8);
        EXPECT_EQ(pool.frees_elsewhere(), 0U);
        clink::fault::Registry::instance().release(points::kBeforeSharedChunkRelease);
    }
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.flush();
    sink.close();
    const std::int64_t n = kBatches * kRows;
    EXPECT_EQ(sum, 4 * (n * (n - 1) / 2));
    EXPECT_EQ(clink::fault::Registry::instance().hits(points::kBeforeSharedChunkRelease),
              static_cast<std::uint64_t>(kBatches));
    EXPECT_EQ(pool.bytes_allocated(), 0);
    EXPECT_EQ(pool.frees_elsewhere(), 0U) << "a reused array was freed off the task thread";
    EXPECT_GT(pool.frees_on_owner(), 0U);
    EXPECT_TRUE(ns_each_landed(*direct.server, 0, n));
    EXPECT_EQ(direct.server->rows(kNsTable), static_cast<std::size_t>(n));
#endif
}

// A chunk built without reuse never reaches the release path, and every chunk
// that does is let go of exactly once: a row batch, a declined columnar batch
// and an IPC-decoded one are not held, a builder-made one is.
TEST(NativeSinkRelease, OnlyAChunkThatReusesTheInputsArraysIsHandedToTheTaskThread) {
#if !defined(CLINK_FAULT_INJECTION)
    GTEST_SKIP() << "needs the fault points compiled in";
#else
    fake::FakeTable table = ns_table();
    table.columns = {{"id", "Int64", DefaultKind::None, 1}, {"v", "Int64", DefaultKind::None, 2}};
    table.columns[1].type = "Nullable(Int64)";
    NsDirect direct(table);
    direct.params["sql_column_types"] = "id:BIGINT;v:BIGINT";
    NativeSink& sink = direct.open();
    const clink::fault::ScopedFault observed(clink::fault::Rule{
        .point = points::kBeforeSharedChunkRelease,
        .ordinal = 0,
        .action = clink::fault::Action::Observe,
    });
    const auto hits = [] {
        return clink::fault::Registry::instance().hits(points::kBeforeSharedChunkRelease);
    };
    // Rows: built cell by cell.
    sink.on_data(ns_rows(0, 10));
    // Builder-made: reused.
    const auto built = ns_int_sidecar(10, 20);
    ASSERT_TRUE(sink.on_data_columnar(
        Batch<sql::Row>{built, static_cast<std::size_t>(10), sql::row_materialize_fn()}));
    // The same rows read back from an IPC frame: cell by cell.
    auto out = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(out, built->schema()).ValueOrDie();
    ASSERT_TRUE(writer->WriteRecordBatch(*ns_int_sidecar(20, 30)).ok());
    ASSERT_TRUE(writer->Close().ok());
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(
                      std::make_shared<arrow::io::BufferReader>(out->Finish().ValueOrDie()))
                      .ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> decoded;
    ASSERT_TRUE(reader->ReadNext(&decoded).ok());
    ASSERT_TRUE(sink.on_data_columnar(
        Batch<sql::Row>{decoded, static_cast<std::size_t>(10), sql::row_materialize_fn()}));
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(hits(), 1U);
    sink.flush();
    sink.close();
    EXPECT_EQ(hits(), 1U);
    EXPECT_TRUE(ns_each_landed(*direct.server, 0, 30));
#endif
}

// A sidecar of id (int64), s (utf8, "t<id>") and d (decimal128(9, 2),
// id + 0.25) rows [from, to), allocated from `pool`. With `unfit`, row `from`
// has an s that begins with the decimal sentinel and a d of ten digits, so
// neither column can be taken as it is.
std::shared_ptr<arrow::RecordBatch> ns_text_sidecar(std::int64_t from,
                                                    std::int64_t to,
                                                    arrow::MemoryPool* pool,
                                                    bool unfit = false) {
    arrow::Int64Builder times(pool);
    arrow::Int64Builder ids(pool);
    arrow::StringBuilder texts(pool);
    arrow::Decimal128Builder decimals(arrow::decimal128(9, 2), pool);
    for (std::int64_t id = from; id < to; ++id) {
        const bool odd = unfit && id == from;
        EXPECT_TRUE(times.AppendNull().ok());
        EXPECT_TRUE(ids.Append(id).ok());
        EXPECT_TRUE(
            texts.Append((odd ? std::string(1, '\x01') : std::string()) + "t" + std::to_string(id))
                .ok());
        EXPECT_TRUE(decimals.Append(arrow::Decimal128(odd ? 1'234'567'890 : id * 100 + 25)).ok());
    }
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("event_time", arrow::int64()),
                                                   arrow::field("id", arrow::int64()),
                                                   arrow::field("s", arrow::utf8()),
                                                   arrow::field("d", arrow::decimal128(9, 2))}),
                                    to - from,
                                    {times.Finish().ValueOrDie(),
                                     ids.Finish().ValueOrDie(),
                                     texts.Finish().ValueOrDie(),
                                     decimals.Finish().ValueOrDie()});
}

fake::FakeTable ns_text_table() {
    fake::FakeTable table = ns_table();
    table.columns = {{"id", "Int64", DefaultKind::None, 1},
                     {"s", "String", DefaultKind::None, 2},
                     {"d", "Nullable(Decimal(9, 2))", DefaultKind::None, 3}};
    return table;
}

// The same, for a chunk whose text and decimal columns are reused: the
// INSERT retains it, since its String column points into the chunk, and lets
// go of it once acknowledged. One INSERT a batch, so the writer lets go of
// each chunk while the next batch is still to come.
TEST(NativeSinkRelease, AChunkThatReusesTextAndDecimalsIsRetainedAndStillFreedOnTheTaskThread) {
#if !defined(CLINK_FAULT_INJECTION)
    GTEST_SKIP() << "needs the fault points compiled in";
#else
    fake::ThreadRecordingPool pool;
    NsDirect direct(ns_text_table());
    constexpr std::int64_t kBatches = 8;
    constexpr std::int64_t kRows = 40;
    direct.params["sql_column_types"] = "id:BIGINT;s:VARCHAR;d:DECIMAL(9, 2)";
    direct.params["batch_rows"] = std::to_string(kRows);
    NativeSink& sink = direct.open();
    const clink::fault::ScopedFault held(clink::fault::Rule{
        .point = points::kBeforeSharedChunkRelease,
        .ordinal = 0,
        .action = clink::fault::Action::Block,
    });
    std::size_t text_bytes = 0;
    for (std::int64_t b = 0; b < kBatches; ++b) {
        auto sidecar = ns_text_sidecar(b * kRows, (b + 1) * kRows, &pool);
        {
            const Batch<sql::Row> batch{
                sidecar, static_cast<std::size_t>(kRows), sql::row_materialize_fn()};
            ASSERT_TRUE(sink.on_data_columnar(batch));
        }
        // The INSERT has been acknowledged and the writer is parked at the
        // point with the chunk, so the drop below comes first.
        ASSERT_TRUE(ns_eventually([b] {
            return clink::fault::Registry::instance().hits(points::kBeforeSharedChunkRelease) ==
                   static_cast<std::uint64_t>(b + 1);
        }));
        const auto& texts = static_cast<const arrow::StringArray&>(*sidecar->column(2));
        text_bytes += static_cast<std::size_t>(texts.total_values_length());
        // The sidecar has no nulls, so these are the only buffers the three
        // reused columns keep: the id values, the text offsets and values,
        // and the decimal values.
        const auto capacity = [&](int column, int buffer) {
            return sidecar->column(column)->data()->buffers[buffer]->capacity();
        };
        for (int column = 1; column <= 3; ++column) {
            ASSERT_EQ(sidecar->column(column)->data()->buffers[0], nullptr);
        }
        const std::int64_t reused =
            capacity(1, 1) + capacity(2, 1) + capacity(2, 2) + capacity(3, 1);
        sidecar.reset();
        // The chunk still holds the three reused columns: without the text,
        // the pool would hold only the id and decimal buffers.
        EXPECT_GE(pool.bytes_allocated(), reused);
        EXPECT_EQ(pool.frees_elsewhere(), 0U);
        clink::fault::Registry::instance().release(points::kBeforeSharedChunkRelease);
    }
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink.flush();
    sink.close();
    const std::int64_t n = kBatches * kRows;
    EXPECT_GT(text_bytes, 0U);
    EXPECT_EQ(pool.bytes_allocated(), 0);
    EXPECT_EQ(pool.frees_elsewhere(), 0U) << "a reused array was freed off the task thread";
    EXPECT_GT(pool.frees_on_owner(), 0U);
    EXPECT_TRUE(ns_each_landed(*direct.server, 0, n));
    EXPECT_EQ(direct.server->inserts(kNsTable).size(), static_cast<std::size_t>(kBatches));
    for (const auto& block : direct.server->landed(kNsTable)) {
        for (const auto& row : block.values) {
            EXPECT_EQ(row.at(1), "t" + row.at(0));
            EXPECT_EQ(row.at(2), row.at(0) + ".25");
        }
    }
#endif
}

// A batch whose text holds a value that begins with the decimal sentinel, and
// whose decimal holds one past its precision, is converted cell by cell like
// any batch the intake does not reuse: the sentinel is stripped, the decimal
// is NULL, and the chunk shares nothing with the input, so it never reaches
// the release path. Its neighbour, without either value, does.
TEST(NativeSinkRelease, ATextOrDecimalThatCannotPassAsItIsIsCopiedAndNotHandedOver) {
#if !defined(CLINK_FAULT_INJECTION)
    GTEST_SKIP() << "needs the fault points compiled in";
#else
    fake::FakeTable table = ns_text_table();
    table.columns = {{"s", "String", DefaultKind::None, 1},
                     {"d", "Nullable(Decimal(9, 2))", DefaultKind::None, 2}};
    NsDirect direct(table);
    direct.params["sql_column_types"] = "s:VARCHAR;d:DECIMAL(9, 2)";
    NativeSink& sink = direct.open();
    const clink::fault::ScopedFault observed(clink::fault::Rule{
        .point = points::kBeforeSharedChunkRelease,
        .ordinal = 0,
        .action = clink::fault::Action::Observe,
    });
    const auto hits = [] {
        return clink::fault::Registry::instance().hits(points::kBeforeSharedChunkRelease);
    };
    const auto unfit = ns_text_sidecar(0, 10, arrow::default_memory_pool(), true);
    ASSERT_TRUE(sink.on_data_columnar(
        Batch<sql::Row>{unfit, static_cast<std::size_t>(10), sql::row_materialize_fn()}));
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    EXPECT_EQ(hits(), 0U);
    const auto fit = ns_text_sidecar(10, 20, arrow::default_memory_pool());
    ASSERT_TRUE(sink.on_data_columnar(
        Batch<sql::Row>{fit, static_cast<std::size_t>(10), sql::row_materialize_fn()}));
    sink.on_barrier(CheckpointBarrier{CheckpointId{2}});
    EXPECT_EQ(hits(), 1U);
    sink.flush();
    sink.close();
    EXPECT_EQ(hits(), 1U);
    std::map<std::string, std::string> landed;
    for (const auto& block : direct.server->landed(kNsTable)) {
        for (const auto& row : block.values) {
            landed[row.at(0)] = row.at(1);
        }
    }
    ASSERT_EQ(landed.size(), 20U);
    EXPECT_EQ(landed.at("t0"), "NULL");
    EXPECT_EQ(landed.at("t1"), "1.25");
    EXPECT_EQ(landed.at("t10"), "10.25");
    EXPECT_EQ(landed.at("t19"), "19.25");
#endif
}

// --- The at-least-once contract across a crash ---------------------------------------

// Drives the Sink calls directly, as a restarted subtask would see them: data,
// a barrier, more data, then the sink destroyed without either close, then a
// new sink fed the rows from that barrier on. SinkContractSuite is not used:
// it holds the two-phase contract and fails any weaker record.
TEST(NativeSinkContract, RowsBeforeTheBarrierLandAsOftenAsAcknowledgedAndNoRowIsLost) {
    NsRig rig;
    rig.params["batch_rows"] = "50";
    MetricsRegistry registry;
    RuntimeContext ctx(OperatorId{42}, "clickhouse_native_sink", nullptr, &registry);
    const std::int64_t since = ns_log_mark();
    {
        auto first = rig.build();
        first->attach_runtime(&ctx);
        first->open();
        first->on_data(ns_rows(0, 100));
        first->on_barrier(CheckpointBarrier{CheckpointId{1}});
        // The barrier returned, so every row before it is acknowledged.
        EXPECT_TRUE(ns_each_landed(*rig.server, 0, 100));
        // Fifty more fill an INSERT that lands; the last 25 are still in the
        // sink when it dies.
        first->on_data(ns_rows(100, 175));
        ASSERT_TRUE(ns_eventually([&rig] { return rig.server->rows(kNsTable) == 150; }));
        first->attach_runtime(nullptr);
    }
    const auto crashed = ns_logs_with(ns_logs_since(since), "clickhouse native sink cancelled:");
    ASSERT_EQ(crashed.size(), 1U);
    EXPECT_TRUE(ns_has(crashed.front().message, " abandoned_rows=25 ")) << crashed.front().message;

    // The restart replays from checkpoint 1.
    auto second = rig.build();
    second->attach_runtime(&ctx);
    second->open();
    second->on_data(ns_rows(100, 175));
    second->on_barrier(CheckpointBarrier{CheckpointId{2}});
    second->flush();
    second->close();
    second->attach_runtime(nullptr);

    const auto counts = ns_id_counts(*rig.server);
    for (std::int64_t id = 0; id < 175; ++id) {
        const auto it = counts.find(id);
        const std::size_t seen = it == counts.end() ? 0 : it->second;
        if (id < 100) {
            EXPECT_EQ(seen, 1U) << "id " << id << " was acknowledged once before the barrier";
        } else {
            EXPECT_GE(seen, 1U) << "id " << id << " was lost";
        }
    }
}

}  // namespace
}  // namespace clink::clickhouse::native
