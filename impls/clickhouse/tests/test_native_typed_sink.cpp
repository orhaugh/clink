// The typed helper (clink/clickhouse/native_sink.hpp) end to end: a
// CLINK_FIELDS struct written by make_clickhouse_native_sink<T> from a
// Dag-direct LocalExecutor, and by the factory register_clickhouse_native_sink<T>
// registers, against the fake server through the transport seam. The cases
// cover what lands, the barrier contract, the guarantee and the chain rule,
// the barrier modes and option refusals, the open report of the typed kind,
// and a batcher that cannot build a chunk. Every case ends by checking that
// no client was destroyed mid-INSERT, and puts the process-wide transport
// factory back.

#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/clickhouse/native_sink.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fake_transport.hpp"
#include "native/column_plan.hpp"
#include "native/errors.hpp"
#include "native/insert_transport.hpp"
#include "native/metrics.hpp"

// The struct every case writes: one field of each kind the column plan takes
// from a batcher, unsigned and optional included. CLINK_FIELDS specialises a
// clink template, so these live at namespace scope.
struct TsLeg {
    std::string venue;
    double px;
};
CLINK_FIELDS(TsLeg, venue, px);

struct TsTrade {
    std::int64_t id;
    std::uint32_t qty;
    double px;
    std::string venue;
    std::optional<std::uint64_t> volume;
    std::vector<std::uint8_t> flags;
    std::map<std::string, std::uint16_t> counts;
    TsLeg leg;
};
CLINK_FIELDS(TsTrade, id, qty, px, venue, volume, flags, counts, leg);

// Never registered as a channel type.
struct TsUnregistered {
    std::int64_t id;
};
CLINK_FIELDS(TsUnregistered, id);

namespace clink::clickhouse::native {
namespace {

using namespace std::chrono_literals;
namespace fake = clink::clickhouse::native::testing;

const std::string kTsTable = "trades";

TsTrade ts_trade(std::int64_t id) {
    TsTrade t;
    t.id = id;
    t.qty = static_cast<std::uint32_t>(id * 3);
    t.px = static_cast<double>(id) / 4.0;
    t.venue = "v" + std::to_string(id % 7);
    if (id % 3 != 0) {
        t.volume = (1ULL << 63) + static_cast<std::uint64_t>(id);
    }
    t.flags = {static_cast<std::uint8_t>(id % 256), 255};
    t.counts = {{"a", static_cast<std::uint16_t>(id % 65536)}};
    t.leg = TsLeg{"L" + std::to_string(id), -static_cast<double>(id) / 2.0};
    return t;
}

Batch<TsTrade> ts_rows(std::int64_t from, std::int64_t to) {
    Batch<TsTrade> batch;
    for (std::int64_t id = from; id < to; ++id) {
        batch.emplace(ts_trade(id));
    }
    return batch;
}

std::string ts_float(double v) {
    std::array<char, 64> buf{};
    const auto res = std::to_chars(buf.data(), buf.data() + buf.size(), v);
    return std::string(buf.data(), res.ptr);
}

// A row as the fake renders it (fake_transport.hpp): text at the top level as
// its bytes, inside a composite quoted.
std::vector<std::string> ts_rendered(const TsTrade& t) {
    return {std::to_string(t.id),
            std::to_string(t.qty),
            ts_float(t.px),
            t.venue,
            t.volume ? std::to_string(*t.volume) : std::string("NULL"),
            "[" + std::to_string(t.flags.at(0)) + "," + std::to_string(t.flags.at(1)) + "]",
            "{'a':" + std::to_string(t.counts.at("a")) + "}",
            "('" + t.leg.venue + "'," + ts_float(t.leg.px) + ")"};
}

fake::FakeTable ts_table() {
    fake::FakeTable t;
    t.database = "db";
    t.name = kTsTable;
    t.engine = "MergeTree";
    t.engine_full = "MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100";
    t.dedup_window = 100;
    const std::vector<std::pair<std::string, std::string>> columns = {
        {"id", "Int64"},
        {"qty", "UInt32"},
        {"px", "Float64"},
        {"venue", "String"},
        {"volume", "Nullable(UInt64)"},
        {"flags", "Array(UInt8)"},
        {"counts", "Map(String, UInt16)"},
        {"leg", "Tuple(venue String, px Float64)"}};
    std::uint32_t position = 1;
    for (const auto& [name, type] : columns) {
        t.columns.push_back(TargetColumn{name, type, DefaultKind::None, position++});
    }
    return t;
}

// One step of the source's script: rows [from, to), or a barrier.
struct TsStep {
    std::int64_t from{0};
    std::int64_t to{0};
    std::uint64_t checkpoint{0};  // a barrier when set
};

TsStep ts_data(std::int64_t from, std::int64_t to) {
    return TsStep{from, to, 0};
}

TsStep ts_barrier(std::uint64_t checkpoint) {
    return TsStep{0, 0, checkpoint};
}

class TsSource final : public Source<TsTrade> {
public:
    explicit TsSource(std::vector<TsStep> steps) : steps_(std::move(steps)) {}

    bool produce(Emitter<TsTrade>& out) override {
        if (this->cancelled() || next_ >= steps_.size()) {
            return false;
        }
        const TsStep& step = steps_[next_++];
        if (step.checkpoint != 0) {
            return out.emit_barrier(CheckpointBarrier{CheckpointId{step.checkpoint}});
        }
        return out.emit_data(ts_rows(step.from, step.to));
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "typed_sink_test.source"; }

private:
    std::vector<TsStep> steps_;
    std::size_t next_{0};
};

class TsDiscardSink final : public Sink<TsTrade> {
public:
    void on_data(const Batch<TsTrade>& /*batch*/) override {}
    [[nodiscard]] std::string name() const override { return "typed_sink_test.discard"; }
};

using TsConfigure = std::function<void(JobConfig&)>;

// One fake server, the process-wide factory pointed at it, and a job of the
// scripted source into a typed sink.
struct TsRig {
    TsRig() : server(std::make_shared<fake::FakeServer>()) {
        server->add_table(ts_table());
        set_transport_factory_for_testing(fake::fake_factory(server));
        params = {{"database", "db"}, {"table", kTsTable}, {"batch_interval_ms", "3600000"}};
        config.metrics = &metrics;
        config.on_checkpoint_ack = [this](CheckpointId id, bool ok, const std::string& /*why*/) {
            const std::uint64_t landed = server->rows(kTsTable);
            const std::lock_guard<std::mutex> lock(acks_mu);
            acks.emplace_back(id.value(), ok);
            rows_at_ack.push_back(landed);
        };
    }

    ~TsRig() {
        exec.reset();
        sink.reset();
        EXPECT_EQ(server->destroyed_mid_insert(), 0U) << "a client was destroyed mid-INSERT";
        set_transport_factory_for_testing(nullptr);
    }

    TsRig(const TsRig&) = delete;
    TsRig& operator=(const TsRig&) = delete;
    TsRig(TsRig&&) = delete;
    TsRig& operator=(TsRig&&) = delete;

    void start(std::vector<TsStep> steps,
               std::shared_ptr<Sink<TsTrade>> given = nullptr,
               const TsConfigure& configure = {}) {
        sink = given ? std::move(given) : make_clickhouse_native_sink<TsTrade>(params);
        Dag dag;
        auto h = dag.add_source<TsTrade>(std::make_shared<TsSource>(std::move(steps)));
        dag.add_sink<TsTrade>(h, sink);
        JobConfig job = config;
        if (configure) {
            configure(job);
        }
        exec = std::make_unique<LocalExecutor>(std::move(dag), std::move(job));
        exec->start();
        exec->await_termination();
    }

    [[nodiscard]] std::vector<std::string> errors() const {
        std::vector<std::string> out;
        for (const auto& [op, message] : exec->operator_errors()) {
            out.push_back(message);
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

    std::shared_ptr<fake::FakeServer> server;
    std::map<std::string, std::string> params;
    MetricsRegistry metrics;
    JobConfig config;
    std::mutex acks_mu;
    std::vector<std::pair<std::uint64_t, bool>> acks;
    std::vector<std::uint64_t> rows_at_ack;
    std::shared_ptr<Sink<TsTrade>> sink;
    std::unique_ptr<LocalExecutor> exec;
};

bool ts_has(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

std::int64_t ts_log_mark() {
    const std::int64_t mark = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    std::this_thread::sleep_for(3ms);
    return mark;
}

std::vector<std::string> ts_logs_with(std::int64_t since, const std::string& text) {
    std::vector<std::string> out;
    for (const auto& r : LogBuffer::global().tail(1024, "", since, "sink.clickhouse")) {
        if (ts_has(r.message, text)) {
            out.push_back(r.message);
        }
    }
    return out;
}

std::uint64_t ts_global_refusals(const std::string& code) {
    return MetricsRegistry::global()
        .counter(std::string(metric::kRefusalsTotal) + "{reason=\"" + code + "\"}")
        .value();
}

// The refusal `params` meets when the sink is made, or nullopt if it is made.
std::optional<NativeSinkError> ts_refusal(const std::map<std::string, std::string>& params) {
    try {
        (void)make_clickhouse_native_sink<TsTrade>(params);
    } catch (const NativeSinkError& e) {
        return e;
    }
    return std::nullopt;
}

// --- A clean run --------------------------------------------------------------

TEST(NativeTypedSinkRun, EveryRowLandsOnceAsTheStructHeldItAndEachBarrierIsAcknowledgedAfterIt) {
    TsRig rig;
    rig.start(
        {ts_data(0, 100), ts_barrier(1), ts_data(100, 250), ts_barrier(2), ts_data(250, 300)});
    EXPECT_TRUE(rig.errors().empty());
    EXPECT_EQ(rig.server->rows(kTsTable), 300U);
    EXPECT_EQ(rig.acknowledged(), (std::vector<std::uint64_t>{1, 2}));
    {
        const std::lock_guard<std::mutex> lock(rig.acks_mu);
        EXPECT_EQ(rig.rows_at_ack, (std::vector<std::uint64_t>{100, 250}));
    }
    // Every row once, and every value as the struct held it: the unsigned
    // values past the signed range, the nulls, the list, the map and the
    // nested struct included. event_time never reached the table.
    std::map<std::int64_t, std::vector<std::string>> landed;
    for (const auto& block : rig.server->landed(kTsTable)) {
        for (const auto& row : block.values) {
            ASSERT_EQ(row.size(), 8U);
            EXPECT_TRUE(landed.emplace(std::stoll(row.at(0)), row).second)
                << "row " << row.at(0) << " landed twice";
        }
    }
    ASSERT_EQ(landed.size(), 300U);
    for (const auto& [id, row] : landed) {
        EXPECT_EQ(row, ts_rendered(ts_trade(id))) << "row " << id;
    }
    // One INSERT per barrier and one for the end of input, each named for the
    // sink in its log_comment.
    EXPECT_EQ(rig.server->inserts(kTsTable).size(), 3U);
    std::size_t tagged = 0;
    for (const auto& sql : rig.server->statements()) {
        tagged += ts_has(sql, "log_comment='clink:clickhouse_native_sink:sub0:") ? 1 : 0;
    }
    EXPECT_EQ(tagged, 3U);
}

// --- The guarantee, the chain rule and the barrier modes ---------------------

TEST(NativeTypedSinkGuarantee, TheSinkGatesTheAckAndTakesNoColumnarHook) {
    const TsRig rig;
    const auto sink = make_clickhouse_native_sink<TsTrade>(rig.params);
    EXPECT_TRUE(sink->gates_checkpoint_ack());
    EXPECT_FALSE(sink->supports_columnar());
    EXPECT_EQ(sink->name(), "clickhouse_native_sink");
    EXPECT_EQ(rig.server->connects(), 0U) << "making the sink connected";
}

TEST(NativeTypedSinkGuarantee, TheSinkMustBeTheOnlySinkOnItsChainWhicheverIsAddedFirst) {
    const TsRig rig;
    for (const bool native_first : {true, false}) {
        SCOPED_TRACE(native_first ? "native sink first" : "native sink second");
        Dag dag;
        auto h = dag.add_source<TsTrade>(std::make_shared<TsSource>(std::vector<TsStep>{}));
        auto native = make_clickhouse_native_sink<TsTrade>(rig.params);
        std::shared_ptr<Sink<TsTrade>> other = std::make_shared<TsDiscardSink>();
        dag.add_sink<TsTrade>(h, native_first ? native : other);
        try {
            dag.add_sink<TsTrade>(h, native_first ? other : native);
            ADD_FAILURE() << "a second sink shared the native sink's chain";
        } catch (const std::logic_error& e) {
            EXPECT_TRUE(
                ts_has(e.what(), "clickhouse_native_sink must be the only sink on its chain"))
                << e.what();
        }
    }
}

// Every barrier mode is accepted, as on the SQL path: a fan-in upstream aligns
// every barrier whatever its stamp.
TEST(NativeTypedSinkGuarantee, UnalignedOrAdaptiveCheckpointsCommitAsAlignedOnes) {
    const std::vector<std::pair<std::string, TsConfigure>> modes = {
        {"unaligned", [](JobConfig& c) { c.unaligned_checkpoints = true; }},
        {"adaptive", [](JobConfig& c) { c.adaptive_barrier_mode = true; }},
    };
    for (const auto& [label, configure] : modes) {
        SCOPED_TRACE(label);
        TsRig rig;
        rig.start({ts_data(0, 10), ts_barrier(1)}, nullptr, configure);
        EXPECT_TRUE(rig.errors().empty());
        EXPECT_EQ(rig.acknowledged(), std::vector<std::uint64_t>{1});
        EXPECT_GE(rig.server->connects(), 1U);
    }
}

TEST(NativeTypedSinkOptions, AnOptionThatWouldChangeTheGuaranteeIsRefusedWhenTheSinkIsMade) {
    const TsRig rig;
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"delivery_guarantee", "exactly_once"}, {"mode", "upsert"}, {"changelog", "true"}};
    for (const auto& [key, value] : cases) {
        SCOPED_TRACE(key + "=" + value);
        const std::uint64_t before = ts_global_refusals(code::kDeliveryUnsupported);
        auto params = rig.params;
        params[key] = value;
        const auto refused = ts_refusal(params);
        ASSERT_TRUE(refused);
        EXPECT_EQ(refused->code(), code::kDeliveryUnsupported) << refused->what();
        EXPECT_EQ(ts_global_refusals(code::kDeliveryUnsupported), before + 1);
    }
    EXPECT_EQ(rig.server->connects(), 0U);
}

TEST(NativeTypedSinkOptions, SqlColumnTypesAndAnUnknownKeyAreRefusedWhenTheSinkIsMade) {
    const TsRig rig;
    auto typed = rig.params;
    typed["sql_column_types"] = "id:BIGINT";
    const auto declared = ts_refusal(typed);
    ASSERT_TRUE(declared);
    EXPECT_EQ(declared->code(), code::kOptionInvalid);
    EXPECT_TRUE(ts_has(declared->what(),
                       "the typed sink takes its column types from the ArrowBatcher schema; "
                       "remove sql_column_types"))
        << declared->what();

    auto unknown = rig.params;
    unknown["tls_server_name"] = "ch-1";
    const std::uint64_t before = ts_global_refusals(code::kUnknownOption);
    const auto misspelt = ts_refusal(unknown);
    ASSERT_TRUE(misspelt);
    EXPECT_EQ(misspelt->code(), code::kUnknownOption);
    EXPECT_TRUE(ts_has(misspelt->what(), "unknown option 'tls_server_name'")) << misspelt->what();
    EXPECT_EQ(ts_global_refusals(code::kUnknownOption), before + 1);
    EXPECT_EQ(rig.server->connects(), 0U);
}

TEST(NativeTypedSinkOptions, ABatcherWithoutASchemaOrABuildIsRefusedWhenTheSinkIsMade) {
    const TsRig rig;
    ArrowBatcher<TsTrade> no_schema = make_columnar_arrow_batcher<TsTrade>();
    no_schema.schema = {};
    EXPECT_THROW((void)make_clickhouse_native_sink<TsTrade>(no_schema, rig.params),
                 std::invalid_argument);
    ArrowBatcher<TsTrade> no_build = make_columnar_arrow_batcher<TsTrade>();
    no_build.build = {};
    EXPECT_THROW((void)make_clickhouse_native_sink<TsTrade>(no_build, rig.params),
                 std::invalid_argument);
}

// --- The factory --------------------------------------------------------------

// Registered once per process, as a job's register function would.
const std::string& ts_registered_op_type() {
    static const std::string op_type = [] {
        plugin::PluginRegistry reg;
        reg.register_type<TsTrade>();
        register_clickhouse_native_sink<TsTrade>(reg);
        return "clickhouse_native_sink_" +
               reg.type_registry().channel_for_typeid(typeid(TsTrade).name());
    }();
    return op_type;
}

std::shared_ptr<Sink<TsTrade>> ts_build(const std::map<std::string, std::string>& params,
                                        std::uint32_t subtask,
                                        std::uint32_t parallelism) {
    const auto* factory =
        cluster::OperatorRegistry::default_instance().find_sink(ts_registered_op_type(), "TsTrade");
    if (factory == nullptr) {
        throw std::logic_error(ts_registered_op_type() + " is not registered");
    }
    cluster::OperatorBuildContext ctx;
    ctx.params = params;
    ctx.subtask_idx = subtask;
    ctx.parallelism = parallelism;
    return std::static_pointer_cast<Sink<TsTrade>>(factory->build(ctx));
}

TEST(NativeTypedSinkFactory, TheRegistrationIsPrefixFirstOnTheStructsOwnChannel) {
    EXPECT_EQ(ts_registered_op_type(), "clickhouse_native_sink_TsTrade");
    // The registration helper leaves the type to the caller.
    plugin::PluginRegistry reg;
    try {
        register_clickhouse_native_sink<TsUnregistered>(reg);
        ADD_FAILURE() << "registered a sink for a type with no channel";
    } catch (const std::runtime_error& e) {
        EXPECT_TRUE(ts_has(e.what(), "call register_type<T>() first")) << e.what();
    }
    // An explicit name is taken as given.
    EXPECT_NO_THROW(register_clickhouse_native_sink<TsTrade>(reg, "clickhouse_native_sink_trades"));
    EXPECT_NE(cluster::OperatorRegistry::default_instance().find_sink(
                  "clickhouse_native_sink_trades", "TsTrade"),
              nullptr);
}

TEST(NativeTypedSinkFactory, SubtaskZeroReportsTheTypedOpenTheArrowColumnPlanAndTheOptions) {
    TsRig rig;
    const std::int64_t since = ts_log_mark();
    rig.start({ts_data(0, 10)}, ts_build(rig.params, 0, 1));
    EXPECT_TRUE(rig.errors().empty());
    EXPECT_EQ(rig.server->rows(kTsTable), 10U);

    const auto opens = ts_logs_with(since, "clickhouse native sink open:");
    ASSERT_EQ(opens.size(), 1U);
    EXPECT_TRUE(ts_has(opens.front(),
                       "subtask=0/1 factory=make_clickhouse_native_sink input=typed mode=append "
                       "delivery=at_least_once "))
        << opens.front();
    EXPECT_TRUE(ts_has(opens.front(), " columns=8 omitted=0 ")) << opens.front();

    const auto plan = ts_logs_with(since, "clickhouse native sink column plan:");
    ASSERT_EQ(plan.size(), 1U);
    for (const char* line : {"`id`: arrow=int64 target=Int64 conversion=copy",
                             "`qty`: arrow=uint32 target=UInt32 conversion=copy",
                             "`volume`: arrow=uint64 target=Nullable(UInt64)"}) {
        EXPECT_TRUE(ts_has(plan.front(), line)) << "missing '" << line << "' in: " << plan.front();
    }
    EXPECT_FALSE(ts_has(plan.front(), "sql=")) << plan.front();

    const auto described = ts_logs_with(since, "clickhouse native sink options:");
    ASSERT_EQ(described.size(), 1U);
    EXPECT_TRUE(ts_has(described.front(), "\n  columns=8 from the batcher schema"))
        << described.front();
    EXPECT_FALSE(ts_has(described.front(), "sql_column_types")) << described.front();
}

TEST(NativeTypedSinkFactory, ASecondSubtaskSaysWhichItIsAndLeavesThePlanToSubtaskZero) {
    TsRig rig;
    const std::int64_t since = ts_log_mark();
    rig.start({ts_data(0, 10)}, ts_build(rig.params, 1, 2));
    EXPECT_TRUE(rig.errors().empty());
    const auto opens = ts_logs_with(since, "clickhouse native sink open: subtask=1/2 ");
    ASSERT_EQ(opens.size(), 1U);
    EXPECT_TRUE(ts_has(opens.front(), " factory=make_clickhouse_native_sink input=typed "))
        << opens.front();
    EXPECT_TRUE(ts_logs_with(since, "column plan:").empty());
    EXPECT_TRUE(ts_logs_with(since, "clickhouse native sink options:").empty());
    bool sub1 = false;
    for (const auto& sql : rig.server->statements()) {
        sub1 = sub1 || ts_has(sql, ":sub1:1'");
    }
    EXPECT_TRUE(sub1);
}

// --- A batcher that cannot build a chunk --------------------------------------

TEST(NativeTypedSinkBatcher, ABatcherThatBuildsNoChunkFailsTheTaskAndLandsNothing) {
    TsRig rig;
    ArrowBatcher<TsTrade> broken = make_columnar_arrow_batcher<TsTrade>();
    broken.build = [](const Batch<TsTrade>& /*batch*/) -> std::shared_ptr<arrow::RecordBatch> {
        return nullptr;
    };
    const std::int64_t since = ts_log_mark();
    rig.start({ts_data(0, 10), ts_barrier(1)},
              make_clickhouse_native_sink<TsTrade>(std::move(broken), rig.params));
    const auto errors = rig.errors();
    ASSERT_EQ(errors.size(), 1U);
    EXPECT_TRUE(
        ts_has(errors.front(), "clickhouse native sink: the ArrowBatcher could not build a chunk"))
        << errors.front();
    EXPECT_TRUE(rig.acknowledged().empty());
    EXPECT_EQ(rig.server->rows(kTsTable), 0U);
    EXPECT_TRUE(rig.server->inserts(kTsTable).empty());
    // The writer was stopped there and then, which logs its summary.
    EXPECT_EQ(ts_logs_with(since, "clickhouse native sink cancelled:").size(), 1U);
}

// A batcher whose build throws fails the task with its own error, and the
// sink stops the writer before the error leaves on_data, as for a null chunk:
// the runner skips both closes once on_data has thrown.
TEST(NativeTypedSinkBatcher, ABatcherWhoseBuildThrowsStopsTheWriterAndLandsNothing) {
    TsRig rig;
    ArrowBatcher<TsTrade> throwing = make_columnar_arrow_batcher<TsTrade>();
    throwing.build = [](const Batch<TsTrade>& /*batch*/) -> std::shared_ptr<arrow::RecordBatch> {
        throw std::runtime_error("typed_sink_test: the batcher refused the batch");
    };
    const std::int64_t since = ts_log_mark();
    rig.start({ts_data(0, 10), ts_barrier(1)},
              make_clickhouse_native_sink<TsTrade>(std::move(throwing), rig.params));
    const auto errors = rig.errors();
    ASSERT_EQ(errors.size(), 1U);
    EXPECT_TRUE(ts_has(errors.front(), "typed_sink_test: the batcher refused the batch"))
        << errors.front();
    // Stopped by the sink itself: the rig still holds the sink, so the
    // summary cannot come from its destructor.
    EXPECT_EQ(ts_logs_with(since, "clickhouse native sink cancelled:").size(), 1U);
    EXPECT_TRUE(rig.acknowledged().empty());
    EXPECT_EQ(rig.server->rows(kTsTable), 0U);
    EXPECT_TRUE(rig.server->inserts(kTsTable).empty());
}

}  // namespace
}  // namespace clink::clickhouse::native
