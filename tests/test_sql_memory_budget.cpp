#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/dag_builder_registry.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/partitioned_list.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_output.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/sql/spill_heap.hpp"
#include "clink/sql/spill_store.hpp"
#include "clink/sql/state_ttl.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {
using namespace clink;
using namespace clink::sql;

std::shared_ptr<Operator<Row, Row>> make_operator(const std::string& type,
                                                  const std::string& fn = "sum",
                                                  bool ttl = false,
                                                  const std::string& spill_dir = {},
                                                  bool changelog = false,
                                                  bool distinct = false) {
    static const bool installed = [] {
        cluster::ensure_built_ins_registered();
        plugin::PluginRegistry registry;
        sql::install(registry);
        return true;
    }();
    (void)installed;
    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        type, std::string{kChannelRow}, std::string{kChannelRow});
    if (!factory)
        throw std::runtime_error("missing test factory");
    cluster::OperatorBuildContext context;
    context.params["group_keys"] = "k";
    context.params["spill_dir"] = spill_dir;
    context.params["emit_changelog"] = changelog ? "true" : "false";
    context.params["aggregates"] =
        "[{\"name\":\"s\",\"fn\":\"" + fn +
        "\",\"input_column\":\"v\",\"distinct\":" + (distinct ? "true" : "false") + "}]";
    context.params["time_column"] = "ts";
    context.params["size_ms"] = "1000";
    if (ttl) {
        context.params["state_ttl_ms"] = "100";
        context.params["state_ttl_domain"] = "event_time";
    }
    auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(context));
    op->set_uid("memory-test-aggregate");
    return op;
}

void feed(Operator<Row, Row>& op, Emitter<Row>& out, int key, config::JsonValue value, int ts = 0) {
    Row row;
    row.values["k"] = config::JsonValue{key};
    row.values["v"] = std::move(value);
    row.values["ts"] = config::JsonValue{ts};
    Batch<Row> batch;
    batch.emplace(std::move(row), EventTime::from_millis(ts));
    op.process(StreamElement<Row>::data(std::move(batch)), out);
}

TEST(SqlMemoryBudget, HighCardinalityAggregateFailsWithinConfiguredAccountedLimit) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    RuntimeContext context(
        operator_id_from_uid("memory-test-aggregate"), "aggregate", nullptr, nullptr);
    context.set_memory_budget(budget);
    {
        auto op = make_operator("aggregate_row");
        op->attach_runtime(&context);
        op->open();
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        bool refused = false;
        for (int i = 0; i < 1000; ++i) {
            try {
                feed(*op, out, i, config::JsonValue{1});
            } catch (const MemoryLimitExceeded&) {
                refused = true;
                break;
            }
        }
        EXPECT_TRUE(refused);
        EXPECT_LE(budget->usage().peak, budget->limit());
        op->close();
        op->attach_runtime(nullptr);
    }
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(SqlMemoryBudget, OneGrowingArrayAggregateConsumesBudget) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    RuntimeContext context(
        operator_id_from_uid("memory-test-aggregate"), "aggregate", nullptr, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "array_agg");
    op->attach_runtime(&context);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    bool refused = false;
    for (int i = 0; i < 100; ++i) {
        try {
            feed(*op, out, 1, config::JsonValue{std::string(200, 'x')});
        } catch (const MemoryLimitExceeded&) {
            refused = true;
            break;
        }
    }
    EXPECT_TRUE(refused);
    op->close();
    op->attach_runtime(nullptr);
}

TEST(SqlMemoryBudget, WindowExpiryReleasesPaneMemory) {
    auto budget = std::make_shared<MemoryBudget>(1024 * 1024);
    RuntimeContext context(
        operator_id_from_uid("memory-test-aggregate"), "window", nullptr, nullptr);
    context.set_memory_budget(budget);
    {
        auto op = make_operator("tumbling_window_row");
        op->attach_runtime(&context);
        op->open();
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        for (int i = 0; i < 50; ++i)
            feed(*op, out, i, config::JsonValue{1});
        const auto retained = budget->usage().used;
        EXPECT_GT(retained, 0u);
        op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(1000)}), out);
        EXPECT_LT(budget->usage().used, retained);
        op->close();
        op->attach_runtime(nullptr);
    }
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(SqlMemoryBudget, ColumnarAggregateAndWindowStateEnforceBudget) {
    arrow::Int64Builder keys, values, times;
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(keys.Append(i).ok());
        ASSERT_TRUE(values.Append(1).ok());
        ASSERT_TRUE(times.Append(0).ok());
    }
    std::shared_ptr<arrow::Array> k, v, ts;
    ASSERT_TRUE(keys.Finish(&k).ok());
    ASSERT_TRUE(values.Finish(&v).ok());
    ASSERT_TRUE(times.Finish(&ts).ok());
    const auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("k", arrow::int64()),
                                                               arrow::field("v", arrow::int64()),
                                                               arrow::field("ts", arrow::int64())}),
                                                100,
                                                {k, v, ts});
    for (const auto* type : {"aggregate_row", "tumbling_window_row"}) {
        auto budget = std::make_shared<MemoryBudget>(1024);
        RuntimeContext context(
            operator_id_from_uid("memory-test-aggregate"), type, nullptr, nullptr);
        context.set_memory_budget(budget);
        auto op = make_operator(type);
        op->attach_runtime(&context);
        op->open();
        ASSERT_TRUE(op->supports_columnar());
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        auto element = StreamElement<Row>::data(columnar_row_batch(batch));
        EXPECT_THROW(op->process_columnar(element, out), MemoryLimitExceeded) << type;
        op->close();
        op->attach_runtime(nullptr);
    }
}

TEST(SqlMemoryBudget, AggregateTtlReleasesExpiredGroups) {
    auto budget = std::make_shared<MemoryBudget>(1024 * 1024);
    RuntimeContext context(
        operator_id_from_uid("memory-test-aggregate"), "aggregate", nullptr, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "sum", true);
    op->attach_runtime(&context);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(1)}), out);
    for (int i = 0; i < 20; ++i)
        feed(*op, out, i, config::JsonValue{1}, 1);
    EXPECT_GT(budget->usage().used, 0u);
    op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(1000)}), out);
    EXPECT_EQ(budget->usage().used, 0u);
    op->close();
    op->attach_runtime(nullptr);
}

TEST(SqlMemoryBudget, FailedAggregateDestructionRemovesServingCallbacks) {
    auto budget = std::make_shared<MemoryBudget>(512);
    RuntimeContext context(
        operator_id_from_uid("memory-test-aggregate"), "aggregate", nullptr, nullptr);
    context.set_memory_budget(budget);
    context.set_runner_identity("memory-failure", 0);
    const auto slot = queryable_state::compose_subtask_slot("memory-failure", 0, "agg");
    auto& registry = queryable_state::Registry::global();
    {
        auto op = make_operator("aggregate_row", "array_agg");
        op->attach_runtime(&context);
        op->open();
        ASSERT_TRUE(registry.has_json_slot(slot));
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        EXPECT_THROW(feed(*op, out, 1, config::JsonValue{std::string(2048, 'x')}),
                     MemoryLimitExceeded);
        // Deliberately omit close(), as happens when the runner unwinds.
    }
    EXPECT_FALSE(registry.has_json_slot(slot));
    EXPECT_FALSE(registry.has_json_scan(slot));
}

TEST(SqlMemoryBudget, RestoreAccountsRecoveredAggregateState) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    const auto id = operator_id_from_uid("memory-test-aggregate");
    {
        RuntimeContext context(id, "aggregate", backend.get(), nullptr);
        auto op = make_operator("aggregate_row");
        op->attach_runtime(&context);
        op->open();
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        for (int i = 0; i < 20; ++i)
            feed(*op, out, i, config::JsonValue{1});
        op->snapshot_timers(*backend, id);
        op->close();
        op->attach_runtime(nullptr);
    }
    auto budget = std::make_shared<MemoryBudget>(512);
    RuntimeContext restored(id, "aggregate", backend.get(), nullptr);
    restored.set_memory_budget(budget);
    {
        auto op = make_operator("aggregate_row");
        op->attach_runtime(&restored);
        EXPECT_THROW(op->open(), MemoryLimitExceeded);
        op->attach_runtime(nullptr);
    }
    EXPECT_EQ(budget->usage().used, 0u);
}

struct SpillDirectory {
    std::filesystem::path path;
    SpillDirectory() {
        auto pattern =
            (std::filesystem::temp_directory_path() / "clink-spill-test-XXXXXX").string();
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error("test spill directory");
        path = pattern;
    }
    ~SpillDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
    std::size_t files() const {
        std::size_t count = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(path))
            count += entry.is_regular_file();
        return count;
    }
};

TEST(SqlMemoryBudget, SpillPreservesChangelogAndRetractionsAcrossAggregateFamilies) {
    for (const auto* fn : {"sum", "count", "min", "max", "array_agg", "string_agg"}) {
        SCOPED_TRACE(fn);
        SpillDirectory dir;
        auto budget = std::make_shared<MemoryBudget>(8192);
        std::vector<std::string> baseline, spilled;
        for (bool use_spill : {false, true}) {
            RuntimeContext context(
                operator_id_from_uid("memory-test-aggregate"), "agg", nullptr, nullptr);
            if (use_spill)
                context.set_memory_budget(budget);
            auto op = make_operator(
                "aggregate_row", fn, false, use_spill ? dir.path.string() : "", true, true);
            op->attach_runtime(&context);
            op->open();
            auto& outputs = use_spill ? spilled : baseline;
            Emitter<Row> out([&](StreamElement<Row> e) {
                for (const auto& rec : e.as_data())
                    outputs.push_back(
                        config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                return true;
            });
            for (int round = 0; round < 4; ++round) {
                for (int key = 0; key < 80; ++key) {
                    Row row;
                    row.values["k"] = config::JsonValue{key};
                    row.values["v"] = config::JsonValue{round == 3 ? 4 : 1};
                    set_row_kind(row, round == 2 ? kRowKindDelete : kRowKindInsert);
                    Batch<Row> batch;
                    batch.emplace(std::move(row));
                    op->process(StreamElement<Row>::data(std::move(batch)), out);
                }
            }
            if (use_spill) {
                EXPECT_GE(dir.files(), 80u);
                EXPECT_LE(budget->usage().peak, budget->limit());
                EXPECT_EQ(budget->usage().used, 0u);
            }
        }
        EXPECT_EQ(spilled, baseline);
        EXPECT_TRUE(std::filesystem::is_empty(dir.path));
    }
}

TEST(SqlMemoryBudget, SpillColumnarAggregateRevisitsGroupsWithoutLosingTotals) {
    SpillDirectory dir;
    auto budget = std::make_shared<MemoryBudget>(8192);
    RuntimeContext context(operator_id_from_uid("memory-test-aggregate"), "agg", nullptr, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "sum", false, dir.path.string());
    op->attach_runtime(&context);
    op->open();
    std::map<int, double> sums;
    Emitter<Row> out([&](StreamElement<Row> e) {
        for (const auto& rec : e.as_data())
            sums[static_cast<int>(rec.value().values.at("k").as_int())] =
                rec.value().values.at("s").as_number();
        return true;
    });
    arrow::Int64Builder keys, values;
    for (int key = 0; key < 100; ++key) {
        ASSERT_TRUE(keys.Append(key).ok());
        ASSERT_TRUE(values.Append(2).ok());
    }
    std::shared_ptr<arrow::Array> k, v;
    ASSERT_TRUE(keys.Finish(&k).ok());
    ASSERT_TRUE(values.Finish(&v).ok());
    auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("k", arrow::int64()), arrow::field("v", arrow::int64())}),
        100,
        {k, v});
    for (int round = 0; round < 3; ++round)
        EXPECT_TRUE(op->process_columnar(StreamElement<Row>::data(columnar_row_batch(batch)), out));
    EXPECT_GE(dir.files(), 100u);
    ASSERT_EQ(sums.size(), 100u);
    for (auto [key, sum] : sums)
        EXPECT_DOUBLE_EQ(sum, 6);
}

TEST(SqlMemoryBudget, SpilledGroupsSurviveCanonicalSnapshotAndFreshSpillDirectory) {
    SpillDirectory dir;
    const auto id = operator_id_from_uid("memory-test-aggregate");
    InMemoryStateBackend backend;
    auto budget = std::make_shared<MemoryBudget>(8192);
    {
        RuntimeContext context(id, "agg", &backend, nullptr);
        context.set_memory_budget(budget);
        auto op = make_operator("aggregate_row", "sum", false, dir.path.string());
        op->attach_runtime(&context);
        op->open();
        Emitter<Row> out([](StreamElement<Row>) { return true; });
        for (int key = 0; key < 100; ++key)
            feed(*op, out, key, config::JsonValue{3});
        op->snapshot_timers(backend, id);
    }
    EXPECT_TRUE(std::filesystem::is_empty(dir.path));
    auto snapshot = backend.snapshot(CheckpointId{1});
    InMemoryStateBackend restored;
    restored.restore(snapshot);
    RuntimeContext context(id, "agg", &restored, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "sum", false, dir.path.string());
    op->attach_runtime(&context);
    op->open();
    EXPECT_GE(dir.files(), 100u);
    int emitted = 0;
    Emitter<Row> out([&](StreamElement<Row> e) {
        for (const auto& rec : e.as_data()) {
            EXPECT_DOUBLE_EQ(rec.value().values.at("s").as_number(), 8);
            ++emitted;
        }
        return true;
    });
    for (int key = 0; key < 100; ++key)
        feed(*op, out, key, config::JsonValue{5});
    EXPECT_EQ(emitted, 100);
}

TEST(SqlMemoryBudget, SpillStillRefusesOneOversizedGroup) {
    SpillDirectory dir;
    auto budget = std::make_shared<MemoryBudget>(4096);
    RuntimeContext context(operator_id_from_uid("memory-test-aggregate"), "agg", nullptr, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "array_agg", false, dir.path.string());
    op->attach_runtime(&context);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    EXPECT_THROW(feed(*op, out, 1, config::JsonValue{std::string(8192, 'x')}), MemoryLimitExceeded);
    EXPECT_LE(budget->usage().peak, budget->limit());
    op.reset();
    EXPECT_EQ(budget->usage().used, 0u);
    EXPECT_TRUE(std::filesystem::is_empty(dir.path));
}

TEST(SqlMemoryBudget, QueryableLookupAndBoundedScanIncludeSpilledGroups) {
    SpillDirectory dir;
    auto budget = std::make_shared<MemoryBudget>(8192);
    RuntimeContext context(operator_id_from_uid("memory-test-aggregate"), "agg", nullptr, nullptr);
    context.set_memory_budget(budget);
    context.set_runner_identity("spill-serving", 0);
    const auto slot = queryable_state::compose_subtask_slot("spill-serving", 0, "agg");
    auto op = make_operator("aggregate_row", "sum", false, dir.path.string());
    op->attach_runtime(&context);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    for (int key = 0; key < 100; ++key)
        feed(*op, out, key, config::JsonValue{3});
    auto& registry = queryable_state::Registry::global();
    const auto value = registry.lookup_json(slot, "15");
    ASSERT_TRUE(value);
    EXPECT_DOUBLE_EQ(config::parse(*value).as_object().at("s").as_number(), 3);
    EXPECT_FALSE(registry.lookup_json(slot, "missing"));
    const auto scan = registry.scan_json(slot, 7);
    ASSERT_TRUE(scan);
    EXPECT_EQ(scan->entries.size(), 7u);
    EXPECT_TRUE(scan->truncated);
}

TEST(SqlMemoryBudget, TtlExpiresSpilledGroupsAndPreventsCheckpointResurrection) {
    SpillDirectory dir;
    auto budget = std::make_shared<MemoryBudget>(32768);
    InMemoryStateBackend backend;
    const auto id = operator_id_from_uid("memory-test-aggregate");
    RuntimeContext context(id, "agg", &backend, nullptr);
    context.set_memory_budget(budget);
    auto op = make_operator("aggregate_row", "sum", true, dir.path.string());
    op->attach_runtime(&context);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(1)}), out);
    for (int key = 0; key < 50; ++key)
        feed(*op, out, key, config::JsonValue{3}, 1);
    ASSERT_GT(dir.files(), 0u);
    op->snapshot_timers(backend, id);
    op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(1000)}), out);
    EXPECT_EQ(dir.files(), 0u);
    EXPECT_EQ(budget->usage().used, 0u);
    op->snapshot_timers(backend, id);
    op.reset();
    auto restored = make_operator("aggregate_row", "sum", true, dir.path.string());
    restored->attach_runtime(&context);
    restored->open();
    Emitter<Row> check([](StreamElement<Row> e) {
        for (const auto& rec : e.as_data())
            EXPECT_DOUBLE_EQ(rec.value().values.at("s").as_number(), 4);
        return true;
    });
    feed(*restored, check, 0, config::JsonValue{4}, 1001);
}

TEST(SqlMemoryBudget, TtlMetadataIsBoundedBeforeTheFirstWatermark) {
    auto budget = std::make_shared<MemoryBudget>(4096);
    {
        StateTtlTracker tracker(100, true);
        tracker.bind_memory_budget(budget);
        bool refused = false;
        for (int key = 0; key < 1000; ++key) {
            try {
                tracker.touch(std::to_string(key));
            } catch (const MemoryLimitExceeded&) {
                refused = true;
                break;
            }
        }
        EXPECT_TRUE(refused);
        EXPECT_LE(budget->usage().peak, budget->limit());
    }
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(SqlMemoryBudget, SpillStoreChecksContentAndKeepsNoResidentKeyIndex) {
    SpillDirectory dir;
    {
        SpillStore store(dir.path.string());
        const SpillStore::Bytes value{std::byte{1}, std::byte{2}};
        const std::string key(2048, 'k');
        store.put(key, value);
        EXPECT_EQ(store.get(key), value);
        store.put(key, SpillStore::Bytes{std::byte{3}});
        EXPECT_EQ(store.get(key), SpillStore::Bytes{std::byte{3}});
        EXPECT_FALSE(store.get("missing"));
        std::filesystem::path file;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir.path))
            if (entry.is_regular_file())
                file = entry.path();
        ASSERT_FALSE(file.empty());
        // Truncation must be an error, never interpreted as a missing group.
        std::filesystem::resize_file(file, 12);
        EXPECT_THROW(store.get(key), std::runtime_error);
    }
    EXPECT_TRUE(std::filesystem::is_empty(dir.path));
}

struct SpillEnvironment {
    std::optional<std::string> previous;
    explicit SpillEnvironment(const std::string& path) {
        if (const auto* value = std::getenv("CLINK_SQL_SPILL_DIR"))
            previous = value;
        ::setenv("CLINK_SQL_SPILL_DIR", path.c_str(), 1);
    }
    ~SpillEnvironment() {
        if (previous)
            ::setenv("CLINK_SQL_SPILL_DIR", previous->c_str(), 1);
        else
            ::unsetenv("CLINK_SQL_SPILL_DIR");
    }
};

std::shared_ptr<Operator<Row, Row>> family_operator(
    const std::string& type,
    const std::string& rank = "row_number",
    const std::map<std::string, std::string>& overrides = {}) {
    (void)make_operator("aggregate_row");
    const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
        type, std::string{kChannelRow}, std::string{kChannelRow});
    if (!factory)
        throw std::runtime_error("missing family factory: " + type);
    cluster::OperatorBuildContext ctx;
    ctx.params = {{"group_keys", "k"},
                  {"time_column", "ts"},
                  {"size_ms", "1000"},
                  {"slide_ms", "500"},
                  {"step_ms", "250"},
                  {"gap_ms", "500"},
                  {"partition_columns", "k"},
                  {"order_column", "ts"},
                  {"sort_columns", "v"},
                  {"sort_descending", "1"},
                  {"count", "2"},
                  {"rank_kind", rank},
                  {"aggregates", R"([{"name":"s","fn":"sum","input_column":"v"}])"},
                  {"outputs", R"([{"name":"s","fn":"sum","input_column":"v","frame_start":1}])"}};
    for (const auto& [key, value] : overrides)
        ctx.params[key] = value;
    auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(ctx));
    op->set_uid("memory-test-family");
    return op;
}

const std::vector<std::string> map_families = {"tumbling_window_row",
                                               "hopping_window_row",
                                               "cumulate_window_row",
                                               "session_window_row",
                                               "over_aggregate_row",
                                               "last_n_agg_row",
                                               "top_n_per_key_row"};

TEST(SqlMemoryBudget, WorkingFamiliesSpillAndRestoreWithIdenticalPerKeyChangelog) {
    for (const auto& type : map_families) {
        for (const auto& rank : {"row_number", "rank", "dense_rank"}) {
            if (type != "top_n_per_key_row" && std::string(rank) != "row_number")
                continue;
            SCOPED_TRACE(type + ":" + rank);
            using Outputs = std::map<std::string, std::vector<std::string>>;
            Outputs expected, actual;
            for (bool spilling : {false, true}) {
                SpillDirectory dir;
                SpillEnvironment env(spilling ? dir.path.string() : "");
                auto budget = std::make_shared<MemoryBudget>(16384);
                InMemoryStateBackend backend;
                auto& result = spilling ? actual : expected;
                Emitter<Row> out([&](StreamElement<Row> e) {
                    if (e.is_data())
                        for (const auto& rec : e.as_data()) {
                            const auto& row = rec.value();
                            result[row.values.at("k").serialize(0)].push_back(
                                config::JsonValue{to_json_object(row.values)}.serialize(0));
                        }
                    return true;
                });
                const auto id = operator_id_from_uid("memory-test-family");
                // Baseline remains live. Spilled execution recovers at the midpoint.
                RuntimeContext ctx(id, type, &backend, nullptr);
                if (spilling)
                    ctx.set_memory_budget(budget);
                auto op = family_operator(type, rank);
                op->attach_runtime(&ctx);
                op->open();
                for (int round = 0; round < 4; ++round) {
                    for (int key = 0; key < 80; ++key)
                        feed(*op,
                             out,
                             key,
                             config::JsonValue{round == 2 ? 2 : round + 1},
                             round * 100);
                    if (round == 1 && spilling) {
                        EXPECT_GE(dir.files(), 80u);
                        op->snapshot_timers(backend, id);
                        auto snapshot = backend.snapshot(CheckpointId{1});
                        op.reset();
                        EXPECT_EQ(budget->usage().used, 0u);
                        EXPECT_TRUE(std::filesystem::is_empty(dir.path));
                        backend.restore(snapshot);
                        op = family_operator(type, rank);
                        op->attach_runtime(&ctx);
                        op->restore_timers(backend, id);
                        op->open();
                    }
                }
                op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(2000)}),
                            out);
                // Revisit after the mutation scan to exercise the replacement store.
                for (int key = 0; key < 80; ++key)
                    feed(*op, out, key, config::JsonValue{9}, 2100);
                op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(4000)}),
                            out);
                EXPECT_LE(budget->usage().peak, budget->limit());
                op.reset();
                EXPECT_EQ(budget->usage().used, 0u);
            }
            EXPECT_FALSE(expected.empty());
            EXPECT_EQ(actual, expected);
        }
    }
}

TEST(SqlMemoryBudget, WorkingFamiliesRefuseWithoutSpillAndRejectOversizedPartitions) {
    for (const auto& type : map_families) {
        for (bool spilling : {false, true}) {
            SCOPED_TRACE(type + (spilling ? " oversized" : " no spill"));
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(4096);
            RuntimeContext ctx(operator_id_from_uid("memory-test-family"), type, nullptr, nullptr);
            ctx.set_memory_budget(budget);
            auto op = family_operator(type);
            op->attach_runtime(&ctx);
            op->open();
            Emitter<Row> out([](StreamElement<Row>) { return true; });
            EXPECT_THROW(
                {
                    for (int key = 0; key < 200; ++key) {
                        // A long group key is retained by every family, even SUM windows.
                        Row row;
                        row.values["k"] = config::JsonValue{spilling ? std::string(8192, 'x')
                                                                     : std::to_string(key)};
                        row.values["v"] = config::JsonValue{1};
                        row.values["ts"] = config::JsonValue{0};
                        Batch<Row> batch;
                        batch.emplace(std::move(row), EventTime::from_millis(0));
                        op->process(StreamElement<Row>::data(std::move(batch)), out);
                    }
                },
                MemoryLimitExceeded);
            op.reset();
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_LE(budget->usage().peak, budget->limit());
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
        }
    }
}

TEST(SqlMemoryBudget, ColumnarWindowsSpillWholeGroupsWithoutInvalidatingPaneReferences) {
    for (const auto* type : {"tumbling_window_row",
                             "hopping_window_row",
                             "cumulate_window_row",
                             "session_window_row"}) {
        SCOPED_TRACE(type);
        std::vector<std::string> expected, actual;
        for (bool spilling : {false, true}) {
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(16384);
            RuntimeContext ctx(operator_id_from_uid("memory-test-family"), type, nullptr, nullptr);
            if (spilling)
                ctx.set_memory_budget(budget);
            auto op = family_operator(type);
            op->attach_runtime(&ctx);
            op->open();
            auto& result = spilling ? actual : expected;
            Emitter<Row> out([&](StreamElement<Row> e) {
                if (e.is_data())
                    for (const auto& rec : e.as_data())
                        result.push_back(
                            config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                return true;
            });
            arrow::Int64Builder keys, values, times;
            for (int round = 0; round < 3; ++round)
                for (int key = 0; key < 80; ++key) {
                    ASSERT_TRUE(keys.Append(key).ok());
                    ASSERT_TRUE(values.Append(round + 1).ok());
                    ASSERT_TRUE(times.Append(round * 100).ok());
                }
            std::shared_ptr<arrow::Array> k, v, ts;
            ASSERT_TRUE(keys.Finish(&k).ok());
            ASSERT_TRUE(values.Finish(&v).ok());
            ASSERT_TRUE(times.Finish(&ts).ok());
            auto batch =
                arrow::RecordBatch::Make(arrow::schema({arrow::field("k", arrow::int64()),
                                                        arrow::field("v", arrow::int64()),
                                                        arrow::field("ts", arrow::int64())}),
                                         240,
                                         {k, v, ts});
            for (int repeat = 0; repeat < 2; ++repeat)
                EXPECT_TRUE(
                    op->process_columnar(StreamElement<Row>::data(columnar_row_batch(batch)), out));
            if (spilling)
                EXPECT_GE(dir.files(), 80u);
            op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(2000)}),
                        out);
            std::sort(result.begin(), result.end());
        }
        EXPECT_FALSE(expected.empty());
        EXPECT_EQ(actual, expected);
    }
}

TEST(SqlMemoryBudget, JoinPartitionsSpillAcrossBarrierRecoveryAndOuterExpiry) {
    (void)make_operator("aggregate_row");
    for (const auto* type : {"equi_join_row", "interval_join_row", "semi_join_row"}) {
        SCOPED_TRACE(type);
        const bool interval = std::string(type) == "interval_join_row";
        const bool semi = std::string(type) == "semi_join_row";
        std::vector<std::string> expected, actual;
        for (bool spilling : {false, true}) {
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(16384);
            InMemoryStateBackend backend;
            auto& result = spilling ? actual : expected;
            std::size_t matched = 0;
            for (int phase = 0; phase < (semi ? 3 : 2); ++phase) {
                Dag dag;
                using Channel = BoundedChannel<StreamElement<Row>>;
                StageHandle<Row> left{std::make_shared<Channel>(16), 0};
                StageHandle<Row> right{std::make_shared<Channel>(16), 0};
                plugin::BuildContext build;
                build.params = {{"left_key_column", "k"},
                                {"right_key_column", "k"},
                                {"left_alias", "l"},
                                {"right_alias", "r"},
                                {"left_columns", "k,v,ts"},
                                {"right_columns", "k,v,ts"},
                                {"join_type", "left_outer"},
                                {"left_ts_column", "ts"},
                                {"right_ts_column", "ts"},
                                {"lower_offset_ms", "0"},
                                {"upper_offset_ms", "0"},
                                {"anti", "1"},
                                {"null_aware", "1"}};
                const auto* builder = cluster::DagBuilderRegistry::default_instance().find(type);
                ASSERT_NE(builder, nullptr);
                auto built = (*builder)(dag, {std::any{left}, std::any{right}}, build);
                auto output = std::any_cast<StageHandle<Row>>(built.main_handle);
                dag.set_runner_identity(output.runner_index, "join", "memory-test-join");
                Batch<Row> rows;
                for (int key = 0; key < (phase == 0 ? (semi ? 81 : 80) : (semi ? 1 : 40)); ++key) {
                    Row row;
                    row.values["k"] = semi && (phase == 1 || key == 80) ? config::JsonValue{}
                                                                        : config::JsonValue{key};
                    row.values["v"] = config::JsonValue{phase + 1};
                    // Exact integer precision must survive the interval spill codec.
                    row.values["ts"] = config::JsonValue{std::int64_t{9007199254740993LL}};
                    rows.emplace(std::move(row));
                }
                (phase != 1 ? left.output : right.output)
                    ->push(StreamElement<Row>::data(std::move(rows)));
                for (const auto& ch : {left.output, right.output}) {
                    if (phase == 1 && interval)
                        ch->push(StreamElement<Row>::watermark(Watermark::max()));
                    ch->push(StreamElement<Row>::barrier(
                        CheckpointBarrier{CheckpointId{static_cast<std::uint64_t>(phase + 1)}}));
                    ch->close();
                }
                RuntimeContext ctx(
                    operator_id_from_uid("memory-test-join"), type, &backend, nullptr);
                if (spilling)
                    ctx.set_memory_budget(budget);
                bool captured = false;
                std::optional<Snapshot> saved;
                ctx.set_checkpoint_ack([&](CheckpointId checkpoint, bool ok, std::string error) {
                    EXPECT_TRUE(ok) << error;
                    saved = backend.snapshot(checkpoint);
                    if (spilling && phase == 0)
                        EXPECT_GE(dir.files(), 80u);
                    captured = true;
                });
                dag.runners().at(output.runner_index).run(ctx, [&] { return captured; });
                ASSERT_TRUE(captured);
                ASSERT_TRUE(saved.has_value());
                while (auto element = output.output->try_pop()) {
                    if (element->is_data())
                        for (const auto& rec : element->as_data()) {
                            if (interval && !rec.value().values.at("r_v").is_null())
                                ++matched;
                            result.push_back(
                                config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                        }
                }
                backend.restore(*saved);
            }
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
            std::sort(result.begin(), result.end());
            EXPECT_EQ(result.size(), interval ? 80u : (semi ? 162u : 160u));
            if (interval)
                EXPECT_EQ(matched, 40u);
        }
        EXPECT_EQ(actual, expected);
    }
}

TEST(SqlMemoryBudget, GlobalTopNAccountsRetainedHeapAndReleasesOnFlush) {
    SpillEnvironment env("");
    auto budget = std::make_shared<MemoryBudget>(8192);
    RuntimeContext ctx(operator_id_from_uid("memory-test-family"), "topn", nullptr, nullptr);
    ctx.set_memory_budget(budget);
    auto op = family_operator("top_n_row");
    op->attach_runtime(&ctx);
    op->open();
    Emitter<Row> out([](StreamElement<Row>) { return true; });
    feed(*op, out, 1, config::JsonValue{1});
    EXPECT_GT(budget->usage().used, 0u);
    op->process(StreamElement<Row>::watermark(Watermark::max()), out);
    EXPECT_EQ(budget->usage().used, 0u);
    EXPECT_THROW(feed(*op, out, 1, config::JsonValue{std::string(16384, 'z')}),
                 MemoryLimitExceeded);
    op.reset();
    EXPECT_EQ(budget->usage().used, 0u);
}

TEST(SqlMemoryBudget, JoinMapsAndNullWildcardRowsRefuseOverBudget) {
    (void)make_operator("aggregate_row");
    for (const auto* type : {"equi_join_row", "interval_join_row", "semi_join_row"}) {
        for (bool spilling : {false, true}) {
            SCOPED_TRACE(type);
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(4096);
            {
                Dag dag;
                using Channel = BoundedChannel<StreamElement<Row>>;
                StageHandle<Row> left{std::make_shared<Channel>(16), 0};
                StageHandle<Row> right{std::make_shared<Channel>(16), 0};
                plugin::BuildContext build;
                build.params = {{"left_alias", "l"},
                                {"right_alias", "r"},
                                {"left_key_column", "k"},
                                {"right_key_column", "k"},
                                {"left_ts_column", "ts"},
                                {"right_ts_column", "ts"},
                                {"lower_offset_ms", "0"},
                                {"upper_offset_ms", "1000"},
                                {"anti", "1"},
                                {"null_aware", "1"}};
                const auto* builder = cluster::DagBuilderRegistry::default_instance().find(type);
                ASSERT_NE(builder, nullptr);
                auto built = (*builder)(dag, {std::any{left}, std::any{right}}, build);
                auto output = std::any_cast<StageHandle<Row>>(built.main_handle);
                dag.set_runner_identity(output.runner_index, "join", "memory-test-join");
                Batch<Row> rows;
                for (int key = 0; key < 100; ++key) {
                    Row row;
                    row.values["k"] = spilling && std::string(type) == "semi_join_row"
                                          ? config::JsonValue{}
                                          : config::JsonValue{key};
                    row.values["v"] =
                        config::JsonValue{spilling ? std::string(8192, 'x') : "small"};
                    row.values["ts"] = config::JsonValue{0};
                    rows.emplace(std::move(row));
                }
                left.output->push(StreamElement<Row>::data(std::move(rows)));
                left.output->close();
                right.output->close();
                RuntimeContext ctx(
                    operator_id_from_uid("memory-test-join"), type, nullptr, nullptr);
                ctx.set_memory_budget(budget);
                EXPECT_THROW(dag.runners().at(output.runner_index).run(ctx, [] { return false; }),
                             MemoryLimitExceeded);
            }
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_LE(budget->usage().peak, budget->limit());
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
        }
    }
}

TEST(SqlMemoryBudget, GlobalTopNSpillsBeyondBudgetAndStreamsOrderedOffsetResults) {
    (void)make_operator("aggregate_row");
    std::vector<std::string> expected, actual;
    for (bool spilling : {false, true}) {
        SpillDirectory dir;
        SpillEnvironment env(spilling ? dir.path.string() : "");
        auto budget = std::make_shared<MemoryBudget>(8192);
        RuntimeContext ctx(
            operator_id_from_uid("memory-test-topn-spill"), "topn", nullptr, nullptr);
        if (spilling)
            ctx.set_memory_budget(budget);
        cluster::OperatorBuildContext build;
        build.params = {
            {"sort_columns", "v,k"}, {"sort_descending", "1,0"}, {"count", "80"}, {"offset", "25"}};
        const auto* factory = cluster::OperatorRegistry::default_instance().find_operator(
            "top_n_row", std::string{kChannelRow}, std::string{kChannelRow});
        ASSERT_NE(factory, nullptr);
        auto op = std::static_pointer_cast<Operator<Row, Row>>(factory->build(build));
        op->set_uid("memory-test-topn-spill");
        op->attach_runtime(&ctx);
        op->open();
        auto& result = spilling ? actual : expected;
        std::size_t largest_batch = 0;
        Emitter<Row> out([&](StreamElement<Row> e) {
            if (e.is_data()) {
                largest_batch = std::max(largest_batch, e.as_data().size());
                for (const auto& rec : e.as_data())
                    result.push_back(
                        config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
            }
            return true;
        });
        for (int key = 0; key < 250; ++key)
            feed(*op,
                 out,
                 key,
                 key % 13 == 0 ? config::JsonValue{} : config::JsonValue{(key * 71) % 97});
        if (spilling)
            EXPECT_EQ(dir.files(), 105u);
        op->process(StreamElement<Row>::watermark(Watermark::max()), out);
        op->flush(out);  // terminal watermark plus final hook must not duplicate results
        EXPECT_EQ(result.size(), 80u);
        if (spilling) {
            EXPECT_EQ(largest_batch, 1u);
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_LE(budget->usage().peak, budget->limit());
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
        }
    }
    EXPECT_EQ(actual, expected);
}

TEST(SqlMemoryBudget, SpillHeapKeepsBoundedFanInAndValidatesScratch) {
    SpillDirectory dir;
    auto budget = std::make_shared<MemoryBudget>(32);
    {
        SpillHeap<std::int64_t> heap(
            dir.path.string(),
            budget,
            int64_codec(),
            [](auto) { return sizeof(std::int64_t); },
            std::greater<std::int64_t>{});
        for (std::int64_t value = 0; value < 100; ++value)
            heap.push(value);
        EXPECT_EQ(heap.top().value, 99);
        heap.replace_top(-1);
        heap.reverse_order();
        for (std::int64_t value = -1; value < 99; ++value)
            EXPECT_EQ(heap.pop().value, value);
        EXPECT_EQ(heap.size(), 0u);
        EXPECT_EQ(budget->usage().used, 0u);
        EXPECT_LE(budget->usage().peak, budget->limit());
        heap.push(3);
        for (const auto& file : std::filesystem::recursive_directory_iterator(dir.path)) {
            if (file.is_regular_file()) {
                std::ofstream corrupt(file.path(), std::ios::binary | std::ios::trunc);
                corrupt << "invalid";
            }
        }
        EXPECT_THROW(heap.top(), std::runtime_error);
    }
    EXPECT_TRUE(std::filesystem::is_empty(dir.path));
}

TEST(SqlMemoryBudget, NullWildcardEntriesSpillAndRecoverWithoutWholeIndexHydration) {
    (void)make_operator("aggregate_row");
    for (int mode = 0; mode < 3; ++mode) {
        const bool spilling = mode != 0;
        const bool legacy = mode == 2;
        SCOPED_TRACE(spilling);
        SpillDirectory dir;
        SpillEnvironment env(spilling ? dir.path.string() : "");
        auto budget = std::make_shared<MemoryBudget>(16384);
        InMemoryStateBackend backend;
        std::map<int, std::vector<std::string>> output_kinds;
        if (legacy) {
            config::JsonArray probes;
            for (int key = 0; key < 80; ++key) {
                config::JsonObject row, probe;
                row["k"] = config::JsonValue{};
                row["v"] = config::JsonValue{key};
                probe["row"] = config::JsonValue{std::move(row)};
                probe["emitted"] = config::JsonValue{true};
                probes.emplace_back(std::move(probe));
                output_kinds[key].push_back(std::string(kRowKindInsert));
            }
            config::JsonObject root;
            root["probes"] = config::JsonValue{std::move(probes)};
            root["rights"] = config::JsonValue{config::JsonArray{}};
            RuntimeContext seed(
                operator_id_from_uid("memory-test-wildcards"), "wildcards", &backend, nullptr);
            seed.keyed_state<std::string, std::string>("saNull", string_codec(), string_codec())
                .put("", config::JsonValue{std::move(root)}.serialize(0));
        }
        for (int phase = 0; phase < 4; ++phase) {
            Dag dag;
            using Channel = BoundedChannel<StreamElement<Row>>;
            StageHandle<Row> left{std::make_shared<Channel>(16), 0};
            StageHandle<Row> right{std::make_shared<Channel>(16), 0};
            plugin::BuildContext build;
            build.params = {{"left_key_column", "k,v"},
                            {"right_key_column", "k,v"},
                            {"anti", "1"},
                            {"null_aware", "1"}};
            const auto* builder =
                cluster::DagBuilderRegistry::default_instance().find("semi_join_row");
            ASSERT_NE(builder, nullptr);
            auto built = (*builder)(dag, {std::any{left}, std::any{right}}, build);
            auto output = std::any_cast<StageHandle<Row>>(built.main_handle);
            dag.set_runner_identity(output.runner_index, "wildcards", "memory-test-wildcards");
            Batch<Row> rows;
            const bool probes = phase == 0 || phase == 3;
            for (int key = 0; key < (legacy && phase == 0 ? 0 : (probes ? 80 : 40)); ++key) {
                Row row;
                row.values["k"] = phase == 1 ? config::JsonValue{1} : config::JsonValue{};
                row.values["v"] = config::JsonValue{probes ? key : key * 2 + (phase == 2 ? 1 : 0)};
                rows.emplace(std::move(row));
            }
            (probes ? left.output : right.output)->push(StreamElement<Row>::data(std::move(rows)));
            for (const auto& ch : {left.output, right.output}) {
                ch->push(StreamElement<Row>::barrier(
                    CheckpointBarrier{CheckpointId{static_cast<std::uint64_t>(phase + 1)}}));
                ch->close();
            }
            RuntimeContext ctx(
                operator_id_from_uid("memory-test-wildcards"), "wildcards", &backend, nullptr);
            if (spilling)
                ctx.set_memory_budget(budget);
            std::optional<Snapshot> saved;
            ctx.set_checkpoint_ack([&](CheckpointId checkpoint, bool ok, std::string error) {
                EXPECT_TRUE(ok) << error;
                if (spilling && phase == 0)
                    EXPECT_GE(dir.files(), 80u);
                auto legacy_slot = ctx.keyed_state<std::string, std::string>(
                    "saNull", string_codec(), string_codec());
                EXPECT_FALSE(legacy_slot.get("").has_value());
                saved = backend.snapshot(checkpoint);
            });
            dag.runners().at(output.runner_index).run(ctx, [&] { return saved.has_value(); });
            ASSERT_TRUE(saved.has_value());
            while (auto e = output.output->try_pop()) {
                if (e->is_data())
                    for (const auto& rec : e->as_data()) {
                        const auto key = static_cast<int>(rec.value().values.at("v").as_int());
                        output_kinds[key].push_back(std::string(row_kind_of(rec.value())));
                    }
            }
            backend.restore(*saved);
        }
        ASSERT_EQ(output_kinds.size(), 80u);
        for (const auto& [key, kinds] : output_kinds)
            EXPECT_EQ(kinds,
                      (std::vector<std::string>{std::string(kRowKindInsert),
                                                std::string(kRowKindDelete)}))
                << key;
        EXPECT_EQ(budget->usage().used, 0u);
        EXPECT_LE(budget->usage().peak, budget->limit());
        EXPECT_TRUE(std::filesystem::is_empty(dir.path));
    }
}

TEST(SqlMemoryBudget, OneRankingOrLastNPartitionExceedsBudgetAndRestoresEntryState) {
    for (const auto* family : {"top_n_per_key_row", "last_n_agg_row"}) {
        for (const auto* rank : {"row_number", "rank", "dense_rank"}) {
            if (std::string(family) == "last_n_agg_row" && std::string(rank) != "row_number")
                continue;
            SCOPED_TRACE(std::string(family) + ":" + rank);
            std::vector<std::string> expected;
            for (int mode = 0; mode < 4; ++mode) {
                SCOPED_TRACE(mode);
                // 0: uninterrupted, 1: entry restore, 2: restore with no memory
                // configuration, 3: migrate the legacy whole-partition format.
                SpillDirectory dir;
                SpillEnvironment env(mode == 1 || mode == 2 ? dir.path.string() : "");
                auto budget = std::make_shared<MemoryBudget>(8192);
                const auto id = operator_id_from_uid("memory-test-family");
                InMemoryStateBackend backend;
                RuntimeContext ctx(id, family, &backend, nullptr);
                if (mode == 1 || mode == 2)
                    ctx.set_memory_budget(budget);
                const std::map<std::string, std::string> params = {
                    {"count", std::string(rank) == "row_number" ? "40" : "2"},
                    {"outputs",
                     R"([{"name":"s","fn":"sum","input_column":"v","frame_start":39}])"}};
                auto op = family_operator(family, rank, params);
                op->attach_runtime(&ctx);
                op->open();
                std::vector<std::string> actual;
                std::size_t largest_batch = 0;
                Emitter<Row> out([&](StreamElement<Row> e) {
                    if (e.is_data()) {
                        largest_batch = std::max(largest_batch, e.as_data().size());
                        for (const auto& rec : e.as_data())
                            actual.push_back(
                                config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                    }
                    return true;
                });
                auto send = [&](int index, int value, bool retract = false) {
                    Row row;
                    row.values["k"] = config::JsonValue{1};
                    row.values["v"] = config::JsonValue{value};
                    row.values["ts"] = config::JsonValue{(index * 73) % 211};
                    row.values["payload"] =
                        config::JsonValue{std::string(256, 'x') + std::to_string(index)};
                    if (retract)
                        set_row_kind(row, kRowKindDelete);
                    Batch<Row> batch;
                    batch.emplace(std::move(row));
                    op->process(StreamElement<Row>::data(std::move(batch)), out);
                };
                for (int index = 0; index < 48; ++index) {
                    send(index, 10);
                    if (index == 23 && mode != 0) {
                        if (mode != 3)
                            EXPECT_GT(dir.files(), 24u);
                        op->snapshot_timers(backend, id);
                        auto snapshot = backend.snapshot(CheckpointId{1});
                        op.reset();
                        EXPECT_EQ(budget->usage().used, 0u);
                        EXPECT_TRUE(std::filesystem::is_empty(dir.path));
                        backend.restore(snapshot);
                        if (mode == 2) {
                            ::setenv("CLINK_SQL_SPILL_DIR", "", 1);
                            ctx.set_memory_budget({});
                        }
                        if (mode == 3) {
                            ::setenv("CLINK_SQL_SPILL_DIR", dir.path.c_str(), 1);
                            ctx.set_memory_budget(budget);
                        }
                        op = family_operator(family, rank, params);
                        op->attach_runtime(&ctx);
                        op->restore_timers(backend, id);
                        op->open();
                    }
                }
                send(80, 20);
                send(81, 30);
                if (std::string(family) == "last_n_agg_row") {
                    for (int index = 0; index < 48; ++index)
                        send(index, 10, true);
                    send(80, 20, true);
                    send(81, 30, true);
                }
                op->snapshot_timers(backend, id);
                auto snapshot = backend.snapshot(CheckpointId{2});
                op.reset();
                backend.restore(snapshot);
                op = family_operator(family, rank, params);
                op->attach_runtime(&ctx);
                op->restore_timers(backend, id);
                op->open();
                send(162, 40);  // stale rows and drained frames must not resurrect
                op.reset();
                EXPECT_EQ(budget->usage().used, 0u);
                EXPECT_LE(budget->usage().peak, budget->limit());
                EXPECT_TRUE(std::filesystem::is_empty(dir.path));
                if (mode == 0)
                    expected = actual;
                else {
                    EXPECT_EQ(actual, expected);
                    EXPECT_LE(largest_batch, 64u);
                }
            }
        }
    }
}
}  // namespace

TEST(SqlMemoryBudget, EntryPartitionsRemainCompleteAfterRescaleAndDeletion) {
    SpillDirectory dir;
    SpillEnvironment env(dir.path.string());
    InMemoryStateBackend backend;
    const auto id = operator_id_from_uid("entry-rescale");
    RuntimeContext ctx(id, "entries", &backend, nullptr);
    ctx.set_memory_budget(std::make_shared<MemoryBudget>(4096));
    PartitionedList<std::string> entries;
    ASSERT_TRUE(entries.bind(&ctx, "test", string_codec(), [](const auto& value) {
        return sizeof(value) + value.capacity();
    }));
    for (int key = 0; key < 16; ++key)
        for (int row = 0; row < 32; ++row)
            entries.append(std::to_string(key), std::string(256, 'a') + std::to_string(row));
    entries.snapshot();
    entries.truncate("0", 3);
    entries.erase_group("1");
    entries.snapshot();
    const auto snapshot = backend.snapshot(CheckpointId{1});
    std::size_t total = 0;
    for (KeyGroup first = 0; first < kNumKeyGroups; first += 16) {
        InMemoryStateBackend restored;
        const KeyGroupRange range{first, static_cast<KeyGroup>(first + 16)};
        restored.restore(snapshot, range);
        RuntimeContext restored_ctx(id, "entries", &restored, nullptr);
        PartitionedList<std::string> recovered;
        ASSERT_TRUE(recovered.bind(&restored_ctx, "test", string_codec(), [](const auto& value) {
            return sizeof(value) + value.capacity();
        }));
        ASSERT_TRUE(recovered.restored());
        recovered.groups([&](const auto& key, auto count) {
            EXPECT_TRUE(range.contains(key_group_for_key(string_codec().encode(key))));
            EXPECT_NE(key, "1");
            EXPECT_EQ(count, key == "0" ? 3u : 32u);
            recovered.scan(key, [&](auto index, const auto& value) {
                EXPECT_EQ(value, std::string(256, 'a') + std::to_string(index));
                ++total;
                return true;
            });
        });
    }
    EXPECT_EQ(total, 14u * 32u + 3u);
}

TEST(SqlMemoryBudget, OneJoinPartitionExceedsBudgetAcrossRecoveryAndOuterExpiry) {
    (void)make_operator("aggregate_row");
    for (const auto* type : {"equi_join_row", "interval_join_row"}) {
        SCOPED_TRACE(type);
        const bool interval = std::string(type) == "interval_join_row";
        std::vector<std::string> expected, actual;
        for (bool spilling : {false, true}) {
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(16384);
            InMemoryStateBackend backend;
            auto& result = spilling ? actual : expected;
            std::size_t matched = 0;
            for (int phase = 0; phase < (interval ? 2 : 4); ++phase) {
                Dag dag;
                using Channel = BoundedChannel<StreamElement<Row>>;
                StageHandle<Row> left{std::make_shared<Channel>(16), 0};
                StageHandle<Row> right{std::make_shared<Channel>(16), 0};
                plugin::BuildContext build;
                build.params = {{"left_key_column", "k"},
                                {"right_key_column", "k"},
                                {"left_alias", "l"},
                                {"right_alias", "r"},
                                {"left_columns", "k,v,ts"},
                                {"right_columns", "k,v,ts"},
                                {"join_type", "left_outer"},
                                {"left_ts_column", "ts"},
                                {"right_ts_column", "ts"},
                                {"lower_offset_ms", "0"},
                                {"upper_offset_ms", "0"},
                                {"anti", "1"},
                                {"null_aware", "1"}};
                const auto* builder = cluster::DagBuilderRegistry::default_instance().find(type);
                ASSERT_NE(builder, nullptr);
                auto built = (*builder)(dag, {std::any{left}, std::any{right}}, build);
                auto output = std::any_cast<StageHandle<Row>>(built.main_handle);
                dag.set_runner_identity(output.runner_index, "join", "memory-test-join");
                Batch<Row> rows;
                for (int key = 0; key < ((phase == 0 || phase == 3) ? 80 : 2); ++key) {
                    Row row;
                    row.values["k"] = config::JsonValue{1};
                    row.values["v"] =
                        config::JsonValue{std::string(256, 'x') + std::to_string(key)};
                    // Exact integer precision must survive the interval spill codec.
                    row.values["ts"] = config::JsonValue{std::int64_t{9007199254740993LL}};
                    if (phase >= 2)
                        set_row_kind(row, kRowKindDelete);
                    rows.emplace(std::move(row));
                }
                ((phase == 0 || phase == 3) ? left.output : right.output)
                    ->push(StreamElement<Row>::data(std::move(rows)));
                for (const auto& ch : {left.output, right.output}) {
                    if (phase == 1 && interval)
                        ch->push(StreamElement<Row>::watermark(Watermark::max()));
                    ch->push(StreamElement<Row>::barrier(
                        CheckpointBarrier{CheckpointId{static_cast<std::uint64_t>(phase + 1)}}));
                    ch->close();
                }
                RuntimeContext ctx(
                    operator_id_from_uid("memory-test-join"), type, &backend, nullptr);
                if (spilling)
                    ctx.set_memory_budget(budget);
                bool captured = false;
                std::optional<Snapshot> saved;
                ctx.set_checkpoint_ack([&](CheckpointId checkpoint, bool ok, std::string error) {
                    EXPECT_TRUE(ok) << error;
                    saved = backend.snapshot(checkpoint);
                    if (spilling && phase == 0)
                        EXPECT_GE(dir.files(), 80u);
                    captured = true;
                });
                dag.runners().at(output.runner_index).run(ctx, [&] { return captured; });
                ASSERT_TRUE(captured);
                ASSERT_TRUE(saved.has_value());
                while (auto element = output.output->try_pop()) {
                    if (element->is_data())
                        for (const auto& rec : element->as_data()) {
                            if (interval && !rec.value().values.at("r_v").is_null())
                                ++matched;
                            result.push_back(
                                config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                        }
                }
                backend.restore(*saved);
            }
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
            std::sort(result.begin(), result.end());
            EXPECT_EQ(result.size(), interval ? 160u : 640u);
            if (interval)
                EXPECT_EQ(matched, 160u);
        }
        EXPECT_EQ(actual, expected);
    }
}

class SqlEntryWindowBudget : public ::testing::TestWithParam<const char*> {};
TEST_P(SqlEntryWindowBudget, OversizedKeyRecoversMergesExpiresAndRejectsLateRows) {
    const std::string type = GetParam();
    std::vector<std::string> expected;
    for (int mode = 0; mode < 4; ++mode) {
        SCOPED_TRACE(mode);
        SpillDirectory dir;
        SpillEnvironment env(mode == 1 || mode == 2 ? dir.path.string() : "");
        auto budget = std::make_shared<MemoryBudget>(8192);
        InMemoryStateBackend backend;
        const auto id = operator_id_from_uid("memory-test-family");
        RuntimeContext ctx(id, type, &backend, nullptr);
        if (mode == 1 || mode == 2)
            ctx.set_memory_budget(budget);
        auto op = family_operator(type);
        op->attach_runtime(&ctx);
        op->open();
        std::vector<std::string> actual;
        Emitter<Row> out([&](StreamElement<Row> element) {
            if (element.is_data())
                for (const auto& record : element.as_data())
                    actual.push_back(
                        config::JsonValue{to_json_object(record.value().values)}.serialize(0));
            return true;
        });
        auto recover = [&](int checkpoint) {
            op->snapshot_timers(backend, id);
            auto saved = backend.snapshot(CheckpointId{static_cast<std::uint64_t>(checkpoint)});
            op.reset();
            EXPECT_EQ(budget->usage().used, 0u);
            backend.restore(saved);
            if (mode == 2) {
                ::setenv("CLINK_SQL_SPILL_DIR", "", 1);
                ctx.set_memory_budget({});
            } else if (mode == 3) {
                ::setenv("CLINK_SQL_SPILL_DIR", dir.path.c_str(), 1);
                ctx.set_memory_budget(budget);
            }
            op = family_operator(type);
            op->attach_runtime(&ctx);
            op->restore_timers(backend, id);
            op->open();
        };
        for (int index = 0; index < 40; ++index)
            feed(*op, out, 1, config::JsonValue{index + 1}, ((index * 37) % 40) * 2000);
        if (mode)
            recover(1);
        // Bridge the first two sessions through three arriving records.
        for (int time : {500, 1000, 1500})
            feed(*op, out, 1, config::JsonValue{100}, time);
        op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(50000)}), out);
        recover(2);
        feed(*op, out, 1, config::JsonValue{999}, 0);
        op->process(StreamElement<Row>::watermark(Watermark::max()), out);
        recover(3);
        op->process(StreamElement<Row>::watermark(Watermark::max()), out);
        std::sort(actual.begin(), actual.end());
        if (!mode)
            expected = actual;
        else
            EXPECT_EQ(actual, expected);
        EXPECT_FALSE(actual.empty());
        op.reset();
        EXPECT_EQ(budget->usage().used, 0u);
    }
}
INSTANTIATE_TEST_SUITE_P(Windows,
                         SqlEntryWindowBudget,
                         ::testing::Values("tumbling_window_row",
                                           "hopping_window_row",
                                           "cumulate_window_row",
                                           "session_window_row"));

TEST(SqlMemoryBudget, OverPendingAndFrameHistoryExceedBudgetAcrossRecovery) {
    const std::map<std::string, std::string> params{{"outputs", R"([
        {"name":"running","fn":"sum","input_column":"v"},
        {"name":"rows","fn":"sum","input_column":"v","frame_mode":1,"frame_start":59},
        {"name":"range","fn":"sum","input_column":"v","frame_mode":2,"frame_start":5000},
        {"name":"previous","fn":"lag","input_column":"v","lag_offset":50},
        {"name":"first","fn":"first_value","input_column":"v"},
        {"name":"last","fn":"last_value","input_column":"v"}])"}};
    std::vector<std::string> expected;
    for (int mode = 0; mode < 4; ++mode) {
        SCOPED_TRACE(mode);
        SpillDirectory dir;
        SpillEnvironment env(mode == 1 || mode == 2 ? dir.path.string() : "");
        auto budget = std::make_shared<MemoryBudget>(8192);
        InMemoryStateBackend backend;
        const auto id = operator_id_from_uid("memory-test-family");
        RuntimeContext ctx(id, "over", &backend, nullptr);
        if (mode == 1 || mode == 2)
            ctx.set_memory_budget(budget);
        auto make = [&] { return family_operator("over_aggregate_row", "row_number", params); };
        auto op = make();
        op->attach_runtime(&ctx);
        op->open();
        std::vector<std::string> actual;
        Emitter<Row> out([&](StreamElement<Row> element) {
            if (element.is_data())
                for (const auto& record : element.as_data())
                    actual.push_back(
                        config::JsonValue{to_json_object(record.value().values)}.serialize(0));
            return true;
        });
        auto recover = [&](int checkpoint) {
            op->snapshot_timers(backend, id);
            auto saved = backend.snapshot(CheckpointId{static_cast<std::uint64_t>(checkpoint)});
            op.reset();
            backend.restore(saved);
            if (mode == 2) {
                ::setenv("CLINK_SQL_SPILL_DIR", "", 1);
                ctx.set_memory_budget({});
            } else if (mode == 3) {
                ::setenv("CLINK_SQL_SPILL_DIR", dir.path.c_str(), 1);
                ctx.set_memory_budget(budget);
            }
            op = make();
            op->attach_runtime(&ctx);
            op->restore_timers(backend, id);
            op->open();
        };
        auto send = [&](int index, int time) {
            Row row;
            row.values["k"] = config::JsonValue{1};
            row.values["v"] = config::JsonValue{index};
            row.values["ts"] = config::JsonValue{time};
            row.values["payload"] = config::JsonValue{std::string(256, 'x')};
            Batch<Row> batch;
            batch.emplace(std::move(row));
            op->process(StreamElement<Row>::data(std::move(batch)), out);
        };
        for (int index = 0; index < 80; ++index)
            send(index, ((index * 37) % 80) * 100);
        if (mode)
            recover(1);
        // Ties after recovery must retain arrival order.
        send(100, 7500);
        op->process(StreamElement<Row>::watermark(Watermark{EventTime::from_millis(6500)}), out);
        recover(2);
        send(101, 7500);
        send(999, 0);  // dropped against the restored watermark
        op->process(StreamElement<Row>::watermark(Watermark::max()), out);
        recover(3);
        op->process(StreamElement<Row>::watermark(Watermark::max()), out);
        if (!mode)
            expected = actual;
        else
            EXPECT_EQ(actual, expected);
        EXPECT_EQ(actual.size(), 82u);
        op.reset();
        EXPECT_EQ(budget->usage().used, 0u);
    }
}

TEST(SqlMemoryBudget, GroupByAccumulatorsSpillIndependentlyAndRetainChangelog) {
    std::string specs = "[";
    for (int i = 0; i < 12; ++i) {
        if (i)
            specs += ",";
        specs += "{\"name\":\"s" + std::to_string(i) +
                 "\",\"fn\":\"count\",\"input_column\":\"v\",\"distinct\":true}";
    }
    specs += "]";
    const std::map<std::string, std::string> params{{"aggregates", specs},
                                                    {"emit_changelog", "true"}};
    {
        // The same group must overflow the unsplit working bucket, proving
        // this test exercises more than ordinary multi-key eviction.
        SpillEnvironment env("");
        RuntimeContext ctx(
            operator_id_from_uid("memory-test-family"), "aggregate", nullptr, nullptr);
        ctx.set_memory_budget(std::make_shared<MemoryBudget>(16384));
        auto op = family_operator("aggregate_row", "row_number", params);
        op->attach_runtime(&ctx);
        op->open();
        Emitter<Row> discard([](StreamElement<Row>) { return true; });
        EXPECT_THROW(
            {
                for (int value = 0; value < 48; ++value)
                    feed(*op, discard, 1, config::JsonValue{value});
            },
            MemoryLimitExceeded);
    }
    std::vector<std::string> expected;
    for (int mode = 0; mode < 3; ++mode) {
        SCOPED_TRACE(mode);
        SpillDirectory dir;
        SpillEnvironment env(mode == 1 ? dir.path.string() : "");
        auto budget = std::make_shared<MemoryBudget>(16384);
        InMemoryStateBackend backend;
        const auto id = operator_id_from_uid("memory-test-family");
        RuntimeContext ctx(id, "aggregate", &backend, nullptr);
        if (mode == 1)
            ctx.set_memory_budget(budget);
        auto make = [&] { return family_operator("aggregate_row", "row_number", params); };
        auto op = make();
        op->attach_runtime(&ctx);
        op->open();
        std::vector<std::string> actual;
        Emitter<Row> out([&](StreamElement<Row> element) {
            if (element.is_data())
                for (const auto& record : element.as_data())
                    actual.push_back(
                        config::JsonValue{to_json_object(record.value().values)}.serialize(0));
            return true;
        });
        auto send = [&](int value, bool retract = false) {
            Row row;
            row.values["k"] = config::JsonValue{1};
            row.values["v"] = config::JsonValue{value};
            if (retract)
                set_row_kind(row, kRowKindDelete);
            Batch<Row> batch;
            batch.emplace(std::move(row));
            op->process(StreamElement<Row>::data(std::move(batch)), out);
        };
        for (int i = 0; i < 48; ++i)
            send(i);
        op->snapshot_timers(backend, id);
        auto saved = backend.snapshot(CheckpointId{1});
        op.reset();
        backend.restore(saved);
        if (mode == 2) {
            ::setenv("CLINK_SQL_SPILL_DIR", dir.path.c_str(), 1);
            ctx.set_memory_budget(budget);
        }
        op = make();
        op->attach_runtime(&ctx);
        op->restore_timers(backend, id);
        op->open();
        send(47);  // duplicate must not change COUNT DISTINCT
        for (int i = 0; i < 48; ++i)
            send(i, true);
        send(47, true);
        if (!mode)
            expected = actual;
        else
            EXPECT_EQ(actual, expected);
        EXPECT_FALSE(actual.empty());
        op.reset();
        EXPECT_EQ(budget->usage().used, 0u);
    }
}

TEST(SqlMemoryBudget, NullAwareExactBucketExceedsBudgetAndRecoversPoisonedFlags) {
    (void)make_operator("aggregate_row");
    for (bool wildcard : {false, true}) {
        SCOPED_TRACE(wildcard);
        const auto* type = "semi_join_row";
        SCOPED_TRACE(type);
        const bool interval = false;
        std::vector<std::string> expected, actual;
        for (bool spilling : {false, true}) {
            SpillDirectory dir;
            SpillEnvironment env(spilling ? dir.path.string() : "");
            auto budget = std::make_shared<MemoryBudget>(16384);
            InMemoryStateBackend backend;
            auto& result = spilling ? actual : expected;
            std::size_t matched = 0;
            for (int phase = 0; phase < 3; ++phase) {
                Dag dag;
                using Channel = BoundedChannel<StreamElement<Row>>;
                StageHandle<Row> left{std::make_shared<Channel>(16), 0};
                StageHandle<Row> right{std::make_shared<Channel>(16), 0};
                plugin::BuildContext build;
                build.params = {{"left_key_column", "k"},
                                {"right_key_column", "k"},
                                {"left_alias", "l"},
                                {"right_alias", "r"},
                                {"left_columns", "k,v,ts"},
                                {"right_columns", "k,v,ts"},
                                {"join_type", "left_outer"},
                                {"left_ts_column", "ts"},
                                {"right_ts_column", "ts"},
                                {"lower_offset_ms", "0"},
                                {"upper_offset_ms", "0"},
                                {"anti", "1"},
                                {"null_aware", "1"}};
                const auto* builder = cluster::DagBuilderRegistry::default_instance().find(type);
                ASSERT_NE(builder, nullptr);
                auto built = (*builder)(dag, {std::any{left}, std::any{right}}, build);
                auto output = std::any_cast<StageHandle<Row>>(built.main_handle);
                dag.set_runner_identity(output.runner_index, "join", "memory-test-join");
                Batch<Row> rows;
                for (int key = 0; key < (phase == 0 ? 80 : 1); ++key) {
                    Row row;
                    row.values["k"] =
                        phase == 1 && wildcard ? config::JsonValue{} : config::JsonValue{1};
                    row.values["v"] =
                        config::JsonValue{std::string(256, 'x') + std::to_string(key)};
                    // Exact integer precision must survive the interval spill codec.
                    row.values["ts"] = config::JsonValue{std::int64_t{9007199254740993LL}};
                    rows.emplace(std::move(row));
                }
                (phase == 1 ? right.output : left.output)
                    ->push(StreamElement<Row>::data(std::move(rows)));
                for (const auto& ch : {left.output, right.output}) {
                    if (phase == 1 && interval)
                        ch->push(StreamElement<Row>::watermark(Watermark::max()));
                    ch->push(StreamElement<Row>::barrier(
                        CheckpointBarrier{CheckpointId{static_cast<std::uint64_t>(phase + 1)}}));
                    ch->close();
                }
                RuntimeContext ctx(
                    operator_id_from_uid("memory-test-join"), type, &backend, nullptr);
                if (spilling)
                    ctx.set_memory_budget(budget);
                bool captured = false;
                std::optional<Snapshot> saved;
                ctx.set_checkpoint_ack([&](CheckpointId checkpoint, bool ok, std::string error) {
                    EXPECT_TRUE(ok) << error;
                    saved = backend.snapshot(checkpoint);
                    if (spilling && phase == 0)
                        EXPECT_GE(dir.files(), 80u);
                    captured = true;
                });
                dag.runners().at(output.runner_index).run(ctx, [&] { return captured; });
                ASSERT_TRUE(captured);
                ASSERT_TRUE(saved.has_value());
                while (auto element = output.output->try_pop()) {
                    if (element->is_data())
                        for (const auto& rec : element->as_data()) {
                            if (interval && !rec.value().values.at("r_v").is_null())
                                ++matched;
                            result.push_back(
                                config::JsonValue{to_json_object(rec.value().values)}.serialize(0));
                        }
                }
                backend.restore(*saved);
            }
            EXPECT_EQ(budget->usage().used, 0u);
            EXPECT_TRUE(std::filesystem::is_empty(dir.path));
            std::sort(result.begin(), result.end());
            EXPECT_EQ(result.size(), 160u);
            if (interval)
                EXPECT_EQ(matched, 160u);
        }
        EXPECT_EQ(actual, expected);
    }
}
