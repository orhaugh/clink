#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/queryable_state/registry.hpp"
#include "clink/runtime/memory_budget.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/row.hpp"
#include "clink/sql/row_columnar_output.hpp"
#include "clink/sql/row_kind.hpp"
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
                EXPECT_EQ(dir.files(), 80u);
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
    EXPECT_EQ(dir.files(), 100u);
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
    EXPECT_EQ(dir.files(), 100u);
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
}  // namespace
