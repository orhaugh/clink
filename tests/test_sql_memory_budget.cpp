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
#include "clink/state/in_memory_state_backend.hpp"

namespace {
using namespace clink;
using namespace clink::sql;

std::shared_ptr<Operator<Row, Row>> make_operator(const std::string& type,
                                                  const std::string& fn = "sum",
                                                  bool ttl = false) {
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
    context.params["aggregates"] =
        "[{\"name\":\"s\",\"fn\":\"" + fn + "\",\"input_column\":\"v\",\"distinct\":false}]";
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
}  // namespace
