// State Processor API - Savepoint reader/writer tests. Exercises:
//
//   * Empty savepoint -> write entries -> snapshot -> load -> read back
//   * File round-trip (write_to_file + load_from_file) preserves state
//   * operators() / slots() discover what's in a savepoint
//   * Round-trip from a real LocalExecutor job: snapshot a running job's
//     backend, open as a Savepoint, read keyed state offline
//   * Restore a fresh job from a Savepoint built offline

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state_processor/savepoint.hpp"
#include "clink/state_processor/state_diff.hpp"

using namespace clink;
using namespace clink::state_processor;

TEST(StateProcessor, CreateWriteReadInMemoryRoundTrip) {
    auto sp = Savepoint::create();
    auto ks = sp.keyed_state<std::string, std::int64_t>(
        OperatorId{42}, "counts", string_codec(), int64_codec());
    ks.put("alpha", 1);
    ks.put("beta", 22);
    ks.put("gamma", 333);

    // Round-trip via the in-memory Snapshot blob: take the bytes,
    // load a fresh Savepoint from them, verify all KVs visible.
    auto snap = sp.snapshot(CheckpointId{7});
    EXPECT_EQ(snap.checkpoint_id.value(), 7u);
    EXPECT_FALSE(snap.bytes.empty());

    auto reopened = Savepoint::load_from_snapshot(std::move(snap));
    auto ks2 = reopened.keyed_state<std::string, std::int64_t>(
        OperatorId{42}, "counts", string_codec(), int64_codec());
    EXPECT_EQ(ks2.get("alpha"), std::optional<std::int64_t>{1});
    EXPECT_EQ(ks2.get("beta"), std::optional<std::int64_t>{22});
    EXPECT_EQ(ks2.get("gamma"), std::optional<std::int64_t>{333});
    EXPECT_FALSE(ks2.get("missing").has_value());
}

TEST(StateProcessor, FileRoundTripPreservesState) {
    const auto tmp = std::filesystem::temp_directory_path() / "clink_state_processor_test.snap";
    std::filesystem::remove(tmp);

    {
        auto sp = Savepoint::create();
        auto ks = sp.keyed_state<std::int64_t, std::string>(
            OperatorId{99}, "labels", int64_codec(), string_codec());
        ks.put(1, "one");
        ks.put(2, "two");
        ks.put(3, "three");
        sp.write_to_file(tmp);
    }

    auto sp = Savepoint::load_from_file(tmp);
    auto ks = sp.keyed_state<std::int64_t, std::string>(
        OperatorId{99}, "labels", int64_codec(), string_codec());
    EXPECT_EQ(ks.get(1), std::optional<std::string>{"one"});
    EXPECT_EQ(ks.get(2), std::optional<std::string>{"two"});
    EXPECT_EQ(ks.get(3), std::optional<std::string>{"three"});

    std::filesystem::remove(tmp);
}

TEST(StateProcessor, OperatorsAndSlotsListing) {
    auto sp = Savepoint::create();
    sp.keyed_state<std::string, std::int64_t>(OperatorId{1}, "users", string_codec(), int64_codec())
        .put("a", 100);
    sp.keyed_state<std::string, std::int64_t>(
          OperatorId{1}, "sessions", string_codec(), int64_codec())
        .put("b", 200);
    sp.keyed_state<std::string, std::int64_t>(OperatorId{2}, "users", string_codec(), int64_codec())
        .put("c", 300);

    auto ops = sp.operators();
    std::vector<std::uint64_t> op_values;
    for (auto o : ops) {
        op_values.push_back(o.value());
    }
    std::sort(op_values.begin(), op_values.end());
    EXPECT_EQ(op_values, (std::vector<std::uint64_t>{1, 2}));

    auto slots1 = sp.slots(OperatorId{1});
    std::sort(slots1.begin(), slots1.end());
    EXPECT_EQ(slots1, (std::vector<std::string>{"sessions", "users"}));

    auto slots2 = sp.slots(OperatorId{2});
    EXPECT_EQ(slots2, (std::vector<std::string>{"users"}));
}

// KeyedProcessFunction that maintains a per-key counter so the test can
// snapshot the underlying backend mid-flight and inspect it via the
// State Processor API.
class CounterFn final : public KeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open(RuntimeContext& ctx) override {
        state_ = std::make_unique<KeyedState<std::int64_t, std::int64_t>>(
            ctx.keyed_state<std::int64_t, std::int64_t>("counter", int64_codec(), int64_codec()));
    }
    void process_element(const std::int64_t& v,
                         ProcessFunctionContext<std::int64_t>& /*ctx*/,
                         Collector<std::int64_t>& out) override {
        const auto prev = state_->get(current_key()).value_or(0);
        const auto next = prev + v;
        state_->put(current_key(), next);
        out.collect(next);
    }

private:
    std::unique_ptr<KeyedState<std::int64_t, std::int64_t>> state_;
};

TEST(StateProcessor, ReadStateFromRealJobSnapshot) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{
        Record<std::int64_t>{1},
        Record<std::int64_t>{1},
        Record<std::int64_t>{2},
        Record<std::int64_t>{1},
        Record<std::int64_t>{2},
    });
    auto h_src = dag.add_source<std::int64_t>(src);

    auto fn = std::make_shared<CounterFn>();
    auto adapter = std::make_shared<
        ::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>>(
        fn, [](const std::int64_t& v) { return v; });
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Sanity check: pipeline output is the running totals - 1, 2 for
    // key=1; 2, 4 for key=2; 3 for key=1 again.
    EXPECT_EQ(sink->collected().size(), 5u);

    // Snapshot the live backend and open it as a Savepoint.
    auto sp = Savepoint::load_from_snapshot(backend->snapshot(CheckpointId{1}));
    // The operator id used by the adapter - by default the runner
    // assigns ids in the order ops were added. Source = 1, operator =
    // 2, sink = 3 in this Dag. We verify by checking which operator
    // has the "counter" slot.
    auto ops = sp.operators();
    OperatorId counter_op{0};
    for (auto o : ops) {
        auto slots = sp.slots(o);
        for (const auto& s : slots) {
            if (s == "counter") {
                counter_op = o;
                break;
            }
        }
        if (counter_op.value() != 0) {
            break;
        }
    }
    ASSERT_NE(counter_op.value(), 0u) << "counter slot not found in any operator";

    auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
        counter_op, "counter", int64_codec(), int64_codec());
    EXPECT_EQ(ks.get(1), std::optional<std::int64_t>{3});  // 1 + 1 + 1
    EXPECT_EQ(ks.get(2), std::optional<std::int64_t>{4});  // 2 + 2
}

// Helper that constructs the exact same Dag shape twice - once to
// discover the operator id (by running a no-op job and inspecting the
// resulting backend), and once to actually run the seeded job. Both
// invocations use the same op name and the same stage layout, so the
// Dag's derive_id hash is identical across them.
namespace {

template <typename SetupDag>
auto build_counter_dag(SetupDag setup) {
    Dag dag;
    auto src = setup(dag);
    auto fn = std::make_shared<CounterFn>();
    auto adapter = std::make_shared<
        ::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>>(
        fn, [](const std::int64_t& v) { return v; });
    auto h_op = dag.add_operator<std::int64_t, std::int64_t>(src, adapter);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_op, sink);
    return std::make_tuple(std::move(dag), sink);
}

}  // namespace

TEST(StateProcessor, SeedFreshJobFromOfflineSavepoint) {
    // The Dag assigns op ids by hashing "stage<idx>/<op_name>". To
    // seed an offline savepoint at the right OperatorId, we run a
    // no-op pass first to discover the id - this matches the
    // realistic State Processor use case ("take a prior savepoint,
    // mutate, restore") rather than depending on internal hash math.
    OperatorId counter_op{0};
    {
        auto probe_backend = std::make_shared<InMemoryStateBackend>();
        auto [dag, sink] = build_counter_dag([](Dag& d) {
            auto src = std::make_shared<VectorSource<std::int64_t>>(
                std::vector<Record<std::int64_t>>{Record<std::int64_t>{1}});
            return d.add_source<std::int64_t>(src);
        });
        (void)sink;
        JobConfig cfg;
        cfg.state_backend = probe_backend;
        LocalExecutor exec(std::move(dag), std::move(cfg));
        exec.run();

        auto sp = Savepoint::load_from_snapshot(probe_backend->snapshot(CheckpointId{1}));
        for (auto o : sp.operators()) {
            for (const auto& s : sp.slots(o)) {
                if (s == "counter") {
                    counter_op = o;
                    break;
                }
            }
            if (counter_op.value() != 0) {
                break;
            }
        }
    }
    ASSERT_NE(counter_op.value(), 0u) << "could not discover counter op id";

    // Build a Savepoint seeding (1 -> 1000, 2 -> 2000) under the
    // discovered op id.
    auto sp = Savepoint::create();
    auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
        counter_op, "counter", int64_codec(), int64_codec());
    ks.put(1, 1000);
    ks.put(2, 2000);

    // Start a fresh job with restore_from set to that snapshot.
    auto fresh_backend = std::make_shared<InMemoryStateBackend>();
    auto [dag, sink] = build_counter_dag([](Dag& d) {
        auto src = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{
            Record<std::int64_t>{1},
            Record<std::int64_t>{2},
        });
        return d.add_source<std::int64_t>(src);
    });

    JobConfig cfg;
    cfg.state_backend = fresh_backend;
    cfg.restore_from = sp.snapshot(CheckpointId{1});
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto collected = sink->collected();
    std::sort(collected.begin(), collected.end());
    EXPECT_EQ(collected, (std::vector<std::int64_t>{1001, 2002}));
}

// ---- state_diff.hpp (the time-travel diff primitive) -----------------------

TEST(StateDiff, CollectEntriesPreservesStructure) {
    auto sp = Savepoint::create();
    auto counts = sp.keyed_state<std::string, std::int64_t>(
        OperatorId{7}, "counts", string_codec(), int64_codec());
    counts.put("alpha", 1);
    counts.put("beta", 2);
    auto totals = sp.keyed_state<std::int64_t, std::int64_t>(
        OperatorId{7}, "totals", int64_codec(), int64_codec());
    totals.put(42, 100);

    auto entries = collect_entries(sp);
    ASSERT_EQ(entries.size(), 1u);
    const auto& slots = entries.at(OperatorId{7});
    ASSERT_EQ(slots.size(), 2u);
    EXPECT_EQ(slots.at("counts").size(), 2u);
    EXPECT_EQ(slots.at("totals").size(), 1u);
    // The user key is the raw codec bytes after the '|' separator: the
    // string codec stores the text itself.
    EXPECT_EQ(slots.at("counts").count("alpha"), 1u);
    EXPECT_EQ(slots.at("counts").count("beta"), 1u);
}

TEST(StateDiff, DiffReportsAddedRemovedChanged) {
    auto a = Savepoint::create();
    auto b = Savepoint::create();
    auto ka =
        a.keyed_state<std::string, std::int64_t>(OperatorId{9}, "s", string_codec(), int64_codec());
    auto kb =
        b.keyed_state<std::string, std::int64_t>(OperatorId{9}, "s", string_codec(), int64_codec());
    ka.put("stays", 1);
    kb.put("stays", 1);  // unchanged
    ka.put("mutates", 10);
    kb.put("mutates", 20);  // changed
    ka.put("vanishes", 5);  // removed
    kb.put("appears", 7);   // added

    auto report = diff_entries(collect_entries(a), collect_entries(b));
    ASSERT_EQ(report.slots.size(), 1u);
    const auto& d = report.slots[0];
    EXPECT_EQ(d.op.value(), 9u);
    EXPECT_EQ(d.slot, "s");
    EXPECT_EQ(d.added, 1u);
    EXPECT_EQ(d.removed, 1u);
    EXPECT_EQ(d.changed, 1u);
    EXPECT_EQ(d.unchanged, 1u);
    ASSERT_EQ(d.samples.size(), 3u);
    EXPECT_EQ(report.total_entries_a, 3u);
    EXPECT_EQ(report.total_entries_b, 3u);
    EXPECT_FALSE(report.identical());

    // Identical inputs -> identical report.
    auto same = diff_entries(collect_entries(a), collect_entries(a));
    EXPECT_TRUE(same.identical());
    EXPECT_EQ(same.slots.size(), 0u);
}

TEST(StateDiff, MaxSamplesBoundsSamplesNotCounts) {
    auto a = Savepoint::create();
    auto b = Savepoint::create();
    auto kb = b.keyed_state<std::int64_t, std::int64_t>(
        OperatorId{3}, "grow", int64_codec(), int64_codec());
    for (std::int64_t i = 0; i < 50; ++i) {
        kb.put(i, i);
    }
    auto report = diff_entries(collect_entries(a), collect_entries(b), /*max_samples=*/5);
    ASSERT_EQ(report.slots.size(), 1u);
    EXPECT_EQ(report.slots[0].added, 50u);
    EXPECT_EQ(report.slots[0].samples.size(), 5u);
    // 0 = unbounded.
    auto full = diff_entries(collect_entries(a), collect_entries(b), /*max_samples=*/0);
    EXPECT_EQ(full.slots[0].samples.size(), 50u);
}

TEST(StateDiff, MergeEntriesUnionsDisjointSubtasks) {
    auto sub0 = Savepoint::create();
    auto sub1 = Savepoint::create();
    sub0.keyed_state<std::string, std::int64_t>(OperatorId{5}, "s", string_codec(), int64_codec())
        .put("left", 1);
    sub1.keyed_state<std::string, std::int64_t>(OperatorId{5}, "s", string_codec(), int64_codec())
        .put("right", 2);

    auto merged = collect_entries(sub0);
    merge_entries(merged, collect_entries(sub1));
    EXPECT_EQ(merged.at(OperatorId{5}).at("s").size(), 2u);
}

TEST(StateDiff, RenderBytesFormats) {
    // Printable text renders quoted.
    EXPECT_EQ(render_bytes("hello"), "\"hello\"");
    // 8-byte buffers additionally show the little-endian int64 reading.
    std::string eight(8, '\0');
    eight[0] = 42;
    const auto rendered = render_bytes(eight);
    EXPECT_NE(rendered.find("(int64 42)"), std::string::npos);
    // Non-printable, non-8-byte renders as hex.
    std::string raw;
    raw.push_back('\x01');
    raw.push_back('\x02');
    EXPECT_EQ(render_bytes(raw), "0x0102");
    // Truncation marker carries the true size.
    const std::string longtext(100, 'a');
    EXPECT_NE(render_bytes(longtext, 16).find("[100 bytes]"), std::string::npos);
}

// ----- Parquet export (the analytics projection) -----

#include <arrow/table.h>
#include <parquet/arrow/reader.h>

#include "clink/state_processor/parquet_export.hpp"

// write_state_parquet writes the decoded entry model as one Parquet
// table; reading it back with the standard Parquet reader reproduces
// every column of every entry, including operator-state rows (key
// group >= 128) and binary values.
TEST(StateExportParquet, RoundTripsAllColumns) {
    auto sp = Savepoint::create();
    auto ks = sp.keyed_state<std::string, std::int64_t>(
        OperatorId{7}, "counts", string_codec(), int64_codec());
    ks.put("alpha", 1);
    ks.put("beta", 22);
    // A second slot on another operator.
    auto ks2 = sp.keyed_state<std::string, std::string>(
        OperatorId{9}, "names", string_codec(), string_codec());
    ks2.put("k", "value-bytes");
    // An operator-state row (reserved prefix byte, no key group).
    const std::string op_key = std::string{"\xFF"} + "offsets|p0";
    const std::string op_val = "12345678";
    sp.backend().put(OperatorId{9}, std::string_view{op_key}, std::string_view{op_val});

    const auto entries = collect_entries(sp);
    const auto path = std::filesystem::temp_directory_path() / "clink_state_export_test.parquet";
    std::filesystem::remove(path);
    write_state_parquet(entries, {}, path);

    // Read back with the standard Parquet reader.
    auto open_result = arrow::io::ReadableFile::Open(path.string());
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto reader_result = parquet::arrow::OpenFile(*open_result, arrow::default_memory_pool());
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    std::shared_ptr<arrow::Table> table;
    ASSERT_TRUE((*reader_result)->ReadTable(&table).ok());

    ASSERT_EQ(table->num_columns(), 5);
    EXPECT_EQ(table->schema()->field(0)->name(), "op_id");
    EXPECT_EQ(table->schema()->field(1)->name(), "key_group");
    EXPECT_EQ(table->schema()->field(2)->name(), "slot");
    EXPECT_EQ(table->schema()->field(3)->name(), "user_key");
    EXPECT_EQ(table->schema()->field(4)->name(), "value_bytes");

    // Row count = every keyed entry plus the operator-state row.
    std::size_t expected_rows = 0;
    for (const auto& [op, slots] : entries) {
        for (const auto& [slot, kv] : slots) {
            expected_rows += kv.size();
        }
    }
    ASSERT_EQ(static_cast<std::size_t>(table->num_rows()), expected_rows);
    ASSERT_GE(expected_rows, 4u);

    // Spot-check content: rebuild the (op, slot, user_key -> value) view
    // from the table and compare against the collected model.
    auto combined = table->CombineChunks(arrow::default_memory_pool());
    ASSERT_TRUE(combined.ok());
    table = *combined;
    const auto& op_col = static_cast<const arrow::UInt64Array&>(*table->column(0)->chunk(0));
    const auto& kg_col = static_cast<const arrow::UInt8Array&>(*table->column(1)->chunk(0));
    const auto& slot_col = static_cast<const arrow::StringArray&>(*table->column(2)->chunk(0));
    const auto& key_col = static_cast<const arrow::BinaryArray&>(*table->column(3)->chunk(0));
    const auto& val_col = static_cast<const arrow::BinaryArray&>(*table->column(4)->chunk(0));
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        const OperatorId op{op_col.Value(i)};
        const std::string slot{slot_col.GetView(i)};
        const std::string user_key{key_col.GetView(i)};
        const auto it = entries.at(op).at(slot).find(user_key);
        ASSERT_NE(it, entries.at(op).at(slot).end());
        EXPECT_EQ(std::string{val_col.GetView(i)}, it->second.value);
        EXPECT_EQ(kg_col.Value(i), it->second.key_group);
    }
    // The operator-state row surfaced with its reserved prefix byte.
    bool saw_op_state = false;
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        if (kg_col.Value(i) == 0xFF) {
            saw_op_state = true;
            EXPECT_EQ(std::string{slot_col.GetView(i)}, "offsets");
            EXPECT_EQ(std::string{val_col.GetView(i)}, "12345678");
        }
    }
    EXPECT_TRUE(saw_op_state);
}

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "clink/state/snapshot_arrow_writer.hpp"

// The format contract (docs/internals/state-snapshot-format.md): every
// stream the canonical writer produces is stamped clink.format_version=1,
// and version stamps ride clink.state_versions when present.
TEST(StateSnapshotFormat, StreamsCarryTheFormatVersionMarker) {
    InMemoryStateBackend backend;
    backend.put(OperatorId{1}, "k", std::string_view{"v"});
    StateVersionMap versions;
    versions.set(OperatorId{1}, "CounterState", 2);
    backend.set_state_versions(versions);
    auto snap = backend.snapshot(CheckpointId{1});

    auto buffer =
        std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(snap.bytes.data()),
                                        static_cast<int64_t>(snap.bytes.size()));
    auto reader = *arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer));
    const auto& meta = reader->schema()->metadata();
    ASSERT_TRUE(meta != nullptr);
    auto v = meta->Get(kSnapshotFormatVersionKey);
    ASSERT_TRUE(v.ok());
    EXPECT_EQ(*v, std::string{kSnapshotFormatVersion});
    auto sv = meta->Get(kStateVersionsMetadataKey);
    ASSERT_TRUE(sv.ok());
    EXPECT_EQ(StateVersionMap::unpack(*sv).pack(), versions.pack());

    // An empty-versions stream still carries the format marker.
    InMemoryStateBackend bare;
    auto snap2 = bare.snapshot(CheckpointId{2});
    auto buffer2 =
        std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(snap2.bytes.data()),
                                        static_cast<int64_t>(snap2.bytes.size()));
    auto reader2 = *arrow::ipc::RecordBatchStreamReader::Open(
        std::make_shared<arrow::io::BufferReader>(buffer2));
    ASSERT_TRUE(reader2->schema()->metadata() != nullptr);
    EXPECT_TRUE(reader2->schema()->metadata()->Get(kSnapshotFormatVersionKey).ok());
}
