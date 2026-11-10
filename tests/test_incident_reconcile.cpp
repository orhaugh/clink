// Destination reconciliation (include/clink/incident/reconcile.hpp): the
// canonical cells both sides are reduced to, the comparison and its verdicts,
// the sink runner's interval evidence, and whole reconciliations of collected
// bundles against a destination that serves rows from memory. The ClickHouse
// reader is exercised against real servers in impls/clickhouse/tests.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/config/decimal.hpp"
#include "clink/config/json.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/sha256.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/incident/reconcile.hpp"
#include "clink/incident/reference.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/record_capture.hpp"
#include "clink/runtime/sink_evidence.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"

namespace fs = std::filesystem;
using namespace clink::incident;
using namespace clink::incident::reconcile;
using clink::config::JsonValue;
using namespace std::chrono_literals;

namespace {

constexpr JobId kReconcileJob = 11;
constexpr std::uint64_t kReconcileSink = 9001;
const std::string kServerUuid = "11111111-2222-4333-8444-555555555555";
const std::string kTableUuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";

void reconcile_spill(const fs::path& p, const std::string& body) {
    fs::create_directories(p.parent_path());
    std::ofstream(p, std::ios::binary | std::ios::trunc) << body;
}

std::string reconcile_slurp(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream s;
    s << in.rdbuf();
    return s.str();
}

ColumnType type_of(const std::string& spelling) {
    return parse_column_type(spelling);
}

// ---- canonical cells -------------------------------------------------------------

TEST(ReconcileCanon, TypesAreReadFromTheDestinationsSpelling) {
    EXPECT_EQ(type_of("Int64").kind, TypeKind::Int);
    EXPECT_EQ(type_of("UInt8").kind, TypeKind::UInt);
    EXPECT_TRUE(type_of("Nullable(Int32)").nullable);
    EXPECT_EQ(type_of("LowCardinality(String)").kind, TypeKind::String);
    EXPECT_EQ(type_of("LowCardinality(Nullable(String))").kind, TypeKind::String);
    const auto d = type_of("Decimal(18, 2)");
    EXPECT_EQ(d.kind, TypeKind::Decimal);
    EXPECT_EQ(d.scale, 2);
    EXPECT_EQ(type_of("Decimal64(4)").scale, 4);
    const auto t = type_of("DateTime64(6, 'UTC')");
    EXPECT_EQ(t.kind, TypeKind::DateTime64);
    EXPECT_EQ(t.precision, 6);
    EXPECT_EQ(type_of("DateTime('Europe/London')").kind, TypeKind::DateTime);
    EXPECT_EQ(type_of("Date32").kind, TypeKind::Date);
    EXPECT_EQ(type_of("FixedString(4)").kind, TypeKind::Unsupported);
    EXPECT_EQ(type_of("Array(Int64)").kind, TypeKind::Unsupported);
    EXPECT_FALSE(type_of("Map(String, Int64)").unsupported_reason.empty());
}

TEST(ReconcileCanon, NullIsAValueOfItsOwn) {
    EXPECT_EQ(canon_expected(JsonValue{}, type_of("Nullable(Int64)")), "N");
    EXPECT_NE(canon_expected(JsonValue{}, type_of("Nullable(String)")),
              canon_expected(JsonValue{std::string{}}, type_of("Nullable(String)")));
    EXPECT_NE(canon_expected(JsonValue{}, type_of("Nullable(Int64)")),
              canon_expected(JsonValue{std::int64_t{0}}, type_of("Nullable(Int64)")));
    EXPECT_EQ(display("N"), "NULL");
}

TEST(ReconcileCanon, IntegersAreExactAndWholeDoublesCountAsIntegers) {
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{-42}}, type_of("Int64")), "I-42");
    EXPECT_EQ(canon_expected(JsonValue{9.0}, type_of("Int64")), "I9");
    EXPECT_EQ(canon_expected(JsonValue{9.5}, type_of("Int64")).front(), '!');
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{-1}}, type_of("UInt32")).front(), '!');
    EXPECT_EQ(canon::integer("007"), "I7");
    EXPECT_EQ(canon::integer("-0"), "I0");
    EXPECT_EQ(canon::integer("1e3").front(), '!');
}

TEST(ReconcileCanon, FloatsCompareByTheirBits) {
    const auto f64 = type_of("Float64");
    EXPECT_EQ(canon_expected(JsonValue{0.1}, f64), canon::float64_bits(0x3FB999999999999AULL));
    // The bits the server reports for 0.1 (reinterpretAsUInt64), as a live
    // 26.8 server did.
    EXPECT_EQ(canon::float64_bits(4591870180066957722ULL), canon_expected(JsonValue{0.1}, f64));
    EXPECT_NE(canon_expected(JsonValue{-0.0}, f64), canon_expected(JsonValue{0.0}, f64));
    EXPECT_EQ(canon::float64_bits(0x7FF8000000000001ULL),
              canon::float64_bits(0xFFF8000000000000ULL));
    EXPECT_EQ(canon_expected(JsonValue{0.1}, type_of("Float32")), canon::float32_bits(1036831949U));
    EXPECT_EQ(display(canon_expected(JsonValue{0.1}, f64)), "0.1");
}

TEST(ReconcileCanon, DecimalsAreUnscaledAtTheColumnsScale) {
    const auto d = type_of("Decimal(18, 2)");
    const std::string tagged = std::string{clink::config::kDecimalSentinel} + "12.30";
    EXPECT_EQ(canon_expected(JsonValue{tagged}, d), "D1230@2");
    // The server prints 12.30 as 12.3; both are the same value at scale 2.
    EXPECT_EQ(canon::decimal("12.3", 2), "D1230@2");
    EXPECT_EQ(canon::decimal("-0.05", 2), "D-5@2");
    EXPECT_EQ(canon::decimal("-0.00", 2), "D0@2");
    EXPECT_EQ(canon::decimal("1.234", 2).front(), '!');
    EXPECT_EQ(canon::decimal("1.230", 2), "D123@2");
    EXPECT_EQ(display("D-5@2"), "-0.05");
    EXPECT_EQ(display("D1230@2"), "12.30");
}

TEST(ReconcileCanon, TimestampsAreTicksAtTheColumnsPrecision) {
    const JsonValue ms{std::int64_t{1767323045678}};
    EXPECT_EQ(canon_expected(ms, type_of("DateTime64(3)")), "T1767323045678@3");
    EXPECT_EQ(canon_expected(ms, type_of("DateTime64(6, 'UTC')")), "T1767323045678000@6");
    EXPECT_EQ(canon_expected(ms, type_of("DateTime")).front(), '!');
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{1767323045000}}, type_of("DateTime")),
              "T1767323045@0");
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{-1}}, type_of("DateTime64(3)")), "T-1@3");
    EXPECT_EQ(display("T1767323045678@3"), "2026-01-02T03:04:05.678Z");
    EXPECT_EQ(display("T-1@3"), "1969-12-31T23:59:59.999Z");
}

TEST(ReconcileCanon, DatesAreDaysWhetherWrittenAsDaysOrText) {
    const auto d = type_of("Date");
    EXPECT_EQ(canon_expected(JsonValue{std::string{"2026-01-02"}}, d), "d20455");
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{20455}}, d), "d20455");
    EXPECT_EQ(canon_expected(JsonValue{std::string{"1950-06-07"}}, type_of("Date32")), "d-7148");
    EXPECT_EQ(display("d-25567"), "1900-01-01");
}

TEST(ReconcileCanon, StringsAndBooleansAreExact) {
    EXPECT_EQ(canon_expected(JsonValue{std::string{"a'b\\c"}}, type_of("String")), "Sa'b\\c");
    EXPECT_EQ(canon_expected(JsonValue{true}, type_of("Bool")), "B1");
    EXPECT_EQ(canon_expected(JsonValue{std::int64_t{1}}, type_of("Bool")).front(), '!');
    EXPECT_EQ(display("Sab\"c"), "\"ab\\\"c\"");
}

// ---- the comparison ----------------------------------------------------------------

Row row_of(std::int64_t id, std::int64_t amount) {
    return {"I" + std::to_string(id), "I" + std::to_string(amount)};
}

TEST(ReconcileCompare, AnExactDestinationMatches) {
    const std::vector<Row> e = {row_of(1, 10), row_of(2, 20)};
    const auto c = compare(e, {0}, e, e, 10);
    EXPECT_EQ(c.matched, 2U);
    EXPECT_EQ(c.missing + c.duplicated + c.mismatched + c.unexpected + c.unstable_keys, 0U);
}

TEST(ReconcileCompare, EachKindOfDiscrepancyIsTold) {
    const std::vector<Row> e = {row_of(1, 10), row_of(2, 20), row_of(3, 30), row_of(4, 40)};
    // 1 matches; 2 is missing; 3 is changed; 4 is there twice, and with a
    // third row of other values.
    const std::vector<Row> o = {
        row_of(1, 10), row_of(3, 31), row_of(4, 40), row_of(4, 40), row_of(4, 41)};
    const auto c = compare(e, {0}, o, o, 10);
    EXPECT_EQ(c.matched, 2U);  // 1 and 4
    EXPECT_EQ(c.missing, 1U);
    EXPECT_EQ(c.mismatched, 1U);
    EXPECT_EQ(c.duplicated, 1U);
    EXPECT_EQ(c.unexpected, 1U);
    ASSERT_EQ(c.listed.size(), 4U);
    for (const auto& d : c.listed) {
        if (d.kind == Discrepancy::Kind::Mismatch) {
            ASSERT_EQ(d.differing.size(), 1U);
            EXPECT_EQ(d.differing.front(), 1U);
        }
    }
}

TEST(ReconcileCompare, EqualCountsAndTotalsWithDifferentRowsStillFail) {
    // Same number of rows and the same sum of amounts: 10 + 20 = 15 + 15.
    const std::vector<Row> e = {row_of(1, 10), row_of(2, 20)};
    const std::vector<Row> o = {row_of(1, 15), row_of(2, 15)};
    const auto c = compare(e, {0}, o, o, 10);
    EXPECT_EQ(c.observed_rows, c.expected_rows);
    EXPECT_EQ(c.mismatched, 2U);
    EXPECT_EQ(c.matched, 0U);
}

TEST(ReconcileCompare, AKeyReadDifferentlyTwiceIsUnstableNotADiscrepancy) {
    const std::vector<Row> e = {row_of(1, 10), row_of(2, 20)};
    const std::vector<Row> first = {row_of(1, 10)};
    const std::vector<Row> second = {row_of(1, 10), row_of(2, 20)};
    const auto c = compare(e, {0}, first, second, 10);
    EXPECT_EQ(c.unstable_keys, 1U);
    EXPECT_EQ(c.missing, 0U);
    EXPECT_EQ(c.matched, 1U);
}

TEST(ReconcileCompare, TheListIsBoundedAndTheCountsAreNot) {
    std::vector<Row> e;
    for (int i = 0; i < 50; ++i) {
        e.push_back(row_of(i, i));
    }
    const auto c = compare(e, {0}, {}, {}, 5);
    EXPECT_EQ(c.missing, 50U);
    EXPECT_EQ(c.listed.size(), 5U);
}

// ---- the sink runner's interval evidence ----------------------------------------------

class CountingSource final : public clink::Source<std::string> {
public:
    bool produce(clink::Emitter<std::string>& out) override {
        if (this->cancelled()) {
            return false;
        }
        clink::Batch<std::string> b;
        const int n = next_.fetch_add(1);
        b.emplace("{\"id\":" + std::to_string(n) + "}");
        out.emit_data(std::move(b));
        std::this_thread::sleep_for(2ms);
        return true;
    }
    std::string name() const override { return "counting"; }

private:
    std::atomic<int> next_{0};
};

class EvidenceSink final : public clink::Sink<std::string>, public clink::IntervalEvidenceSink {
public:
    void on_data(const clink::Batch<std::string>& batch) override {
        const std::lock_guard<std::mutex> lock(mu_);
        pending_ += batch.size();
    }
    void on_barrier(clink::CheckpointBarrier b) override {
        const std::lock_guard<std::mutex> lock(mu_);
        last_barrier_ = b.id().value();
        flushed_ = pending_;
        pending_ = 0;
    }
    void arm_interval_evidence() override { armed_ = true; }
    std::string interval_evidence(std::uint64_t checkpoint_id) noexcept override {
        const std::lock_guard<std::mutex> lock(mu_);
        return "{\"connector\":\"test\",\"asked_for\":" + std::to_string(checkpoint_id) +
               ",\"last_barrier\":" + std::to_string(last_barrier_) +
               ",\"rows_acknowledged\":" + std::to_string(flushed_) + "}";
    }
    [[nodiscard]] bool armed() const { return armed_; }
    std::string name() const override { return "evidence_sink"; }

private:
    std::mutex mu_;
    std::uint64_t pending_{0};
    std::uint64_t flushed_{0};
    std::uint64_t last_barrier_{0};
    std::atomic<bool> armed_{false};
};

TEST(ReconcileSinkRunner, EachBarrierLeavesTheIntervalsInputAndEvidence) {
    const auto dir =
        fs::temp_directory_path() / ("clink_reconcile_runner_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    clink::CheckpointCoordinator::Config cfg;
    cfg.interval = 40ms;
    clink::CheckpointCoordinator coord(backend, cfg);

    clink::Dag dag;
    auto src = std::make_shared<CountingSource>();
    auto sink = std::make_shared<EvidenceSink>();
    auto h0 = dag.add_source<std::string>(src);
    const auto codec = std::make_shared<const clink::Codec<std::string>>(clink::string_codec());
    dag.add_sink<std::string>(h0, sink, codec);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());
    const clink::OperatorId sink_id = dag.runners().back().id;

    clink::JobConfig jc;
    jc.capture_dir = dir.string();
    clink::LocalExecutor exec(std::move(dag), jc);
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(400ms);
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    EXPECT_TRUE(sink->armed());
    const auto sub = dir / ("op-" + std::to_string(sink_id.value())) / "subtask-0";
    ASSERT_TRUE(fs::is_directory(sub)) << sub;
    std::vector<std::uint64_t> epochs;
    for (const auto& e : fs::directory_iterator(sub)) {
        const auto n = e.path().filename().string();
        if (n.ends_with(".sink.json")) {
            epochs.push_back(std::stoull(n.substr(6, n.size() - 6 - 10)));
        }
    }
    std::sort(epochs.begin(), epochs.end());
    ASSERT_GE(epochs.size(), 2U) << "fewer than two barriers reached the sink";
    std::uint64_t prev = 0;
    for (const auto epoch : epochs) {
        const auto doc = clink::config::parse(
            reconcile_slurp(sub / ("epoch-" + std::to_string(epoch) + ".sink.json")));
        EXPECT_EQ(doc.at("format").as_string(), clink::capture::kSinkIntervalFormat);
        EXPECT_EQ(static_cast<std::uint64_t>(doc.at("checkpoint").as_int()), epoch);
        // The chain of barriers the runner saw, unbroken.
        EXPECT_EQ(static_cast<std::uint64_t>(doc.at("previous_barrier").as_int()), prev);
        prev = epoch;
        // The sink's own account, asked for at this barrier, after its hook.
        EXPECT_EQ(static_cast<std::uint64_t>(doc.at("sink").at("asked_for").as_int()), epoch);
        EXPECT_EQ(static_cast<std::uint64_t>(doc.at("sink").at("last_barrier").as_int()), epoch);
        const auto seen = static_cast<std::uint64_t>(doc.at("records").at("seen").as_int());
        EXPECT_EQ(static_cast<std::uint64_t>(doc.at("sink").at("rows_acknowledged").as_int()),
                  seen);
        const auto cap = sub / ("epoch-" + std::to_string(epoch) + ".cap");
        if (seen > 0) {
            EXPECT_EQ(doc.at("records").at("capture").as_string(), "written");
            const auto bytes = reconcile_slurp(cap);
            const auto parsed = clink::capture::read_capture_events<std::string>(
                std::span<const std::byte>{reinterpret_cast<const std::byte*>(bytes.data()),
                                           bytes.size()},
                clink::string_codec());
            ASSERT_TRUE(parsed.has_value());
            EXPECT_EQ(parsed->first.records_seen, seen);
        } else {
            EXPECT_EQ(doc.at("records").at("capture").as_string(), "empty");
        }
    }
    fs::remove_all(dir);
}

TEST(ReconcileSinkRunner, ASinkGivenNoCodecIsNotRecorded) {
    const auto dir =
        fs::temp_directory_path() / ("clink_reconcile_runner_off_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    auto backend = std::make_shared<clink::InMemoryStateBackend>();
    clink::CheckpointCoordinator::Config cfg;
    cfg.interval = 40ms;
    clink::CheckpointCoordinator coord(backend, cfg);
    clink::Dag dag;
    auto src = std::make_shared<CountingSource>();
    auto sink = std::make_shared<EvidenceSink>();
    dag.add_sink<std::string>(dag.add_source<std::string>(src), sink);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());
    clink::JobConfig jc;
    jc.capture_dir = dir.string();
    clink::LocalExecutor exec(std::move(dag), jc);
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(200ms);
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();
    EXPECT_FALSE(sink->armed());
    EXPECT_FALSE(fs::exists(dir)) << "a sink registered without a codec wrote evidence";
    fs::remove_all(dir);
}

TEST(ReconcileSinkRunner, ASinksOpSpecCarriesNoCredential) {
    const auto redacted = clink::capture::redact_credentials(
        {{"password", "hunter2"}, {"table", "events"}, {"sasl.password", "x"}});
    EXPECT_EQ(redacted.at("password"), "<redacted>");
    EXPECT_EQ(redacted.at("sasl.password"), "<redacted>");
    EXPECT_EQ(redacted.at("table"), "events");
}

// ---- whole reconciliations ------------------------------------------------------------

std::string destination_json(
    const std::string& engine = "MergeTree",
    const std::string& create =
        "CREATE TABLE default.events (id Int64) ENGINE = MergeTree ORDER BY id",
    const std::string& table_uuid = kTableUuid) {
    using clink::config::JsonArray;
    using clink::config::JsonObject;
    JsonObject server;
    server["name"] = JsonValue{std::string{"fake"}};
    server["version"] = JsonValue{std::string{"26.8.15"}};
    server["tested_line"] = JsonValue{true};
    server["endpoint"] = JsonValue{std::string{"127.0.0.1:9000"}};
    server["uuid"] = JsonValue{kServerUuid};
    server["host_name"] = JsonValue{std::string{"fake"}};
    JsonArray columns;
    for (const auto& [name, declared, target] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"id", "BIGINT", "Int64"},
             {"amount", "DECIMAL(18, 2)", "Decimal(18, 2)"},
             {"note", "VARCHAR", "Nullable(String)"}}) {
        JsonObject c;
        c["name"] = JsonValue{name};
        c["declared"] = JsonValue{declared};
        c["target"] = JsonValue{target};
        columns.emplace_back(std::move(c));
    }
    JsonObject d;
    d["database"] = JsonValue{std::string{"default"}};
    d["table"] = JsonValue{std::string{"events"}};
    d["server"] = JsonValue{std::move(server)};
    d["engine"] = JsonValue{engine};
    d["table_uuid"] = JsonValue{table_uuid};
    d["engine_full"] = JsonValue{std::string{"MergeTree ORDER BY id"}};
    d["create_table_query"] = JsonValue{create};
    d["metadata_modification_time"] = JsonValue{std::string{"2026-01-01 00:00:00"}};
    d["dedup"] = JsonValue{std::string{"non_replicated_deduplication_window=100 (table)"}};
    d["keeps_dedup_log"] = JsonValue{true};
    d["async_insert"] = JsonValue{std::string{"async_insert=0 (table)"}};
    d["token_on_resend"] = JsonValue{std::string{"kept"}};
    d["columns"] = JsonValue{std::move(columns)};
    d["omitted_columns"] = JsonValue{JsonArray{}};
    return JsonValue{std::move(d)}.serialize(0);
}

std::string row_json(std::int64_t id, const std::string& amount, const std::string& note = "") {
    std::string out =
        "{\"amount\":\"\\u0001" + amount + "\",\"id\":" + std::to_string(id) + ",\"note\":";
    out += note.empty() ? "null" : "\"" + note + "\"";
    return out + "}";
}

Row observed_row(std::int64_t id,
                 const std::string& amount_canon,
                 const std::string& note_canon = "N") {
    return {"I" + std::to_string(id), amount_canon, note_canon};
}

class FakeDestination final : public Destination {
public:
    DestinationDescription desc;
    std::vector<Row> rows;
    std::vector<Mutation> muts;
    std::function<void(int)> before_read;
    std::optional<std::string> fail;
    int reads{0};
    // The parts marker's text; bumped by before_marker to show a change.
    int parts_version{1};
    std::function<void(int)> before_marker;
    int markers{0};

    FakeDestination() {
        desc.server_uuid = kServerUuid;
        desc.host_name = "fake";
        desc.version = "26.8.15.10";
        desc.database = "default";
        desc.table = "events";
        desc.table_uuid = kTableUuid;
        desc.engine = "MergeTree";
        desc.engine_full = "MergeTree ORDER BY id";
        desc.create_table_query =
            "CREATE TABLE default.events (id Int64) ENGINE = MergeTree ORDER BY id";
        desc.metadata_modification_time = "2026-01-01 00:00:00";
        desc.columns = {
            {"id", "Int64"}, {"amount", "Decimal(18, 2)"}, {"note", "Nullable(String)"}};
        desc.now = "2026-11-10 12:00:00.000";
    }
    DestinationDescription describe() override { return desc; }
    std::vector<Mutation> mutations() override { return muts; }
    std::vector<Row> read(const std::vector<Column>& /*columns*/,
                          const std::vector<std::size_t>& key,
                          const std::vector<Row>& keys) override {
        ++reads;
        if (before_read) {
            before_read(reads);
        }
        if (fail) {
            throw DestinationUnavailable(*fail);
        }
        std::set<Row> wanted(keys.begin(), keys.end());
        std::vector<Row> out;
        for (const auto& r : rows) {
            Row k;
            for (const auto i : key) {
                k.push_back(r.at(i));
            }
            if (wanted.contains(k)) {
                out.push_back(r);
            }
        }
        return out;
    }
    std::vector<Row> read_all(const std::vector<Column>& /*columns*/,
                              std::uint64_t max_rows) override {
        if (fail) {
            throw DestinationUnavailable(*fail);
        }
        if (rows.size() > max_rows) {
            throw DestinationUnavailable("more than " + std::to_string(max_rows) + " rows");
        }
        return rows;
    }
    std::string parts_marker() override {
        ++markers;
        if (before_marker) {
            before_marker(markers);
        }
        return "all_1_1_0:" + std::to_string(rows.size()) + ":v" + std::to_string(parts_version);
    }
};

// A reference artifact for `doc`, as a writer of the format writes one: the
// header, a line per row (cells id, amount, note), and the end line with its
// hashes. `edit` changes the lines before they are written.
void write_reference(const fs::path& path,
                     const JsonValue& doc,
                     const std::vector<Row>& rows,
                     bool complete = true,
                     const std::vector<std::string>& incomplete = {},
                     const std::function<void(std::vector<std::string>&)>& edit = {}) {
    using clink::config::JsonObject;
    JsonObject h;
    h["format"] = JsonValue{std::string{kReferenceFormat}};
    h["version"] = JsonValue{kReferenceVersion};
    h["scope"] = doc;
    JsonObject impl;
    impl["name"] = JsonValue{std::string{"test-reference"}};
    impl["version"] = JsonValue{std::string{"1"}};
    h["implementation"] = JsonValue{std::move(impl)};
    const std::string header = JsonValue{std::move(h)}.serialize(0) + "\n";
    std::vector<std::string> lines{header};
    clink::Sha256 rows_hash;
    for (const auto& r : rows) {
        JsonObject cells;
        cells["id"] = JsonValue{r.at(0)};
        cells["amount"] = JsonValue{r.at(1)};
        cells["note"] = JsonValue{r.at(2)};
        JsonObject line;
        line["row"] = JsonValue{std::move(cells)};
        const std::string text = JsonValue{std::move(line)}.serialize(0) + "\n";
        rows_hash.update(text.data(), text.size());
        lines.push_back(text);
    }
    clink::Sha256 header_hash;
    header_hash.update(header.data(), header.size());
    JsonObject end;
    end["rows"] = JsonValue{static_cast<std::int64_t>(rows.size())};
    end["rows_sha256"] = JsonValue{clink::Sha256::to_hex(rows_hash.finalize())};
    end["header_sha256"] = JsonValue{clink::Sha256::to_hex(header_hash.finalize())};
    end["complete"] = JsonValue{complete};
    clink::config::JsonArray why;
    for (const auto& w : incomplete) {
        why.emplace_back(w);
    }
    end["incomplete"] = JsonValue{std::move(why)};
    JsonObject endline;
    endline["end"] = JsonValue{std::move(end)};
    lines.push_back(JsonValue{std::move(endline)}.serialize(0) + "\n");
    if (edit) {
        edit(lines);
    }
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    for (const auto& l : lines) {
        out << l;
    }
}

class ReconcileBundle : public ::testing::Test {
protected:
    fs::path root;
    fs::path ckpt;
    fs::path cap;
    FakeDestination dest;

    void SetUp() override {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        root = fs::temp_directory_path() /
               ("clink_reconcile_" + std::to_string(::getpid()) + "_" + info->name());
        fs::remove_all(root);
        ckpt = root / "ckpt";
        cap = root / "capture";
        fs::create_directories(ckpt);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    [[nodiscard]] std::string state_root() const {
        return clink::cluster::job_state_root(ckpt.string(), kReconcileJob);
    }

    void write_checkpoint(std::uint64_t id, const std::set<std::uint32_t>& subs = {0, 1}) const {
        for (const auto sub : subs) {
            auto sp = clink::state_processor::Savepoint::create();
            auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
                clink::OperatorId{42}, "counts", clink::int64_codec(), clink::int64_codec());
            ks.put(static_cast<std::int64_t>(sub), static_cast<std::int64_t>(id));
            const auto snap = sp.snapshot(clink::CheckpointId{id});
            const auto path = fs::path{clink::state_dir_for(state_root(), 1, sub)} /
                              ("checkpoint-" + std::to_string(id) + ".snap");
            std::string bytes(snap.bytes.size(), '\0');
            for (std::size_t i = 0; i < bytes.size(); ++i) {
                bytes[i] = static_cast<char>(snap.bytes[i]);
            }
            reconcile_spill(path, bytes);
            clink::state::write_checkpoint_meta(path, id, snap.bytes.data(), snap.bytes.size());
        }
        reconcile_spill(
            clink::cluster::completed_marker_dir_for(ckpt.string(), kReconcileJob) /
                ("COMPLETED-" + std::to_string(id)),
            clink::cluster::completed_marker_body(kReconcileJob,
                                                  id,
                                                  1,
                                                  subs,
                                                  clink::cluster::recorded_state_location(
                                                      ckpt.string(), kReconcileJob, state_root())));
    }

    // A checkpoint whose participants hold Kafka source positions as the
    // source writes them: one operator-state row per partition, the next
    // offset to read as eight little-endian bytes, and, when `log` is given,
    // the row naming the log they belong to.
    void write_checkpoint_with_offsets(
        std::uint64_t id,
        const std::map<std::uint32_t, std::map<int, std::int64_t>>& rows_by_sub,
        const std::string& log = "") const {
        std::set<std::uint32_t> subs;
        for (const auto& [sub, rows] : rows_by_sub) {
            subs.insert(sub);
            clink::InMemoryStateBackend be;
            if (!log.empty()) {
                be.put_operator_state(clink::OperatorId{4242}, "__kafka_identity__", log);
            }
            for (const auto& [partition, offset] : rows) {
                std::string value(8, '\0');
                for (int i = 0; i < 8; ++i) {
                    value[static_cast<std::size_t>(i)] =
                        static_cast<char>((static_cast<std::uint64_t>(offset) >> (i * 8)) & 0xFF);
                }
                be.put_operator_state(
                    clink::OperatorId{4242}, "__kafka_off__:" + std::to_string(partition), value);
            }
            const auto snap = be.snapshot(clink::CheckpointId{id});
            const auto path = fs::path{clink::state_dir_for(state_root(), 1, sub)} /
                              ("checkpoint-" + std::to_string(id) + ".snap");
            reconcile_spill(
                path,
                std::string(reinterpret_cast<const char*>(snap.bytes.data()), snap.bytes.size()));
            clink::state::write_checkpoint_meta(path, id, snap.bytes.data(), snap.bytes.size());
        }
        reconcile_spill(
            clink::cluster::completed_marker_dir_for(ckpt.string(), kReconcileJob) /
                ("COMPLETED-" + std::to_string(id)),
            clink::cluster::completed_marker_body(kReconcileJob,
                                                  id,
                                                  1,
                                                  subs,
                                                  clink::cluster::recorded_state_location(
                                                      ckpt.string(), kReconcileJob, state_root())));
    }

    enum class Mode { Written, Disarmed, Truncated };

    // One sink subtask's interval as the runner writes it: its captured input
    // and its evidence, with the ClickHouse sink's account.
    void write_interval(std::uint64_t epoch,
                        std::uint64_t prev,
                        std::uint32_t sink_subtask,
                        std::uint32_t parallelism,
                        const std::vector<std::string>& rows,
                        Mode mode = Mode::Written,
                        const std::string& destination = destination_json(),
                        const std::string& resend = "") const {
        const std::size_t capture_idx = 10 + sink_subtask;
        clink::capture::SinkIntervalHeader h;
        h.op = clink::OperatorId{kReconcileSink};
        h.subtask_idx = capture_idx;
        h.checkpoint = epoch;
        h.previous_barrier = prev;
        h.attempt = "00000000000000aa";
        h.records_seen = rows.size();
        h.records_stored = rows.size();
        h.truncated = mode == Mode::Truncated;
        h.capture = mode == Mode::Disarmed ? "disarmed" : (rows.empty() ? "empty" : "written");
        if (!rows.empty() && mode != Mode::Disarmed) {
            std::vector<clink::capture::CaptureEvent<std::string>> events;
            for (const auto& r : rows) {
                events.emplace_back(clink::Record<std::string>{r});
            }
            clink::capture::CaptureFileHeader fh;
            fh.truncated = mode == Mode::Truncated;
            fh.records_seen = rows.size();
            const auto bytes =
                clink::capture::encode_capture_file(fh, events, clink::string_codec());
            reconcile_spill(cap / ("op-" + std::to_string(kReconcileSink)) /
                                ("subtask-" + std::to_string(capture_idx)) /
                                ("epoch-" + std::to_string(epoch) + ".cap"),
                            std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
        }
        std::string inserts = "[";
        if (!rows.empty()) {
            inserts += R"({"token":"clink1-00-1","seq":1,"rows":)" + std::to_string(rows.size()) +
                       R"(,"attempts":)" + (resend.empty() ? "1" : "2") + R"(,"in_doubt":)" +
                       (resend.empty() ? "false" : "true") + R"(,"resend":)" +
                       (resend.empty() ? "null" : "\"" + resend + "\"") +
                       R"(,"server":"fake 26.8.15 at 127.0.0.1:9000","acknowledged_at_ms":1})";
        }
        inserts += "]";
        const std::string sink =
            R"({"connector":"clickhouse_native","guarantee":"at_least_once","subtask":)" +
            std::to_string(sink_subtask) + R"(,"parallelism":)" + std::to_string(parallelism) +
            R"(,"destination":)" + destination + R"(,"token_nonce":"00","inserts":)" + inserts +
            R"(,"inserts_unlisted":0,"rows_acknowledged":)" + std::to_string(rows.size()) + "}";
        ASSERT_TRUE(clink::capture::write_sink_interval(cap, h, sink));
    }

    [[nodiscard]] fs::path collect_bundle(std::uint64_t from_epoch,
                                          std::uint64_t to_epoch,
                                          const std::string& name = "bundle") const {
        CollectOptions o;
        o.checkpoint_dir = ckpt.string();
        o.job = kReconcileJob;
        o.commit_confirmation = false;
        o.include_state = true;
        o.include_captures = true;
        o.capture_dir = cap.string();
        o.epochs = std::pair{from_epoch, to_epoch};
        o.out = (root / name).string();
        return collect(o).bundle;
    }

    // Checkpoints 1 and 2 complete; the sink's two subtasks wrote rows 1-4 in
    // the interval ending at barrier 2, and the destination holds them.
    fs::path healthy() {
        write_checkpoint(1);
        write_checkpoint(2);
        write_interval(1, 0, 0, 2, {row_json(100, "1.00")});
        write_interval(1, 0, 1, 2, {});
        write_interval(2, 1, 0, 2, {row_json(1, "10.50"), row_json(2, "20.00", "late")});
        write_interval(2, 1, 1, 2, {row_json(3, "-0.05"), row_json(4, "7")});
        dest.rows = {observed_row(100, "D100@2"),
                     observed_row(1, "D1050@2"),
                     observed_row(2, "D2000@2", "Slate"),
                     observed_row(3, "D-5@2"),
                     observed_row(4, "D700@2")};
        return collect_bundle(1, 2);
    }

    // healthy(), with the Kafka source's positions and log in both checkpoints,
    // which a reference's scope needs.
    static constexpr const char* kLog =
        R"({"version":1,"topic":"events","cluster_id":"c1","topic_id":"t1"})";
    // Two empty intervals of both sink subtasks after checkpoint 2: the run
    // had stopped writing, which a whole-table read needs to see.
    void quiet_after_two() const {
        write_checkpoint_with_offsets(3, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
        write_checkpoint_with_offsets(4, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
        for (const std::uint64_t e : {std::uint64_t{3}, std::uint64_t{4}}) {
            write_interval(e, e - 1, 0, 2, {});
            write_interval(e, e - 1, 1, 2, {});
        }
    }

    fs::path audited() {
        write_checkpoint_with_offsets(1, {{0, {{0, 10}}}, {1, {{1, 20}}}}, kLog);
        write_checkpoint_with_offsets(2, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
        quiet_after_two();
        write_interval(1, 0, 0, 2, {row_json(100, "1.00")});
        write_interval(1, 0, 1, 2, {});
        write_interval(2, 1, 0, 2, {row_json(1, "10.50"), row_json(2, "20.00", "late")});
        write_interval(2, 1, 1, 2, {row_json(3, "-0.05"), row_json(4, "7")});
        dest.rows = {observed_row(100, "D100@2"),
                     observed_row(1, "D1050@2"),
                     observed_row(2, "D2000@2", "Slate"),
                     observed_row(3, "D-5@2"),
                     observed_row(4, "D700@2")};
        return collect_bundle(1, 4);
    }

    // The rows the job owes checkpoint 2 in audited(), as a reference computes them.
    static std::vector<Row> owed() {
        return {observed_row(1, "D1050@2"),
                observed_row(2, "D2000@2", "Slate"),
                observed_row(3, "D-5@2"),
                observed_row(4, "D700@2")};
    }

    // The audit's scope document, as incident-scope writes it.
    static JsonValue scope_of(const fs::path& bundle, std::uint64_t checkpoint, bool whole_run) {
        const Bundle b = read_bundle(bundle);
        return scope_document(b, load_scope(b, checkpoint, 0, whole_run));
    }

    reconcile::Report audit(const fs::path& bundle,
                            const std::vector<Row>& reference,
                            Options o = {},
                            bool complete = true,
                            const std::vector<std::string>& incomplete = {}) {
        const auto path = root / "reference.ndjson";
        const bool whole = o.from_run_start || o.coverage == Options::Coverage::CompleteTable;
        write_reference(path,
                        scope_of(bundle, o.checkpoint != 0 ? o.checkpoint : 2, whole),
                        reference,
                        complete,
                        incomplete);
        o.reference = path.string();
        return reconcile(bundle, o);
    }

    reconcile::Report reconcile(const fs::path& bundle, Options o = {}) {
        if (o.key.empty()) {
            o.key = {"id"};
        }
        o.settle = 0ms;
        if (o.checkpoint == 0) {
            o.checkpoint = 2;
        }
        return run(
            bundle,
            [&](const std::string&, const std::string&) -> std::unique_ptr<Destination> {
                return std::make_unique<ForwardingDestination>(dest);
            },
            o);
    }

    // The factory hands over ownership; the test keeps the fake.
    class ForwardingDestination final : public Destination {
    public:
        explicit ForwardingDestination(FakeDestination& d) : d_(d) {}
        DestinationDescription describe() override { return d_.describe(); }
        std::vector<Mutation> mutations() override { return d_.mutations(); }
        std::vector<Row> read(const std::vector<Column>& c,
                              const std::vector<std::size_t>& k,
                              const std::vector<Row>& keys) override {
            return d_.read(c, k, keys);
        }
        std::vector<Row> read_all(const std::vector<Column>& c, std::uint64_t max_rows) override {
            return d_.read_all(c, max_rows);
        }
        std::string parts_marker() override { return d_.parts_marker(); }

    private:
        FakeDestination& d_;
    };
};

const Check& check_of(const reconcile::Report& r, const std::string& id) {
    for (const auto& c : r.checks) {
        if (c.id == id) {
            return c;
        }
    }
    static Check none;
    ADD_FAILURE() << "no check " << id;
    return none;
}

TEST_F(ReconcileBundle, AHealthyCheckpointReconciles) {
    const auto r = reconcile(healthy());
    ASSERT_FALSE(r.refused.has_value()) << *r.refused;
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    ASSERT_TRUE(r.comparison.has_value());
    // The interval ending at barrier 2 only: row 100 belongs to checkpoint 1.
    EXPECT_EQ(r.comparison->expected_rows, 4U);
    EXPECT_EQ(r.comparison->matched, 4U);
    EXPECT_EQ(r.scope.epochs, std::vector<std::uint64_t>{2});
    EXPECT_EQ(r.scope.starts_after, 1U);
    const auto json = clink::config::parse(r.render_json());
    EXPECT_EQ(json.at("format").as_string(), kReportFormat);
    EXPECT_EQ(json.at("version").as_int(), kReportVersion);
    EXPECT_EQ(json.at("result").as_string(), "satisfied");
    EXPECT_FALSE(json.at("observation").at("from").as_string().empty());
}

TEST_F(ReconcileBundle, MissingExtraDuplicatedAndChangedRowsEachFail) {
    const auto bundle = healthy();
    const auto base = dest.rows;
    // Missing.
    dest.rows = {base[0], base[1], base[2], base[3]};
    auto r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.comparison->missing, 1U);
    // Duplicated.
    dest.rows = base;
    dest.rows.push_back(base[2]);
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.comparison->duplicated, 1U);
    EXPECT_NE(check_of(r, "reconcile.rows").details.end(),
              std::find_if(check_of(r, "reconcile.rows").details.begin(),
                           check_of(r, "reconcile.rows").details.end(),
                           [](const std::string& d) {
                               return d.find("records no resend") != std::string::npos;
                           }));
    // Changed.
    dest.rows = base;
    dest.rows[3] = observed_row(3, "D-6@2");
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.comparison->mismatched, 1U);
    // Unexpected: a second row for an in-scope key with other values.
    dest.rows = base;
    dest.rows.push_back(observed_row(4, "D701@2"));
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.comparison->unexpected, 1U);
    // Rows of other keys are outside the scope.
    dest.rows = base;
    dest.rows.push_back(observed_row(999, "D1@2"));
    EXPECT_EQ(reconcile(bundle).exit_code(), 0);
}

TEST_F(ReconcileBundle, EqualCountsAndTotalsWithDifferentRowsFail) {
    const auto bundle = healthy();
    // 10.50 + 20.00 = 15.25 + 15.25, and every row is still there.
    dest.rows[1] = observed_row(1, "D1525@2");
    dest.rows[2] = observed_row(2, "D1525@2", "Slate");
    const auto r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.comparison->observed_rows, r.comparison->expected_rows);
    EXPECT_EQ(r.comparison->mismatched, 2U);
}

TEST_F(ReconcileBundle, DuplicatesTheEvidenceExplainsAreAttributed) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_interval(
        2, 1, 0, 1, {row_json(1, "1.00")}, Mode::Written, destination_json(), "may_duplicate");
    dest.rows = {observed_row(1, "D100@2"), observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 1);
    const auto text = r.render_text();
    EXPECT_NE(text.find("resent after a send left them in doubt"), std::string::npos) << text;
}

TEST_F(ReconcileBundle, AKeptTokenResendThatDidNotDuplicatePassesAndSaysSo) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_interval(
        2, 1, 0, 1, {row_json(1, "1.00")}, Mode::Written, destination_json(), "deduplicated");
    dest.rows = {observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    EXPECT_NE(r.render_text().find("none is duplicated"), std::string::npos);
}

TEST_F(ReconcileBundle, ACheckpointThatDidNotCompleteWidensTheScope) {
    write_checkpoint(1);
    // Checkpoint 2 was triggered and its barrier passed the sink, but it never
    // completed; checkpoint 3 did, so it completed both intervals.
    write_checkpoint(3);
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    write_interval(3, 2, 0, 1, {row_json(2, "2.00")});
    dest.rows = {observed_row(1, "D100@2"), observed_row(2, "D200@2")};
    Options o;
    o.checkpoint = 3;
    const auto r = reconcile(collect_bundle(2, 3), o);
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    EXPECT_EQ(r.scope.epochs, (std::vector<std::uint64_t>{2, 3}));
    EXPECT_EQ(r.comparison->expected_rows, 2U);
}

TEST_F(ReconcileBundle, WrongRunCheckpointOrDestinationIsRefused) {
    const auto bundle = healthy();
    Options o;
    o.job = 99;
    EXPECT_EQ(reconcile(bundle, o).exit_code(), 2);
    o = {};
    o.checkpoint = 7;
    const auto r = reconcile(bundle, o);
    EXPECT_EQ(r.exit_code(), 2);
    EXPECT_NE(r.refused->find("not a completed checkpoint"), std::string::npos);
    dest.desc.table_uuid = "00000000-0000-0000-0000-000000000000";
    EXPECT_EQ(reconcile(bundle).exit_code(), 2);
    dest = FakeDestination{};
    dest.desc.server_uuid = "ffffffff-0000-0000-0000-000000000000";
    EXPECT_EQ(reconcile(bundle).exit_code(), 2);
    o = {};
    o.table = "other";
    EXPECT_EQ(reconcile(bundle, o).exit_code(), 2);
    o = {};
    o.key = {"nope"};
    EXPECT_EQ(reconcile(bundle, o).exit_code(), 2);
}

TEST_F(ReconcileBundle, MissingTruncatedOrDisarmedEvidenceCannotPass) {
    write_checkpoint(1);
    write_checkpoint(2);
    // Parallelism 2, one subtask's evidence absent.
    write_interval(2, 1, 0, 2, {row_json(1, "1.00")});
    dest.rows = {observed_row(1, "D100@2")};
    auto r = reconcile(collect_bundle(2, 2, "missing"));
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_EQ(check_of(r, "reconcile.scope").verdict, Verdict::Unavailable);

    fs::remove_all(cap);
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")}, Mode::Truncated);
    r = reconcile(collect_bundle(2, 2, "truncated"));
    EXPECT_EQ(r.exit_code(), 3);

    fs::remove_all(cap);
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")}, Mode::Disarmed);
    r = reconcile(collect_bundle(2, 2, "disarmed"));
    EXPECT_EQ(r.exit_code(), 3);

    // Checkpoint 6 did not complete and 7 did, so 7 completed both intervals,
    // and the bundle holds only the one ending at 7.
    fs::remove_all(cap);
    write_interval(6, 2, 0, 1, {row_json(1, "1.00")});
    write_interval(7, 6, 0, 1, {row_json(2, "2.00")});
    write_checkpoint(7);
    Options o;
    o.checkpoint = 7;
    r = reconcile(collect_bundle(7, 7, "short"), o);
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_NE(check_of(r, "reconcile.scope").reason.find("barrier 6"), std::string::npos);
}

TEST_F(ReconcileBundle, AnEmptyScopeOrALedgerThatDisagreesWithTheInputCannotPass) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_interval(2, 1, 0, 1, {});
    auto r = reconcile(collect_bundle(2, 2, "empty"));
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_NE(check_of(r, "reconcile.rows").reason.find("nothing to compare"), std::string::npos);

    // The sink acknowledged more rows than its recorded input holds.
    fs::remove_all(cap);
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    const auto ev =
        cap / ("op-" + std::to_string(kReconcileSink)) / "subtask-10" / "epoch-2.sink.json";
    auto doc = clink::config::parse(reconcile_slurp(ev));
    doc.as_object()["sink"].as_object()["rows_acknowledged"] = JsonValue{std::int64_t{5}};
    reconcile_spill(ev, doc.serialize(2));
    dest.rows = {observed_row(1, "D100@2")};
    r = reconcile(collect_bundle(2, 2, "ledger"));
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_NE(check_of(r, "reconcile.scope").reason.find("acknowledged 5 rows"), std::string::npos);
}

TEST_F(ReconcileBundle, AModifiedBundleFailsIntegrityAndIsNotRead) {
    const auto bundle = healthy();
    const auto ev = bundle / "capture" / ("op-" + std::to_string(kReconcileSink)) / "subtask-10" /
                    "epoch-2.cap";
    ASSERT_TRUE(fs::exists(ev));
    std::ofstream(ev, std::ios::app) << "x";
    const auto r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(dest.reads, 0) << "a bundle that failed integrity was compared";
}

TEST_F(ReconcileBundle, QueryLimitsTimeoutsAndInstabilityCannotPass) {
    const auto bundle = healthy();
    dest.fail = "Timeout exceeded: elapsed 1001 ms";
    auto r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_EQ(check_of(r, "reconcile.rows").verdict, Verdict::Unavailable);
    dest.fail.reset();
    // A row of the scope lands between the two reads.
    const auto base = dest.rows;
    dest.rows.pop_back();
    dest.reads = 0;
    dest.before_read = [&](int n) {
        if (n == 2) {
            dest.rows = base;
        }
    };
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_EQ(r.comparison->unstable_keys, 1U);
    dest.before_read = {};
    dest.rows = base;
    dest.muts = {{"mutation_3.txt", "(DELETE WHERE id = 3)", false, "2026-11-10 12:00:00"}};
    EXPECT_EQ(reconcile(bundle).exit_code(), 3);
    // A mutation done before the read is the table's state: noted, and the rows decide.
    dest.muts = {
        {"mutation_3.txt", "(UPDATE amount = 0 WHERE id = 3)", true, "2026-11-10 12:00:00"}};
    dest.rows[3] = observed_row(3, "D0@2");
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.mutations_noted.size(), 1U);
}

TEST_F(ReconcileBundle, AnEngineThatRewritesRowsOrATtlIsUnsupported) {
    const auto bundle = healthy();
    dest.desc.engine = "ReplacingMergeTree";
    auto r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_EQ(check_of(r, "reconcile.destination").verdict, Verdict::Unsupported);
    dest = FakeDestination{};
    dest.rows.clear();
    dest.desc.create_table_query =
        "CREATE TABLE default.events (id Int64) ENGINE = MergeTree ORDER BY id TTL now()";
    EXPECT_EQ(reconcile(bundle).exit_code(), 3);
    dest = FakeDestination{};
    dest.desc.columns[1].second = "Decimal(18, 4)";
    r = reconcile(bundle);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_NE(check_of(r, "reconcile.destination").reason.find("was Decimal(18, 2)"),
              std::string::npos);
}

TEST_F(ReconcileBundle, AKeyThatDoesNotIdentifyARowIsUnsupported) {
    write_checkpoint(1);
    write_checkpoint(2);
    write_interval(2, 1, 0, 1, {row_json(1, "1.00"), row_json(1, "2.00")});
    dest.rows = {observed_row(1, "D100@2"), observed_row(1, "D200@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_EQ(check_of(r, "reconcile.scope").verdict, Verdict::Unsupported);
}

TEST_F(ReconcileBundle, SourcePositionsAreTheSourcesOwnNotACopyARestoreLeftElsewhere) {
    write_checkpoint_with_offsets(1, {{0, {{0, 10}}}, {1, {{1, 20}}}});
    // After a restore every subtask holds a copy of the positions at the
    // restore point, and only the source's own subtasks move on.
    write_checkpoint_with_offsets(2, {{0, {{0, 50}}}, {1, {{1, 60}}}, {2, {{0, 10}, {1, 20}}}});
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    dest.rows = {observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    ASSERT_EQ(r.scope.source.ranges.size(), 2U);
    for (const auto& range : r.scope.source.ranges) {
        ASSERT_TRUE(range.from && range.to);
        EXPECT_EQ(*range.from, range.partition == 0 ? 10 : 20);
        EXPECT_EQ(*range.to, range.partition == 0 ? 50 : 60);
    }
    EXPECT_TRUE(r.scope.source.from_complete);
    EXPECT_TRUE(r.scope.source.to_complete);
    bool noted = false;
    for (const auto& n : r.scope.source.notes) {
        noted = noted || n.find("set aside") != std::string::npos;
    }
    EXPECT_TRUE(noted);
}

// The log a source's positions belong to, as the source records it beside
// them: the topic, the cluster's id and the topic's. The scope names it, and
// says when the two ends recorded different logs, since a topic deleted and
// created again starts its offsets at 0 and the scope's offsets are then not
// one range. Either way the verdict is the destination's against the sink's
// evidence, which the source's log does not change.
std::string reconcile_source_log(const std::string& topic_id) {
    return R"({"version":1,"topic":"events","cluster_id":"5L6g3nShT-eMCtK--X86sw","topic_id":")" +
           topic_id + R"("})";
}

TEST_F(ReconcileBundle, TheScopeNamesTheLogItsSourcePositionsBelongTo) {
    write_checkpoint_with_offsets(
        1, {{0, {{0, 10}}}}, reconcile_source_log("GvDNfajHQH-CpHyoVE2vjw"));
    write_checkpoint_with_offsets(
        2, {{0, {{0, 50}}}}, reconcile_source_log("GvDNfajHQH-CpHyoVE2vjw"));
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    dest.rows = {observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    ASSERT_EQ(r.scope.source.identities.size(), 1U);
    const auto& id = r.scope.source.identities.front();
    EXPECT_EQ(id.op, 4242U);
    ASSERT_TRUE(id.start.has_value() && id.end.has_value());
    EXPECT_EQ(id.end->topic, "events");
    EXPECT_EQ(id.end->cluster_id, "5L6g3nShT-eMCtK--X86sw");
    EXPECT_EQ(id.end->topic_id, "GvDNfajHQH-CpHyoVE2vjw");
    EXPECT_EQ(id.same_log(), std::optional<bool>{true});

    const auto json = clink::config::parse(r.render_json());
    const auto& identities = json.at("scope").at("source").at("identities").as_array();
    ASSERT_EQ(identities.size(), 1U);
    EXPECT_EQ(identities.front().at("op").as_string(), "4242");
    EXPECT_EQ(identities.front().at("end").at("topic_id").as_string(), "GvDNfajHQH-CpHyoVE2vjw");
    EXPECT_TRUE(identities.front().at("same_log").as_bool());
    EXPECT_NE(r.render_text().find("topic 'events' on cluster 5L6g3nShT-eMCtK--X86sw, topic id "
                                   "GvDNfajHQH-CpHyoVE2vjw"),
              std::string::npos)
        << r.render_text();
}

TEST_F(ReconcileBundle, PositionsOfTwoLogsAreNotOneRange) {
    write_checkpoint_with_offsets(
        1, {{0, {{0, 10}}}}, reconcile_source_log("GvDNfajHQH-CpHyoVE2vjw"));
    write_checkpoint_with_offsets(
        2, {{0, {{0, 5}}}}, reconcile_source_log("ONi0POJbSLG7hFMOFT2-cg"));
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    dest.rows = {observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    ASSERT_EQ(r.scope.source.identities.size(), 1U);
    EXPECT_EQ(r.scope.source.identities.front().same_log(), std::optional<bool>{false});
    bool noted = false;
    for (const auto& n : r.scope.source.notes) {
        noted = noted || (n.find("GvDNfajHQH-CpHyoVE2vjw") != std::string::npos &&
                          n.find("ONi0POJbSLG7hFMOFT2-cg") != std::string::npos &&
                          n.find("not a range of one log") != std::string::npos);
    }
    EXPECT_TRUE(noted) << r.render_text();
    EXPECT_FALSE(clink::config::parse(r.render_json())
                     .at("scope")
                     .at("source")
                     .at("identities")
                     .as_array()
                     .front()
                     .at("same_log")
                     .as_bool());
}

TEST_F(ReconcileBundle, CheckpointsThatRecordNoSourceLogSaySo) {
    write_checkpoint_with_offsets(1, {{0, {{0, 10}}}});
    write_checkpoint_with_offsets(2, {{0, {{0, 50}}}});
    write_interval(2, 1, 0, 1, {row_json(1, "1.00")});
    dest.rows = {observed_row(1, "D100@2")};
    const auto r = reconcile(collect_bundle(2, 2));
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    ASSERT_EQ(r.scope.source.identities.size(), 1U);
    EXPECT_FALSE(r.scope.source.identities.front().start.has_value());
    EXPECT_FALSE(r.scope.source.identities.front().end.has_value());
    bool noted = false;
    for (const auto& n : r.scope.source.notes) {
        noted = noted || n.find("record no log for the sources' positions") != std::string::npos;
    }
    EXPECT_TRUE(noted) << r.render_text();
}

// ---- the independent reference audit -------------------------------------------

// Multisets of whole rows, paired by key: multiplicity counts, a further copy
// of an expected row is a duplicate, a row left under its key is changed, and
// the rest are missing or unexpected. Keys need not be unique.
TEST(ReconcileMultiset, MultiplicityPairingAndInstabilityAreCounted) {
    const Row a = {"I1", "D100@2", "N"};
    const Row a2 = {"I1", "D200@2", "N"};
    const Row b = {"I2", "D300@2", "N"};
    const std::vector<std::size_t> key{0};
    auto c = compare_multiset({a, a}, key, {a}, {a}, 10);
    EXPECT_EQ(c.matched, 1U);
    EXPECT_EQ(c.missing, 1U);
    c = compare_multiset({a}, key, {a, a, a}, {a, a, a}, 10);
    EXPECT_EQ(c.matched, 1U);
    EXPECT_EQ(c.duplicated, 2U);
    EXPECT_EQ(c.unexpected, 0U);
    c = compare_multiset({a, a}, key, {a, a}, {a, a}, 10);
    EXPECT_EQ(c.matched, 2U);
    EXPECT_EQ(c.duplicated + c.missing + c.unexpected + c.mismatched, 0U);
    c = compare_multiset({a}, key, {a2}, {a2}, 10);
    EXPECT_EQ(c.mismatched, 1U);
    ASSERT_EQ(c.listed.size(), 1U);
    EXPECT_EQ(c.listed[0].differing, std::vector<std::size_t>{1});
    c = compare_multiset({a}, key, {a, b}, {a, b}, 10);
    EXPECT_EQ(c.unexpected, 1U);
    c = compare_multiset({}, key, {}, {}, 10);
    EXPECT_EQ(c.expected_rows + c.observed_rows, 0U);
    // A key read differently twice is not compared.
    c = compare_multiset({a, b}, key, {a, b}, {a2, b}, 10);
    EXPECT_EQ(c.unstable_keys, 1U);
    EXPECT_EQ(c.matched, 1U);
    EXPECT_EQ(c.mismatched, 0U);
}

TEST(ReconcileMultiset, OnUniqueKeysItAgreesWithTheKeyedComparison) {
    const std::vector<std::size_t> key{0};
    const std::vector<Row> expected = {{"I1", "Sa"}, {"I2", "Sb"}, {"I3", "Sc"}, {"I4", "Sd"}};
    const std::vector<Row> observed = {
        {"I1", "Sa"}, {"I2", "Sx"}, {"I4", "Sd"}, {"I4", "Sd"}, {"I5", "Se"}};
    const auto m = compare_multiset(expected, key, observed, observed, 10);
    const auto k = compare(expected, key, observed, observed, 10);
    EXPECT_EQ(std::tuple(m.matched, m.missing, m.mismatched, m.unexpected, m.duplicated),
              std::tuple(k.matched, k.missing, k.mismatched, k.unexpected, k.duplicated));
}

TEST(ReconcileMultiset, ACellIsCanonicalOnlyForItsColumnsType) {
    const auto t = [](const char* spelling) { return parse_column_type(spelling); };
    EXPECT_TRUE(canonical_for("I-12", t("Int64")));
    EXPECT_FALSE(canonical_for("I-12", t("UInt64")));
    EXPECT_FALSE(canonical_for("I012", t("Int64")));
    EXPECT_FALSE(canonical_for("I-0", t("Int64")));
    EXPECT_TRUE(canonical_for("D-5@2", t("Decimal(18, 2)")));
    EXPECT_FALSE(canonical_for("D-5@3", t("Decimal(18, 2)")));
    EXPECT_TRUE(canonical_for("F3ff0000000000000", t("Float64")));
    EXPECT_FALSE(canonical_for("F3FF0000000000000", t("Float64")));
    EXPECT_TRUE(canonical_for("Fnan", t("Float64")));
    EXPECT_TRUE(canonical_for("f3f800000", t("Float32")));
    EXPECT_TRUE(canonical_for("T1700000000123@3", t("DateTime64(3, 'UTC')")));
    EXPECT_FALSE(canonical_for("T1700000000123@6", t("DateTime64(3, 'UTC')")));
    EXPECT_TRUE(canonical_for("B1", t("Bool")));
    EXPECT_TRUE(canonical_for("S", t("String")));
    EXPECT_TRUE(canonical_for("N", t("Nullable(String)")));
    EXPECT_TRUE(canonical_for("d19000", t("Date")));
    EXPECT_FALSE(canonical_for("!not exact", t("Decimal(18, 2)")));
    EXPECT_FALSE(canonical_for("", t("String")));
}

// The healthy run: the reference, the captured input and the destination
// agree, every check passes, and the result says what it covered.
TEST_F(ReconcileBundle, AReferenceThatAgreesWithTheJobAndTheDestinationPasses) {
    const auto r = audit(audited(), owed());
    ASSERT_FALSE(r.refused.has_value()) << *r.refused;
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    for (const char* id : {"reconcile.scope",
                           "reference.scope",
                           "reference.capture",
                           "reconcile.destination",
                           "reconcile.rows",
                           "reference.destination"}) {
        EXPECT_EQ(check_of(r, id).verdict, Verdict::Passed) << id << ": " << r.render_text();
    }
    EXPECT_TRUE(r.findings.empty());
    EXPECT_EQ(r.coverage.mode, "selected-keys");
    EXPECT_EQ(r.coverage.keys_selected, 4U);
    EXPECT_FALSE(r.coverage.complete_established);
    EXPECT_NE(r.render_text().find("for the selected keys; rows of other keys were not read"),
              std::string::npos);
    const auto json = clink::config::parse(r.render_json());
    EXPECT_TRUE(json.at("audit").at("reference").at("complete").as_bool());
    EXPECT_EQ(json.at("audit").at("reference").at("rows").as_int(), 4);
    EXPECT_EQ(json.at("coverage").at("mode").as_string(), "selected-keys");
}

// The decisive case: the job computes a wrong value and writes it faithfully.
// The captured input and the destination agree, so reconciliation alone
// passes; the reference does not, and the audit names the transformation.
TEST_F(ReconcileBundle, AWrongTransformationWrittenFaithfullyFailsAgainstTheReference) {
    const auto bundle = audited();
    auto reference = owed();
    reference[0] = observed_row(1, "D1051@2");  // what the transformation should have computed
    EXPECT_EQ(reconcile(bundle).exit_code(), 0) << "reconciliation alone cannot see it";
    const auto r = audit(bundle, reference);
    EXPECT_EQ(r.exit_code(), 1) << r.render_text();
    EXPECT_EQ(check_of(r, "reconcile.rows").verdict, Verdict::Passed);
    EXPECT_EQ(check_of(r, "reference.capture").verdict, Verdict::Failed);
    EXPECT_EQ(check_of(r, "reference.destination").verdict, Verdict::Failed);
    ASSERT_TRUE(r.reference_capture.has_value());
    EXPECT_EQ(r.reference_capture->mismatched, 1U);
    ASSERT_FALSE(r.findings.empty());
    EXPECT_EQ(r.findings.front().relationship, "transformation");
    bool delivery = false;
    for (const auto& f : r.findings) {
        delivery = delivery || f.relationship == "delivery";
    }
    EXPECT_FALSE(delivery) << "the delivery was faithful";
}

// A row the job dropped is looked for under the reference's key, and one it
// introduced is read under the captured key: each fails where it belongs.
TEST_F(ReconcileBundle, ARowDroppedOrIntroducedByTheJobFails) {
    const auto bundle = audited();
    auto with_five = owed();
    with_five.push_back(observed_row(5, "D500@2"));
    auto r = audit(bundle, with_five);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.reference_capture->missing, 1U);
    EXPECT_EQ(r.reference_destination->missing, 1U);
    EXPECT_EQ(r.coverage.keys_selected, 5U) << "the reference's key is read too";
    EXPECT_EQ(check_of(r, "reconcile.rows").verdict, Verdict::Passed);

    auto without_four = owed();
    without_four.pop_back();
    r = audit(bundle, without_four);
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(r.reference_capture->unexpected, 1U);
    EXPECT_EQ(r.reference_destination->unexpected, 1U);
}

// A destination fault with a right transformation: the delivery fails, the
// transformation passes, and the findings say which.
TEST_F(ReconcileBundle, ADeliveryFaultIsToldApartFromTheTransformation) {
    const auto bundle = audited();
    dest.rows.erase(dest.rows.begin() + 3);                    // row 3 lost
    dest.rows.push_back(observed_row(2, "D2000@2", "Slate"));  // row 2 twice
    const auto r = audit(bundle, owed());
    EXPECT_EQ(r.exit_code(), 1);
    EXPECT_EQ(check_of(r, "reference.capture").verdict, Verdict::Passed);
    EXPECT_EQ(check_of(r, "reconcile.rows").verdict, Verdict::Failed);
    EXPECT_EQ(check_of(r, "reference.destination").verdict, Verdict::Failed);
    std::set<std::string> rel;
    for (const auto& f : r.findings) {
        rel.insert(f.relationship);
    }
    EXPECT_TRUE(rel.contains("delivery"));
    EXPECT_FALSE(rel.contains("transformation"));
}

// A reference that says it is incomplete is never compared, and leaves the
// audit partial; a failure observed elsewhere still decides the result.
TEST_F(ReconcileBundle, AnIncompleteReferenceIsPartialAndAFailureStillWins) {
    const auto bundle = audited();
    auto r = audit(bundle, owed(), {}, false, {"partition 0: offsets [10, 12) expired"});
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_EQ(check_of(r, "reference.scope").verdict, Verdict::Unavailable);
    EXPECT_NE(check_of(r, "reference.scope").reason.find("expired"), std::string::npos);
    EXPECT_EQ(check_of(r, "reference.capture").verdict, Verdict::Unavailable);
    dest.rows.pop_back();
    r = audit(bundle, owed(), {}, false, {"partition 0: offsets [10, 12) expired"});
    EXPECT_EQ(r.exit_code(), 1);
}

// Malformed, unfinished, tampered, or computed for another scope: refused,
// with what is wrong.
TEST_F(ReconcileBundle, AReferenceThatIsMalformedOrForAnotherScopeIsRefused) {
    const auto bundle = audited();
    const auto path = root / "reference.ndjson";
    const auto refused_with = [&](const std::string& why) {
        Options o;
        o.reference = path.string();
        const auto r = reconcile(bundle, o);
        EXPECT_EQ(r.exit_code(), 2) << r.render_text();
        EXPECT_TRUE(r.refused.has_value() && r.refused->find(why) != std::string::npos)
            << (r.refused ? *r.refused : std::string{"not refused"});
    };
    write_reference(path, scope_of(bundle, 1, false), owed());
    refused_with("checkpoint is 1 in the reference and 2 in this audit's scope");
    const auto doc = scope_of(bundle, 2, false);
    write_reference(path, doc, owed(), true, {}, [](auto& lines) { lines.pop_back(); });
    refused_with("no end line");
    write_reference(path, doc, owed(), true, {}, [](auto& lines) {
        lines[1].replace(lines[1].find("D1050@2"), 7, "D1051@2");
    });
    refused_with("do not hash");
    write_reference(path, doc, {observed_row(1, "D105@1")});
    refused_with("not a canonical cell of Decimal(18, 2)");
    write_reference(path, doc, owed(), true, {}, [](auto& lines) {
        lines[1].replace(lines[1].find("\"id\""), 4, "\"ID\"");
    });
    refused_with("not a column of the scope");
    write_reference(path, doc, owed(), true, {}, [](auto& lines) { lines.push_back(lines[1]); });
    refused_with("lines after its end line");
    write_reference(path, doc, owed(), false, {});
    refused_with("does not say why");
}

// Selected keys cannot see a row of a key neither side names; the dedicated
// table, read whole, can, and the report says which coverage it had.
TEST_F(ReconcileBundle, AnUnrelatedKeyFailsOnlyAWholeTableRead) {
    const auto bundle = audited();
    dest.rows.push_back(observed_row(999, "D1@2"));
    auto whole_run = owed();
    whole_run.insert(whole_run.begin(), observed_row(100, "D100@2"));
    Options sel;
    sel.from_run_start = true;
    auto r = audit(bundle, whole_run, sel);
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    EXPECT_FALSE(r.coverage.complete_established);
    EXPECT_NE(r.coverage.complete_reason.find("not established"), std::string::npos);

    Options whole;
    whole.coverage = Options::Coverage::CompleteTable;
    r = audit(bundle, whole_run, whole);
    EXPECT_EQ(r.exit_code(), 1) << r.render_text();
    EXPECT_EQ(check_of(r, "destination.coverage").verdict, Verdict::Failed);
    EXPECT_TRUE(r.coverage.complete_established);
    EXPECT_EQ(r.table_comparison->unexpected, 1U);
    EXPECT_EQ(check_of(r, "reference.destination").verdict, Verdict::Passed);

    dest.rows.pop_back();
    r = audit(bundle, whole_run, whole);
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    EXPECT_EQ(check_of(r, "destination.coverage").verdict, Verdict::Passed);
    EXPECT_EQ(r.coverage.table_rows_read, 5U);
}

// A whole-table read is a claim about one state of the table: parts that
// change across it, a table past the bound, or rows the sink received after
// the checkpoint leave it unavailable.
TEST_F(ReconcileBundle, AWholeTableReadNeedsAStillBoundedTableAndNothingAfter) {
    const auto bundle = audited();
    auto whole_run = owed();
    whole_run.insert(whole_run.begin(), observed_row(100, "D100@2"));
    Options whole;
    whole.coverage = Options::Coverage::CompleteTable;
    dest.before_marker = [&](int n) {
        if (n % 2 == 0) {
            ++dest.parts_version;
        }
    };
    auto r = audit(bundle, whole_run, whole);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_NE(check_of(r, "destination.coverage").reason.find("parts changed"), std::string::npos);
    dest.before_marker = {};
    whole.max_table_rows = 4;
    r = audit(bundle, whole_run, whole);
    EXPECT_EQ(r.exit_code(), 3);
    EXPECT_NE(check_of(r, "destination.coverage").reason.find("could not be read whole"),
              std::string::npos);
}

TEST_F(ReconcileBundle, RowsTheSinkReceivedAfterTheCheckpointLeaveTheWholeTableUnestablished) {
    write_checkpoint_with_offsets(1, {{0, {{0, 10}}}, {1, {{1, 20}}}}, kLog);
    write_checkpoint_with_offsets(2, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
    write_interval(1, 0, 0, 2, {row_json(1, "10.50")});
    write_interval(1, 0, 1, 2, {});
    write_interval(2, 1, 0, 2, {});
    write_interval(2, 1, 1, 2, {});
    write_interval(3, 2, 0, 2, {row_json(7, "1.00")});  // after checkpoint 2, never completed
    write_interval(3, 2, 1, 2, {});
    dest.rows = {observed_row(1, "D1050@2"), observed_row(7, "D100@2")};
    const auto bundle = collect_bundle(1, 3);
    Options whole;
    whole.coverage = Options::Coverage::CompleteTable;
    const auto r = audit(bundle, {observed_row(1, "D1050@2")}, whole);
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_NE(check_of(r, "destination.coverage").reason.find("after checkpoint 2"),
              std::string::npos);
}

// Without two empty intervals of every subtask after the checkpoint, the
// bundle does not show the run had stopped writing.
TEST_F(ReconcileBundle, AWholeTableReadNeedsTheRunSeenQuietAfterTheCheckpoint) {
    write_checkpoint_with_offsets(1, {{0, {{0, 10}}}, {1, {{1, 20}}}}, kLog);
    write_checkpoint_with_offsets(2, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
    write_checkpoint_with_offsets(3, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
    write_interval(1, 0, 0, 2, {row_json(1, "10.50")});
    write_interval(1, 0, 1, 2, {});
    write_interval(2, 1, 0, 2, {});
    write_interval(2, 1, 1, 2, {});
    write_interval(3, 2, 0, 2, {});
    write_interval(3, 2, 1, 2, {});
    dest.rows = {observed_row(1, "D1050@2")};
    const auto bundle = collect_bundle(1, 3);
    Options whole;
    whole.coverage = Options::Coverage::CompleteTable;
    const auto r = audit(bundle, {observed_row(1, "D1050@2")}, whole);
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_NE(check_of(r, "destination.coverage").reason.find("two are needed"), std::string::npos)
        << check_of(r, "destination.coverage").reason;
}

// An empty expected output does not pass by having nothing to compare: with
// selected keys it is unavailable; read whole, an empty dedicated table passes
// and a table holding anything fails.
TEST_F(ReconcileBundle, AnEmptyScopeDoesNotPassVacuously) {
    write_checkpoint_with_offsets(1, {{0, {{0, 10}}}, {1, {{1, 20}}}}, kLog);
    write_checkpoint_with_offsets(2, {{0, {{0, 15}}}, {1, {{1, 25}}}}, kLog);
    write_interval(1, 0, 0, 2, {});
    write_interval(1, 0, 1, 2, {});
    write_interval(2, 1, 0, 2, {});
    write_interval(2, 1, 1, 2, {});
    quiet_after_two();
    dest.rows.clear();
    const auto bundle = collect_bundle(1, 4);
    Options sel;
    sel.from_run_start = true;
    auto r = audit(bundle, {}, sel);
    EXPECT_EQ(r.exit_code(), 3) << r.render_text();
    EXPECT_EQ(check_of(r, "reference.capture").verdict, Verdict::Passed)
        << "both sides established, both empty";
    EXPECT_EQ(check_of(r, "reference.destination").verdict, Verdict::Unavailable);
    Options whole;
    whole.coverage = Options::Coverage::CompleteTable;
    r = audit(bundle, {}, whole);
    EXPECT_EQ(r.exit_code(), 0) << r.render_text();
    EXPECT_EQ(check_of(r, "destination.coverage").verdict, Verdict::Passed);
    EXPECT_EQ(check_of(r, "reference.destination").verdict, Verdict::Passed);
    dest.rows = {observed_row(42, "D1@2")};
    r = audit(bundle, {}, whole);
    EXPECT_EQ(r.exit_code(), 1) << r.render_text();
    EXPECT_EQ(check_of(r, "destination.coverage").verdict, Verdict::Failed);
}

// The command: its usage refusals, a request the bundle does not fit, and a
// destination it cannot reach, each with the exit the workflow's conventions
// give it, and its JSON report versioned.
int reconcile_cli(const std::string& args, std::string* out) {
    const std::string cmd = std::string{CLINK_CLI_BINARY} + " incident-reconcile " + args + " 2>&1";
    FILE* pipe = ::popen(cmd.c_str(), "r");
    if (pipe == nullptr) {
        return -1;
    }
    std::array<char, 4096> buf{};
    std::size_t n = 0;
    while ((n = std::fread(buf.data(), 1, buf.size(), pipe)) > 0) {
        out->append(buf.data(), n);
    }
    const int status = ::pclose(pipe);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

TEST_F(ReconcileBundle, TheCommandExitsAsItsReportSays) {
    const auto bundle = healthy();
    std::string out;
    EXPECT_EQ(reconcile_cli(bundle.string() + " --key=id", &out), 2) << out;
    EXPECT_NE(out.find("--host is required"), std::string::npos) << out;
    out.clear();
    EXPECT_EQ(reconcile_cli(bundle.string() + " --host=127.0.0.1", &out), 2) << out;
    EXPECT_NE(out.find("--key is required"), std::string::npos) << out;
    out.clear();
    EXPECT_EQ(
        reconcile_cli(bundle.string() + " --host=127.0.0.1 --key=id --tls-verify=false", &out), 2);
    out.clear();
    EXPECT_EQ(
        reconcile_cli(
            bundle.string() + " --host=127.0.0.1 --key=id --password-env=CLINK_RECONCILE_UNSET_VAR",
            &out),
        2);
    EXPECT_NE(out.find("not set"), std::string::npos) << out;
    out.clear();
    EXPECT_EQ(
        reconcile_cli((root / "nowhere").string() + " --host=127.0.0.1 --key=id --json", &out), 2);
    out.clear();
    // Nothing listens on port 1: the destination cannot be read, which is a
    // partial result, not a failure and never a pass.
    const int code = reconcile_cli(bundle.string() +
                                       " --host=127.0.0.1 --port=1 --key=id --checkpoint=2 --json "
                                       "--query-timeout-ms=2000",
                                   &out);
    EXPECT_EQ(code, 3) << out;
    const auto json_at = out.find('{');
    ASSERT_NE(json_at, std::string::npos) << out;
    const auto doc = clink::config::parse(out.substr(json_at));
    EXPECT_EQ(doc.at("format").as_string(), kReportFormat);
    EXPECT_EQ(doc.at("version").as_int(), kReportVersion);
    EXPECT_EQ(doc.at("result").as_string(), "partial");
    EXPECT_EQ(doc.at("exit_code").as_int(), 3);
}

TEST_F(ReconcileBundle, TheExpectationExportsAsCanonicalCells) {
    const auto bundle = healthy();
    const auto r = reconcile(bundle);
    const auto nd = expected_ndjson(r.scope);
    EXPECT_NE(nd.find(R"("amount":"D1050@2")"), std::string::npos) << nd;
    EXPECT_NE(nd.find(R"("note":"N")"), std::string::npos);
    EXPECT_EQ(std::count(nd.begin(), nd.end(), '\n'), 4);
}

}  // namespace
