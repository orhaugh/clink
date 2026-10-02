// The open-time target probe over the fake server: the settings gate, the
// line-conditional settings and the pinned squash thresholds, the engine
// families, the effective async_insert from the table, the server and the
// replicated defaults, the deduplication window report, and the Distributed
// probe's rule that a refusal needs a read that succeeded and showed the
// problem, while an unreachable replica surfaces as the client's own error.

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>
#include <gtest/gtest.h>

#include "fake_transport.hpp"
#include "native/errors.hpp"
#include "native/statements.hpp"
#include "native/target_table.hpp"

namespace {

using namespace std::chrono_literals;
namespace ch = ::clickhouse;
namespace native = clink::clickhouse::native;
using native::DefaultKind;
using native::DistributedTarget;
using native::Endpoint;
using native::EngineFamily;
using native::InsertTransport;
using native::MetaQuery;
using native::NativeSinkError;
using native::ResultSet;
using native::SinkOptions;
using native::TargetColumn;
using native::TargetInfo;
using native::testing::FakeServer;
using native::testing::FakeTable;
using native::testing::FakeTransport;
using native::testing::Fault;
using native::testing::Step;

const Endpoint kTtEp{"ch-1", 9000};
const Endpoint kTtEp2{"ch-2", 9440};

FakeTable tt_events(const std::string& engine = "MergeTree",
                    const std::string& engine_full =
                        "MergeTree ORDER BY id SETTINGS "
                        "index_granularity = 8192") {
    FakeTable t;
    t.database = "db";
    t.name = "events";
    t.engine = engine;
    t.engine_full = engine_full;
    t.columns = {{"id", "Int64", DefaultKind::None, 1}, {"s", "String", DefaultKind::None, 2}};
    return t;
}

FakeTable tt_replicated(const std::string& settings = "") {
    return tt_events("ReplicatedMergeTree",
                     "ReplicatedMergeTree('/t/events', '{replica}') ORDER BY id" + settings);
}

std::shared_ptr<FakeServer> tt_server(const FakeTable& table) {
    auto server = std::make_shared<FakeServer>();
    server->add_table(table);
    return server;
}

SinkOptions tt_options(const std::string& table = "events") {
    SinkOptions o;
    o.endpoints = {kTtEp};
    o.database = "db";
    o.table = table;
    o.user = "writer";
    return o;
}

TargetInfo tt_probe(const std::shared_ptr<FakeServer>& server,
                    const SinkOptions& opts = tt_options()) {
    FakeTransport t(server);
    t.connect(kTtEp);
    return native::probe_target(t, opts);
}

struct TtRefusal {
    std::string code;
    std::string what;
};

template <typename F>
TtRefusal tt_refusal(F&& f) {
    try {
        f();
    } catch (const NativeSinkError& e) {
        return {e.code(), e.what()};
    }
    ADD_FAILURE() << "expected a refusal";
    return {};
}

bool tt_contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

// Forwards to a fake transport and lets a test step in on one kind of
// metadata read, to fail it or rewrite its answer, without depending on the
// order the probe issues its reads in.
class TtHookedTransport final : public InsertTransport {
public:
    using Hook = std::function<ResultSet(MetaQuery, const std::string&, InsertTransport&)>;
    TtHookedTransport(std::unique_ptr<InsertTransport> inner, Hook hook)
        : inner_(std::move(inner)), hook_(std::move(hook)) {}

    void connect(const Endpoint& endpoint) override { inner_->connect(endpoint); }
    [[nodiscard]] bool connected() const noexcept override { return inner_->connected(); }
    [[nodiscard]] const native::ServerIdentity& server() const override { return inner_->server(); }
    ResultSet select(MetaQuery kind, const std::string& sql) override {
        return hook_(kind, sql, *inner_);
    }
    std::vector<native::HeaderColumn> begin_insert(const std::string& sql) override {
        return inner_->begin_insert(sql);
    }
    void send_block(const ch::Block& block) override { inner_->send_block(block); }
    void end_insert() override { inner_->end_insert(); }
    void abandon() noexcept override { inner_->abandon(); }
    void interrupt() noexcept override { inner_->interrupt(); }
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept override {
        inner_->set_deadline(deadline);
    }
    [[nodiscard]] native::TransportCounters counters() const noexcept override {
        return inner_->counters();
    }

private:
    std::unique_ptr<InsertTransport> inner_;
    Hook hook_;
};

// Probes through a hook that arms `fault` on the server just before the
// first read of `kind`, so the fake's own fault machinery raises it on
// exactly that read.
TargetInfo tt_probe_failing(const std::shared_ptr<FakeServer>& server,
                            MetaQuery kind,
                            Fault fault) {
    fault.step = Step::Select;
    bool armed = false;
    TtHookedTransport t(std::make_unique<FakeTransport>(server),
                        [&](MetaQuery k, const std::string& sql, InsertTransport& inner) {
                            if (k == kind && !armed) {
                                armed = true;
                                server->inject(fault);
                            }
                            return inner.select(k, sql);
                        });
    t.connect(kTtEp);
    return native::probe_target(t, tt_options());
}

Fault tt_server_fault(int code, const std::string& message) {
    Fault f;
    f.kind = Fault::Kind::ServerError;
    f.code = code;
    f.message = message;
    return f;
}

std::size_t tt_count_statements(const FakeServer& server, const std::string& part) {
    std::size_t n = 0;
    for (const auto& sql : server.statements()) {
        n += tt_contains(sql, part) ? 1 : 0;
    }
    return n;
}

// An initiator holding a Distributed table over cluster "c", whose two
// replicas each hold the local table.
struct TtCluster {
    std::shared_ptr<FakeServer> initiator = std::make_shared<FakeServer>();
    std::shared_ptr<FakeServer> r1 = std::make_shared<FakeServer>();
    std::shared_ptr<FakeServer> r2 = std::make_shared<FakeServer>();

    explicit TtCluster(const std::string& local_engine = "ReplicatedMergeTree") {
        initiator->add_table(
            tt_events("Distributed", "Distributed('c', 'db', 'events_local', rand())"));
        set_local(*r1, local_engine, "");
        set_local(*r2, local_engine, "");
        initiator->add_cluster("c", {r1.get(), r2.get()});
    }

    static void set_local(FakeServer& replica, const std::string& engine, const std::string& tail) {
        FakeTable t = tt_events(engine, engine + "('/t/events', '{replica}') ORDER BY id" + tail);
        if (engine == "MergeTree") {
            t.engine_full = engine + " ORDER BY id" + tail;
        } else if (engine == "Memory") {
            t.engine_full = engine + tail;
        }
        t.name = "events_local";
        replica.add_table(t);
    }
};

// ---------------------------------------------------------------------------
// Pieces

TEST(NativeTargetTablePieces, EngineFamiliesFollowThePrefix) {
    EXPECT_EQ(native::engine_family("MergeTree"), EngineFamily::MergeTree);
    EXPECT_EQ(native::engine_family("ReplacingMergeTree"), EngineFamily::MergeTree);
    EXPECT_EQ(native::engine_family("AggregatingMergeTree"), EngineFamily::MergeTree);
    EXPECT_EQ(native::engine_family("ReplicatedMergeTree"), EngineFamily::ReplicatedMergeTree);
    EXPECT_EQ(native::engine_family("ReplicatedReplacingMergeTree"),
              EngineFamily::ReplicatedMergeTree);
    EXPECT_EQ(native::engine_family("SharedMergeTree"), EngineFamily::SharedMergeTree);
    EXPECT_EQ(native::engine_family("SharedReplacingMergeTree"), EngineFamily::SharedMergeTree);
    EXPECT_EQ(native::engine_family("Distributed"), EngineFamily::Distributed);
    EXPECT_EQ(native::engine_family("Null"), EngineFamily::Null);
    for (const char* other :
         {"Buffer", "Memory", "Log", "TinyLog", "Kafka", "View", "MaterializedView", "Merge", ""}) {
        EXPECT_EQ(native::engine_family(other), EngineFamily::Other) << other;
    }
}

TEST(NativeTargetTablePieces, TestedLinesAre26Point3And26Point8) {
    EXPECT_TRUE(native::is_tested_line(26, 3));
    EXPECT_TRUE(native::is_tested_line(26, 8));
    EXPECT_FALSE(native::is_tested_line(26, 4));
    EXPECT_FALSE(native::is_tested_line(25, 8));
    EXPECT_FALSE(native::is_tested_line(27, 3));
}

TEST(NativeTargetTablePieces, EngineFullSettingReadsTheTopLevelSettingsClause) {
    const std::string full =
        "MergeTree ORDER BY id SETTINGS index_granularity = 8192, async_insert = 1";
    EXPECT_EQ(native::engine_full_setting(full, "async_insert"), "1");
    EXPECT_EQ(native::engine_full_setting(full, "index_granularity"), "8192");
    EXPECT_EQ(native::engine_full_setting(full, "storage_policy"), std::nullopt);
    EXPECT_EQ(native::engine_full_setting("MergeTree ORDER BY id", "async_insert"), std::nullopt);
}

TEST(NativeTargetTablePieces, EngineFullSettingKeepsQuotedValuesWhole) {
    const std::string full =
        "MergeTree ORDER BY id SETTINGS storage_policy = 'hot, cold', async_insert = '0'";
    EXPECT_EQ(native::engine_full_setting(full, "storage_policy"), "'hot, cold'");
    EXPECT_EQ(native::engine_full_setting(full, "async_insert"), "'0'");
}

TEST(NativeTargetTablePieces, EngineFullSettingIgnoresSettingsInsideQuotesAndBrackets) {
    // The word SETTINGS in a ZooKeeper path, a back-quoted column and a TTL
    // expression is not the clause.
    const std::string full =
        "ReplicatedMergeTree('/t/x SETTINGS async_insert = 1', '{replica}') "
        "ORDER BY (id, ` SETTINGS async_insert = 1`) "
        "TTL toDateTime(ts) + toIntervalDay(1) SETTINGS index_granularity = 8192";
    EXPECT_EQ(native::engine_full_setting(full, "async_insert"), std::nullopt);
    EXPECT_EQ(native::engine_full_setting(full, "index_granularity"), "8192");
}

TEST(NativeTargetTablePieces, EngineFullSettingCopesWithASettingsHeavyClause) {
    const std::string full =
        "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/t', '{replica}', ver) "
        "PARTITION BY toYYYYMM(ts) ORDER BY (tenant, id) "
        "SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, "
        "merge_selector_algorithm = 'Simple', replicated_deduplication_window = 1000, "
        "async_insert = true, `max_suspicious_broken_parts` = 5, "
        "storage_policy = 'it''s \\'quoted\\''";
    EXPECT_EQ(native::engine_full_setting(full, "async_insert"), "true");
    EXPECT_EQ(native::engine_full_setting(full, "replicated_deduplication_window"), "1000");
    EXPECT_EQ(native::engine_full_setting(full, "max_suspicious_broken_parts"), "5");
    EXPECT_EQ(native::engine_full_setting(full, "storage_policy"), "'it''s \\'quoted\\''");
}

// The server applies a setting named twice in order, so the table has the
// last value.
TEST(NativeTargetTablePieces, EngineFullSettingTakesTheLastOfARepeatedSetting) {
    const std::string full =
        "MergeTree ORDER BY id SETTINGS async_insert = 0, index_granularity = 8192, "
        "async_insert = 1";
    EXPECT_EQ(native::engine_full_setting(full, "async_insert"), "1");
    EXPECT_EQ(native::engine_full_setting(full, "index_granularity"), "8192");
}

// The storage definition prints SETTINGS last, so a bare SETTINGS word
// earlier in it, in a TTL expression say, is not the clause.
TEST(NativeTargetTablePieces, EngineFullSettingReadsTheLastSettingsClause) {
    EXPECT_EQ(native::engine_full_setting("MergeTree ORDER BY id TTL SETTINGS + toIntervalDay(1) "
                                          "WHERE x = 1 SETTINGS async_insert = 1",
                                          "async_insert"),
              "1");
    EXPECT_EQ(native::engine_full_setting("MergeTree ORDER BY id TTL SETTINGS + toIntervalDay(1) "
                                          "SETTINGS index_granularity = 8192",
                                          "async_insert"),
              std::nullopt);
}

TEST(NativeTargetTablePieces, EngineFullSettingGivesNothingForTextItCannotScan) {
    EXPECT_EQ(native::engine_full_setting("MergeTree ORDER BY (id SETTINGS async_insert = 1",
                                          "async_insert"),
              std::nullopt);
    EXPECT_EQ(native::engine_full_setting("MergeTree ORDER BY id SETTINGS async_insert = 'x",
                                          "async_insert"),
              std::nullopt);
}

TEST(NativeTargetTablePieces, ParseDistributedReadsQuotedArguments) {
    const auto d = native::parse_distributed("Distributed('c', 'db', 'events_local', rand())");
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->cluster, "c");
    EXPECT_EQ(d->database, "db");
    EXPECT_EQ(d->table, "events_local");
    EXPECT_EQ(d->replicas, 0u);
}

TEST(NativeTargetTablePieces, ParseDistributedReadsBareAndBackQuotedIdentifiers) {
    const auto bare = native::parse_distributed("Distributed(c_1, db, events_local)");
    ASSERT_TRUE(bare.has_value());
    EXPECT_EQ(bare->cluster, "c_1");
    EXPECT_EQ(bare->database, "db");
    EXPECT_EQ(bare->table, "events_local");

    const auto mixed = native::parse_distributed(
        "Distributed('my''c', `my db`, 'lo\\'cal', cityHash64(tenant, id), 'policy') "
        "SETTINGS fsync_after_insert = 0");
    ASSERT_TRUE(mixed.has_value());
    EXPECT_EQ(mixed->cluster, "my'c");
    EXPECT_EQ(mixed->database, "my db");
    EXPECT_EQ(mixed->table, "lo'cal");
}

TEST(NativeTargetTablePieces, ParseDistributedRefusesWhatItCannotRead) {
    for (const char* full : {
             "Distributed('c', currentDatabase(), 't')",
             "Distributed('c', '', 't')",
             "Distributed('c', 'db')",
             "Distributed('c', 'db', 'a' || 'b')",
             "Distributed('c', 'db', 't'",
             "Distributed",
             "MergeTree ORDER BY id",
             "Distributed('', 'db', 't')",
             "Distributed('c', 'db', 9t)",
         }) {
        EXPECT_EQ(native::parse_distributed(full), std::nullopt) << full;
    }
}

// ---------------------------------------------------------------------------
// The settings gate and the server line

TEST(NativeTargetTable, AMergeTreeTargetOnA26Point8ServerPasses) {
    FakeTable table = tt_events();
    table.columns = {{"id", "Int64", DefaultKind::None, 1},
                     {"s", "LowCardinality(String)", DefaultKind::Default, 2},
                     {"m", "UInt64", DefaultKind::Materialized, 3},
                     {"a", "UInt64", DefaultKind::Alias, 4},
                     {"e", "String", DefaultKind::Ephemeral, 5}};
    auto server = tt_server(table);
    const TargetInfo info = tt_probe(server);

    EXPECT_EQ(info.server.display_name, server->display_name());
    EXPECT_EQ(info.server.major, 26u);
    EXPECT_EQ(info.server.minor, 8u);
    EXPECT_EQ(info.server.endpoint, kTtEp);
    EXPECT_TRUE(info.tested_line);
    EXPECT_TRUE(info.keep_token_on_resend);
    EXPECT_TRUE(info.caps.deduplicate_insert);
    EXPECT_TRUE(info.caps.use_strict_insert_block_limits);
    EXPECT_FALSE(info.caps.quorum);
    EXPECT_EQ(info.caps.min_insert_block_size_rows, 1048449u);
    EXPECT_EQ(info.caps.min_insert_block_size_bytes, 268402944u);
    EXPECT_EQ(info.caps.max_partitions_per_insert_block, 100u);
    EXPECT_EQ(info.engine, "MergeTree");
    EXPECT_EQ(info.family, EngineFamily::MergeTree);
    EXPECT_FALSE(info.distributed.has_value());

    ASSERT_EQ(info.columns.size(), 5u);
    const std::vector<std::pair<std::string, DefaultKind>> kinds = {
        {"id", DefaultKind::None},
        {"s", DefaultKind::Default},
        {"m", DefaultKind::Materialized},
        {"a", DefaultKind::Alias},
        {"e", DefaultKind::Ephemeral}};
    for (std::size_t i = 0; i < kinds.size(); ++i) {
        EXPECT_EQ(info.columns[i].name, kinds[i].first);
        EXPECT_EQ(info.columns[i].default_kind, kinds[i].second);
        EXPECT_EQ(info.columns[i].position, i + 1);
    }
    EXPECT_EQ(info.columns[1].type, "LowCardinality(String)");

    EXPECT_EQ(info.async_report, "async_insert=0 (table unset, server default 0)");
    EXPECT_EQ(info.dedup_report, "non_replicated_deduplication_window=0 (server default)");
    EXPECT_EQ(info.dedup_window, 0u);
    EXPECT_FALSE(info.keeps_dedup_log);
}

TEST(NativeTargetTable, EveryProbeReadIsBoundedByTheMetadataBudget) {
    TtCluster c;
    SinkOptions opts = tt_options();
    opts.receive_timeout = 3500ms;
    (void)tt_probe(c.initiator, opts);
    const auto statements = c.initiator->statements();
    ASSERT_GE(statements.size(), 7u);
    for (const auto& sql : statements) {
        const std::string tail = "SETTINGS max_execution_time=4, timeout_overflow_mode='throw'";
        EXPECT_TRUE(sql.ends_with(
            tt_contains(sql, "clusterAllReplicas(") ? tail + ", skip_unavailable_shards=0" : tail))
            << sql;
        EXPECT_TRUE(sql.starts_with("SELECT CAST(")) << sql;
    }
}

TEST(NativeTargetTable, AMissingRequiredSettingRefusesWithTheServerAndTheSetting) {
    auto server = tt_server(tt_events());
    server->set_version(25, 3, 2);
    server->remove_setting("distributed_foreground_insert");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kServerSettingsUnsupported);
    EXPECT_EQ(r.what,
              "[clickhouse.server_settings_unsupported] server 25.3.2 at ch-1:9000 lacks settings "
              "the native sink sends with every INSERT: distributed_foreground_insert. Use a "
              "server line that has them; 26.3 and 26.8 are tested.");
    // Nothing about the table is read from a server that fails the gate.
    EXPECT_EQ(server->statements().size(), 1u);
}

TEST(NativeTargetTable, EachRequiredSettingIsRequired) {
    for (const auto& name : native::required_settings()) {
        auto server = tt_server(tt_events());
        server->remove_setting(name);
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
        EXPECT_EQ(r.code, native::code::kServerSettingsUnsupported) << name;
        EXPECT_TRUE(tt_contains(r.what, "every INSERT: " + name + ". Use")) << r.what;
    }
}

TEST(NativeTargetTable, EveryMissingSettingIsNamedInOneRefusal) {
    auto server = tt_server(tt_events());
    server->remove_setting("log_comment");
    server->remove_setting("min_insert_block_size_bytes");
    server->remove_setting("deduplicate_insert");
    server->remove_setting("insert_deduplicate");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kServerSettingsUnsupported);
    EXPECT_TRUE(tt_contains(r.what,
                            "every INSERT: log_comment, min_insert_block_size_bytes, "
                            "deduplicate_insert or insert_deduplicate. Use"))
        << r.what;
}

TEST(NativeTargetTable, EitherDeduplicationSwitchIsEnough) {
    auto older = tt_server(tt_events());
    older->remove_setting("deduplicate_insert");
    const TargetInfo a = tt_probe(older);
    EXPECT_FALSE(a.caps.deduplicate_insert);

    auto newer = tt_server(tt_events());
    newer->remove_setting("insert_deduplicate");
    const TargetInfo b = tt_probe(newer);
    EXPECT_TRUE(b.caps.deduplicate_insert);
}

TEST(NativeTargetTable, LineConditionalSettingsMissingOn26Point3DoNotRefuse) {
    auto server = tt_server(tt_events());
    server->set_version(26, 3, 4);
    for (const auto& name : native::line_conditional_settings()) {
        server->remove_setting(name);
    }
    const TargetInfo info = tt_probe(server);
    EXPECT_FALSE(info.caps.deduplicate_insert);
    EXPECT_FALSE(info.caps.use_strict_insert_block_limits);
    EXPECT_TRUE(info.tested_line);
    EXPECT_TRUE(info.keep_token_on_resend);
}

TEST(NativeTargetTable, UntestedLinesAreAcceptedWithoutKeepingTokens) {
    for (const auto& [major, minor] :
         std::vector<std::pair<std::uint64_t, std::uint64_t>>{{25, 8}, {26, 4}, {27, 1}}) {
        auto server = tt_server(tt_events());
        server->set_version(major, minor, 1);
        const TargetInfo info = tt_probe(server);
        EXPECT_FALSE(info.tested_line) << major << "." << minor;
        EXPECT_FALSE(info.keep_token_on_resend) << major << "." << minor;
        EXPECT_EQ(info.server.major, major);
        EXPECT_EQ(info.server.minor, minor);
    }
}

TEST(NativeTargetTable, TheSquashThresholdsAndTheQuorumAreReadFromTheServer) {
    auto server = tt_server(tt_events());
    server->set_setting("min_insert_block_size_rows", "5000");
    server->set_setting("min_insert_block_size_bytes", "1234567");
    server->set_setting("max_partitions_per_insert_block", "7");
    server->set_setting("insert_quorum", "2");
    const TargetInfo info = tt_probe(server);
    EXPECT_EQ(info.caps.min_insert_block_size_rows, 5000u);
    EXPECT_EQ(info.caps.min_insert_block_size_bytes, 1234567u);
    EXPECT_EQ(info.caps.max_partitions_per_insert_block, 7u);
    EXPECT_TRUE(info.caps.quorum);

    server->set_setting("insert_quorum", "auto");
    EXPECT_TRUE(tt_probe(server).caps.quorum);
    server->set_setting("insert_quorum", "0");
    EXPECT_FALSE(tt_probe(server).caps.quorum);
}

TEST(NativeTargetTable, ASquashThresholdThatIsNotANumberRefuses) {
    auto server = tt_server(tt_events());
    server->set_setting("min_insert_block_size_rows", "lots");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kServerSettingsUnsupported);
    EXPECT_TRUE(tt_contains(r.what, "min_insert_block_size_rows='lots'")) << r.what;
}

TEST(NativeTargetTable, AReprobeOnANewClientDescribesTheServerItReached) {
    auto first = tt_server(tt_events());
    auto second = tt_server(tt_events());
    second->set_version(26, 3, 2);
    second->remove_setting("use_strict_insert_block_limits");
    second->set_setting("min_insert_block_size_rows", "99");

    FakeTransport t(
        std::map<Endpoint, std::shared_ptr<FakeServer>>{{kTtEp, first}, {kTtEp2, second}});
    t.connect(kTtEp);
    const TargetInfo before = native::probe_target(t, tt_options());
    t.abandon();
    t.connect(kTtEp2);
    const TargetInfo after = native::probe_target(t, tt_options());

    EXPECT_EQ(before.server.display_name, first->display_name());
    EXPECT_EQ(after.server.display_name, second->display_name());
    EXPECT_EQ(after.server.endpoint, kTtEp2);
    EXPECT_EQ(after.server.minor, 3u);
    EXPECT_TRUE(before.caps.use_strict_insert_block_limits);
    EXPECT_FALSE(after.caps.use_strict_insert_block_limits);
    // Each probe reports what its own server says; keeping the opener's
    // thresholds is the writer's job.
    EXPECT_EQ(before.caps.min_insert_block_size_rows, 1048449u);
    EXPECT_EQ(after.caps.min_insert_block_size_rows, 99u);

    // A server whose default turned asynchronous refuses on the re-probe.
    second->set_merge_tree_setting("async_insert", "1");
    t.abandon();
    t.connect(kTtEp2);
    EXPECT_EQ(tt_refusal([&] { (void)native::probe_target(t, tt_options()); }).code,
              native::code::kTargetAsyncInsert);
}

// ---------------------------------------------------------------------------
// Target metadata and engines

TEST(NativeTargetTable, AMissingTargetRefuses) {
    auto server = std::make_shared<FakeServer>();
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetMissing);
    EXPECT_TRUE(tt_contains(r.what, "`db`.`events` does not exist")) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "create it first; the sink does not create tables")) << r.what;
}

TEST(NativeTargetTable, SharedMergeTreeTargetsAreRefused) {
    for (const char* engine : {"SharedMergeTree", "SharedReplacingMergeTree"}) {
        auto server = tt_server(tt_events(engine, std::string(engine) + " ORDER BY id"));
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
        EXPECT_EQ(r.code, native::code::kTargetEngineUnsupported) << engine;
        EXPECT_TRUE(tt_contains(r.what, engine)) << r.what;
        EXPECT_TRUE(tt_contains(r.what, "SharedMergeTree targets are not supported")) << r.what;
    }
}

TEST(NativeTargetTable, EnginesOutsideTheAcceptedFamiliesAreRefusedByName) {
    for (const char* engine : {"Buffer", "Memory", "Log", "MaterializedView", "Kafka"}) {
        auto server = tt_server(tt_events(engine, engine));
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
        EXPECT_EQ(r.code, native::code::kTargetEngineUnsupported) << engine;
        EXPECT_TRUE(tt_contains(r.what, std::string("uses engine ") + engine + ",")) << r.what;
    }
}

TEST(NativeTargetTable, ANullTargetTakesTheQueryLevelSetting) {
    auto server = tt_server(tt_events("Null", "Null"));
    server->set_merge_tree_setting("async_insert", "1");  // never applies to a Null table
    const TargetInfo info = tt_probe(server);
    EXPECT_EQ(info.family, EngineFamily::Null);
    EXPECT_EQ(info.dedup_window, 0u);
    EXPECT_FALSE(info.keeps_dedup_log);
    EXPECT_TRUE(tt_contains(info.async_report, "async_insert=0 (Null table")) << info.async_report;
    EXPECT_EQ(tt_count_statements(*server, "merge_tree_settings"), 0u);
}

TEST(NativeTargetTable, AnUnknownDefaultKindIsUnreadable) {
    auto server = tt_server(tt_events());
    TtHookedTransport t(std::make_unique<FakeTransport>(server),
                        [](MetaQuery k, const std::string& sql, InsertTransport& inner) {
                            ResultSet rs = inner.select(k, sql);
                            if (k == MetaQuery::Columns) {
                                rs.rows.at(1).at(2) = "COMPUTED";
                            }
                            return rs;
                        });
    t.connect(kTtEp);
    const TtRefusal r = tt_refusal([&] { (void)native::probe_target(t, tt_options()); });
    EXPECT_EQ(r.code, native::code::kTargetUnreadable);
    EXPECT_TRUE(tt_contains(r.what, "column `s`")) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "'COMPUTED'")) << r.what;
}

TEST(NativeTargetTable, AShortResultRowIsAProtocolErrorNotARefusal) {
    auto server = tt_server(tt_events());
    TtHookedTransport t(std::make_unique<FakeTransport>(server),
                        [](MetaQuery k, const std::string& sql, InsertTransport& inner) {
                            ResultSet rs = inner.select(k, sql);
                            if (k == MetaQuery::Table) {
                                rs.rows.at(0).resize(1);
                            }
                            return rs;
                        });
    t.connect(kTtEp);
    EXPECT_THROW((void)native::probe_target(t, tt_options()), ch::ProtocolError);
}

// ---------------------------------------------------------------------------
// Effective async_insert

TEST(NativeTargetTable, ATableSettingAsyncInsertRefusesWithTheAlter) {
    auto server = tt_server(tt_events(
        "MergeTree", "MergeTree ORDER BY id SETTINGS index_granularity = 8192, async_insert = 1"));
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_EQ(r.what,
              "[clickhouse.target_async_insert] `db`.`events` takes inserts asynchronously "
              "(table setting async_insert=1), which the native sink refuses while upstream "
              "issue #121174 can lose acknowledged rows on that path. Run: ALTER TABLE "
              "`db`.`events` MODIFY SETTING async_insert = 0");
}

TEST(NativeTargetTable, EverySpellingOfOnAndOffIsRead) {
    for (const char* on : {"1", "'1'", "true", "TRUE"}) {
        auto server = tt_server(tt_events(
            "MergeTree", std::string("MergeTree ORDER BY id SETTINGS async_insert = ") + on));
        EXPECT_EQ(tt_refusal([&] { (void)tt_probe(server); }).code,
                  native::code::kTargetAsyncInsert)
            << on;
    }
    for (const char* off : {"0", "'0'", "false"}) {
        auto server = tt_server(tt_events(
            "MergeTree", std::string("MergeTree ORDER BY id SETTINGS async_insert = ") + off));
        // The table's own 0 wins over a server default of 1.
        server->set_merge_tree_setting("async_insert", "1");
        const TargetInfo info = tt_probe(server);
        EXPECT_EQ(info.async_report, "async_insert=0 (table setting)") << off;
    }
}

TEST(NativeTargetTable, AnUnreadableTableValueRefuses) {
    auto server = tt_server(
        tt_events("MergeTree", "MergeTree ORDER BY id SETTINGS async_insert = 'sometimes'"));
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "async_insert='sometimes'")) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "cannot read as 0 or 1")) << r.what;
}

TEST(NativeTargetTable, AServerDefaultAsyncInsertRefusesNamingMergeTree) {
    auto server = tt_server(tt_events());
    server->set_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "(table unset, server default async_insert=1 in <merge_tree>)"))
        << r.what;
    EXPECT_TRUE(tt_contains(r.what, "<merge_tree><async_insert>0</async_insert></merge_tree>"))
        << r.what;
}

TEST(NativeTargetTable, ALineWithoutTheAsyncInsertRowCountsAsZeroAndSaysSo) {
    auto server = tt_server(tt_events());
    server->remove_merge_tree_setting("async_insert");
    const TargetInfo info = tt_probe(server);
    EXPECT_TRUE(tt_contains(info.async_report, "no MergeTree-level async_insert"))
        << info.async_report;
}

TEST(NativeTargetTable, AReplicatedRowWinsOverMergeTreeSettings) {
    auto refused = tt_server(tt_replicated());
    refused->set_replicated_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(refused); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "in <replicated_merge_tree>")) << r.what;
    EXPECT_TRUE(tt_contains(
        r.what, "<replicated_merge_tree><async_insert>0</async_insert></replicated_merge_tree>"))
        << r.what;

    auto accepted = tt_server(tt_replicated());
    accepted->set_merge_tree_setting("async_insert", "1");
    accepted->set_replicated_merge_tree_setting("async_insert", "0");
    const TargetInfo info = tt_probe(accepted);
    EXPECT_EQ(info.family, EngineFamily::ReplicatedMergeTree);
    EXPECT_EQ(info.async_report,
              "async_insert=0 (table unset, server default 0 from replicated_merge_tree_settings)");
}

TEST(NativeTargetTable, AMergeTreeDefaultInheritedByReplicatedTablesNamesMergeTree) {
    auto server = tt_server(tt_replicated());
    server->set_merge_tree_setting("async_insert", "1");  // the replicated view inherits it
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "in <merge_tree>)")) << r.what;
}

TEST(NativeTargetTable, AnAbsentReplicatedSettingsTableFallsBackAndIsReported) {
    auto server = tt_server(tt_replicated());
    server->set_replicated_merge_tree_settings_table(false);
    const TargetInfo info = tt_probe(server);
    EXPECT_TRUE(tt_contains(info.async_report, "replicated_merge_tree_settings absent"))
        << info.async_report;
    EXPECT_TRUE(tt_contains(info.dedup_report, "replicated_merge_tree_settings absent"))
        << info.dedup_report;
    EXPECT_EQ(info.dedup_window, 10000u);

    server->set_merge_tree_setting("async_insert", "1");
    EXPECT_EQ(tt_refusal([&] { (void)tt_probe(server); }).code, native::code::kTargetAsyncInsert);
}

TEST(NativeTargetTable, APlainMergeTreeTargetDoesNotReadReplicatedSettings) {
    auto server = tt_server(tt_events());
    server->set_replicated_merge_tree_settings_table(false);
    (void)tt_probe(server);
    EXPECT_EQ(tt_count_statements(*server, "system.replicated_merge_tree_settings"), 0u);
}

TEST(NativeTargetTable, OtherErrorsOnTheReplicatedSettingsReadGoThrough) {
    auto server = tt_server(tt_replicated());
    EXPECT_THROW((void)tt_probe_failing(server,
                                        MetaQuery::ReplicatedMergeTreeSettings,
                                        tt_server_fault(ch::TIMEOUT_EXCEEDED, "too slow")),
                 ch::ServerException);
}

// ---------------------------------------------------------------------------
// Deduplication window

TEST(NativeTargetTable, DedupWindowSourcesForReplicatedTargets) {
    const TargetInfo by_default = tt_probe(tt_server(tt_replicated()));
    EXPECT_EQ(by_default.dedup_window, 10000u);
    EXPECT_TRUE(by_default.keeps_dedup_log);
    EXPECT_EQ(by_default.dedup_report, "replicated_deduplication_window=10000 (server default)");

    auto overridden = tt_server(tt_replicated());
    overridden->set_replicated_merge_tree_setting("replicated_deduplication_window", "500");
    EXPECT_EQ(tt_probe(overridden).dedup_window, 500u);

    const TargetInfo by_table =
        tt_probe(tt_server(tt_replicated(" SETTINGS replicated_deduplication_window = 100")));
    EXPECT_EQ(by_table.dedup_window, 100u);
    EXPECT_EQ(by_table.dedup_report, "replicated_deduplication_window=100 (table)");

    const TargetInfo none =
        tt_probe(tt_server(tt_replicated(" SETTINGS replicated_deduplication_window = 0")));
    EXPECT_EQ(none.dedup_window, 0u);
    EXPECT_FALSE(none.keeps_dedup_log);
}

TEST(NativeTargetTable, DedupWindowSourcesForMergeTreeTargets) {
    const TargetInfo by_table = tt_probe(tt_server(tt_events(
        "MergeTree", "MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 50")));
    EXPECT_EQ(by_table.dedup_window, 50u);
    EXPECT_TRUE(by_table.keeps_dedup_log);
    EXPECT_EQ(by_table.dedup_report, "non_replicated_deduplication_window=50 (table)");

    auto server = tt_server(tt_events());
    server->set_merge_tree_setting("non_replicated_deduplication_window", "20");
    const TargetInfo by_server = tt_probe(server);
    EXPECT_EQ(by_server.dedup_window, 20u);
    EXPECT_EQ(by_server.dedup_report, "non_replicated_deduplication_window=20 (server default)");
}

// ---------------------------------------------------------------------------
// Distributed targets

TEST(NativeTargetTable, ADistributedTargetChecksEveryReplica) {
    TtCluster c;
    const TargetInfo info = tt_probe(c.initiator);
    EXPECT_EQ(info.family, EngineFamily::Distributed);
    ASSERT_TRUE(info.distributed.has_value());
    EXPECT_EQ(info.distributed->cluster, "c");
    EXPECT_EQ(info.distributed->database, "db");
    EXPECT_EQ(info.distributed->table, "events_local");
    EXPECT_EQ(info.distributed->replicas, 2u);
    // The INSERT is checked against the Distributed table's own columns.
    ASSERT_EQ(info.columns.size(), 2u);
    EXPECT_EQ(info.columns[0].name, "id");
    EXPECT_TRUE(tt_contains(info.async_report, c.r1->display_name())) << info.async_report;
    EXPECT_TRUE(tt_contains(info.async_report, c.r2->display_name())) << info.async_report;
    // The first replica's window is reported, and no log is assumed.
    EXPECT_EQ(info.dedup_window, 10000u);
    EXPECT_FALSE(info.keeps_dedup_log);
    EXPECT_TRUE(tt_contains(info.dedup_report, c.r1->display_name())) << info.dedup_report;
}

TEST(NativeTargetTable, AShardLocalTableSettingRefusesNamingTheReplica) {
    TtCluster c;
    TtCluster::set_local(*c.r2, "ReplicatedMergeTree", " SETTINGS async_insert = 1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what,
                            "`db`.`events_local` on replica " + c.r2->replica_name() +
                                " of cluster `c` takes inserts asynchronously (table setting "
                                "async_insert=1)"))
        << r.what;
    EXPECT_TRUE(tt_contains(r.what, "Run on " + c.r2->replica_name() + ": ALTER TABLE")) << r.what;
}

TEST(NativeTargetTable, AShardLocalServerDefaultRefusesNamingTheReplica) {
    TtCluster c("MergeTree");
    c.r2->set_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "on replica " + c.r2->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "in the configuration of " + c.r2->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "<merge_tree><async_insert>0")) << r.what;
}

TEST(NativeTargetTable, AShardLocalReplicatedOverrideRefusesNamingTheReplica) {
    TtCluster c;
    c.r1->set_replicated_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "on replica " + c.r1->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "<replicated_merge_tree>")) << r.what;
}

TEST(NativeTargetTable, ReplicasWithoutReplicatedSettingsFallBackToMergeTree) {
    TtCluster c;
    c.r1->set_replicated_merge_tree_settings_table(false);
    c.r2->set_replicated_merge_tree_settings_table(false);
    const TargetInfo info = tt_probe(c.initiator);
    EXPECT_EQ(tt_count_statements(*c.initiator, "replicated_merge_tree_settings"), 2u);
    EXPECT_EQ(info.async_report,
              "async_insert=0 on every replica of cluster `c` (" + c.r1->replica_name() +
                  ": async_insert=0 (table unset, server default 0, "
                  "replicated_merge_tree_settings absent, merge_tree_settings used); " +
                  c.r2->replica_name() +
                  ": async_insert=0 (table unset, server default 0, "
                  "replicated_merge_tree_settings absent, merge_tree_settings used))");

    c.r1->set_merge_tree_setting("async_insert", "1");
    EXPECT_EQ(tt_refusal([&] { (void)tt_probe(c.initiator); }).code,
              native::code::kTargetAsyncInsert);
}

// One replica on a line without system.replicated_merge_tree_settings fails
// the read for every replica. The others still have a <replicated_merge_tree>
// section, which merge_tree_settings does not show, so they cannot be taken
// to inherit <merge_tree>.
TEST(NativeTargetTable, AMixedClusterDoesNotReadAReplicatedReplicaFromMergeTree) {
    for (const char* override_value : {"1", "0"}) {
        SCOPED_TRACE(override_value);
        TtCluster c;
        c.r1->set_replicated_merge_tree_setting("async_insert", override_value);
        c.r2->set_replicated_merge_tree_settings_table(false);
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
        EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
        EXPECT_TRUE(tt_contains(r.what,
                                "`db`.`events_local` on replica " + c.r1->replica_name() +
                                    " of cluster `c` has table async_insert unset, and the "
                                    "native sink cannot read the <replicated_merge_tree> "
                                    "default it inherits: system.replicated_merge_tree_settings "
                                    "is missing on " +
                                    c.r2->replica_name()))
            << r.what;
        EXPECT_TRUE(tt_contains(r.what, "code 60")) << r.what;
        EXPECT_TRUE(tt_contains(r.what,
                                "Run on " + c.r1->replica_name() +
                                    ": ALTER TABLE `db`.`events_local` MODIFY SETTING "
                                    "async_insert = 0"))
            << r.what;
    }
}

// The table's own setting overrides the server default, so where it is set
// the default that could not be read does not matter.
TEST(NativeTargetTable, AMixedClusterAcceptsAReplicaWhoseTableSetsAsyncInsert) {
    TtCluster c;
    c.r1->set_replicated_merge_tree_setting("async_insert", "1");
    TtCluster::set_local(*c.r1, "ReplicatedMergeTree", " SETTINGS async_insert = 0");
    c.r2->set_replicated_merge_tree_settings_table(false);
    const TargetInfo info = tt_probe(c.initiator);
    EXPECT_TRUE(
        tt_contains(info.async_report, c.r1->replica_name() + ": async_insert=0 (table setting)"))
        << info.async_report;
    EXPECT_TRUE(tt_contains(info.async_report,
                            c.r2->replica_name() +
                                ": async_insert=0 (table unset, server default 0, "
                                "replicated_merge_tree_settings absent"))
        << info.async_report;
    EXPECT_TRUE(tt_contains(info.dedup_report,
                            "replicated_deduplication_window=0 (server default unreadable, "
                            "counted as 0) on " +
                                c.r1->replica_name()))
        << info.dedup_report;
}

TEST(NativeTargetTable, AnUnknownTableErrorNoMissingTableExplainsGoesThrough) {
    TtCluster c;
    try {
        (void)tt_probe_failing(c.initiator,
                               MetaQuery::ClusterReplicatedMergeTreeSettings,
                               tt_server_fault(ch::UNKNOWN_TABLE, "something else is missing"));
        ADD_FAILURE() << "expected the read's error";
    } catch (const NativeSinkError& e) {
        ADD_FAILURE() << "a refusal: " << e.what();
    } catch (const ch::ServerException& e) {
        EXPECT_EQ(e.GetCode(), ch::UNKNOWN_TABLE);
        EXPECT_EQ(e.GetException().display_text, "something else is missing");
    }
}

TEST(NativeTargetTable, AnUnreadableShardLocalValueRefuses) {
    TtCluster c;
    TtCluster::set_local(*c.r1, "ReplicatedMergeTree", " SETTINGS async_insert = maybe");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "on replica " + c.r1->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "async_insert=maybe")) << r.what;
}

TEST(NativeTargetTable, AShardLocalEngineOutsideMergeTreeRefusesNamingTheReplica) {
    TtCluster c;
    TtCluster::set_local(*c.r2, "Memory", "");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "on replica " + c.r2->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, "uses engine Memory")) << r.what;
}

TEST(NativeTargetTable, FewerLocalTablesThanReplicasRefusesTargetMissing) {
    TtCluster c;
    auto r3 = std::make_shared<FakeServer>();  // has no events_local
    c.initiator->add_cluster("c", {c.r1.get(), c.r2.get(), r3.get()});
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetMissing);
    EXPECT_TRUE(tt_contains(r.what, "exists on 2 of the 3 replicas of cluster `c`")) << r.what;
    EXPECT_TRUE(tt_contains(r.what, c.r1->display_name())) << r.what;
    EXPECT_TRUE(tt_contains(r.what, c.r2->display_name())) << r.what;
}

TEST(NativeTargetTable, AnUnreachableReplicaPropagatesAsTheClientsErrorNotARefusal) {
    TtCluster c;
    c.initiator->set_unreadable_replica("c", 1);
    try {
        (void)tt_probe(c.initiator);
        ADD_FAILURE() << "expected the cluster read to fail";
    } catch (const NativeSinkError& e) {
        ADD_FAILURE() << "a refusal for an unreachable replica: " << e.what();
    } catch (const ch::ServerException& e) {
        EXPECT_EQ(e.GetCode(), ch::ALL_CONNECTION_TRIES_FAILED);
    }

    // Once the replica answers again the same probe passes.
    c.initiator->set_unreadable_replica("c", 1, false);
    EXPECT_EQ(tt_probe(c.initiator).distributed->replicas, 2u);
}

TEST(NativeTargetTable, AConnectionResetOnAClusterReadPropagates) {
    for (const MetaQuery kind : {MetaQuery::ClusterTables,
                                 MetaQuery::ClusterMergeTreeSettings,
                                 MetaQuery::ClusterReplicatedMergeTreeSettings}) {
        TtCluster c;
        Fault reset;
        reset.kind = Fault::Kind::SystemError;
        reset.code = ECONNRESET;
        try {
            (void)tt_probe_failing(c.initiator, kind, reset);
            ADD_FAILURE() << "expected the reset to surface";
        } catch (const std::system_error& e) {
            EXPECT_EQ(e.code().value(), ECONNRESET);
        }
    }
}

TEST(NativeTargetTable, OtherServerErrorsOnAClusterReadPropagate) {
    TtCluster c;
    EXPECT_THROW((void)tt_probe_failing(c.initiator,
                                        MetaQuery::ClusterMergeTreeSettings,
                                        tt_server_fault(ch::TIMEOUT_EXCEEDED, "too slow")),
                 ch::ServerException);
}

TEST(NativeTargetTable, APermissionErrorOnAClusterReadRefusesWithTheGrants) {
    for (const MetaQuery kind : {MetaQuery::ClusterReplicaCount,
                                 MetaQuery::ClusterTables,
                                 MetaQuery::ClusterMergeTreeSettings,
                                 MetaQuery::ClusterReplicatedMergeTreeSettings}) {
        for (const int code : {ch::DATABASE_ACCESS_DENIED, ch::ACCESS_DENIED}) {
            TtCluster c;
            const TtRefusal r = tt_refusal([&] {
                (void)tt_probe_failing(
                    c.initiator, kind, tt_server_fault(code, "not enough privileges"));
            });
            EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
            EXPECT_TRUE(tt_contains(r.what, "not enough privileges")) << r.what;
            EXPECT_TRUE(tt_contains(r.what, "GRANT REMOTE ON *.* TO `writer`")) << r.what;
            EXPECT_TRUE(tt_contains(r.what, "system.merge_tree_settings")) << r.what;
        }
    }
}

TEST(NativeTargetTable, APermissionErrorOnALocalReadGoesThrough) {
    auto server = tt_server(tt_events());
    EXPECT_THROW(
        (void)tt_probe_failing(
            server, MetaQuery::Table, tt_server_fault(ch::ACCESS_DENIED, "not enough privileges")),
        ch::ServerException);
}

TEST(NativeTargetTable, AClusterWithNoReplicasIsUnreadable) {
    auto server = tt_server(tt_events("Distributed", "Distributed('nowhere', 'db', 'local')"));
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetUnreadable);
    EXPECT_TRUE(tt_contains(r.what, "cluster `nowhere`")) << r.what;
}

TEST(NativeTargetTable, DistributedArgumentsThatCannotBeReadRefuse) {
    auto server =
        tt_server(tt_events("Distributed", "Distributed('c', currentDatabase(), 'local')"));
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetUnreadable);
    EXPECT_TRUE(tt_contains(r.what, "currentDatabase()")) << r.what;
}

TEST(NativeTargetTable, ASharedLocalTableBehindDistributedIsRefused) {
    TtCluster c;
    FakeTable shared = tt_events("SharedMergeTree", "SharedMergeTree ORDER BY id");
    shared.name = "events_local";
    c.r1->add_table(shared);
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetEngineUnsupported);
    EXPECT_TRUE(tt_contains(r.what, "on replica " + c.r1->display_name())) << r.what;
}

// ---------------------------------------------------------------------------
// Which replica a row came from

// Two instances on one machine report the same hostName(). Each row carries
// the server's UUID as well, so one instance's settings are never taken for
// the other's.
TEST(NativeTargetTable, InstancesSharingAHostNameAreCheckedApart) {
    TtCluster c("MergeTree");
    c.r1->set_host_name("box");
    c.r2->set_host_name("box");
    c.r2->set_tcp_port(9001);
    c.r2->set_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "`db`.`events_local` on replica box:9001 of cluster `c`"))
        << r.what;

    c.r2->set_merge_tree_setting("async_insert", "0");
    const TargetInfo info = tt_probe(c.initiator);
    EXPECT_TRUE(tt_contains(info.async_report, "box:9000: async_insert=0")) << info.async_report;
    EXPECT_TRUE(tt_contains(info.async_report, "box:9001: async_insert=0")) << info.async_report;
}

// With the same host name and port, as two TLS-only instances that both
// report the default tcpPort() give, the reports add the UUID so that each
// names one server.
TEST(NativeTargetTable, ReplicasSharingAHostAndPortAreNamedByTheirUuid) {
    TtCluster c("MergeTree");
    c.r1->set_host_name("box");
    c.r2->set_host_name("box");
    // The second replica's rows come after the first's, so a fold by name
    // would keep the first replica's 0 for both.
    c.r2->set_merge_tree_setting("async_insert", "1");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(
        r.what, "on replica box:9000 (server " + c.r2->server_uuid() + ") of cluster `c`"))
        << r.what;
}

// The settings read succeeded but has no rows for a replica the tables read
// named. That replica was never read, which is no evidence that it is
// synchronous.
TEST(NativeTargetTable, AReplicaMissingFromASettingsReadIsUnreadable) {
    for (const MetaQuery kind :
         {MetaQuery::ClusterMergeTreeSettings, MetaQuery::ClusterReplicatedMergeTreeSettings}) {
        TtCluster c;
        c.r2->set_merge_tree_setting("async_insert", "1");
        const std::string r2_uuid = c.r2->server_uuid();
        TtHookedTransport t(
            std::make_unique<FakeTransport>(c.initiator),
            [&](MetaQuery k, const std::string& sql, InsertTransport& inner) {
                ResultSet rs = inner.select(k, sql);
                if (k == kind) {
                    std::erase_if(rs.rows, [&](const auto& row) { return row.at(1) == r2_uuid; });
                }
                return rs;
            });
        t.connect(kTtEp);
        const TtRefusal r = tt_refusal([&] { (void)native::probe_target(t, tt_options()); });
        EXPECT_EQ(r.code, native::code::kTargetUnreadable);
        EXPECT_TRUE(tt_contains(
            r.what,
            "`db`.`events_local` on replica " + c.r2->replica_name() +
                " of cluster `c` is missing from the read of system." +
                (kind == MetaQuery::ClusterMergeTreeSettings ? "merge_tree_settings"
                                                             : "replicated_merge_tree_settings")))
            << r.what;
    }
}

// One replica giving one setting two values in a read is not evidence of
// either.
TEST(NativeTargetTable, TwoValuesForOneReplicasSettingAreUnreadable) {
    TtCluster c("MergeTree");
    TtHookedTransport t(std::make_unique<FakeTransport>(c.initiator),
                        [&](MetaQuery k, const std::string& sql, InsertTransport& inner) {
                            ResultSet rs = inner.select(k, sql);
                            if (k == MetaQuery::ClusterMergeTreeSettings) {
                                std::vector<std::string> extra = rs.rows.front();
                                extra.at(3) = "1";
                                rs.rows.push_back(extra);
                            }
                            return rs;
                        });
    t.connect(kTtEp);
    const TtRefusal r = tt_refusal([&] { (void)native::probe_target(t, tt_options()); });
    EXPECT_EQ(r.code, native::code::kTargetUnreadable);
    EXPECT_TRUE(tt_contains(
        r.what, "gave replica " + c.r1->replica_name() + " both async_insert=0 and async_insert=1"))
        << r.what;
}

// A profile with skip_unavailable_shards=1 would have the server leave an
// unreachable replica out of the tables read, which would then look like a
// replica without the local table and refuse for good. The pinned setting
// keeps it the client's error, which the retry loop waits out.
TEST(NativeTargetTable, AProfileThatSkipsUnavailableShardsStillFailsTheRead) {
    TtCluster c;
    c.initiator->set_setting("skip_unavailable_shards", "1");
    c.initiator->set_unreadable_replica("c", 1);
    try {
        (void)tt_probe(c.initiator);
        ADD_FAILURE() << "expected the cluster read to fail";
    } catch (const NativeSinkError& e) {
        ADD_FAILURE() << "a refusal for an unreachable replica: " << e.what();
    } catch (const ch::ServerException& e) {
        EXPECT_EQ(e.GetCode(), ch::ALL_CONNECTION_TRIES_FAILED);
    }
    c.initiator->set_unreadable_replica("c", 1, false);
    EXPECT_EQ(tt_probe(c.initiator).distributed->replicas, 2u);
}

// ---------------------------------------------------------------------------
// A cluster argument with macros

// The initiator's Distributed table over `cluster_text`, with replicas
// r1 and r2 listed as cluster `name`.
void tt_point_at(TtCluster& c, const std::string& cluster_text, const std::string& name) {
    c.initiator->add_table(tt_events(
        "Distributed", "Distributed(" + cluster_text + ", 'db', 'events_local', rand())"));
    c.initiator->add_cluster(name, {c.r1.get(), c.r2.get()});
}

TEST(NativeTargetTable, AClusterMacroIsExpandedFromSystemMacros) {
    TtCluster c;
    tt_point_at(c, "'{cluster}'", "prod");
    c.initiator->set_macro("cluster", "prod");
    const TargetInfo info = tt_probe(c.initiator);
    ASSERT_TRUE(info.distributed.has_value());
    EXPECT_EQ(info.distributed->cluster, "prod");
    EXPECT_EQ(info.distributed->replicas, 2u);
    EXPECT_TRUE(tt_contains(info.async_report,
                            "on every replica of cluster `prod` ('{cluster}' in its definition)"))
        << info.async_report;
    EXPECT_EQ(tt_count_statements(*c.initiator, "clusterAllReplicas('prod', "), 3u);
    EXPECT_EQ(tt_count_statements(*c.initiator, "FROM system.macros"), 1u);
}

// The server expands every macro in the text, and again in what a macro
// gives, as long as braces are left.
TEST(NativeTargetTable, NestedAndEmbeddedMacrosAreExpandedAsTheServerDoes) {
    TtCluster c;
    tt_point_at(c, "'{env}-{region}_x'", "prod-eu_main_x");
    c.initiator->set_macro("env", "{tier}");
    c.initiator->set_macro("tier", "prod");
    c.initiator->set_macro("region", "eu_{name}");
    c.initiator->set_macro("name", "main");
    EXPECT_EQ(tt_probe(c.initiator).distributed->cluster, "prod-eu_main_x");
}

TEST(NativeTargetTable, AClusterWithoutMacrosDoesNotReadSystemMacros) {
    TtCluster c;
    (void)tt_probe(c.initiator);
    EXPECT_EQ(tt_count_statements(*c.initiator, "system.macros"), 0u);
}

TEST(NativeTargetTable, AMacroTheServerDoesNotDefineIsSaidPlainly) {
    for (const char* macro : {"cluster", "server_uuid"}) {
        TtCluster c;
        tt_point_at(c, "'{" + std::string(macro) + "}'", "prod");
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
        EXPECT_EQ(r.code, native::code::kTargetUnreadable);
        EXPECT_TRUE(tt_contains(r.what,
                                "`db`.`events` is a Distributed table whose cluster argument '{" +
                                    std::string(macro) + "}' uses the macro {" + macro +
                                    "}, which system.macros on this server does not define"))
            << r.what;
        EXPECT_FALSE(tt_contains(r.what, "system.clusters")) << r.what;
    }
}

TEST(NativeTargetTable, MacrosTheServerCannotExpandEitherAreUnreadable) {
    {
        TtCluster c;
        tt_point_at(c, "'{cluster'", "prod");
        c.initiator->set_macro("cluster", "prod");
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
        EXPECT_EQ(r.code, native::code::kTargetUnreadable);
        EXPECT_TRUE(tt_contains(r.what, "does not close it")) << r.what;
    }
    {
        TtCluster c;
        tt_point_at(c, "'{loop}'", "prod");
        c.initiator->set_macro("loop", "{loop}");
        const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
        EXPECT_EQ(r.code, native::code::kTargetUnreadable);
        EXPECT_TRUE(tt_contains(r.what, "nests macros more than 10 deep")) << r.what;
    }
    {
        // Ten rounds are allowed, as on the server.
        TtCluster c;
        tt_point_at(c, "'{m0}'", "deep");
        for (int i = 0; i < 9; ++i) {
            c.initiator->set_macro("m" + std::to_string(i), "{m" + std::to_string(i + 1) + "}");
        }
        c.initiator->set_macro("m9", "deep");
        EXPECT_EQ(tt_probe(c.initiator).distributed->cluster, "deep");
    }
}

TEST(NativeTargetTable, AnExpandedClusterSystemClustersLacksSaysWhereTheNameCameFrom) {
    TtCluster c;
    tt_point_at(c, "'{cluster}'", "prod");
    c.initiator->set_macro("cluster", "staging");
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(c.initiator); });
    EXPECT_EQ(r.code, native::code::kTargetUnreadable);
    EXPECT_TRUE(tt_contains(r.what,
                            "over cluster `staging` ('{cluster}' in its definition), which "
                            "system.clusters on this server does not list"))
        << r.what;
}

TEST(NativeTargetTable, APermissionErrorOnTheMacrosReadRefusesWithItsGrant) {
    TtCluster c;
    tt_point_at(c, "'{cluster}'", "prod");
    c.initiator->set_macro("cluster", "prod");
    const TtRefusal r = tt_refusal([&] {
        (void)tt_probe_failing(c.initiator,
                               MetaQuery::ClusterReplicaCount,
                               tt_server_fault(ch::ACCESS_DENIED, "not enough privileges"));
    });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "GRANT SELECT ON system.macros TO `writer`")) << r.what;
    // A cluster named outright needs no such grant.
    TtCluster plain;
    const TtRefusal p = tt_refusal([&] {
        (void)tt_probe_failing(plain.initiator,
                               MetaQuery::ClusterReplicaCount,
                               tt_server_fault(ch::ACCESS_DENIED, "not enough privileges"));
    });
    EXPECT_FALSE(tt_contains(p.what, "system.macros")) << p.what;
}

// ---------------------------------------------------------------------------
// The table's own settings, as the server applies them

TEST(NativeTargetTable, ARepeatedTableSettingIsReadAsItsLastValue) {
    auto server = tt_server(
        tt_events("MergeTree",
                  "MergeTree ORDER BY id SETTINGS async_insert = 0, index_granularity = 8192, "
                  "async_insert = 1"));
    const TtRefusal r = tt_refusal([&] { (void)tt_probe(server); });
    EXPECT_EQ(r.code, native::code::kTargetAsyncInsert);
    EXPECT_TRUE(tt_contains(r.what, "table setting async_insert=1")) << r.what;
}

}  // namespace
