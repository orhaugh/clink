// The PostgreSQL CDC source against a real server: the SQL CDC contract
// (postgres_cdc_source with changelog_mode) driven through its checkpoint
// hooks the way the runtime drives it. SKIPPED unless
// CLINK_POSTGRES_CDC_TEST_DSN names a server running with wal_level=logical
// (docker/integration-services.yml's postgres-cdc service).
//
// Every case keeps two oracles. The table itself: once the source has emitted
// a marker row committed after the workload, the state rebuilt from the
// emitted change rows - latest __version per key, deletes removed - must equal
// SELECT over the table, value for value. And the history: every row version
// the workload committed after the source started must appear among the
// emitted rows, so an update lost behind a later update of the same key is
// caught even though the final state hides it.
//
// The downstream is modelled as transactional: a restore from checkpoint N
// drops whatever was emitted after N's barrier, as a two-phase-commit sink
// would. With that, a stream replay is checked strictly - no change twice, none
// missing - and a snapshot restarted from scratch is checked for convergence.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#endif

#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#ifdef CLINK_HAS_POSTGRES

using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::Source;
using clink::StateBackend;
using clink::StreamElement;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {

const char* dsn_env() {
    return std::getenv("CLINK_POSTGRES_CDC_TEST_DSN");
}

// ---- SQL on a plain connection ---------------------------------------------

struct PgResult {
    std::vector<std::vector<std::optional<std::string>>> rows;
};

PgResult sql(const std::string& dsn, const std::string& statement) {
    PGconn* c = PQconnectdb(dsn.c_str());
    if (PQstatus(c) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(c);
        PQfinish(c);
        throw std::runtime_error("connect: " + err);
    }
    PGresult* r = PQexec(c, statement.c_str());
    const auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(c);
        PQclear(r);
        PQfinish(c);
        throw std::runtime_error(statement.substr(0, 120) + " -> " + err);
    }
    PgResult out;
    for (int i = 0; i < PQntuples(r); ++i) {
        std::vector<std::optional<std::string>> row;
        for (int j = 0; j < PQnfields(r); ++j) {
            if (PQgetisnull(r, i, j) != 0) {
                row.emplace_back(std::nullopt);
            } else {
                row.emplace_back(std::string{PQgetvalue(r, i, j)});
            }
        }
        out.rows.push_back(std::move(row));
    }
    PQclear(r);
    PQfinish(c);
    return out;
}

std::string scalar(const std::string& dsn, const std::string& statement) {
    const auto r = sql(dsn, statement);
    if (r.rows.empty() || r.rows[0].empty() || !r.rows[0][0]) {
        return {};
    }
    return *r.rows[0][0];
}

// A connection kept open across statements, for transactions a test holds.
class Session {
public:
    explicit Session(const std::string& dsn) : c_(PQconnectdb(dsn.c_str())) {
        if (PQstatus(c_) != CONNECTION_OK) {
            throw std::runtime_error(std::string{"connect: "} + PQerrorMessage(c_));
        }
    }
    ~Session() { PQfinish(c_); }
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;
    Session(Session&&) = delete;
    Session& operator=(Session&&) = delete;

    void exec(const std::string& statement) {
        PGresult* r = PQexec(c_, statement.c_str());
        const auto st = PQresultStatus(r);
        const std::string err = PQerrorMessage(c_);
        PQclear(r);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            throw std::runtime_error(statement.substr(0, 120) + " -> " + err);
        }
    }
    // Rows of a RETURNING clause, first column of each.
    std::vector<std::string> returning(const std::string& statement) {
        PGresult* r = PQexec(c_, statement.c_str());
        if (PQresultStatus(r) != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(c_);
            PQclear(r);
            throw std::runtime_error(statement.substr(0, 120) + " -> " + err);
        }
        std::vector<std::string> out;
        for (int i = 0; i < PQntuples(r); ++i) {
            out.emplace_back(PQgetvalue(r, i, 0));
        }
        PQclear(r);
        return out;
    }

private:
    PGconn* c_;
};

// ---- the captured table ----------------------------------------------------
//
// Each row's content is a function of (id, rev), and every update increments
// rev, so (id, rev) names one row version and its expected values.

constexpr const char* kDeclared =
    "id:BIGINT;rev:INTEGER;status:VARCHAR;amount:DECIMAL(12, 2);"
    "at:TIMESTAMP(3) WITH TIME ZONE;flag:BOOLEAN;day:DATE;note:VARCHAR;"
    "__op:VARCHAR;__version:BIGINT;__deleted:BOOLEAN;__lsn:VARCHAR";

// The SET list giving a row version `rev_expr` its content.
std::string content(const std::string& rev_expr) {
    return "rev = " + rev_expr + ", status = 's' || ((" + rev_expr + ") % 5), amount = (id * 7 + " +
           rev_expr + ") + 0.25, at = timestamptz '2026-01-01 00:00:00+00' + (id * 1000 + " +
           rev_expr + ") * interval '1 millisecond', flag = (id + " + rev_expr +
           ") % 2 = 0, day = date '2026-01-01' + (id % 365)::int, note = 'n' || id || '-' || (" +
           rev_expr + ")";
}

std::string insert_rows(const std::string& table, std::int64_t from, std::int64_t to) {
    return "INSERT INTO " + table +
           " (id, rev, status, amount, at, flag, day, note) SELECT g, 1, 's1', g * 7 + 1.25, "
           "timestamptz '2026-01-01 00:00:00+00' + (g * 1000 + 1) * interval '1 millisecond', "
           "(g + 1) % 2 = 0, date '2026-01-01' + (g % 365)::int, 'n' || g || '-1' FROM "
           "generate_series(" +
           std::to_string(from) + "::bigint, " + std::to_string(to) +
           "::bigint) g RETURNING id::text || ':' || rev";
}

std::string update_rows(const std::string& table, const std::string& where) {
    return "UPDATE " + table + " SET " + content("rev + 1") + " WHERE " + where +
           " RETURNING id::text || ':' || rev";
}

std::string delete_rows(const std::string& table, const std::string& where) {
    return "DELETE FROM " + table + " WHERE " + where + " RETURNING id::text || ':deleted'";
}

// Everything one test creates on the server, dropped on the way out.
class Fixture {
public:
    // `partitioned`: the table is partitioned by range of id into _low (below
    // 1000), _mid (below 1,000,000) and _high, and published through its root.
    explicit Fixture(std::string tag, bool full_identity = true, bool partitioned = false)
        : dsn_(dsn_env()),
          table_("cdc_" + tag + "_" + std::to_string(::getpid())),
          slot_("slot_" + tag + "_" + std::to_string(::getpid())),
          pub_("pub_" + tag + "_" + std::to_string(::getpid())) {
        cleanup();
        sql(dsn_,
            "CREATE TABLE " + table_ +
                " (id bigint PRIMARY KEY, rev int NOT NULL, status text, amount numeric(12, 2), at "
                "timestamptz(3), flag boolean, day date, note text)" +
                (partitioned ? " PARTITION BY RANGE (id)" : ""));
        std::vector<std::string> identity_of = {table_};
        if (partitioned) {
            identity_of = {table_ + "_low", table_ + "_mid", table_ + "_high"};
            sql(dsn_,
                "CREATE TABLE " + identity_of[0] + " PARTITION OF " + table_ +
                    " FOR VALUES FROM (MINVALUE) TO (1000)");
            sql(dsn_,
                "CREATE TABLE " + identity_of[1] + " PARTITION OF " + table_ +
                    " FOR VALUES FROM (1000) TO (1000000)");
            sql(dsn_,
                "CREATE TABLE " + identity_of[2] + " PARTITION OF " + table_ +
                    " FOR VALUES FROM (1000000) TO (MAXVALUE)");
        }
        if (full_identity) {
            for (const auto& t : identity_of) {
                sql(dsn_, "ALTER TABLE " + t + " REPLICA IDENTITY FULL");
            }
        }
        sql(dsn_,
            "CREATE PUBLICATION " + pub_ + " FOR TABLE " + table_ +
                (partitioned ? " WITH (publish_via_partition_root = true)" : ""));
    }
    ~Fixture() {
        try {
            cleanup();
        } catch (...) {
        }
    }
    Fixture(const Fixture&) = delete;
    Fixture& operator=(const Fixture&) = delete;
    Fixture(Fixture&&) = delete;
    Fixture& operator=(Fixture&&) = delete;

    void cleanup() {
        // A slot still held by a session that is going away refuses a drop
        // for a moment.
        for (int i = 0; i < 50; ++i) {
            const auto active = scalar(
                dsn_, "SELECT active FROM pg_replication_slots WHERE slot_name = '" + slot_ + "'");
            if (active.empty()) {
                break;
            }
            if (active == "f") {
                (void)sql(dsn_, "SELECT pg_drop_replication_slot('" + slot_ + "')");
                break;
            }
            std::this_thread::sleep_for(100ms);
        }
        (void)sql(dsn_, "DROP PUBLICATION IF EXISTS " + pub_);
        (void)sql(dsn_, "DROP TABLE IF EXISTS " + table_);
    }

    [[nodiscard]] const std::string& dsn() const { return dsn_; }
    [[nodiscard]] const std::string& table() const { return table_; }
    [[nodiscard]] const std::string& slot() const { return slot_; }
    [[nodiscard]] const std::string& pub() const { return pub_; }

    std::map<std::string, std::string> params(const std::string& mode = "append") const {
        return {{"conninfo", dsn_},
                {"slot_name", slot_},
                {"publication_names", pub_},
                {"changelog_mode", mode},
                {"table", "public." + table_},
                {"primary_key", "id"},
                {"declared_types", kDeclared},
                {"snapshot_batch_rows", "50"},
                {"poll_interval_ms", "10"},
                {"status_interval_ms", "100"},
                {"reconnect_timeout_ms", "30000"}};
    }

    // The slot's confirmed position, or 0 when none / no slot.
    [[nodiscard]] std::uint64_t confirmed() const {
        const auto text =
            scalar(dsn_,
                   "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" +
                       slot_ + "'");
        return lsn(text);
    }

    static std::uint64_t lsn(const std::string& text) {
        const auto slash = text.find('/');
        if (slash == std::string::npos) {
            return 0;
        }
        return (std::stoull(text.substr(0, slash), nullptr, 16) << 32) |
               std::stoull(text.substr(slash + 1), nullptr, 16);
    }

private:
    std::string dsn_;
    std::string table_;
    std::string slot_;
    std::string pub_;
};

// ---- a source run, and the transactional downstream it feeds --------------

struct Change {
    std::int64_t id{0};
    std::int64_t rev{0};
    std::string op;
    std::uint64_t version{0};
    bool deleted{false};
    clink::config::JsonObject row;
};

class Downstream {
public:
    void add(const std::string& json) {
        const auto v = clink::config::parse(json);
        Change c;
        c.row = v.as_object();
        c.id = c.row.at("id").as_int();
        c.rev = c.row.at("rev").is_null() ? 0 : c.row.at("rev").as_int();
        c.op = c.row.at("__op").as_string();
        c.version = static_cast<std::uint64_t>(c.row.at("__version").as_int());
        c.deleted = c.row.at("__deleted").as_bool();
        changes_.push_back(std::move(c));
    }
    void mark(std::uint64_t ckpt) { at_[ckpt] = changes_.size(); }
    void reset_to(std::uint64_t ckpt) { changes_.resize(at_.at(ckpt)); }
    [[nodiscard]] const std::vector<Change>& changes() const { return changes_; }
    [[nodiscard]] bool saw(std::int64_t id) const {
        return std::any_of(
            changes_.begin(), changes_.end(), [&](const Change& c) { return c.id == id; });
    }
    // The stream (not a snapshot) has delivered a change to `id`. The
    // catch-up boundary: a snapshot taken again after a restore may already
    // hold the marker row while the stream it replays from is still behind it.
    [[nodiscard]] bool streamed(std::int64_t id) const {
        return std::any_of(changes_.begin(), changes_.end(), [&](const Change& c) {
            return c.id == id && c.op != "snapshot";
        });
    }

    // Latest version per key, deletes removed: id -> rev.
    [[nodiscard]] std::map<std::int64_t, std::int64_t> state() const {
        std::map<std::int64_t, const Change*> latest;
        for (const auto& c : changes_) {
            auto it = latest.find(c.id);
            if (it == latest.end() || c.version >= it->second->version) {
                latest[c.id] = &c;
            }
        }
        std::map<std::int64_t, std::int64_t> out;
        for (const auto& [id, c] : latest) {
            if (!c->deleted) {
                out[id] = c->rev;
            }
        }
        return out;
    }

private:
    std::vector<Change> changes_;
    std::map<std::uint64_t, std::size_t> at_;
};

std::shared_ptr<Source<std::string>> build(const std::map<std::string, std::string>& params) {
    const auto* sf = clink::cluster::OperatorRegistry::default_instance().find_source(
        "postgres_cdc_source", "string");
    if (sf == nullptr) {
        throw std::runtime_error("postgres_cdc_source is not registered");
    }
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = params;
    return std::static_pointer_cast<Source<std::string>>(sf->build(ctx));
}

class CdcRun {
public:
    CdcRun(std::map<std::string, std::string> params, Downstream& down)
        : src_(build(params)), down_(down) {}

    std::shared_ptr<Source<std::string>>& source() { return src_; }

    // Restore from checkpoint `ckpt` of `backends`, as the runtime does
    // before open(), and rewind the downstream to that checkpoint's barrier.
    void restore(std::map<std::uint64_t, InMemoryStateBackend>& backends, std::uint64_t ckpt) {
        ASSERT_TRUE(src_->restore_offset(backends.at(ckpt), op_));
        down_.reset_to(ckpt);
    }

    void open() { src_->open(); }

    // One produce() call; false when the source ended its stream.
    bool step() {
        Emitter<std::string> em(Emitter<std::string>::Forward([this](StreamElement<std::string> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    down_.add(r.value());
                }
            }
            return true;
        }));
        return src_->produce(em);
    }

    // produce() until `done` or the deadline; fails the test on the deadline.
    void pump_until(const std::function<bool()>& done, std::chrono::seconds limit = 60s) {
        const auto deadline = Clock::now() + limit;
        while (!done()) {
            ASSERT_LT(Clock::now(), deadline) << "the source did not get there in time";
            ASSERT_TRUE(step()) << "the source ended its stream";
        }
    }

    // A checkpoint barrier between produce() calls.
    void checkpoint(std::map<std::uint64_t, InMemoryStateBackend>& backends, std::uint64_t ckpt) {
        src_->snapshot_offset(backends[ckpt], op_, CheckpointId{ckpt});
        down_.mark(ckpt);
    }
    void complete(std::uint64_t ckpt) { src_->notify_checkpoint_complete(CheckpointId{ckpt}); }
    void abort(std::uint64_t ckpt) { src_->notify_checkpoint_aborted(CheckpointId{ckpt}); }
    void close() { src_->close(); }

    // A worker killed: the source goes away without close(), its sessions with it.
    void crash() { src_.reset(); }

private:
    std::shared_ptr<Source<std::string>> src_;
    Downstream& down_;
    OperatorId op_{1};
};

// The table now, id -> rev.
std::map<std::int64_t, std::int64_t> table_state(const Fixture& f) {
    std::map<std::int64_t, std::int64_t> out;
    for (const auto& row : sql(f.dsn(), "SELECT id, rev FROM " + f.table()).rows) {
        out[std::stoll(*row[0])] = std::stoll(*row[1]);
    }
    return out;
}

// Every emitted row's values are those of its (id, rev): no value was
// corrupted on the way. A delete under REPLICA IDENTITY FULL carries the
// deleted row's values too.
void expect_values_match_their_version(const Downstream& down) {
    for (const auto& c : down.changes()) {
        if (c.row.at("rev").is_null()) {
            continue;  // a key-only delete
        }
        const std::int64_t id = c.id;
        const std::int64_t rev = c.rev;
        EXPECT_EQ(c.row.at("status").as_string(), "s" + std::to_string(rev % 5))
            << id << ":" << rev;
        EXPECT_DOUBLE_EQ(c.row.at("amount").as_number(), static_cast<double>((id * 7) + rev) + 0.25)
            << id << ":" << rev;
        EXPECT_EQ(c.row.at("at").as_int(), 1767225600000LL + (id * 1000) + rev) << id << ":" << rev;
        EXPECT_EQ(c.row.at("flag").as_bool(), (id + rev) % 2 == 0) << id << ":" << rev;
        EXPECT_EQ(c.row.at("note").as_string(),
                  "n" + std::to_string(id) + "-" + std::to_string(rev))
            << id << ":" << rev;
    }
}

// The marker row: committed last, so once it is emitted, everything committed
// before it has been.
constexpr std::int64_t kMarker = 9'000'000;

// produce() on its own thread, so the test thread can commit work while the
// source is parked at a fault point. Releases the point and joins on the way
// out, whatever path the test takes.
class PumpThread {
public:
    PumpThread(CdcRun& run, Downstream& down, std::string parked_at)
        : parked_at_(std::move(parked_at)), thread_([this, &run, &down] {
              try {
                  const auto limit = Clock::now() + 120s;
                  while (!stop_.load() && Clock::now() < limit && run.step()) {
                      if (down.streamed(kMarker)) {
                          break;
                      }
                  }
              } catch (const std::exception& e) {
                  error_ = e.what();
              }
          }) {}
    ~PumpThread() {
        stop_.store(true);
        clink::fault::Registry::instance().release(parked_at_);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    PumpThread(const PumpThread&) = delete;
    PumpThread& operator=(const PumpThread&) = delete;
    PumpThread(PumpThread&&) = delete;
    PumpThread& operator=(PumpThread&&) = delete;

    // Let the source go on and wait for it to reach the marker.
    std::string finish() {
        clink::fault::Registry::instance().release(parked_at_);
        thread_.join();
        return error_;
    }

private:
    std::string parked_at_;
    std::atomic<bool> stop_{false};
    std::string error_;
    std::thread thread_;
};

void commit_marker(const Fixture& f) {
    Session s(f.dsn());
    (void)s.returning(insert_rows(f.table(), kMarker, kMarker));
}

// The history the workload committed: "id:rev" for each row version and
// "id:deleted" for each delete.
class History {
public:
    void add(const std::vector<std::string>& versions) {
        committed_.insert(committed_.end(), versions.begin(), versions.end());
    }
    // Every committed version appears among the emitted changes.
    void expect_all_emitted(const Downstream& down) const {
        std::set<std::string> seen;
        for (const auto& c : down.changes()) {
            seen.insert(std::to_string(c.id) + ":" +
                        (c.deleted ? "deleted" : std::to_string(c.rev)));
        }
        for (const auto& v : committed_) {
            EXPECT_TRUE(seen.count(v) != 0) << "committed version " << v << " was never emitted";
        }
    }
    [[nodiscard]] std::size_t size() const { return committed_.size(); }

private:
    std::vector<std::string> committed_;
};

// No streamed change emitted twice (a snapshot row may be, after a snapshot
// restarts, and is tagged __op='snapshot').
void expect_no_duplicate_changes(const Downstream& down) {
    std::set<std::string> seen;
    for (const auto& c : down.changes()) {
        if (c.op == "snapshot") {
            continue;
        }
        const std::string k = std::to_string(c.id) + "@" + std::to_string(c.version) + "/" + c.op;
        EXPECT_TRUE(seen.insert(k).second) << "change emitted twice: " << k;
    }
}

std::string cursor_bytes(InMemoryStateBackend& backend) {
    constexpr const char* kKey = "__postgres_cdc_cursor__";
    const auto v =
        backend.get_operator_state(OperatorId{1}, StateBackend::KeyView{kKey, std::strlen(kKey)});
    if (!v) {
        return {};
    }
    std::string out;
    for (const std::byte b : *v) {
        out.push_back(static_cast<char>(b));
    }
    return out;
}

#define REQUIRE_SERVER()                                                                     \
    if (dsn_env() == nullptr) {                                                              \
        GTEST_SKIP() << "set CLINK_POSTGRES_CDC_TEST_DSN (docker/integration-services.yml)"; \
    }

}  // namespace

// The exact boundary: the slot exports its snapshot, the snapshot is read
// from it while the workload commits inserts, updates and deletes, and those
// changes arrive through the stream. A key changed during the snapshot shows
// its old version in the snapshot and its new one in the stream, never the
// new one twice and never a gap.
TEST(PostgresCdcContractLive, ASnapshotTakenWhileTheTableChangesMeetsTheStreamExactly) {
    REQUIRE_SERVER();
    Fixture f("exact");
    History history;
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 500));
    }

    // Park the source after its first snapshot batch, commit the workload,
    // then let it go on.
    clink::fault::ScopedFault fault("postgres_cdc.snapshot_batch=block@1");
    Downstream down;
    CdcRun run(f.params(), down);
    run.open();
    PumpThread pump(run, down, "postgres_cdc.snapshot_batch");
    const auto deadline = Clock::now() + 30s;
    while (clink::fault::Registry::instance().hits("postgres_cdc.snapshot_batch") < 1) {
        ASSERT_LT(Clock::now(), deadline) << "the snapshot never emitted its first batch";
        std::this_thread::sleep_for(10ms);
    }
    try {
        Session s(f.dsn());
        history.add(s.returning(update_rows(f.table(), "id <= 20")));  // emitted already
        history.add(s.returning(update_rows(f.table(), "id BETWEEN 400 AND 420")));  // not yet read
        history.add(s.returning(delete_rows(f.table(), "id BETWEEN 30 AND 35")));
        history.add(s.returning(delete_rows(f.table(), "id BETWEEN 450 AND 455")));
        history.add(s.returning(insert_rows(f.table(), 1000, 1050)));
        history.add(s.returning(update_rows(f.table(), "id BETWEEN 1000 AND 1010")));
        commit_marker(f);
    } catch (const std::exception& e) {
        FAIL() << "the workload failed: " << e.what();
    }
    ASSERT_EQ(pump.finish(), "");
    ASSERT_TRUE(down.streamed(kMarker));
    run.close();

    EXPECT_EQ(down.state(), table_state(f));
    history.expect_all_emitted(down);
    expect_values_match_their_version(down);
    expect_no_duplicate_changes(down);
    // The boundary: no snapshot row shows a version committed after the slot.
    for (const auto& c : down.changes()) {
        if (c.op == "snapshot") {
            EXPECT_EQ(c.rev, 1) << "snapshot row " << c.id << " shows a later version";
            EXPECT_EQ(c.version, 0U);
        }
    }
}

TEST(PostgresCdcContractLive, RolledBackWorkNeverAppears) {
    REQUIRE_SERVER();
    Fixture f("rollback");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 10));
    }
    Downstream down;
    CdcRun run(f.params(), down);
    run.open();
    History history;
    {
        Session s(f.dsn());
        s.exec("BEGIN");
        (void)s.returning(insert_rows(f.table(), 100, 110));
        (void)s.returning(update_rows(f.table(), "id <= 5"));
        (void)s.returning(delete_rows(f.table(), "id = 6"));
        s.exec("ROLLBACK");
        s.exec("BEGIN");
        history.add(s.returning(update_rows(f.table(), "id = 7")));
        s.exec("SAVEPOINT sp");
        (void)s.returning(update_rows(f.table(), "id = 8"));
        s.exec("ROLLBACK TO SAVEPOINT sp");
        s.exec("COMMIT");
    }
    commit_marker(f);
    run.pump_until([&] { return down.streamed(kMarker); });
    run.close();
    for (const auto& c : down.changes()) {
        EXPECT_FALSE(c.id >= 100 && c.id <= 110)
            << "a rolled-back insert of " << c.id << " appeared";
        if (c.op != "snapshot") {
            EXPECT_TRUE(c.id == 7 || c.id == kMarker)
                << "a rolled-back change to " << c.id << " appeared";
        }
    }
    history.expect_all_emitted(down);
    EXPECT_EQ(down.state(), table_state(f));
}

// One transaction larger than a produce() batch, with a checkpoint taken
// inside it: the restore skips exactly what the checkpoint holds.
TEST(PostgresCdcContractLive, ATransactionSpanningACheckpointReplaysExactly) {
    REQUIRE_SERVER();
    Fixture f("span");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    History history;
    {
        CdcRun run(params, down);
        run.open();
        {
            Session s(f.dsn());
            s.exec("BEGIN");
            history.add(s.returning(insert_rows(f.table(), 1, 3000)));
            history.add(s.returning(update_rows(f.table(), "id <= 1000")));
            s.exec("COMMIT");
        }
        // The first emitted batch is a part of the transaction.
        run.pump_until([&] { return !down.changes().empty(); });
        ASSERT_LT(down.changes().size(), 4000U);
        run.checkpoint(backends, 1);
        run.complete(1);
        const std::string bytes = cursor_bytes(backends[1]);
        ASSERT_EQ(bytes.size(), 27U);
        EXPECT_EQ(bytes[2], 1) << "the checkpoint is not inside the transaction";
        run.pump_until([&] { return down.changes().size() > 2000; });
        run.crash();
    }
    commit_marker(f);
    CdcRun second(params, down);
    second.restore(backends, 1);
    second.open();
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    expect_no_duplicate_changes(down);
    history.expect_all_emitted(down);
    EXPECT_EQ(down.changes().size(), history.size() + 1) << "every change once, the marker once";
    EXPECT_EQ(down.state(), table_state(f));
}

// A crash after the slot was created and before its snapshot was imported:
// the next run finds the slot and no checkpoint, takes the snapshot fresh and
// streams from the slot's position, which converges.
TEST(PostgresCdcContractLive, ACrashDuringSnapshotSetupConverges) {
    REQUIRE_SERVER();
    Fixture f("setup");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 200));
    }
    Downstream down;
    {
        clink::fault::ScopedFault fault("postgres_cdc.after_slot_created=throw@1");
        CdcRun run(f.params(), down);
        EXPECT_THROW(run.open(), clink::fault::InjectedFault);
        EXPECT_EQ(clink::fault::Registry::instance().hits("postgres_cdc.after_slot_created"), 1U);
        run.crash();
    }
    ASSERT_NE(f.confirmed(), 0U) << "the slot was not created before the crash";
    History history;
    {
        Session s(f.dsn());
        history.add(s.returning(update_rows(f.table(), "id <= 50")));
        history.add(s.returning(delete_rows(f.table(), "id BETWEEN 60 AND 70")));
        history.add(s.returning(insert_rows(f.table(), 300, 320)));
    }
    CdcRun second(f.params(), down);
    second.open();
    commit_marker(f);
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    EXPECT_EQ(down.state(), table_state(f));
    history.expect_all_emitted(down);
    expect_values_match_their_version(down);
}

// A checkpoint completes in the middle of the snapshot, the downstream holds
// part of it, the table changes, and the worker dies. The restore takes the
// snapshot again from scratch and streams from the slot's original position.
TEST(PostgresCdcContractLive, ACrashDuringSnapshotRowsConverges) {
    REQUIRE_SERVER();
    Fixture f("rows");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 500));
    }
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    History history;
    std::uint64_t slot_position = 0;
    {
        CdcRun run(f.params(), down);
        run.open();
        slot_position = f.confirmed();
        run.pump_until([&] { return down.changes().size() >= 150; });
        run.checkpoint(backends, 1);
        run.complete(1);
        EXPECT_EQ(cursor_bytes(backends[1])[1], 1) << "the checkpoint is not in the snapshot phase";
        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id <= 100")));  // held downstream
            history.add(s.returning(delete_rows(f.table(), "id BETWEEN 120 AND 130")));
            history.add(s.returning(update_rows(f.table(), "id BETWEEN 300 AND 310")));
            history.add(s.returning(insert_rows(f.table(), 600, 650)));
        }
        run.pump_until([&] { return down.changes().size() >= 250; });
        run.crash();
    }
    EXPECT_EQ(f.confirmed(), slot_position) << "the slot moved during the snapshot";
    commit_marker(f);
    CdcRun second(f.params(), down);
    second.restore(backends, 1);
    second.open();
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    EXPECT_EQ(down.state(), table_state(f));
    history.expect_all_emitted(down);
    expect_values_match_their_version(down);
    expect_no_duplicate_changes(down);
}

// The worker dies between the last snapshot row and the start of the stream,
// with a checkpoint holding every snapshot row: the restore cannot know the
// snapshot finished as of a position the stream can continue from exactly, so
// it takes it again, and converges.
TEST(PostgresCdcContractLive, ACrashAtTheSnapshotToStreamTransitionConverges) {
    REQUIRE_SERVER();
    Fixture f("transition");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 120));
    }
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    History history;
    {
        clink::fault::ScopedFault fault("postgres_cdc.snapshot_done=throw@1");
        CdcRun run(f.params(), down);
        run.open();
        run.pump_until([&] { return down.changes().size() >= 120; });
        run.checkpoint(backends, 1);
        run.complete(1);
        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id <= 10")));
            history.add(s.returning(delete_rows(f.table(), "id = 11")));
        }
        EXPECT_THROW((void)run.step(), clink::fault::InjectedFault);
        EXPECT_EQ(clink::fault::Registry::instance().hits("postgres_cdc.snapshot_done"), 1U);
        run.crash();
    }
    commit_marker(f);
    CdcRun second(f.params(), down);
    second.restore(backends, 1);
    second.open();
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    EXPECT_EQ(down.state(), table_state(f));
    history.expect_all_emitted(down);
}

// After the transition a checkpoint is in the stream phase: the restore
// streams on from it, exactly, with no second snapshot.
TEST(PostgresCdcContractLive, ARestoreAfterTheTransitionStreamsOnWithoutASecondSnapshot) {
    REQUIRE_SERVER();
    Fixture f("after");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 80));
    }
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    History history;
    {
        CdcRun run(f.params(), down);
        run.open();
        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id <= 5")));
        }
        run.pump_until([&] {
            return std::count_if(down.changes().begin(), down.changes().end(), [](const Change& c) {
                       return c.op == "update";
                   }) >= 5;
        });
        run.checkpoint(backends, 1);
        run.complete(1);
        EXPECT_EQ(cursor_bytes(backends[1])[1], 2) << "the checkpoint is not in the stream phase";
        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id BETWEEN 6 AND 9")));
        }
        run.pump_until([&] { return down.changes().size() >= 89; });
        run.crash();
    }
    commit_marker(f);
    CdcRun second(f.params(), down);
    second.restore(backends, 1);
    second.open();
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    const auto snapshot_rows = std::count_if(down.changes().begin(),
                                             down.changes().end(),
                                             [](const Change& c) { return c.op == "snapshot"; });
    EXPECT_EQ(snapshot_rows, 80) << "the snapshot was taken again";
    expect_no_duplicate_changes(down);
    history.expect_all_emitted(down);
    EXPECT_EQ(down.state(), table_state(f));
}

// The slot is confirmed only up to a completed checkpoint. Before the fix,
// the periodic status update confirmed whatever had been received, and a
// restore from the last completed checkpoint was silently forwarded past
// changes the downstream never committed.
TEST(PostgresCdcContractLive, OnlyACompletedCheckpointConfirmsTheSlot) {
    REQUIRE_SERVER();
    Fixture f("confirm");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    History history;
    {
        CdcRun run(params, down);
        run.open();
        const std::uint64_t start = f.confirmed();
        {
            Session s(f.dsn());
            history.add(s.returning(insert_rows(f.table(), 1, 100)));
        }
        run.pump_until([&] { return down.changes().size() >= 100; });
        run.checkpoint(backends, 1);
        // Status updates go out every 100 ms; give them a second.
        const auto settle = Clock::now() + 1s;
        while (Clock::now() < settle) {
            ASSERT_TRUE(run.step());
        }
        EXPECT_EQ(f.confirmed(), start) << "the slot was confirmed before checkpoint 1 completed";

        run.complete(1);
        run.pump_until([&] { return f.confirmed() > start; }, 20s);
        const std::uint64_t at_one = f.confirmed();

        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id <= 50")));
        }
        run.pump_until([&] { return down.changes().size() >= 150; });
        run.checkpoint(backends, 2);
        run.abort(2);
        {
            Session s(f.dsn());
            history.add(s.returning(delete_rows(f.table(), "id <= 10")));
        }
        run.pump_until([&] { return down.changes().size() >= 160; });
        run.checkpoint(backends, 3);  // never completes
        const auto quiet = Clock::now() + 1s;
        while (Clock::now() < quiet) {
            ASSERT_TRUE(run.step());
        }
        EXPECT_EQ(f.confirmed(), at_one) << "an aborted or pending checkpoint confirmed the slot";
        run.crash();
    }
    // The restore from the last completed checkpoint gets every change after it.
    commit_marker(f);
    CdcRun second(params, down);
    second.restore(backends, 1);
    second.open();
    second.pump_until([&] { return down.streamed(kMarker); });
    second.close();
    expect_no_duplicate_changes(down);
    history.expect_all_emitted(down);
    EXPECT_EQ(down.state(), table_state(f));
}

// The server ends the walsender in the middle of a large transaction. The
// source reconnects by itself and resumes exactly, with no job restart and so
// no rewound downstream. The source is parked after its first batch of the
// transaction while the walsender is ended, so the transaction is far larger
// than what the sockets buffer and the cut really falls inside it.
TEST(PostgresCdcContractLive, ATerminatedReplicationSessionResumesInPlace) {
    REQUIRE_SERVER();
    Fixture f("terminate");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    Downstream down;
    History history;
    CdcRun run(params, down);
    run.open();
    {
        Session s(f.dsn());
        s.exec("BEGIN");
        history.add(s.returning(insert_rows(f.table(), 1, 60000)));
        s.exec("COMMIT");
    }
    clink::fault::ScopedFault fault("postgres_cdc.stream_batch=block@1");
    {
        PumpThread pump(run, down, "postgres_cdc.stream_batch");
        const auto deadline = Clock::now() + 30s;
        while (clink::fault::Registry::instance().hits("postgres_cdc.stream_batch") < 1) {
            ASSERT_LT(Clock::now(), deadline) << "the stream never emitted its first batch";
            std::this_thread::sleep_for(10ms);
        }
        const std::size_t before = down.changes().size();
        ASSERT_LT(before, 60000U) << "the whole transaction was emitted before the cut";
        (void)sql(f.dsn(),
                  "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE "
                  "slot_name = '" +
                      f.slot() + "'");
        {
            Session s(f.dsn());
            history.add(s.returning(update_rows(f.table(), "id <= 100")));
        }
        commit_marker(f);
        ASSERT_EQ(pump.finish(), "");
    }
    ASSERT_TRUE(down.streamed(kMarker));
    run.close();
    expect_no_duplicate_changes(down);
    history.expect_all_emitted(down);
    EXPECT_EQ(down.changes().size(), history.size() + 1);
    EXPECT_EQ(down.state(), table_state(f));
}

TEST(PostgresCdcContractLive, AMissingSlotIsRefusedRatherThanRecreated) {
    REQUIRE_SERVER();
    Fixture f("missing");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    {
        CdcRun run(params, down);
        run.open();
        {
            Session s(f.dsn());
            (void)s.returning(insert_rows(f.table(), 1, 5));
        }
        run.pump_until([&] { return down.changes().size() >= 5; });
        run.checkpoint(backends, 1);
        run.close();
    }
    (void)sql(f.dsn(), "SELECT pg_drop_replication_slot('" + f.slot() + "')");
    CdcRun second(params, down);
    second.restore(backends, 1);
    try {
        second.open();
        FAIL() << "a restore found its slot missing and carried on";
    } catch (const std::exception& e) {
        const std::string what = e.what();
        EXPECT_NE(what.find("does not exist"), std::string::npos) << what;
        EXPECT_NE(what.find("resumes from"), std::string::npos) << what;
    }
    EXPECT_TRUE(
        scalar(f.dsn(),
               "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '" + f.slot() + "'")
            .empty())
        << "the source recreated the slot";
}

// A slot confirmed past the checkpoint (another consumer, or advanced by
// hand): PostgreSQL would start at its own later position and skip the
// changes in between. The source refuses.
TEST(PostgresCdcContractLive, ASlotAdvancedPastTheCheckpointIsRefused) {
    REQUIRE_SERVER();
    Fixture f("advanced");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    {
        CdcRun run(params, down);
        run.open();
        {
            Session s(f.dsn());
            (void)s.returning(insert_rows(f.table(), 1, 5));
        }
        run.pump_until([&] { return down.changes().size() >= 5; });
        run.checkpoint(backends, 1);
        run.close();
    }
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 6, 10));
    }
    (void)sql(f.dsn(),
              "SELECT pg_replication_slot_advance('" + f.slot() + "', pg_current_wal_lsn())");
    CdcRun second(params, down);
    second.restore(backends, 1);
    try {
        second.open();
        FAIL() << "a restore behind the slot's confirmed position carried on";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string{e.what()}.find("beyond the position this job resumes from"),
                  std::string::npos)
            << e.what();
    }
}

// A value the contract cannot carry stops the source, and a restart from the
// last checkpoint meets it again: it is never skipped, and the slot is never
// confirmed past it.
TEST(PostgresCdcContractLive, AValueTheContractCannotCarryStopsTheSourceEveryTime) {
    REQUIRE_SERVER();
    Fixture f("refuse");
    sql(f.dsn(), "ALTER TABLE " + f.table() + " ADD COLUMN score float8");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    params["declared_types"] = std::string{kDeclared} + ";score:DOUBLE";
    std::map<std::uint64_t, InMemoryStateBackend> backends;
    Downstream down;
    std::uint64_t confirmed_before = 0;
    {
        CdcRun run(params, down);
        run.open();
        const std::uint64_t start = f.confirmed();
        {
            Session s(f.dsn());
            (void)s.returning(insert_rows(f.table(), 1, 5));
        }
        run.pump_until([&] { return down.changes().size() >= 5; });
        run.checkpoint(backends, 1);
        run.complete(1);
        run.pump_until([&] { return f.confirmed() > start; }, 20s);
        confirmed_before = f.confirmed();
        sql(f.dsn(), "UPDATE " + f.table() + " SET score = 'NaN' WHERE id = 3");
        bool stopped = false;
        const auto deadline = Clock::now() + 30s;
        while (!stopped && Clock::now() < deadline) {
            try {
                (void)run.step();
            } catch (const std::exception& e) {
                stopped = true;
                EXPECT_NE(std::string{e.what()}.find("score"), std::string::npos) << e.what();
                EXPECT_NE(std::string{e.what()}.find("on_decode_error=fail"), std::string::npos)
                    << e.what();
            }
        }
        ASSERT_TRUE(stopped) << "the NaN was carried or skipped";
        run.crash();
    }
    EXPECT_EQ(f.confirmed(), confirmed_before) << "the slot was confirmed past the bad change";
    CdcRun second(params, down);
    second.restore(backends, 1);
    second.open();
    bool again = false;
    const auto deadline = Clock::now() + 30s;
    while (!again && Clock::now() < deadline) {
        try {
            (void)second.step();
        } catch (const std::exception&) {
            again = true;
        }
    }
    EXPECT_TRUE(again) << "the restart moved past the change it could not carry";
    second.crash();
}

TEST(PostgresCdcContractLive, ADropPolicySkipsTheValueAndSaysSo) {
    REQUIRE_SERVER();
    Fixture f("drop");
    sql(f.dsn(), "ALTER TABLE " + f.table() + " ADD COLUMN score float8");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    params["on_decode_error"] = "drop";
    params["declared_types"] = std::string{kDeclared} + ";score:DOUBLE";
    Downstream down;
    CdcRun run(params, down);
    run.open();
    sql(f.dsn(),
        "INSERT INTO " + f.table() + " (id, rev, score) VALUES (1, 1, 'NaN'), (2, 1, 1.5)");
    commit_marker(f);
    run.pump_until([&] { return down.streamed(kMarker); });
    run.close();
    EXPECT_FALSE(down.saw(1)) << "the NaN row was emitted";
    EXPECT_TRUE(down.saw(2));
}

TEST(PostgresCdcContractLive, MisconfigurationsAreRefusedAtOpenWithTheFix) {
    REQUIRE_SERVER();
    struct Case {
        std::string name;
        std::function<void(Fixture&, std::map<std::string, std::string>&)> setup;
        std::string expect;
    };
    const std::vector<Case> cases = {
        {"missing publication",
         [](Fixture& f, auto& p) { p["publication_names"] = "nope_" + f.pub(); },
         "does not exist"},
        {"table not in the publication",
         [](Fixture& f, auto&) {
             sql(f.dsn(), "ALTER PUBLICATION " + f.pub() + " DROP TABLE " + f.table());
         },
         "is in none of the publications"},
        {"deletes not published",
         [](Fixture& f, auto&) {
             sql(f.dsn(), "ALTER PUBLICATION " + f.pub() + " SET (publish = 'insert, update')");
         },
         "publishes deletes"},
        {"replica identity nothing",
         [](Fixture& f, auto&) {
             sql(f.dsn(), "ALTER TABLE " + f.table() + " REPLICA IDENTITY NOTHING");
         },
         "REPLICA IDENTITY NOTHING"},
        {"another primary key", [](Fixture&, auto& p) { p["primary_key"] = "rev"; }, "primary key"},
        {"no such table",
         [](Fixture& f, auto& p) { p["table"] = "public.nope_" + f.table(); },
         "does not exist"},
        {"a declared column the table lacks",
         [](Fixture&, auto& p) {
             p["declared_types"] = std::string{kDeclared} + ";missing:BIGINT";
         },
         "has no column missing"},
        {"a type pair the contract cannot carry",
         [](Fixture&, auto& p) {
             p["declared_types"] =
                 "id:BIGINT;rev:INTEGER;status:BIGINT;__op:VARCHAR;__version:BIGINT;__deleted:"
                 "BOOLEAN";
         },
         "cannot be read as BIGINT"},
    };
    for (const auto& c : cases) {
        Fixture f("misc");
        auto params = f.params();
        c.setup(f, params);
        Downstream down;
        CdcRun run(params, down);
        try {
            run.open();
            ADD_FAILURE() << c.name << ": open() accepted it";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string{e.what()}.find(c.expect), std::string::npos)
                << c.name << ": " << e.what();
        }
        run.crash();
    }
}

TEST(PostgresCdcContractLive, ARoleWithoutReplicationOrSelectIsRefused) {
    REQUIRE_SERVER();
    Fixture f("perm");
    const std::string role = "cdc_role_" + std::to_string(::getpid());
    sql(f.dsn(), "DROP ROLE IF EXISTS " + role);
    sql(f.dsn(), "CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
    // conninfo for the role: the fixture's DSN with the user and password replaced.
    std::string dsn = f.dsn();
    const auto set_kv = [&](const std::string& key, const std::string& value) {
        const auto at = dsn.find(key + "=");
        if (at == std::string::npos) {
            dsn += " " + key + "=" + value;
            return;
        }
        const auto end = dsn.find(' ', at);
        dsn.replace(at, (end == std::string::npos ? dsn.size() : end) - at, key + "=" + value);
    };
    set_kv("user", role);
    set_kv("password", "pw");
    auto params = f.params();
    params["conninfo"] = dsn;
    {
        Downstream down;
        CdcRun run(params, down);
        try {
            run.open();
            ADD_FAILURE() << "a role without REPLICATION opened";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string{e.what()}.find("REPLICATION"), std::string::npos) << e.what();
        }
        run.crash();
    }
    sql(f.dsn(), "ALTER ROLE " + role + " REPLICATION");
    {
        Downstream down;
        CdcRun run(params, down);
        try {
            run.open();
            ADD_FAILURE() << "a role that cannot read the table took a snapshot";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string{e.what()}.find("GRANT SELECT"), std::string::npos) << e.what();
        }
        run.crash();
    }
    sql(f.dsn(), "DROP ROLE IF EXISTS " + role);
}

// cancel() during the waits on the server returns within a bound: while slot
// creation waits for a transaction open elsewhere, during a slow snapshot
// fetch, and while the stream is idle.
TEST(PostgresCdcContractLive, CancelReturnsPromptlyFromEveryWait) {
    REQUIRE_SERVER();
    Fixture f("cancel");
    // Slot creation waits for transactions running at the time to finish.
    {
        Session blocker(f.dsn());
        blocker.exec("BEGIN");
        blocker.exec("INSERT INTO " + f.table() + " (id, rev) VALUES (1, 1)");
        Downstream down;
        CdcRun run(f.params(), down);
        std::atomic<bool> opened{false};
        std::thread opener([&] {
            try {
                run.open();
            } catch (...) {
            }
            opened.store(true);
        });
        std::this_thread::sleep_for(1500ms);
        EXPECT_FALSE(opened.load()) << "slot creation did not wait for the open transaction";
        const auto cancelled_at = Clock::now();
        run.source()->cancel();
        opener.join();
        EXPECT_LT(Clock::now() - cancelled_at, 3s) << "cancel took too long";
        EXPECT_FALSE(run.step()) << "a cancelled source kept producing";
        blocker.exec("ROLLBACK");
        run.crash();
        // The server abandoned the half-made slot: none is left once the
        // transaction it waited for has ended.
        std::this_thread::sleep_for(1s);
        EXPECT_TRUE(scalar(f.dsn(),
                           "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '" +
                               f.slot() + "'")
                        .empty())
            << "a cancelled slot creation left its slot behind";
    }
    // A slow snapshot fetch.
    {
        clink::PostgresCdcSource::Options o;
        o.conninfo = f.dsn();
        o.slot_name = f.slot() + "_q";
        o.plugin = "pgoutput";
        o.publication_names = f.pub();
        o.enable_initial_snapshot = true;
        o.initial_snapshot = {{.table = "slow", .query = "SELECT pg_sleep(60)"}};
        o.drop_slot_on_close = true;
        clink::PostgresCdcSource src(o);
        src.open();
        std::atomic<bool> done{false};
        std::thread producer([&] {
            Emitter<clink::CdcEvent> em{Emitter<clink::CdcEvent>::Forward{
                [](const StreamElement<clink::CdcEvent>&) { return true; }}};
            try {
                while (src.produce(em)) {
                }
            } catch (...) {
            }
            done.store(true);
        });
        std::this_thread::sleep_for(1s);
        const auto cancelled_at = Clock::now();
        src.cancel();
        producer.join();
        EXPECT_LT(Clock::now() - cancelled_at, 3s)
            << "cancel during a snapshot fetch took too long";
        src.close();
    }
    // An idle stream.
    {
        auto params = f.params();
        params["snapshot_mode"] = "never";
        params["slot_name"] = f.slot() + "_i";
        params["drop_slot_on_close"] = "true";
        Downstream down;
        CdcRun run(params, down);
        run.open();
        std::thread producer([&] {
            while (run.step()) {
            }
        });
        std::this_thread::sleep_for(500ms);
        const auto cancelled_at = Clock::now();
        run.source()->cancel();
        producer.join();
        EXPECT_LT(Clock::now() - cancelled_at, 2s) << "cancel of an idle stream took too long";
        run.close();
    }
}

// Representative values of every supported type, NULL in each column, and an
// out-of-line value an UPDATE leaves unchanged: taken from the old row under
// REPLICA IDENTITY FULL, and a stop under DEFAULT.
TEST(PostgresCdcContractLive, TypesNullsAndUnchangedToastValues) {
    REQUIRE_SERVER();
    Fixture f("types");
    sql(f.dsn(),
        "ALTER TABLE " + f.table() +
            " ADD COLUMN small int2, ADD COLUMN big int8, ADD COLUMN r float4, ADD "
            "COLUMN d float8, ADD COLUMN ts timestamp(3), ADD COLUMN u uuid, ADD COLUMN j "
            "jsonb, ADD COLUMN body text");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    params["declared_types"] = std::string{kDeclared} +
                               ";small:SMALLINT;big:BIGINT;r:REAL;d:DOUBLE;ts:TIMESTAMP(3);u:"
                               "VARCHAR;j:VARCHAR;body:VARCHAR";
    Downstream down;
    CdcRun run(params, down);
    run.open();
    // A body PostgreSQL stores out of line: large and incompressible.
    const std::string big_body =
        "SELECT string_agg(md5(i::text), '') FROM generate_series(1, 1000) i";
    sql(f.dsn(),
        "INSERT INTO " + f.table() +
            " (id, rev, status, amount, at, flag, day, note, small, big, r, d, ts, u, j, "
            "body) VALUES (1, 1, 'a''b\"c', -12.5, '2026-03-04 05:06:07.089+02', true, "
            "'2026-03-04', 'x', -32768, 9223372036854775807, 1.5, -2.25e-10, '2026-03-04 "
            "05:06:07.089', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": [1, 2]}', (" +
            big_body +
            ")), (2, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            "NULL, NULL, NULL)");
    sql(f.dsn(), "UPDATE " + f.table() + " SET rev = 2 WHERE id = 1");
    commit_marker(f);
    run.pump_until([&] { return down.streamed(kMarker); });
    run.close();

    const Change* inserted = nullptr;
    const Change* updated = nullptr;
    const Change* nulls = nullptr;
    for (const auto& c : down.changes()) {
        if (c.id == 1 && c.op == "insert") {
            inserted = &c;
        } else if (c.id == 1 && c.op == "update") {
            updated = &c;
        } else if (c.id == 2) {
            nulls = &c;
        }
    }
    ASSERT_NE(inserted, nullptr);
    ASSERT_NE(updated, nullptr);
    ASSERT_NE(nulls, nullptr);
    const auto& row = inserted->row;
    EXPECT_EQ(row.at("status").as_string(), "a'b\"c");
    EXPECT_DOUBLE_EQ(row.at("amount").as_number(), -12.5);
    EXPECT_EQ(row.at("at").as_int(), 1772593567089LL);  // 2026-03-04 03:06:07.089 UTC
    EXPECT_TRUE(row.at("flag").as_bool());
    EXPECT_EQ(row.at("day").as_string(), "2026-03-04");
    EXPECT_EQ(row.at("small").as_int(), -32768);
    EXPECT_EQ(row.at("big").as_int(), 9223372036854775807LL);
    EXPECT_DOUBLE_EQ(row.at("r").as_number(), 1.5);
    EXPECT_DOUBLE_EQ(row.at("d").as_number(), -2.25e-10);
    EXPECT_EQ(row.at("ts").as_int(), 1772600767089LL);  // read as UTC
    EXPECT_EQ(row.at("u").as_string(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    EXPECT_EQ(row.at("j").as_string(), "{\"k\": [1, 2]}");
    const std::string want_body = scalar(f.dsn(), big_body);
    EXPECT_EQ(row.at("body").as_string(), want_body);
    // The UPDATE left body unchanged; under FULL the old row carried it.
    EXPECT_EQ(updated->row.at("body").as_string(), want_body);
    for (const char* col : {"status",
                            "amount",
                            "at",
                            "flag",
                            "day",
                            "note",
                            "small",
                            "big",
                            "r",
                            "d",
                            "ts",
                            "u",
                            "j",
                            "body"}) {
        EXPECT_TRUE(nulls->row.at(col).is_null()) << col;
    }
}

TEST(PostgresCdcContractLive, AnUnchangedToastValueWithoutFullIdentityStopsTheSource) {
    REQUIRE_SERVER();
    Fixture f("toast", /*full_identity=*/false);
    sql(f.dsn(), "ALTER TABLE " + f.table() + " ADD COLUMN body text");
    auto params = f.params();
    params["snapshot_mode"] = "never";
    params["declared_types"] = std::string{kDeclared} + ";body:VARCHAR";
    Downstream down;
    CdcRun run(params, down);
    run.open();
    sql(f.dsn(),
        "INSERT INTO " + f.table() +
            " (id, rev, body) SELECT 1, 1, string_agg(md5(i::text), '') FROM "
            "generate_series(1, 1000) i");
    sql(f.dsn(), "UPDATE " + f.table() + " SET rev = 2 WHERE id = 1");
    bool stopped = false;
    const auto deadline = Clock::now() + 30s;
    while (!stopped && Clock::now() < deadline) {
        try {
            (void)run.step();
        } catch (const std::exception& e) {
            stopped = true;
            EXPECT_NE(std::string{e.what()}.find("REPLICA IDENTITY FULL"), std::string::npos)
                << e.what();
        }
    }
    EXPECT_TRUE(stopped) << "an update without the unchanged value was emitted";
    EXPECT_FALSE(std::any_of(down.changes().begin(), down.changes().end(), [](const Change& c) {
        return c.op == "update";
    }));
    run.crash();
}

// A partitioned table, captured through its root: the snapshot reads every
// partition, and changes in any partition, a row moved between partitions
// included, arrive under the root's name.
TEST(PostgresCdcContractLive, APartitionedTableIsCapturedThroughItsRoot) {
    REQUIRE_SERVER();
    Fixture f("parts", /*full_identity=*/true, /*partitioned=*/true);
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 2000));
    }
    Downstream down;
    CdcRun run(f.params(), down);
    run.open();
    {
        Session s(f.dsn());
        (void)s.returning(update_rows(f.table(), "id % 7 = 0"));
        (void)s.returning(delete_rows(f.table(), "id % 11 = 0"));
        (void)s.returning(insert_rows(f.table(), 2'000'000, 2'000'050));
        // Moves rows from _low to _mid: a delete there and an insert here.
        (void)s.returning("UPDATE " + f.table() +
                          " SET id = id + 5000 WHERE id BETWEEN 100 AND 120 RETURNING id::text");
    }
    commit_marker(f);
    run.pump_until([&] { return down.streamed(kMarker); });
    EXPECT_EQ(down.state(), table_state(f));
    // A moved row keeps the values it had under its old id: compare those
    // with the table rather than with the values its id implies.
    std::map<std::int64_t, const Change*> latest;
    for (const auto& c : down.changes()) {
        if (c.id >= 5100 && c.id <= 5120) {
            latest[c.id] = &c;
        }
    }
    const auto moved =
        sql(f.dsn(), "SELECT id, note FROM " + f.table() + " WHERE id BETWEEN 5100 AND 5120").rows;
    EXPECT_EQ(moved.size(), latest.size());
    for (const auto& row : moved) {
        const auto it = latest.find(std::stoll(*row[0]));
        ASSERT_NE(it, latest.end()) << *row[0];
        EXPECT_EQ(it->second->op, "insert") << *row[0];
        EXPECT_EQ(it->second->row.at("note").as_string(), *row[1]) << *row[0];
    }

    // What PostgreSQL does not publish under publish_via_partition_root: a
    // TRUNCATE of one partition. The rows stay downstream, as the connector
    // page says.
    sql(f.dsn(), "TRUNCATE " + f.table() + "_low");
    sql(f.dsn(),
        "UPDATE " + f.table() + " SET rev = rev + 1 WHERE id = " + std::to_string(kMarker));
    run.pump_until([&] {
        return std::any_of(down.changes().begin(), down.changes().end(), [](const Change& c) {
            return c.id == kMarker && c.rev == 2;
        });
    });
    run.close();
    const auto held = down.state();
    EXPECT_TRUE(held.count(1) == 1 && table_state(f).count(1) == 0)
        << "a TRUNCATE of one partition reached the source";
}

TEST(PostgresCdcContractLive, APartitionedTablePublishedOtherwiseIsRefused) {
    REQUIRE_SERVER();
    struct Case {
        const char* name;
        std::function<void(Fixture&)> setup;
        const char* expect;
    };
    const std::vector<Case> cases = {
        {"published without publish_via_partition_root",
         [](Fixture& f) {
             sql(f.dsn(),
                 "ALTER PUBLICATION " + f.pub() + " SET (publish_via_partition_root = false)");
         },
         "publish_via_partition_root"},
        {"a partition published on its own",
         [](Fixture& f) {
             sql(f.dsn(), "ALTER PUBLICATION " + f.pub() + " SET TABLE " + f.table() + "_low");
         },
         "under its own name"},
        {"a partition with no replica identity",
         [](Fixture& f) {
             sql(f.dsn(), "ALTER TABLE " + f.table() + "_mid REPLICA IDENTITY NOTHING");
         },
         "_mid has REPLICA IDENTITY NOTHING"},
    };
    for (const auto& c : cases) {
        Fixture f("partsno", /*full_identity=*/true, /*partitioned=*/true);
        c.setup(f);
        Downstream down;
        CdcRun run(f.params(), down);
        try {
            run.open();
            ADD_FAILURE() << c.name << ": open() accepted it";
        } catch (const std::exception& e) {
            EXPECT_NE(std::string{e.what()}.find(c.expect), std::string::npos)
                << c.name << ": " << e.what();
        }
        run.crash();
    }
}

// Microsecond timestamps: refused by default, and with
// on_sub_millisecond='truncate' rounded down to the millisecond in the
// snapshot and the stream, each one counted.
TEST(PostgresCdcContractLive, SubMillisecondTimestampsAreRoundedDownOnlyWhenAsked) {
    REQUIRE_SERVER();
    Fixture f("submilli");
    sql(f.dsn(),
        "ALTER TABLE " + f.table() +
            " ADD COLUMN fine timestamp(6), ADD COLUMN fine_tz timestamptz(6)");
    const auto set_fine = [&](std::int64_t id, const std::string& fine, const std::string& tz) {
        sql(f.dsn(),
            "UPDATE " + f.table() + " SET fine = '" + fine + "', fine_tz = '" + tz +
                "' WHERE id = " + std::to_string(id));
    };
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 1));
    }
    set_fine(1, "2026-03-04 05:06:07.089123", "2026-03-04 05:06:07.089999+00");

    auto params = f.params();
    params["declared_types"] =
        std::string{kDeclared} + ";fine:TIMESTAMP(3);fine_tz:TIMESTAMP(3) WITH TIME ZONE";
    {
        // Refused by default, naming the option.
        Downstream down;
        CdcRun run(params, down);
        run.open();
        bool stopped = false;
        const auto deadline = Clock::now() + 30s;
        while (!stopped && Clock::now() < deadline) {
            try {
                (void)run.step();
            } catch (const std::exception& e) {
                stopped = true;
                EXPECT_NE(std::string{e.what()}.find("on_sub_millisecond"), std::string::npos)
                    << e.what();
            }
        }
        EXPECT_TRUE(stopped) << "a microsecond timestamp was emitted";
        EXPECT_TRUE(down.changes().empty());
        run.crash();
        // The next run starts on a slot of its own, with an exact snapshot.
        const auto drop_by = Clock::now() + 30s;
        while (scalar(f.dsn(),
                      "SELECT active FROM pg_replication_slots WHERE slot_name = '" + f.slot() +
                          "'") == "t" &&
               Clock::now() < drop_by) {
            std::this_thread::sleep_for(100ms);
        }
        (void)sql(f.dsn(), "SELECT pg_drop_replication_slot('" + f.slot() + "')");
    }

    auto& counter = clink::MetricsRegistry::global().counter(clink::metrics::connector_metric_name(
        "timestamps_truncated_total", "postgres_cdc", "source"));
    const std::uint64_t counted_before = counter.value();
    params["on_sub_millisecond"] = "truncate";
    Downstream down;
    CdcRun run(params, down);
    run.open();
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 2, 2));
    }
    // Before 1970 the value rounds down too: -0.5 ms becomes -1 ms.
    set_fine(2, "1969-12-31 23:59:59.9995", "2026-03-04 05:06:07.000001+00");
    commit_marker(f);
    run.pump_until([&] { return down.streamed(kMarker); });
    run.close();

    const Change* snapshot = nullptr;
    const Change* streamed = nullptr;
    for (const auto& c : down.changes()) {
        if (c.id == 1 && c.op == "snapshot") {
            snapshot = &c;
        } else if (c.id == 2 && c.op == "update") {
            streamed = &c;
        }
    }
    ASSERT_NE(snapshot, nullptr);
    ASSERT_NE(streamed, nullptr);
    EXPECT_EQ(snapshot->row.at("fine").as_int(), 1772600767089LL);
    EXPECT_EQ(snapshot->row.at("fine_tz").as_int(), 1772600767089LL);
    EXPECT_EQ(streamed->row.at("fine").as_int(), -1);
    EXPECT_EQ(streamed->row.at("fine_tz").as_int(), 1772600767000LL);
    // Four values lost digits: both columns of the snapshot row and of the
    // update. The insert of row 2 had NULLs, and the marker none.
    EXPECT_EQ(counter.value() - counted_before, 4U);
}

// Upsert mode: snapshot rows and inserts are 'insert', updates
// 'update_after', deletes 'delete', and a key-changing update a delete of the
// old key followed by the new row.
TEST(PostgresCdcContractLive, UpsertModeEmitsAKeyedChangelog) {
    REQUIRE_SERVER();
    Fixture f("upsert");
    {
        Session s(f.dsn());
        (void)s.returning(insert_rows(f.table(), 1, 3));
    }
    auto params = f.params("upsert");
    params["declared_types"] = "id:BIGINT;rev:INTEGER;status:VARCHAR";
    std::vector<clink::config::JsonObject> rows;
    auto src = build(params);
    src->open();
    Emitter<std::string> em(Emitter<std::string>::Forward([&](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                rows.push_back(clink::config::parse(r.value()).as_object());
            }
        }
        return true;
    }));
    sql(f.dsn(), "UPDATE " + f.table() + " SET rev = 2 WHERE id = 1");
    sql(f.dsn(), "DELETE FROM " + f.table() + " WHERE id = 2");
    sql(f.dsn(), "UPDATE " + f.table() + " SET id = 30 WHERE id = 3");
    sql(f.dsn(),
        "INSERT INTO " + f.table() + " (id, rev) VALUES (" + std::to_string(kMarker) + ", 1)");
    const auto deadline = Clock::now() + 30s;
    while (Clock::now() < deadline) {
        ASSERT_TRUE(src->produce(em));
        if (!rows.empty() && rows.back().at("id").as_int() == kMarker) {
            break;
        }
    }
    src->close();
    std::vector<std::string> kinds;
    for (const auto& r : rows) {
        kinds.push_back(std::to_string(r.at("id").as_int()) + ":" + r.at("__row_kind").as_string());
    }
    const std::vector<std::string> want = {"1:insert",
                                           "2:insert",
                                           "3:insert",
                                           "1:update_after",
                                           "2:delete",
                                           "3:delete",
                                           "30:update_after",
                                           std::to_string(kMarker) + ":insert"};
    EXPECT_EQ(kinds, want);
}

#endif  // CLINK_HAS_POSTGRES
