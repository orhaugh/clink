#pragma once

// An in-process stand-in for a ClickHouse server and the transport the native
// sink talks to it through, so the sink's retry, deduplication and abandonment
// rules can be driven deterministically without a server.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "native/insert_transport.hpp"

namespace clink::clickhouse::native::testing {

enum class Step : std::uint8_t { Connect, Select, Begin, Send, End };

// One scripted outcome. Faults fire in arm order on the nth matching call:
// for each step only the earliest-armed fault of that step counts calls, from
// the moment it became the earliest, and a call fires at most one fault. A
// firing fault waits out `delay`, then lands what `landing` says, then runs
// `on_fire`, then raises its outcome.
struct Fault {
    Step step{Step::End};
    enum class Kind : std::uint8_t {
        ServerError,        // ::clickhouse::ServerException{code, message}
        SystemError,        // std::system_error(ECONNRESET or code)
        TlsError,           // ::clickhouse::OpenSSLError(message)
        ProtocolError,      // ::clickhouse::ProtocolError(message)
        CompressionError,   // ::clickhouse::CompressionError(message)
        Unimplemented,      // ::clickhouse::UnimplementedError(message)
        ValidationError,    // ::clickhouse::ValidationError(message)
        BadOptionalAccess,  // std::bad_optional_access
        Other,              // std::runtime_error(message)
        Delay,              // succeed after `delay`
        // Block until interrupt() or abandon(), then throw ECONNABORTED. A
        // connection broken by set_down throws ECONNRESET instead.
        Hang,
        // Block, ignoring interrupt() and the deadline, until
        // FakeServer::release(). The call then fails with ECONNABORTED if the
        // transport was interrupted or abandoned meanwhile, and otherwise goes
        // ahead as if nothing had happened.
        Uninterruptible
    } kind{Kind::ServerError};
    int code{0};
    std::string message;
    // Applied before the outcome. Like a slow server, it gives way to an
    // interrupt, an abandon, a broken connection and the deadline, except
    // under Uninterruptible, which sleeps it out.
    std::chrono::milliseconds delay{0};
    std::size_t nth{1};
    // End only: what reached the table before the failure. A fault whose
    // outcome is success lands the rest of the INSERT afterwards.
    enum class Landing : std::uint8_t {
        Nothing,
        Everything,
        FirstPartitions
    } landing{Landing::Nothing};
    std::size_t landed_partitions{0};
    // Runs on the calling thread when the fault fires, after any Landing and
    // before the outcome is raised: for example FakeServer::land_foreign, to
    // put other writers' INSERTs between an attempt and its resend. No lock of
    // the fake is held, so it may call into the server or the transport.
    std::function<void()> on_fire;
};

struct FakeTable {
    std::string database, name;
    std::string engine;       // "MergeTree", "ReplicatedMergeTree", "Distributed", "Null"
    std::string engine_full;  // with SETTINGS and Distributed(...) arguments
    std::vector<TargetColumn> columns;
    std::size_t dedup_window{0};  // blocks kept in the deduplication log; 0 = none
    // Partition of a row, from the first column's Int64 value; empty = one partition.
    std::function<std::int64_t(std::int64_t)> partition_of;
};

// Values are rendered as text, one string per column in the INSERT's column
// order: integers and floats in decimal, Bool as true or false, String and
// FixedString as their bytes, Date as YYYY-MM-DD, DateTime and DateTime64 in
// UTC as YYYY-MM-DD hh:mm:ss[.fraction] whatever their time zone, Decimal with
// its scale, UUID, IPv4 and IPv6 in their usual text, Enum by name and NULL as
// NULL. Inside Array [a,b], Tuple (a,b) and Map {k:v}, every value that is not
// a number is quoted as 'x', with ' and \ escaped.
struct LandedBlock {
    // The INSERT's token with the block's index, as <token>_<index>. The
    // index counts the INSERT's partitions in the order its rows first
    // reach them. Empty for an INSERT without a token, which is never
    // deduplicated.
    std::string token;
    std::size_t rows{0};
    std::vector<std::vector<std::string>> values;  // rendered per row, for content checks
};

// A block as the server received it, before any squashing.
struct ReceivedBlock {
    std::size_t rows{0};
    // Every column as the client writes it: name, type and the column's own
    // serialisation. Equal bytes mean an identical resend.
    std::string bytes;
    std::vector<std::vector<std::string>> values;
};

// One INSERT attempt: a successful begin_insert and what followed it.
struct ReceivedInsert {
    std::string token;
    std::string sql;
    std::vector<ReceivedBlock> blocks;
    enum class Outcome : std::uint8_t {
        Open,       // still in progress
        Committed,  // end_insert succeeded, or a destroyed client committed it
        Failed,     // a call on it failed, whatever had landed by then
        Abandoned   // abandon() while it was in progress
    } outcome{Outcome::Open};
};

// The server side, shared by every FakeTransport a test creates, so a rebuilt
// client sees the same tables. Thread-safe.
//
// A new server is a 26.8 server whose system.settings lists every setting the
// sink reads, with that line's defaults; whose system.merge_tree_settings has
// async_insert=0, non_replicated_deduplication_window=0 and
// replicated_deduplication_window=10000; which has
// system.replicated_merge_tree_settings; which has no tables, no clusters
// and no macros; and whose host name is its display name, on TCP port 9000.
// Metadata queries are answered by their MetaQuery kind from that
// configuration; the names, tables and clusters they ask about are read from
// the quoted literals of the statement text. A ClusterReplicaCount read of
// system.macros lists the macros.
class FakeServer {
public:
    FakeServer();
    ~FakeServer();
    FakeServer(const FakeServer&) = delete;
    FakeServer& operator=(const FakeServer&) = delete;

    void set_version(std::uint64_t major, std::uint64_t minor, std::uint64_t patch);
    // Replaces the whole system.settings listing.
    void set_settings(std::vector<std::pair<std::string, std::string>> name_values);
    void set_setting(const std::string& name, const std::string& value);  // adds or replaces one
    void remove_setting(const std::string& name);                         // a line that lacks it
    // A server-wide <merge_tree> default, as system.merge_tree_settings shows it.
    void set_merge_tree_setting(const std::string& name, const std::string& value);
    void remove_merge_tree_setting(const std::string& name);  // a line without that row
    // A <replicated_merge_tree> override: system.replicated_merge_tree_settings
    // shows the MergeTree settings with these applied on top.
    void set_replicated_merge_tree_setting(const std::string& name, const std::string& value);
    // false: a line without system.replicated_merge_tree_settings, where reading
    // it fails with code 60, as for any unknown table.
    void set_replicated_merge_tree_settings_table(bool present);
    // Replaces the definition of a table of the same name and keeps what has
    // landed in it.
    void add_table(FakeTable table);
    // system.clusters on this server. A clusterAllReplicas read asks each
    // replica in turn, and each row starts with the replica's
    // hostName():tcpPort() (replica_name()) and its serverUUID()
    // (server_uuid()). system.tables on a replica also lists
    // system.replicated_merge_tree_settings while that table is present.
    void add_cluster(const std::string& name, std::vector<FakeServer*> replicas);
    // What hostName() and tcpPort() return on this server, for instances that
    // share a machine.
    void set_host_name(const std::string& name);
    void set_tcp_port(std::uint16_t port);
    // An entry of the server's <macros> section, as system.macros lists it.
    void set_macro(const std::string& name, const std::string& substitution);
    // While set, every clusterAllReplicas read of `cluster` fails with 279, as a
    // server with skip_unavailable_shards=0 does when it cannot reach a replica.
    // A read under skip_unavailable_shards=1, from its own SETTINGS or else
    // from this server's system.settings, leaves the replica out instead, and
    // also leaves out a replica whose read fails for a missing table.
    void set_unreadable_replica(const std::string& cluster,
                                std::size_t replica,
                                bool unreadable = true);
    void alter_column_type(const std::string& table,
                           const std::string& column,
                           const std::string& type);
    // Arms a fault for every transport connected to this server. Throws
    // std::invalid_argument for nth 0, or for a landing on a step other than
    // End.
    void inject(Fault fault);
    // Down: every Connect fails with ECONNREFUSED. With break_live_connections,
    // every call on a connection made before the switch also throws
    // std::system_error(ECONNRESET), so a live writer reconnects.
    void set_down(bool down, bool break_live_connections = true);
    // Lands `n` blocks of another writer's INSERTs on `table`, each with its
    // own token, so they enter the dedup FIFO like real foreign inserts. They
    // carry no rows, so rows() still counts only the rows the test sent.
    void land_foreign(const std::string& table, std::size_t n);
    // Frees every call held by Kind::Uninterruptible, through any server or
    // faulty() wrapper in the process.
    void release();

    // "fake-<n>", with n unique in the process; also hostName() unless
    // set_host_name changed it.
    [[nodiscard]] const std::string& display_name() const noexcept;
    // hostName():tcpPort(), as a cluster read's first column names this server.
    [[nodiscard]] std::string replica_name() const;
    // serverUUID(), unique in the process.
    [[nodiscard]] const std::string& server_uuid() const noexcept;

    // Observations. A table is named as "database.name" or as just "name";
    // naming one the server does not have throws std::invalid_argument.
    [[nodiscard]] std::vector<LandedBlock> landed(const std::string& table) const;
    [[nodiscard]] std::uint64_t rows(const std::string& table) const;
    // Every INSERT attempt on the table, in the order they began.
    [[nodiscard]] std::vector<ReceivedInsert> inserts(const std::string& table) const;
    [[nodiscard]] std::vector<std::string> statements() const;  // every SQL text received
    // abandon() calls made while an INSERT was open on that transport: after
    // a successful begin_insert and before a successful end_insert, including
    // after a failed one. So an INSERT whose End fails k times and is then
    // dropped counts k.
    [[nodiscard]] std::size_t abandoned_mid_insert() const;
    // A client destroyed while inserting without abandon(): mid-INSERT, or
    // after a begin_insert that failed, which leaves the client inserting.
    // The real client's destructor then sends the end-of-data marker, which
    // commits an open INSERT, so the fake commits too and counts it. It
    // counts even where nothing could commit (a failed begin_insert or
    // INSERT, an interrupted or broken connection). Every sink test asserts
    // this stays 0.
    [[nodiscard]] std::size_t destroyed_mid_insert() const;
    // Every connect() routed to this server, the refused ones included, so a
    // failover test can see which endpoints were tried.
    [[nodiscard]] std::size_t connects() const;

    struct Impl;
    [[nodiscard]] Impl& impl() noexcept { return *impl_; }

private:
    std::unique_ptr<Impl> impl_;
};

class FakeTransport final : public InsertTransport {
public:
    // Every endpoint reaches `server`.
    explicit FakeTransport(std::shared_ptr<FakeServer> server);
    // Each endpoint reaches its own server; one not in the map refuses with
    // ECONNREFUSED.
    explicit FakeTransport(std::map<Endpoint, std::shared_ptr<FakeServer>> servers);
    ~FakeTransport() override;  // commits an open, un-abandoned INSERT (see above)
    FakeTransport(const FakeTransport&) = delete;
    FakeTransport& operator=(const FakeTransport&) = delete;

    // As on the real transport: a call without a connection throws ENOTCONN,
    // one that breaks the client's own call order throws the client's
    // ValidationError, and every call after interrupt() throws ECONNABORTED.
    // A connection that has thrown a std::system_error is dead, and every
    // later call on it throws ECONNRESET until abandon().
    void connect(const Endpoint& endpoint) override;
    [[nodiscard]] bool connected() const noexcept override;
    // The server of the last successful connect; empty before the first.
    [[nodiscard]] const ServerIdentity& server() const override;
    ResultSet select(MetaQuery kind, const std::string& sql) override;
    // The header has the INSERT's column list in its order, each column in
    // client_header_spelling of its table type. A type the client cannot
    // build throws ::clickhouse::UnimplementedError, as inside BeginInsert.
    // Whatever it throws, the client is left inserting, as BeginInsert
    // leaves it. Until abandon(), select and begin_insert then throw the
    // client's ValidationError, send_block lands nothing, and end_insert
    // lands nothing and throws ETIMEDOUT, the client's receive timeout on a
    // reply the server will not send.
    std::vector<HeaderColumn> begin_insert(const std::string& sql) override;
    // A block the server would refuse fails the INSERT with server code 53
    // at end_insert, as the server's reply would. Each column is checked
    // against its table type as the server compares types, not against the
    // header's spelling: Bool is UInt8 to the server, a time zone does not
    // make a different type, and under
    // input_format_native_allow_types_conversion=0, which every sink INSERT
    // sends, the server adds or removes LowCardinality itself, inside an
    // Array or a Tuple too, and inside a Map from 26.8.
    void send_block(const ::clickhouse::Block& block) override;
    void end_insert() override;
    void abandon() noexcept override;
    void interrupt() noexcept override;
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept override;
    // bytes_written counts statement text and serialised blocks, bytes_read
    // the text of results and headers: a stand-in for protocol bytes.
    [[nodiscard]] TransportCounters counters() const noexcept override;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] TransportFactory fake_factory(std::shared_ptr<FakeServer> server);
// Routes each connect(endpoint) to that endpoint's server; an endpoint not in
// the map refuses with ECONNREFUSED. For the failover and re-probe cases.
[[nodiscard]] TransportFactory fake_factory(
    std::map<Endpoint, std::shared_ptr<FakeServer>> servers);

// Wraps any transport (the real one, in the live abandoned-INSERT test) and
// applies the same Fault script before delegating. Landing::Everything
// delegates end_insert before raising the outcome, and Landing::Nothing
// raises it without delegating, so the INSERT stays open on the inner
// transport; FirstPartitions needs a FakeServer and throws std::logic_error
// when it fires. The wrapper reads the deque under its own lock, so arm the
// faults before it is used, or from the thread that drives it.
[[nodiscard]] std::unique_ptr<InsertTransport> faulty(std::unique_ptr<InsertTransport> inner,
                                                      std::shared_ptr<std::deque<Fault>> faults);

}  // namespace clink::clickhouse::native::testing
