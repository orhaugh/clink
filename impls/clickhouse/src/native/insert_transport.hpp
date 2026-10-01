#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <clickhouse/block.h>

#include "native/column_plan.hpp"
#include "native/sink_options.hpp"

namespace clink::clickhouse::native {

struct ServerIdentity {
    std::string display_name;
    std::uint64_t major{0}, minor{0}, patch{0}, revision{0};
    Endpoint endpoint;
};

// Every metadata query selects String columns only (statements.hpp wraps every
// selected expression in CAST(... AS String)), so a result is text. The real
// transport still accepts String, LowCardinality(String) and Nullable(String)
// result columns (a NULL reads as empty text) and throws ProtocolError on any
// other column type.
struct ResultSet {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

// Lets FakeTransport answer a query without parsing its SQL. The SQL text is
// still passed and recorded; statements tests check it.
enum class MetaQuery : std::uint8_t {
    ServerSettings,
    Table,
    Columns,
    MergeTreeSettings,
    ClusterReplicaCount,
    ClusterTables,
    ClusterMergeTreeSettings,
    ReplicatedMergeTreeSettings,
    ClusterReplicatedMergeTreeSettings
};

struct TransportCounters {
    std::uint64_t bytes_written{0};  // compressed protocol bytes, before TLS
    std::uint64_t bytes_read{0};
    std::uint64_t connects{0};
};

// One connection at a time, used from one thread at a time, except
// interrupt(). Every method throws the client library's own exception types
// (::clickhouse::ServerException, std::system_error, ::clickhouse::OpenSSLError,
// ::clickhouse::ProtocolError, ...); error_class.hpp maps them.
class InsertTransport {
public:
    virtual ~InsertTransport() = default;
    // Builds a client for exactly this endpoint (the client connects eagerly).
    // No-op when already connected.
    virtual void connect(const Endpoint& endpoint) = 0;
    [[nodiscard]] virtual bool connected() const noexcept = 0;
    [[nodiscard]] virtual const ServerIdentity& server() const = 0;
    virtual ResultSet select(MetaQuery kind, const std::string& sql) = 0;
    // BeginInsert(sql); returns the header block's columns.
    virtual std::vector<HeaderColumn> begin_insert(const std::string& sql) = 0;
    virtual void send_block(const ::clickhouse::Block& block) = 0;
    virtual void end_insert() = 0;
    // Drops the connection so that nothing it has sent can be committed:
    // poison the socket first, then destroy the client. Safe in any state,
    // including mid-INSERT; the next connect() builds a new client. The only
    // way the sink ever disposes of a client.
    virtual void abandon() noexcept = 0;
    // Callable from any thread: makes a call blocked on this transport return
    // promptly (poison and shut down the socket). Sticky: every later connect()
    // throws std::system_error(ECONNABORTED). Destroys nothing; the owning
    // thread then calls abandon().
    virtual void interrupt() noexcept = 0;
    // Owning thread. Every socket read or write after `deadline` throws
    // std::system_error(ETIMEDOUT), so a begin_insert or end_insert kept busy
    // by Progress or ProfileEvents packets still ends. nullopt clears it; the
    // writer arms it only around those two calls.
    virtual void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept = 0;
    [[nodiscard]] virtual TransportCounters counters() const noexcept = 0;
};

using TransportFactory = std::function<std::unique_ptr<InsertTransport>(const SinkOptions&)>;

[[nodiscard]] std::unique_ptr<InsertTransport> make_clickhouse_transport(const SinkOptions&);

// Test seam: sinks built after the call use `factory`; nullptr restores
// make_clickhouse_transport. Process-wide, guarded by a mutex.
void set_transport_factory_for_testing(TransportFactory factory);
[[nodiscard]] TransportFactory current_transport_factory();

}  // namespace clink::clickhouse::native
