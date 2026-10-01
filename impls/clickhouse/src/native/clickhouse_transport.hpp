#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <clickhouse/block.h>
#include <clickhouse/client.h>

#include "native/column_plan.hpp"
#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"

namespace clink::clickhouse::native {

class SocketControl;

// The options of one client for exactly `endpoint`: no `host`, so the client
// has nothing of its own to iterate; exceptions rethrown, so an EndInsert
// failure is never swallowed; no ping and one send attempt, so the client
// never retries behind the sink's back; every timeout at least 1 ms, because
// the client reads 0 as "never". The real transport builds every client from
// these and from nothing else; exposed so that tests can check them field by
// field. Refuses clickhouse.option_invalid for a timeout below 1 ms, and
// clickhouse.tls_unavailable for secure='true' on a build without TLS.
[[nodiscard]] ::clickhouse::ClientOptions make_client_options(const SinkOptions& options,
                                                              const Endpoint& endpoint);

// The header BeginInsert returned, in the client's type spelling.
[[nodiscard]] std::vector<HeaderColumn> header_columns(const ::clickhouse::Block& header);

// Appends one block of a metadata query's result to `out`, as text. The first
// block with columns names them; every later block must carry the same
// columns. String, Nullable(String), LowCardinality(String) and
// LowCardinality(Nullable(String)) are read, a NULL as empty text; any other
// column type throws ::clickhouse::ProtocolError naming the column.
void append_result_block(const ::clickhouse::Block& block, ResultSet& out);

// The real transport: one ::clickhouse::Client at a time, each for a single
// endpoint and over its own SocketControl. It never calls ResetConnection():
// after any failure the caller abandons the client, and the next connect()
// builds a new one.
class ClickHouseTransport final : public InsertTransport {
public:
    // Refuses clickhouse.tls_unavailable for secure='true' on a build without
    // TLS, before any connect.
    explicit ClickHouseTransport(SinkOptions options);
    // Abandons the client, so a transport dropped mid-INSERT commits nothing.
    ~ClickHouseTransport() override;
    ClickHouseTransport(const ClickHouseTransport&) = delete;
    ClickHouseTransport& operator=(const ClickHouseTransport&) = delete;

    // A CA that OpenSSL cannot load refuses clickhouse.option_invalid, naming
    // the key, before any socket: it is configuration, not an outage.
    void connect(const Endpoint& endpoint) override;
    [[nodiscard]] bool connected() const noexcept override;
    // The server of the last successful connect; empty before the first.
    [[nodiscard]] const ServerIdentity& server() const override;
    ResultSet select(MetaQuery kind, const std::string& sql) override;
    std::vector<HeaderColumn> begin_insert(const std::string& sql) override;
    void send_block(const ::clickhouse::Block& block) override;
    void end_insert() override;
    void abandon() noexcept override;
    void interrupt() noexcept override;
    void set_deadline(
        std::optional<std::chrono::steady_clock::time_point> deadline) noexcept override;
    [[nodiscard]] TransportCounters counters() const noexcept override;

private:
    // Throws std::system_error(ENOTCONN) when there is no client.
    ::clickhouse::Client& client();

    const SinkOptions options_;
    // Owning thread only.
    std::unique_ptr<::clickhouse::Client> client_;
    ServerIdentity server_;

    // Shared with interrupt() and counters(), which may run on other threads.
    mutable std::mutex mu_;
    bool interrupted_{false};
    std::shared_ptr<SocketControl> control_;
    std::optional<std::chrono::steady_clock::time_point> deadline_;
    // What the controls connect() has replaced had counted.
    std::uint64_t retired_written_{0};
    std::uint64_t retired_read_{0};
    std::uint64_t connects_{0};
};

}  // namespace clink::clickhouse::native
