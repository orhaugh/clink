#pragma once

// An in-process stand-in for a ClickHouse server and the transport the native
// sink talks to it through, so the sink's retry, deduplication and abandonment
// rules can be driven deterministically without a server.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "native/insert_transport.hpp"

namespace clink::clickhouse::native::testing {

enum class Step : std::uint8_t { Connect, Select, Begin, Send, End };

// One scripted outcome. Faults fire in arm order on the nth matching call.
struct Fault {
    Step step{Step::End};
    enum class Kind : std::uint8_t {
        ServerError,        // ::clickhouse::ServerException{code, message}
        SystemError,        // std::system_error(ECONNRESET or code)
        TlsError,           // ::clickhouse::OpenSSLError(message)
        ProtocolError,      // ::clickhouse::ProtocolError(message)
        BadOptionalAccess,  // std::bad_optional_access
        Delay,              // succeed after `delay`
        Hang                // block until interrupt() or abandon()
    } kind{Kind::ServerError};
    int code{0};
    std::string message;
    std::chrono::milliseconds delay{0};  // applied before the outcome
    std::size_t nth{1};
    // End only: what reached the table before the failure.
    enum class Landing : std::uint8_t {
        Nothing,
        Everything,
        FirstPartitions
    } landing{Landing::Nothing};
    std::size_t landed_partitions{0};
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

struct LandedBlock {
    std::string token;  // the INSERT's token with the block's index
    std::size_t rows{0};
    std::vector<std::vector<std::string>> values;  // rendered per row, for content checks
};

// The server side, shared by every FakeTransport a test creates, so a rebuilt
// client sees the same tables. Thread-safe.
class FakeServer {
public:
    FakeServer();
    ~FakeServer();
    FakeServer(const FakeServer&) = delete;
    FakeServer& operator=(const FakeServer&) = delete;

    void set_version(std::uint64_t major, std::uint64_t minor, std::uint64_t patch);
    void set_settings(std::vector<std::pair<std::string, std::string>> name_values);
    void set_merge_tree_setting(const std::string& name, const std::string& value);
    void add_table(FakeTable table);
    void add_cluster(const std::string& name, std::vector<FakeServer*> replicas);
    void set_unreadable_replica(const std::string& cluster, std::size_t replica);
    void alter_column_type(const std::string& table,
                           const std::string& column,
                           const std::string& type);
    void inject(Fault fault);
    void set_down(bool down);  // every Connect fails with ECONNREFUSED

    // Observations.
    [[nodiscard]] std::vector<LandedBlock> landed(const std::string& table) const;
    [[nodiscard]] std::uint64_t rows(const std::string& table) const;
    [[nodiscard]] std::vector<std::string> statements() const;  // every SQL text received
    [[nodiscard]] std::size_t abandoned_mid_insert() const;
    // A client destroyed mid-INSERT without abandon(): the real client's
    // destructor commits, so the fake commits too and counts it. Every sink
    // test asserts this stays 0.
    [[nodiscard]] std::size_t destroyed_mid_insert() const;
    [[nodiscard]] std::size_t connects() const;

    struct Impl;
    [[nodiscard]] Impl& impl() noexcept { return *impl_; }

private:
    std::unique_ptr<Impl> impl_;
};

class FakeTransport final : public InsertTransport {
public:
    explicit FakeTransport(std::shared_ptr<FakeServer> server);
    ~FakeTransport() override;  // commits an open, un-abandoned INSERT (see above)
    FakeTransport(const FakeTransport&) = delete;
    FakeTransport& operator=(const FakeTransport&) = delete;

    void connect(const Endpoint& endpoint) override;
    [[nodiscard]] bool connected() const noexcept override;
    [[nodiscard]] const ServerIdentity& server() const override;
    ResultSet select(MetaQuery kind, const std::string& sql) override;
    std::vector<HeaderColumn> begin_insert(const std::string& sql) override;
    void send_block(const ::clickhouse::Block& block) override;
    void end_insert() override;
    void abandon() noexcept override;
    void interrupt() noexcept override;
    [[nodiscard]] TransportCounters counters() const noexcept override;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] TransportFactory fake_factory(std::shared_ptr<FakeServer> server);

// Wraps any transport (the real one, in the live abandoned-INSERT test) and
// applies the same Fault script before delegating.
[[nodiscard]] std::unique_ptr<InsertTransport> faulty(std::unique_ptr<InsertTransport> inner,
                                                      std::shared_ptr<std::deque<Fault>> faults);

}  // namespace clink::clickhouse::native::testing
