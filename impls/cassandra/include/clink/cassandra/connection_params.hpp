#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace clink::cassandra {

// The consistency level a write must reach before the server acknowledges it.
// ANY, SERIAL and LOCAL_SERIAL are not offered: ANY acknowledges a write that no
// replica need hold yet (a hint on the coordinator), and the serial levels govern
// lightweight transactions, which the sinks do not use.
enum class CassandraConsistency : std::uint8_t {
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
};

// The level a name denotes, ignoring case ("local_quorum", "LOCAL_QUORUM"), or
// nullopt for a name that is not one of the levels above.
[[nodiscard]] std::optional<CassandraConsistency> parse_cassandra_consistency(
    std::string_view name);
// The lower-case name of a level ("local_quorum").
[[nodiscard]] std::string_view cassandra_consistency_name(CassandraConsistency c) noexcept;

// Cassandra (DataStax C/C++ driver) connection parameters. Plain fields - no driver types leak
// into the public headers (cassandra.h is confined to the .cpp). Works against Cassandra and
// ScyllaDB (same CQL binary protocol). Auth via username/password (leave both blank for a
// cluster without authentication). There is no TLS option: the sinks connect in plain text.
struct CassandraConnParams {
    std::string contact_points{"127.0.0.1"};  // comma-separated hosts
    int port{9042};
    std::string username;
    std::string password;
    int connect_timeout_ms{5000};
    // How long the driver waits for a node's answer to one request before it
    // fails that request as timed out (the write's outcome is then unknown).
    // Also bounds how long closing the session waits for writes still in flight.
    int request_timeout_ms{12000};
    // The driver's own default, set explicitly so it never depends on the
    // driver's version.
    CassandraConsistency consistency{CassandraConsistency::LocalOne};
};

}  // namespace clink::cassandra
