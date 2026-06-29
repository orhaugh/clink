#pragma once

#include <string>

namespace clink::cassandra {

// Cassandra (DataStax C/C++ driver) connection parameters. Plain fields - no driver types leak
// into the public headers (cassandra.h is confined to the .cpp). Works against Cassandra and
// ScyllaDB (same CQL binary protocol). Auth via username/password (leave blank for no-auth).
struct CassandraConnParams {
    std::string contact_points{"127.0.0.1"};  // comma-separated hosts
    int port{9042};
    std::string username;
    std::string password;
    int connect_timeout_ms{5000};
};

}  // namespace clink::cassandra
