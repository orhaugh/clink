#pragma once

// Thin helpers over mongo-cxx-driver shared by the MongoDB source and sink. Pulls
// the mongocxx/bsoncxx headers, so it is included ONLY by the module's .cpp files -
// the public option headers (mongo_cdc_source.hpp / mongo_sink.hpp) stay
// driver-free so register_factories.cpp and the tests do not need the driver.
//
// mongocxx requires exactly ONE mongocxx::instance per process, created before any
// client and outliving every client. We create it once (call_once) and LEAK it
// (never destroyed) so its teardown cannot race a live client destructor at process
// exit - the same no-cleanup reasoning as the S3 FinalizeS3 / mosquitto_lib notes.
//
// Compiled only where mongo-cxx-driver is found (CLINK_HAS_MONGODB); the whole
// impls/mongodb module is dep-gated on it.

#include <mutex>
#include <string>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

namespace clink::mongodb {

inline void ensure_instance() {
    static std::once_flag once;
    std::call_once(once, [] { new mongocxx::instance{}; });  // leaked: process lifetime
}

// Build a connected client. mongocxx::client is cheap, pools connections
// internally, and is move-assignable. Tests use it for their own reads and
// writes; the source and sink use Connection.
inline mongocxx::client make_client(const std::string& uri) {
    ensure_instance();
    return mongocxx::client{mongocxx::uri{uri}};
}

// The source's and sink's connection: a client from a pool of its own. A
// single client gives up on server selection at its first failed try, so a
// server that is down for seconds failed every attempt at once, and a job
// spent its whole restart budget inside one short outage. A pooled client
// waits for a server for up to serverSelectionTimeoutMS (30 s unless the URI
// says otherwise), so the driver rides out an outage that ends within it: a
// change stream resumes itself, and a write is sent once a server is back.
class Connection {
public:
    explicit Connection(const std::string& uri)
        : pool_((ensure_instance(), mongocxx::uri{uri})), entry_(pool_.acquire()) {}
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    mongocxx::client& client() { return *entry_; }

private:
    mongocxx::pool pool_;  // outlives entry_, which is declared after it
    mongocxx::pool::entry entry_;
};

}  // namespace clink::mongodb
