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
#include <mongocxx/uri.hpp>

namespace clink::mongodb {

inline void ensure_instance() {
    static std::once_flag once;
    std::call_once(once, [] { new mongocxx::instance{}; });  // leaked: process lifetime
}

// Build a connected client. mongocxx::client is cheap, pools connections
// internally, and is move-assignable, so the source/sink hold one directly and
// assign the result of this in open().
inline mongocxx::client make_client(const std::string& uri) {
    ensure_instance();
    return mongocxx::client{mongocxx::uri{uri}};
}

}  // namespace clink::mongodb
