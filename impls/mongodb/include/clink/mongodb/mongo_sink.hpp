#pragma once

// MongoDB sink: each input record is a JSON object string (row_to_json_string on
// the SQL path) parsed to BSON and bulk-written into a collection. Records are
// buffered and flushed on a count / linger threshold and on every checkpoint
// barrier.
//
// DELIVERY = AT-LEAST-ONCE (a failed flush throws -> the job replays the buffered
// batch from the last checkpoint). With on_duplicate='replace' the write is a
// replace_one(filter={key_field: value}, doc).upsert(true), which is idempotent on
// replay (effectively-once by key); plain insert is at-least-once (replay
// re-inserts, so a downstream/_id dedup is needed). The bulk write is ORDERED, so
// the first error surfaces (no silent partial drop). NOTE: with on_duplicate=
// 'replace' a record that LACKS key_field falls back to a plain insert (it cannot
// be matched), so such a record is at-least-once, not idempotent.

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"

namespace clink::mongodb {

struct MongoSinkOptions {
    std::string uri{"mongodb://localhost:27017"};
    std::string database;             // required
    std::string collection;           // required
    bool upsert{false};               // on_duplicate='replace' -> replace_one upsert by key_field
    std::string key_field{"_id"};     // upsert match key
    std::size_t batch_records{1000};  // flush after this many buffered records
    std::chrono::milliseconds max_age{0};  // linger: flush a partial batch this old (0 = off)
    std::string name{"mongo_sink"};
};

// Build a MongoDB sink that bulk-writes JSON-object records into a collection.
std::shared_ptr<Sink<std::string>> make_mongo_sink(const MongoSinkOptions& opts);

}  // namespace clink::mongodb
