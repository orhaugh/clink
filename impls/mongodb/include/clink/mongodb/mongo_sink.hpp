#pragma once

// MongoDB sink: each input record is a JSON object string (row_to_json_string on
// the SQL path) parsed to BSON and bulk-written into a collection. Records are
// buffered and flushed on a count, byte or linger threshold and on every
// checkpoint barrier, and a checkpoint completes only once its barrier's flush
// returned.
//
// DELIVERY = AT-LEAST-ONCE. Every write uses w=majority (a write acknowledged
// by fewer members can be rolled back in a failover after the checkpoint that
// covers it completed), so open() refuses a URI asking for w=0, w=1, a node
// count or a tag. Any failure fails the flush, and the job replays from the
// last completed checkpoint: a batch the server partly applied, a write concern
// error, or a network failure whose outcome is unknown. Nothing of a failed
// batch counts as written.
//
// Two modes:
//   - insert (the default): an unordered insert of each record. A record
//     without an _id gets a fresh one from the driver, so a replay inserts it
//     again. A record that carries its own _id and is already stored exactly as
//     it arrives is the write made before the restore and is written without
//     error; a different document under a stored _id, or a duplicate on any
//     other unique index, fails the flush, naming the _id.
//   - replace (on_duplicate='replace'): an ordered replace_one(filter={key_field:
//     value}).upsert(true) per record, so a key written twice in a batch ends as
//     its later record, and a replay writes each key again in the same order.
//     Every record of a batch must carry key_field as a string, a 32- or 64-bit
//     integer or an ObjectId, checked before anything of the batch is written;
//     and a key_field other than _id needs a unique index on that field alone
//     (open() checks), so a key matches at most one document. Within one sink
//     subtask a replay converges each key to its last record; across parallel
//     subtasks it does so only when the stream is partitioned by key_field.

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
    std::string key_field{"_id"};     // upsert match key: a top-level field
    std::size_t batch_records{1000};  // flush after this many buffered records
    std::size_t max_bytes{16 * 1024 * 1024};  // flush once the buffered JSON reaches this size
    std::chrono::milliseconds max_age{0};     // linger: flush a partial batch this old (0 = off)
    // wtimeout for the majority write concern, unless the URI sets one: a batch
    // that cannot reach a majority fails within this bound.
    std::chrono::milliseconds write_timeout{30000};
    // The SQL table's columns ("name:TYPE;name:TYPE"), set by the SQL planner.
    // When set, each record is a row as row_to_json_string writes it: the sink
    // writes the declared columns with the BSON types src/sql_row.hpp maps them
    // to, and reads the row's __row_kind as its change kind. Under upsert a
    // delete removes the key's document and an update_before is skipped (its
    // update_after replaces the document); the insert sink refuses any kind but
    // insert.
    std::string sql_column_types;
    std::string name{"mongo_sink"};
};

// Build a MongoDB sink that bulk-writes JSON-object records into a collection.
std::shared_ptr<Sink<std::string>> make_mongo_sink(const MongoSinkOptions& opts);

}  // namespace clink::mongodb
