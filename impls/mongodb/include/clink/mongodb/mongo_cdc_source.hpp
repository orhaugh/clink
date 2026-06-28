#pragma once

// MongoDB change-streams CDC source. Watches a collection (or a whole database /
// deployment) and emits each insert / update / replace / delete as one flat JSON
// object string: the changed document image plus __op / __ns / __id metadata
// (clink::mongodb::change_event_to_json), so the SQL Row path binds the document
// fields by name - the same envelope shape as the Postgres / MySQL CDC sources.
//
// DELIVERY = AT-LEAST-ONCE. The checkpoint cursor is the change-stream RESUME
// TOKEN (persisted on snapshot_offset, restored before open()). On restart the
// stream resumes AFTER the checkpointed token, so events between the last
// checkpoint and a crash replay (the downstream sink dedups). A resume token is
// only valid while the oplog still covers it; if the oplog rolled past it, the
// resume fails loudly on open() (no silent gap) and a fresh start is needed.
//
// REQUIRES a replica set (or sharded cluster) - MongoDB change streams are not
// available on a standalone mongod. with full_document_lookup (default) an update
// carries the full post-image (full_document=updateLookup); otherwise an update
// carries only updateDescription.updatedFields.
//
// PARALLELISM: a change stream is a single reader. Only subtask 0 watches; other
// subtasks are dormant (emit nothing), so a parallel job does not open N duplicate
// streams.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"

namespace clink::mongodb {

struct MongoCdcOptions {
    std::string uri{"mongodb://localhost:27017"};
    std::string database;             // watch scope: empty = whole deployment
    std::string collection;           // empty = the whole `database` (requires database set)
    bool full_document_lookup{true};  // full_document=updateLookup so updates carry the post-image
    std::chrono::milliseconds max_await{1000};  // change-stream maxAwaitTime; bounds cancel latency
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mongo_cdc_source"};
};

// Build a MongoDB change-streams CDC source emitting flat-JSON change rows on the
// string channel.
std::shared_ptr<Source<std::string>> make_mongo_cdc_source(const MongoCdcOptions& opts);

}  // namespace clink::mongodb
