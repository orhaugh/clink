#pragma once

// MongoDB change-streams source. Watches a collection (or a whole database /
// deployment) and emits each insert / update / replace / delete as one flat JSON
// object string: the event's document fields plus __op / __ns / __id / __image
// metadata (clink::mongodb::change_event_to_json, which documents each field).
//
// CHANGES ONLY. The source reads the change stream from the moment it first
// opens; it takes no snapshot of what the collection already holds.
//
// DELIVERY = AT-LEAST-ONCE. The checkpoint cursor is the change-stream RESUME
// TOKEN, persisted on snapshot_offset and restored before open(). The token a
// checkpoint records covers only events a produce() pass already handed to the
// runtime (see change_pass.hpp), and on restart the stream resumes after it, so
// events after the checkpoint are delivered again and none before it. The
// source takes a token as soon as it opens, so every checkpoint holds one; a
// restore whose state has none, or whose token the server cannot resume from
// (the oplog has rolled past it, or it is not this stream's), fails the job:
// the source never falls back to the current time.
//
// Events that are not row changes stop the source with an error: drop,
// rename, dropDatabase and invalidate (see mongo_event.hpp). A stream error
// the driver cannot resume from fails the produce() call, and the job restarts
// from its last checkpoint.
//
// REQUIRES a replica set (or sharded cluster) - MongoDB change streams are not
// available on a standalone mongod. full_document decides what an update
// carries: updateLookup (the default) the document as the server finds it when
// it looks it up, after the event, so possibly with later changes, or nothing
// once it is deleted; whenAvailable / required the post-image of that change,
// from a collection with changeStreamPreAndPostImages enabled; off only the
// fields the update set.
//
// PARALLELISM: a change stream is a single reader. Only subtask 0 watches; other
// subtasks are dormant (emit nothing), so a parallel job does not open N duplicate
// streams.

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "clink/mongodb/mongo_event.hpp"
#include "clink/operators/operator_base.hpp"

namespace clink::mongodb {

struct MongoCdcOptions {
    std::string uri{"mongodb://localhost:27017"};
    std::string database;    // watch scope: empty = whole deployment
    std::string collection;  // empty = the whole `database` (requires database set)
    FullDocument full_document{FullDocument::UpdateLookup};
    std::chrono::milliseconds max_await{1000};  // change-stream maxAwaitTime; bounds cancel latency
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"mongo_cdc_source"};
};

// Build a MongoDB change-streams source emitting flat-JSON change rows on the
// string channel. Throws on options that cannot work (a collection without a
// database).
std::shared_ptr<Source<std::string>> make_mongo_cdc_source(const MongoCdcOptions& opts);

}  // namespace clink::mongodb
