#pragma once

// Named CLINK_FAULT_POINT sites for the MongoDB source's and sink's recovery
// tests. They compile to nothing without CLINK_FAULT_INJECTION.

namespace clink::mongodb::points {

// A batch is parsed and checked and about to be written; nothing of it has
// reached the server.
inline constexpr char kSinkBeforeWrite[] = "mongodb.sink_before_write";
// MongoDB has acknowledged the batch and the sink has not yet returned: the
// checkpoint whose barrier asked for it cannot have completed.
inline constexpr char kSinkAfterWrite[] = "mongodb.sink_after_write";
// The source has handed a pass's change events to the runtime; no checkpoint
// covers them yet, so they are in flight.
inline constexpr char kSourceAfterEmit[] = "mongodb.source_after_emit";

}  // namespace clink::mongodb::points
