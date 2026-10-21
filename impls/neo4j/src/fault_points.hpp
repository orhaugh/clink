#pragma once

// Named CLINK_FAULT_POINT sites for the Neo4j sink's recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION.

namespace clink::neo4j::points {

// A batch is encoded and about to be sent; nothing of it has reached the
// server.
inline constexpr char kBeforeRequest[] = "neo4j.before_request";
// The server has reported the batch committed, and the sink has not yet
// returned: the checkpoint whose barrier asked for it cannot have completed.
inline constexpr char kAfterCommit[] = "neo4j.after_commit";

}  // namespace clink::neo4j::points
