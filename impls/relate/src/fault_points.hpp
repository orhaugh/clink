#pragma once

// Named CLINK_FAULT_POINT sites for the Relate sink's recovery tests. They
// compile to nothing without CLINK_FAULT_INJECTION.

namespace clink::relate::points {

// A batch is built and its transaction is about to begin; nothing of it is in
// the store.
inline constexpr char kBeforeCommit[] = "relate.before_commit";
// Relate has committed the batch, and the sink has not yet returned: the
// checkpoint whose barrier asked for it cannot have completed.
inline constexpr char kAfterCommit[] = "relate.after_commit";
// A schema statement has run in its transaction, which is about to commit: a
// commit elsewhere on the store now makes Relate refuse it as a conflict.
inline constexpr char kBeforeSchemaCommit[] = "relate.before_schema_commit";

}  // namespace clink::relate::points
