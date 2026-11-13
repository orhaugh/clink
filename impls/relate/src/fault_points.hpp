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

}  // namespace clink::relate::points
