#pragma once

// Named CLINK_FAULT_POINT sites for the MySQL CDC source's and sinks'
// recovery tests. They compile to nothing without CLINK_FAULT_INJECTION.

namespace clink::mysql::points {

// The initial snapshot has handed a chunk of rows to the runtime and has more
// to read: the source holds every checkpoint barrier until it finishes.
inline constexpr char kSnapshotAfterEmit[] = "mysql_cdc.snapshot_after_emit";
// The change stream has handed rows of a transaction whose commit it has not
// read yet: the source holds every checkpoint barrier until it does.
inline constexpr char kInTransactionAfterEmit[] = "mysql_cdc.in_transaction_after_emit";
// A sink's flush has run every statement of its transaction and not yet sent
// COMMIT: a death here commits nothing of it.
inline constexpr char kSinkBeforeCommit[] = "mysql.sink_before_commit";
// A sink's flush for a checkpoint barrier has committed, and the barrier is not
// acknowledged yet: the checkpoint that would cover those records cannot have
// completed, so a death here makes the restored job write them again.
inline constexpr char kSinkAfterCommit[] = "mysql.sink_after_commit";

}  // namespace clink::mysql::points
