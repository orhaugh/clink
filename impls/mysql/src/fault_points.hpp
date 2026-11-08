#pragma once

// Named CLINK_FAULT_POINT sites for the MySQL CDC source's recovery tests.
// They compile to nothing without CLINK_FAULT_INJECTION.

namespace clink::mysql::points {

// The initial snapshot has handed a chunk of rows to the runtime and has more
// to read: the source holds every checkpoint barrier until it finishes.
inline constexpr char kSnapshotAfterEmit[] = "mysql_cdc.snapshot_after_emit";
// The change stream has handed rows of a transaction whose commit it has not
// read yet: the source holds every checkpoint barrier until it does.
inline constexpr char kInTransactionAfterEmit[] = "mysql_cdc.in_transaction_after_emit";

}  // namespace clink::mysql::points
