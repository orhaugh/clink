#pragma once

// Named CLINK_FAULT_POINT sites for the PostgreSQL CDC source's recovery
// tests. They compile to nothing without CLINK_FAULT_INJECTION. Each marks a
// phase boundary a test can stop or kill the source at, and a reached point
// prints the fault framework's witness line, so a test can prove the fault
// landed in the phase it meant.

namespace clink::postgres::cdc::points {

// The replication slot has just been created; its snapshot is not yet
// imported and no snapshot row has been read.
inline constexpr char kAfterSlotCreated[] = "postgres_cdc.after_slot_created";
// A batch of snapshot rows has been emitted (the ordinal counts batches).
inline constexpr char kSnapshotBatch[] = "postgres_cdc.snapshot_batch";
// The last snapshot row has been emitted and streaming has not started.
inline constexpr char kSnapshotDone[] = "postgres_cdc.snapshot_done";
// START_REPLICATION has been accepted; no change has been read yet.
inline constexpr char kStreamStarted[] = "postgres_cdc.stream_started";
// A batch of streamed changes has been emitted.
inline constexpr char kStreamBatch[] = "postgres_cdc.stream_batch";
// A checkpoint has recorded the source's position; the checkpoint is not yet
// complete, so nothing it covers has been confirmed to the server.
inline constexpr char kOffsetRecorded[] = "postgres_cdc.offset_recorded";
// A completed checkpoint's position has just been confirmed to the server.
inline constexpr char kWalConfirmed[] = "postgres_cdc.wal_confirmed";

}  // namespace clink::postgres::cdc::points
