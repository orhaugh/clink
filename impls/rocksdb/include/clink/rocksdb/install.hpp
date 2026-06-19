// clink::rocksdb::install - register the "rocksdb" scheme with
// clink_core's StateBackendFactory so a job's state_backend URI of
// the form "rocksdb:///path/to/dir" resolves to a RocksDB-backed
// keyed-state store at runtime.
//
// Callers (clink_node, test entry points) invoke this once at
// startup. Idempotent - repeated calls overwrite the prior builder.

#pragma once

namespace clink::rocksdb {

void install();

}  // namespace clink::rocksdb
