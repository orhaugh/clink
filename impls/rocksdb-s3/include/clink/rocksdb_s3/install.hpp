#pragma once

// clink::rocksdb_s3::install - register the remote-state schemes that need
// BOTH the RocksDB backend and the S3 stores, kept in their own target so the
// rocksdb<->arrow-s3 link stays out of clink_core and the two base impls:
//
//   s3+rocksdb://<bucket>/<prefix>[?local=<dir>&endpoint=<url>&region=<r>&anonymous=1]
//       RocksDB keyed state on a local working dir; each checkpoint dir is
//       uploaded to object storage via S3SnapshotStore (DISAGG-2/3), so the
//       checkpoint disaggregates while reads stay local.
//
//   changelog+s3+rocksdb://<bucket>/<prefix>[?local=...]
//       Changelog over a local RocksDB inner; materialization PAYLOADS go to
//       object storage via S3MaterializationStore (DISAGG-0). The small framing
//       blob self-persists to the local working dir.
//
//   changelog+s3://<bucket>/<prefix>[?local=...]
//       Changelog over an in-memory inner; materialization payloads to S3.
//
// Idempotent. Invoked once at startup (clink_node, test entry points).
// Requires clink::rocksdb + clink::s3.

namespace clink::rocksdb_s3 {

void install();

}  // namespace clink::rocksdb_s3
