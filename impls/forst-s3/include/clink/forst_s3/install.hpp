#pragma once

// clink::forst_s3::install - register the remote-state schemes that need
// BOTH the ForSt backend and the S3 stores, kept in their own target so
// the forst<->arrow-s3 link stays out of clink_core and the two base
// impls (mirrors clink::rocksdb_s3):
//
//   s3+forst://<bucket>/<prefix>[?local=<dir>&endpoint=<url>&region=<r>&anonymous=1&cas=1]
//       ForSt keyed state on a local working dir; each checkpoint dir is
//       uploaded to object storage via S3SnapshotStore (or, with ?cas=1,
//       the content-addressed S3CasSnapshotStore), so the checkpoint
//       disaggregates while reads stay local. Because the store defers
//       the durable write, supports_async_persist() is true and the
//       runner splits capture (operator thread) from persist (snapshot
//       worker) - the upload happens off the barrier path.
//
//   changelog+s3+forst://<bucket>/<prefix>[?local=...]
//       Changelog over a local ForSt inner; materialization PAYLOADS go
//       to object storage via S3MaterializationStore. The small framing
//       blob self-persists to the local working dir.
//
//   s3sst+forst://<bucket>/<prefix>[?local=<dir>&endpoint=<url>&region=<r>
//                                    &anonymous=1&sst_cache=<dir>&sst_cache_bytes=<n>]
//       LIVE remote data files: the engine itself runs over a filesystem
//       that keeps the immutable *.sst files on object storage (written
//       once, random-read on block-cache miss, checkpoint links become
//       server-side copies) while the small mutable metadata files stay
//       on the local working dir - state is bounded by object storage,
//       not local disk. Checkpoint metadata publishes to the same object
//       prefix at persist (riding the capture/persist split), so restore
//       needs only the same URI: a fresh machine with an empty disk
//       re-materialises the metadata from the bucket. sst_cache/
//       sst_cache_bytes enable a local LRU read cache populated by
//       write-tee (flush/compaction outputs read locally; the dir must
//       not sit under `local`). Stale checkpoint staging left by a crash
//       ("<subtask>.cp-*.tmp" objects) is swept at construction.
//
// Idempotent. Invoked once at startup (clink_node, test entry points).
// Requires clink::forst + clink::s3.

namespace clink::forst_s3 {

void install();

}  // namespace clink::forst_s3
