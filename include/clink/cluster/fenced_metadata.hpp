#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

namespace clink::cluster {

// A compare-and-set write for control-plane metadata: under a per-target
// advisory file lock, read the epoch recorded at `path`, apply the
// metadata_write_allowed rule against `writer_epoch`, and durably write
// `body` into place (fsync + rename). Returns true iff the body landed.
//
// This closes the read-then-rename window the plain fenced write had: two
// writers racing inside that window could BOTH pass the epoch check, and
// whichever renamed second won - including the staler one, which put a
// superseded coordinator's record on top of the real leader's. With the
// check and the write inside one critical section, the interleave "stale
// reads, fresh writes, stale renames over it" cannot happen: the second
// writer's check runs only after the first writer's rename is complete.
//
// The lock is flock(LOCK_EX) on `<path>.wlock`, taken blocking. flock is
// held by the OPEN FILE DESCRIPTION, so it excludes other threads of this
// process as well as other processes, and the kernel releases it when the
// holder dies - the same crash-release property the HA leader lock relies
// on, so the fenced path gains no new dependency and no stale-lock
// recovery problem. The lock FILE is created once and never unlinked:
// unlink-and-recreate would let a later writer lock a fresh inode while an
// earlier one still holds the old inode, which is the classic way lock
// files quietly stop excluding. The critical section is one small read and
// one rename, so blocking is microseconds in practice.
//
// `epoch_of` extracts the epoch recorded in an existing file at `path`
// (absent/unreadable/unstamped must map to 0). The two metadata families
// spell the key differently ("coordinator_epoch" in job manifests and
// history records, "epoch" in active-leader.json), so the extractor is the
// caller's.
//
// `caller_context` is appended to the refusal log line so the two families
// keep their operationally distinct messages.
[[nodiscard]] bool fenced_metadata_cas_write(
    const std::filesystem::path& path,
    const std::string& body,
    std::uint64_t writer_epoch,
    const std::function<std::uint64_t(const std::string&)>& epoch_of,
    const std::string& caller_context = {});

}  // namespace clink::cluster
