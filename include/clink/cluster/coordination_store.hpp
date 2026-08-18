#pragma once

// The coordination-record store: one seam for every durable control-plane
// record the cluster keeps - COMPLETED-N / CONFIRMED-N checkpoint markers,
// commit receipts, HA job manifests, history records and plugin binaries.
//
// These records have always been POSIX files under checkpoint_dir (which
// doubles as the HA/coordination directory), written with fsync+rename and
// fenced with an epoch CAS under an advisory lock. That shape is correct
// on a local disk or an NFS export, and it is the ONLY shape: object
// storage enters the engine through state_backend_uri, so a cluster whose
// state lives on a bucket still needs a shared filesystem for a handful of
// tiny coordination records. This interface removes that coupling. The
// filesystem implementation is the existing helpers verbatim (same paths,
// same bytes, same fencing); an object-store implementation maps put ->
// atomic PUT, fenced_put -> conditional PUT on the record's etag, list ->
// a prefix listing - the primitives S3 has offered since conditional
// writes landed. Keys are RELATIVE paths under the store's root, and are
// exactly the paths the filesystem layout has always used, so migrating a
// deployment between stores is a copy.
//
// Scheme selection: make_coordination_store() returns the filesystem
// store for plain paths, and dispatches URI schemes through a process
// registry (register_coordination_store_scheme) that object-store
// implementations install at their impl's install() - the same host-
// registry pattern as StateBackendFactory, and for the same dlopen
// reason: a plugin .so must receive a store by data, never construct one
// from its own RTLD_LOCAL copy of this registry.
//
// Out of scope, deliberately: per-subtask state snapshots (the state
// backend's territory) and the HA LEADER lock (flock-based lease
// semantics; moving leadership itself to a bucket is a lease redesign,
// not a record move).

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace clink::cluster {

class CoordinationStore {
public:
    virtual ~CoordinationStore() = default;

    // Durable last-writer-wins write. Throws on failure (callers of the
    // marker writes already treat a throw as "the record did not land").
    virtual void put(std::string_view key, std::string_view body) = 0;

    // Create-once write: returns false (writing nothing) if the key
    // already exists. Atomic against concurrent callers.
    virtual bool put_if_absent(std::string_view key, std::string_view body) = 0;

    // Epoch-fenced compare-and-set: read the epoch recorded at `key` via
    // `epoch_of`, refuse if `writer_epoch` is stale, otherwise durably
    // replace. Returns true iff the body landed. The check and the write
    // are one critical section (see fenced_metadata_cas_write for why the
    // read-then-write window matters).
    virtual bool fenced_put(std::string_view key,
                            std::string_view body,
                            std::uint64_t writer_epoch,
                            const std::function<std::uint64_t(const std::string&)>& epoch_of,
                            const std::string& caller_context) = 0;

    [[nodiscard]] virtual std::optional<std::string> get(std::string_view key) = 0;
    [[nodiscard]] virtual bool exists(std::string_view key) = 0;

    // Keys (relative to the root) under `prefix`, non-recursive is not
    // guaranteed - callers filter. Missing prefix yields an empty list.
    [[nodiscard]] virtual std::vector<std::string> list(std::string_view prefix) = 0;

    // Best-effort delete; absent keys are not an error.
    virtual void remove(std::string_view key) = 0;
};

// Resolve `root_uri` to a store. A bare path (no "<scheme>://") is the
// filesystem store rooted there. A scheme dispatches through the registry;
// an unregistered scheme throws, naming the scheme - a cluster must never
// silently coordinate on the wrong substrate.
[[nodiscard]] std::shared_ptr<CoordinationStore> make_coordination_store(
    const std::string& root_uri);

// Register a builder for "<scheme>://..." roots. Latest registration wins,
// matching the factory registries.
void register_coordination_store_scheme(
    const std::string& scheme,
    std::function<std::shared_ptr<CoordinationStore>(const std::string& root_uri)> builder);

}  // namespace clink::cluster
