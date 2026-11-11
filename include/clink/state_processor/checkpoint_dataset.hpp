#pragma once

// A completed checkpoint of a whole job, exported as one verified,
// independently readable dataset (format clink-state-dataset, version 1;
// docs/internals/state-snapshot-format.md, "Checkpoint datasets").
//
// A dataset is an incident bundle collected with the checkpoint's state and
// nothing else (clink::incident::collect: the engine's restore rule or a named
// checkpoint, every participant's snapshot and sidecar from the marker's state
// root and generation, refused when anything changes during the copy), with
// two files beside it at its root:
//
//   dataset.json     the contract: the job and its graph's fingerprint, the
//                    checkpoint, its generation and status, the rule that
//                    selected it, every participant and what it holds, every
//                    operator's slots and their schemas, the entry table's
//                    hash, and whether the dataset is complete and if not why
//   entries.parquet  one row per stored entry, exact bytes, no typed columns:
//                    op_id, slot, scope (keyed | operator | raw), key_group
//                    (keyed only), participant, user_key, value, stored_key;
//                    one row group per participant, each sorted by identity
//
// An entry's identity is (operator, slot, key group, user key) for keyed
// state: the key group is the key's own (one of 128, whatever the
// parallelism), and the participant that owns it is not part of it, so a
// rescale moves an entry without changing it. Operator state and raw rows are
// (operator, slot, participant, user key): every participant keeps its own,
// and rows are never collapsed because their encoded keys match.
//
// Nothing here loads a plugin, runs code a dataset names, or opens a
// connection. The original checkpoint is only read.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"

namespace clink::state_processor {

inline constexpr const char* kDatasetFormat = "clink-state-dataset";
inline constexpr std::int64_t kDatasetVersion = 1;
inline constexpr const char* kDatasetFile = "dataset.json";
inline constexpr const char* kEntriesFile = "entries.parquet";
inline constexpr const char* kStateDiffFormat = "clink-state-diff";
inline constexpr std::int64_t kStateDiffVersion = 1;

// ---- exporting ---------------------------------------------------------------

struct DatasetOptions {
    std::string checkpoint_dir;  // the job's checkpoint directory (local filesystem)
    std::uint64_t job{0};
    std::string ha_dir;  // optional: the job manifest, for its graph and restore rule
    // Which restore rule applies: nullopt reads it from the job manifest.
    std::optional<bool> commit_confirmation;
    // The checkpoint; nullopt: the one the engine's restore rule chooses.
    std::optional<std::uint64_t> checkpoint;
    // Publish a dataset that is not complete, marked so; otherwise refuse it.
    bool allow_partial{false};
    std::uint64_t max_bytes{1ULL << 30};
    std::uint64_t max_files{10000};
    std::uint64_t max_entries{100000000};
    std::chrono::seconds timeout{600};
    std::string out;  // the dataset directory to publish; must not exist
    // For tests: as CollectOptions::after_copy.
    std::function<void(const std::filesystem::path&)> after_copy;
};

struct DatasetResult {
    std::filesystem::path dataset;
    std::uint64_t checkpoint{0};
    bool complete{false};
    std::vector<std::string> incomplete;
    std::vector<std::string> unsupported;
    std::uint64_t participants{0};
    std::uint64_t entries{0};
    std::uint64_t bytes{0};  // the dataset's size on disk
    double seconds{0};
};

// Nothing was published. `kind` is the collector's refusal kind (changed,
// missing, bound, timeout, other) or "incomplete" for a checkpoint the
// contract does not hold for, without allow_partial.
struct DatasetRefused : std::runtime_error {
    DatasetRefused(const std::string& what, std::string k)
        : std::runtime_error(what), kind(std::move(k)) {}
    std::string kind;
};

[[nodiscard]] DatasetResult export_checkpoint_dataset(const DatasetOptions& options);

// ---- reading -----------------------------------------------------------------

// A slot's schema as the participants' snapshots stamp it: its shape
// fingerprint (clink.state_fingerprints) and the version stamps
// (clink.state_versions) of the state types that name it, or every type of
// its operator when a stamp names no slot.
struct SlotSchema {
    std::optional<std::string> fingerprint;         // hex
    std::map<std::string, std::uint32_t> versions;  // state type -> version
    bool operator==(const SlotSchema&) const = default;
};

struct DatasetParticipant {
    std::uint32_t index{0};      // the job-global subtask index
    std::int64_t row_group{-1};  // in entries.parquet; -1: it holds no entry
    std::uint64_t rows{0};
};

// (operator, slot, scope); scope 0 keyed, 1 operator, 2 raw.
using SlotKey = std::tuple<std::uint64_t, std::string, std::uint8_t>;

struct Dataset {
    std::filesystem::path root;
    config::JsonValue doc;  // dataset.json
    bool complete{false};
    std::uint64_t job{0};
    std::uint64_t checkpoint{0};
    std::uint64_t generation{0};
    // The job's RESTORED record when the dataset was exported: (restore, next).
    std::optional<std::pair<std::uint64_t, std::uint64_t>> restored;
    std::string graph_sha256;  // empty: the dataset holds no job graph
    std::uint64_t entries{0};
    std::vector<DatasetParticipant> participants;
    // (operator, slot) -> its schema stamps.
    std::map<std::pair<std::uint64_t, std::string>, SlotSchema> schemas;
    // Every slot that holds an entry, and how many.
    std::map<SlotKey, std::uint64_t> slots;
    // Operator -> the participants holding any of its rows.
    std::map<std::uint64_t, std::set<std::uint32_t>> operator_participants;
    // The job graph's operators and their parallelism; empty when it holds none.
    std::map<std::uint64_t, std::uint32_t> graph_ops;
};

// A directory that is not a dataset this reader may read, or one whose files
// do not hash to what its dataset.json records.
struct DatasetInvalid : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Opens a dataset and checks it is whole: dataset.json's form, and that
// entries.parquet and the bundle's manifest hash to what it records. The
// bundle's own path rules apply, and no file of the dataset may be a link.
[[nodiscard]] Dataset open_dataset(const std::filesystem::path& root);

// One stored entry. scope: 0 keyed, 1 operator, 2 raw.
struct DatasetEntry {
    std::uint64_t op{0};
    std::string slot;
    std::uint8_t scope{0};
    std::int16_t key_group{-1};  // keyed only
    std::uint32_t participant{0};
    std::string user_key;
    std::string value;
};

[[nodiscard]] const char* scope_name(std::uint8_t scope) noexcept;

// Visits every entry in identity order (operator, slot, scope, key group, and
// for operator and raw rows the participant, then the user key), merging the
// participants' row groups as it streams. Stops when `visit` returns false.
void for_each_entry(const Dataset& dataset, const std::function<bool(const DatasetEntry&)>& visit);

// ---- comparing ---------------------------------------------------------------

struct StateDiffOptions {
    // Compare slots whose schema differs as raw bytes, every such slot
    // labelled raw; otherwise they are not compared.
    bool raw_bytes{false};
    std::size_t max_samples{20};           // per slot
    std::uint64_t max_entries{100000000};  // read across both sides
    std::chrono::seconds timeout{600};
    // Compare a dataset that is not complete; the result is still not
    // established.
    bool allow_partial{false};
};

// One entry that differs, as hex of at most kSampleBytes bytes of each field
// with the field's whole length beside it.
inline constexpr std::size_t kSampleBytes = 64;

struct DiffSample {
    std::string kind;  // added | removed | changed
    std::int16_t key_group{-1};
    std::optional<std::uint32_t> participant_a;
    std::optional<std::uint32_t> participant_b;
    std::string user_key_hex;
    std::uint64_t user_key_bytes{0};
    std::optional<std::string> value_a_hex;
    std::uint64_t value_a_bytes{0};
    std::optional<std::string> value_b_hex;
    std::uint64_t value_b_bytes{0};
};

struct SlotComparison {
    std::uint64_t op{0};
    std::string slot;
    std::string scope;  // keyed | operator | raw
    // compared | raw-compared | schema-changed | subtasks-changed |
    // operator-only-in-a | operator-only-in-b
    std::string status;
    std::string note;
    bool schema_changed{false};
    std::uint64_t entries_a{0};
    std::uint64_t entries_b{0};
    std::uint64_t added{0};
    std::uint64_t removed{0};
    std::uint64_t changed{0};
    std::uint64_t unchanged{0};
    std::uint64_t relocated{0};  // keyed entries whose participant changed
    std::vector<DiffSample> samples;
    [[nodiscard]] bool compared() const { return status == "compared" || status == "raw-compared"; }
};

struct DiffSide {
    std::string path;
    std::uint64_t job{0};
    std::uint64_t checkpoint{0};
    std::uint64_t generation{0};
    bool complete{false};
    std::string graph_sha256;
};

struct DatasetDiff {
    DiffSide a;
    DiffSide b;
    bool same_job{false};
    bool same_graph{false};
    std::vector<std::string> lineage;  // what differs between the two runs' identities
    bool established{true};
    std::vector<std::string> reasons;  // why not, when not
    bool limit_reached{false};
    std::vector<SlotComparison> slots;  // every slot of either side
    std::uint64_t entries_read{0};
    std::size_t max_samples{0};
    // Differences: an entry added, removed or changed in a compared slot, a
    // slot whose schema stamps changed, or an operator on one side only.
    // Exit 0 no differences, both sides complete and every slot compared;
    // 3 not established (a side incomplete, a slot not compared, a limit
    // reached), whatever was found; otherwise 1.
    [[nodiscard]] bool differences() const;
    [[nodiscard]] int exit_code() const;
    [[nodiscard]] std::string render_json() const;
    [[nodiscard]] std::string render_text() const;
};

[[nodiscard]] DatasetDiff diff_datasets(const Dataset& a,
                                        const Dataset& b,
                                        const StateDiffOptions& options);

// Lower-case hex of `bytes`, of at most its first `max` bytes; 0 = all.
[[nodiscard]] std::string hex_bytes(std::string_view bytes, std::size_t max = 0);

}  // namespace clink::state_processor
