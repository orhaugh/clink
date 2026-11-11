#pragma once

// The independent reference audit (docs/internals/incident-verification.md,
// "Independent reference audit"; design record 015).
//
// Destination reconciliation compares a destination with the sink's own record
// of its input, which establishes whether the writes arrived. A transformation
// that computes the wrong rows writes them faithfully, and the comparison
// passes. The audit adds an expectation computed outside the job: the operator
// exports the scope a checkpoint covers (scope_document), runs a reference
// implementation of their own over exactly that source range, and hands its
// output back as a reference artifact. The audit then compares three things
// separately: the reference with the captured sink input (the transformation),
// the captured input with the destination (the delivery), and the reference
// with the destination.
//
// Nothing an artifact names is executed, loaded or connected to. Its hashes
// establish that it is the file its writer finished, not who wrote it, and
// not that its implementation is independent of the job: that is the
// operator's to establish, and the report keeps what the artifact records
// apart from what the audit verified.
//
// The scope document (format clink-audit-scope, version 1), a JSON object:
//
//   job, sink_op, attempts        the run: the job, the sink, its runner attempts
//   graph_sha256                  SHA-256 of the job graph the bundle recorded
//   checkpoint, starts_after,     the bounding checkpoints; starts_after 0 with
//   from_run_start, epochs        from_run_start: from the start of a run with no state
//   sources                       per source operator: topic, cluster_id,
//                                 topic_id, isolation_level, and per partition
//                                 [from, to) (from null: the run's own start)
//   sources_complete              every range and identity was recorded
//   output.columns, semantics     the sink's columns and the canonical cells
//   scope_sha256                  SHA-256 of the document's text without it
//
// The reference artifact (format clink-reference-result, version 1) is NDJSON,
// one object per line:
//
//   {"format": ..., "version": 1, "scope": <the scope document, verbatim>,
//    "implementation": {"name", "version", "config"}, "reader": {...}}
//   {"row": {<column>: <canonical cell>, ...}}       once per expected row
//   {"end": {"rows": N, "rows_sha256": <hex>, "header_sha256": <hex>,
//            "complete": true|false, "incomplete": [why, ...],
//            "verified": [...], "recorded": [...], "read": [...]}}
//
// rows_sha256 is over the bytes of every row line, each with its newline;
// header_sha256 over the header line's. A file without its end line was not
// finished and is refused, as is one whose hashes or count do not hold, whose
// scope is not the audit's, or whose cells are not canonical for their column.
//
// A windowed job's scope is version 2: version 1's fields, and
//
//   transform    the window's rules: kind, size_ms, time_column, group_keys,
//                identity (the sink columns naming one window's row),
//                start_column, end_column, aggregates, watermark (strategy,
//                lag_ms, idleness, assigned_on), lateness (allowed_lateness_ms,
//                late_records), source_parallelism, alignment
//   lineage      restores [{restored_from, first_epoch}], and per completed
//                checkpoint {checkpoint, positions [{op, partition,
//                next_offset}], watermark_ms}: the source's positions and the
//                least watermark the sink's subtasks had at its barrier
//   finality     {checkpoint, watermark_ms}: the recorded watermark windows
//                are final by
//
// with the sources' ranges from the job's start (from null). Its reference is
// version 2: the header adds "semantics", the window rules the reference
// implements, in the transform's field names; the body holds, beside the row
// lines,
//
//   {"window": {"identity": {<identity column>: <cell>, ...},
//               "status": "open" | "undetermined", "end_ms": N, "why": ...}}
//
// once per window it leaves out of the comparison: open at the checkpoint, or
// one whose value or finality the evidence does not decide; and the end line
// adds "windows", "windows_sha256" (over the window lines, as rows_sha256 is
// over the row lines), "source_bounds" [{checkpoint, watermark_ms}], the
// watermark the source's records allow at each lineage checkpoint as the
// reference computed it, and "records", its account of the records it read.
//
// One increment of a windowed job's lineage (`incident-scope --since=M`) is
// version 3: version 2's fields, with
//
//   base         {checkpoint, positions [{op, partition, next_offset}],
//                watermark_ms}: the earlier checkpoint of the lineage it
//                starts at, where the source stood there and the watermark the
//                sink recorded at its barrier
//
// the sources' ranges from the base's positions, and a lineage of the
// completed checkpoints after the base. It owes the windows whose end plus
// the allowed lateness is above the base's watermark and at or below the
// finality's. Its reference is version 3: the header adds "state_in"
// {checkpoint, sha256}, the reference's own window state it started from,
// which must be at the base; the end line adds "state_out" {checkpoint,
// sha256}, the state it left at the scope's checkpoint, which a version 2
// reference may give too. Both are recorded, not verified: the state is the
// reference's word, and its hash only shows it was not changed.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/incident/reconcile.hpp"

namespace clink::incident::reconcile {

inline constexpr const char* kScopeFormat = "clink-audit-scope";
inline constexpr std::int64_t kScopeVersion = 1;
inline constexpr const char* kReferenceFormat = "clink-reference-result";
inline constexpr std::int64_t kReferenceVersion = 1;
inline constexpr const char* kCanonicalSemantics = "clink-canonical-cells-v1";
// A windowed job's scope and reference (below) are version 2 of their formats;
// a record-wise job's stay version 1, unchanged.
inline constexpr std::int64_t kWindowScopeVersion = 2;
inline constexpr std::int64_t kWindowReferenceVersion = 2;
// One increment of a windowed job's lineage, and its reference.
inline constexpr std::int64_t kWindowIncrementScopeVersion = 3;
inline constexpr std::int64_t kWindowIncrementReferenceVersion = 3;
inline constexpr const char* kTumblingWindowKind = "tumbling-window";
inline constexpr const char* kWatermarkStrategy = "per-partition bounded out-of-orderness";

// The scope document for `scope`, from the bundle alone, with its fingerprint.
[[nodiscard]] config::JsonValue scope_document(const Bundle& bundle, const Scope& scope);

// SHA-256 of a scope document's serialised text without its "scope_sha256".
[[nodiscard]] std::string scope_fingerprint(const config::JsonValue& document);

// A reference artifact that cannot be compared: malformed, unfinished, or
// bound to another scope. The CLI exits 2.
struct ReferenceRefused : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// A window a version 2 reference leaves out of the comparison.
struct ExcludedWindow {
    Row identity;        // cells of the transform's identity columns, in that order
    std::string status;  // "open" or "undetermined"
    std::int64_t end_ms{0};
    std::string why;
};

// The watermark a lineage checkpoint's source positions allow, as a reference
// computed it from the records: none when a partition had delivered nothing.
struct SourceBound {
    std::uint64_t checkpoint{0};
    std::optional<std::int64_t> watermark_ms;
};

struct Reference {
    std::string path;
    std::int64_t version{1};
    std::vector<Row> rows;  // canonical, in the scope's column order, multiplicity kept
    // Version 2: the window rules the reference implements, the windows it
    // leaves out, the watermark bounds it computed, and its account of the
    // records it read.
    std::optional<config::JsonValue> semantics;
    std::vector<ExcludedWindow> windows;
    std::string windows_sha256;
    std::vector<SourceBound> source_bounds;
    config::JsonValue records;
    // The reference's own window state it started from (version 3) and left
    // at the scope's checkpoint (version 2 or 3): checkpoint and sha256.
    std::optional<std::pair<std::uint64_t, std::string>> state_in;
    std::optional<std::pair<std::uint64_t, std::string>> state_out;
    bool complete{false};
    std::vector<std::string> incomplete;  // why not, as the artifact says
    std::string rows_sha256;
    std::string header_sha256;
    std::string implementation;  // "<name> <version>", as recorded
    // What the artifact records about where its rows came from, and what its
    // writer says it checked against the source. Recorded, not established by
    // this reader.
    std::vector<std::string> recorded;
    std::vector<std::string> writer_verified;
    // What this reader established: the artifact's form, its hashes, and that
    // its scope is the audit's.
    std::vector<std::string> verified;
};

// Reads an artifact and binds it to `document`, the audit's own scope
// document, whose columns are `columns`. Reads at most `max_bytes`.
[[nodiscard]] Reference read_reference(const std::filesystem::path& path,
                                       const config::JsonValue& document,
                                       const std::vector<Column>& columns,
                                       std::uint64_t max_bytes);

// Where the window rules a version 2 scope declares (its "transform") and the
// ones a reference implements (its "semantics") differ, field by field: the
// kind, size, time column, watermark strategy, lag and idleness, and the
// allowed lateness and treatment of late records. Empty when they agree.
[[nodiscard]] std::vector<std::string> semantics_differences(const config::JsonValue& transform,
                                                             const config::JsonValue& semantics);

// Whether `cell` is a canonical cell a column of `type` can hold.
[[nodiscard]] bool canonical_for(std::string_view cell, const ColumnType& type);

// Two multisets of whole rows, paired by key: rows equal on every column
// match, copies of an expected row beyond its multiplicity are duplicates, and
// what is left under a key pairs as changed, the rest missing or unexpected.
// Keys whose two observations differ are unstable and not compared. Unlike
// compare(), expected keys need not be unique.
[[nodiscard]] Comparison compare_multiset(const std::vector<Row>& expected,
                                          const std::vector<std::size_t>& key,
                                          const std::vector<Row>& first,
                                          const std::vector<Row>& second,
                                          std::size_t list_limit);

}  // namespace clink::incident::reconcile
