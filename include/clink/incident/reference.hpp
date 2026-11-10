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

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
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

// The scope document for `scope`, from the bundle alone, with its fingerprint.
[[nodiscard]] config::JsonValue scope_document(const Bundle& bundle, const Scope& scope);

// SHA-256 of a scope document's serialised text without its "scope_sha256".
[[nodiscard]] std::string scope_fingerprint(const config::JsonValue& document);

// A reference artifact that cannot be compared: malformed, unfinished, or
// bound to another scope. The CLI exits 2.
struct ReferenceRefused : std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct Reference {
    std::string path;
    std::vector<Row> rows;  // canonical, in the scope's column order, multiplicity kept
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
