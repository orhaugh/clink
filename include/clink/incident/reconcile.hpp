#pragma once

// Destination reconciliation (docs/internals/incident-verification.md,
// "Destination reconciliation"): whether a destination holds, for one
// completed checkpoint, exactly the rows a job's sink received in that
// checkpoint's intervals.
//
// The expected rows are the sink's own record of its input, captured by the
// flight recorder at the sink between its barriers (runtime/sink_evidence.hpp)
// and carried in an incident bundle. They come from inside the job, before
// any write, so nothing read from the destination shapes them. The observed
// rows are read from the destination by an explicit, read-only
// reconciliation, selected by a declared key, and both sides are reduced to
// one canonical form per cell, by rules set out below, before they are
// compared as multisets of whole rows.
//
// The destination itself is behind Destination, so the comparison and its
// verdicts are tested without a server; impls/clickhouse reads ClickHouse.

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/incident/bundle.hpp"

namespace clink::incident::reconcile {

inline constexpr const char* kReportFormat = "clink-reconcile-report";
inline constexpr std::int64_t kReportVersion = 1;

// ---- column types and canonical cells ----------------------------------------

// The destination column types a comparison supports, from the type text the
// destination reports (ClickHouse spellings: Int64, Nullable(Decimal(18, 2)),
// DateTime64(3, 'UTC'), LowCardinality(String), ...).
enum class TypeKind : std::uint8_t {
    Int,   // Int8 to Int64
    UInt,  // UInt8 to UInt64
    Float32,
    Float64,
    Bool,
    String,      // String, LowCardinality(String)
    Decimal,     // Decimal(P, S), Decimal32/64/128(S)
    DateTime,    // seconds
    DateTime64,  // ticks of 10^-precision s
    Date,        // days, Date and Date32
    Unsupported
};

struct ColumnType {
    TypeKind kind{TypeKind::Unsupported};
    bool nullable{false};
    int precision{0};  // DateTime64's precision, Decimal's P
    int scale{0};      // Decimal's S
    std::string spelling;
    std::string unsupported_reason;  // when kind is Unsupported
};

[[nodiscard]] ColumnType parse_column_type(std::string_view spelling);

// A cell's canonical text. Two cells are equal exactly when their texts are:
//
//   N                NULL, equal to NULL and to nothing else (not '' or 0)
//   I<n>             an integer, in decimal without a leading + or zeros
//   F<16 hex>        a Float64 by its IEEE-754 bits; f<8 hex> a Float32. Every
//                    NaN is Fnan (fnan); -0.0 and +0.0 differ, as their bits do
//   B0, B1           a Bool
//   S<bytes>         a String's bytes, compared exactly
//   D<n>@<s>         a Decimal as its unscaled integer at the column's scale
//   T<n>@<p>         a DateTime64 as ticks at its precision (DateTime: p = 0)
//   d<n>             a Date or Date32 as days since 1970-01-01
//
// An expected value the column cannot hold exactly canonicalises to text that
// starts with '!' and names why, which equals nothing observed.
namespace canon {
[[nodiscard]] std::string null_cell();
[[nodiscard]] std::string integer(std::string_view decimal_text);  // validated; "!..." if not one
[[nodiscard]] std::string float64_bits(std::uint64_t bits);
[[nodiscard]] std::string float32_bits(std::uint32_t bits);
[[nodiscard]] std::string boolean(bool b);
[[nodiscard]] std::string string(std::string_view bytes);
// `text` a plain decimal ([-]digits[.digits]), held at `scale`.
[[nodiscard]] std::string decimal(std::string_view text, int scale);
[[nodiscard]] std::string ticks(std::int64_t ticks, int precision);
[[nodiscard]] std::string days(std::int64_t days);
}  // namespace canon

// The expected side: a captured Row value (a JSON value, as the Row channel
// carries it: integers, doubles, strings, booleans, null, and decimals as
// tagged strings) as a column of `type` holds it, by the native sink's
// declared rules: a TIMESTAMP is epoch milliseconds, a DATE days or
// YYYY-MM-DD, a DECIMAL exact. Never throws.
[[nodiscard]] std::string canon_expected(const config::JsonValue& value, const ColumnType& type);

// A canonical cell for a report: NULL, numbers in decimal, a float by its
// shortest round-trip text, strings quoted and cut to `max_bytes`, decimals,
// times and dates in ISO form.
[[nodiscard]] std::string display(std::string_view canon, std::size_t max_bytes = 64);

// ---- the destination --------------------------------------------------------

struct Column {
    std::string name;
    ColumnType type;  // the destination's, as the sink planned it
};

using Row = std::vector<std::string>;  // canonical cells, in Column order

struct Mutation {
    std::string id;
    std::string command;
    bool done{false};
    std::string created;  // the destination's own time text
};

// Who the destination is and what its table looks like, as read.
struct DestinationDescription {
    std::string server_uuid;
    std::string host_name;
    std::string version;
    std::string database;
    std::string table;
    std::string table_uuid;
    std::string engine;
    std::string engine_full;
    std::string create_table_query;
    std::string metadata_modification_time;
    std::vector<std::pair<std::string, std::string>> columns;  // name, type, in table order
    std::string now;                                           // the server's clock when described
};

// A read that cannot be made or completed: unreachable, refused, past a
// bound or a timeout. A comparison that meets one is unavailable, never
// passed or failed.
struct DestinationUnavailable : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// The bounds every read observes.
struct ReadBounds {
    std::chrono::milliseconds query_timeout{std::chrono::seconds{30}};
    std::uint64_t max_rows_per_query{100000};
    std::size_t keys_per_query{1000};
};

// The destination, read-only. Implementations issue only reads and must
// throw DestinationUnavailable rather than return part of an answer.
class Destination {
public:
    virtual ~Destination() = default;
    [[nodiscard]] virtual DestinationDescription describe() = 0;
    // The table's mutations, done or not.
    [[nodiscard]] virtual std::vector<Mutation> mutations() = 0;
    // Every row whose key cells (the `key` columns, canonical) are among
    // `keys`, with every column canonical; at most bounds.keys_per_query keys
    // per call.
    [[nodiscard]] virtual std::vector<Row> read(const std::vector<Column>& columns,
                                                const std::vector<std::size_t>& key,
                                                const std::vector<Row>& keys) = 0;
    // Every row of the table, every column canonical, or DestinationUnavailable
    // past `max_rows`: for a claim about the whole table. A destination that
    // cannot read whole throws.
    [[nodiscard]] virtual std::vector<Row> read_all(const std::vector<Column>& columns,
                                                    std::uint64_t max_rows);
    // The table's active parts as one comparable text, so that a whole read can
    // tell its table did not change under it. A destination that cannot say
    // throws DestinationUnavailable.
    [[nodiscard]] virtual std::string parts_marker();
};

// ---- the scope: what the bundle says the sink received ------------------------

struct IntervalEvidence {
    std::uint64_t epoch{0};         // the barrier it ended at
    std::uint64_t subtask{0};       // the capture directory's index
    std::uint64_t sink_subtask{0};  // the sink's own, 0 to parallelism - 1
    std::uint64_t parallelism{0};
    std::uint64_t previous_barrier{0};  // 0: the run's first interval
    std::string attempt;
    std::string capture;  // written, empty or disarmed
    std::uint64_t records_seen{0};
    bool truncated{false};
    std::optional<std::int64_t> watermark_ms;
    std::string path;
    config::JsonValue sink;  // the sink's own object
};

// A Kafka source's position for one partition at the checkpoints bounding the
// scope, read from the checkpoints' own snapshots: the next offset to read, so
// the scope's intervals consumed offsets [from, to) of the partition.
struct SourceRange {
    std::uint64_t op{0};  // the source operator whose state holds it
    std::int32_t partition{0};
    std::optional<std::int64_t> from;  // none: the run began with no position
    std::optional<std::int64_t> to;
};

// The log a Kafka source's positions belong to, as the source recorded it
// beside them: the topic, the cluster's id and the topic's id, each empty
// where the brokers reported none.
struct SourceLog {
    std::string topic;
    std::string cluster_id;
    std::string topic_id;
    bool operator==(const SourceLog&) const = default;
};

// One source's log at the checkpoints bounding the scope. `start` is none
// when the scope starts at the run's start or the checkpoint recorded none
// (a release before identities were recorded); likewise `end`.
struct SourceIdentity {
    std::uint64_t op{0};
    std::optional<SourceLog> start;
    std::optional<SourceLog> end;
    // Whether start and end name one log, comparing what both recorded; none
    // when either is missing.
    [[nodiscard]] std::optional<bool> same_log() const;
};

struct SourcePositions {
    std::vector<SourceRange> ranges;
    std::vector<SourceIdentity> identities;
    // Every participant's snapshot of the bounding checkpoint was read: a
    // source subtask's partitions are not missing for want of its snapshot.
    bool from_complete{false};
    bool to_complete{false};
    std::vector<std::string> topics;  // from the job graph: "<topic> (<op type>)"
    std::string isolation;            // as the job configured it, or librdkafka's default
    std::string isolation_level{"read_committed"};  // the level itself
    bool isolation_set_by_job{false};
    std::vector<std::string> notes;
};

// ---- windowed results ----------------------------------------------------------
//
// The audit of a windowed job (docs/internals/incident-verification.md,
// "Auditing windowed results"; design record 015). One shape is supported: one
// Kafka source, read one partition per source subtask; the per-partition
// bounded-out-of-orderness watermark assigned on every record the source
// delivers, with no idleness; record-wise filters and projections; one keyed
// tumbling window of COUNT and SUM; and the ClickHouse native sink, with
// aligned checkpoints. A window's output depends on every record before it,
// so its scope is the job's whole lineage from a start with no state, through
// the restores the bundle records.

struct WindowAggregate {
    std::string function;  // "count" or "sum"
    std::string input;     // the column it reads; empty for COUNT(*)
    std::string output;    // the sink column it lands in
};

// What the job's graph and manifest declare, as the audit reads them.
struct WindowSemantics {
    std::int64_t size_ms{0};
    std::string time_column;  // event time, epoch milliseconds
    std::vector<std::string> group_keys;
    // The sink columns that identify one window's row: the group keys'
    // outputs, then the window's start and end.
    std::vector<std::string> identity;
    std::string start_column;
    std::string end_column;
    std::vector<WindowAggregate> aggregates;
    std::int64_t lag_ms{0};  // the watermark trails the least partition maximum by this
    std::int64_t allowed_lateness_ms{0};
    std::uint64_t source_parallelism{0};
    std::string alignment;  // "aligned"
    std::string window_op;  // its id in the job graph
};

// What the bundle's job graph and manifest say about state across records.
struct WindowAnalysis {
    // The graph holds a window or another operator that keeps state across
    // records, so the job is not record-wise.
    bool windowed{false};
    std::optional<WindowSemantics> semantics;  // the supported shape
    std::string unsupported;                   // when windowed and not supported: why
};

// Reads the bundle's job manifest. A bundle without one says nothing.
[[nodiscard]] WindowAnalysis analyse_window(const Bundle& bundle);

// A completed checkpoint of the scope's lineage: where the source stood, and
// the watermark the sink had when its barrier arrived, the least over the
// sink's subtasks (none when any had none: then no window had closed by it
// everywhere).
struct LineageCheckpoint {
    std::uint64_t checkpoint{0};
    std::vector<SourceRange> positions;  // `to` is the next offset to read
    std::optional<std::int64_t> watermark_ms;
};

// A restore the lineage passes through: the run that numbered its barriers
// from `first_epoch` began from `restored_from`'s state.
struct LineageRestore {
    std::uint64_t restored_from{0};
    std::uint64_t first_epoch{0};
};

struct Scope {
    JobId job{0};
    std::uint64_t checkpoint{0};
    std::uint64_t sink_op{0};
    // The intervals the checkpoint completed, oldest first: back from the
    // checkpoint to the previous completed one, or to the run's start.
    std::vector<std::uint64_t> epochs;
    std::uint64_t starts_after{0};  // the completed checkpoint before them; 0: the run's start
    bool starts_at_run_start{false};
    // Asked to span every interval from the run's start, through the
    // completed checkpoints between, rather than back to the previous one.
    bool whole_run{false};
    std::vector<IntervalEvidence> intervals;
    config::JsonValue destination;  // as the sink's open found it
    std::vector<Column> columns;    // the sink's planned columns
    std::vector<Row> expected;      // every row the sink received in the scope
    // The interval each expected row was received in (the barrier it ended at).
    std::vector<std::uint64_t> expected_epochs;
    SourcePositions source;
    config::JsonValue delivery;  // the bundle's delivery report, when it has one
    // A windowed job: what it declares, its lineage's completed checkpoints,
    // oldest first, and the restores the lineage passes through. When the job
    // has a window the audit does not support, `window` is empty and
    // `window_unsupported` says why.
    std::optional<WindowSemantics> window;
    std::optional<std::string> window_unsupported;
    std::vector<LineageCheckpoint> lineage;
    std::vector<LineageRestore> restores;
    // When the scope could not be established whole: why, and whether the
    // gap is evidence that is missing (unavailable) or a case this release
    // does not handle (unsupported).
    std::optional<std::string> gap;
    bool gap_unsupported{false};
    std::vector<std::string> notes;
};

// Thrown when the request does not fit the bundle: the checkpoint is not a
// completed one in it, it holds no sink evidence for it, or several sinks do
// and none was named. The CLI exits 2.
struct ScopeRefused : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// The scope of `checkpoint` (0: the bundle's selected checkpoint) for
// `sink_op` (0: the only sink with evidence for it), from a bundle whose
// artifacts the caller has verified. Reads the interval files and the sink's
// captured epochs; a gap is recorded in the scope rather than thrown.
// With `whole_run`, the scope spans every interval from the run's start to the
// checkpoint, through the completed checkpoints between. A windowed job's
// scope, when `whole_run` or `for_audit` (a scope for a reference to be
// computed from) asks for it, spans the job's whole lineage instead: back
// through every restore the bundle records to a run that began with no state,
// with each completed checkpoint's source positions and recorded watermark.
[[nodiscard]] Scope load_scope(const Bundle& bundle,
                               std::uint64_t checkpoint,
                               std::uint64_t sink_op,
                               bool whole_run = false,
                               bool for_audit = false);

// ---- the comparison ----------------------------------------------------------

struct Discrepancy {
    enum class Kind : std::uint8_t { Missing, Duplicate, Mismatch, Unexpected };
    Kind kind{Kind::Missing};
    Row key;
    Row expected;                        // Missing, Duplicate, Mismatch
    Row observed;                        // Duplicate, Mismatch, Unexpected
    std::vector<std::size_t> differing;  // Mismatch: the columns that differ
    std::uint64_t copies{1};             // Duplicate: copies beyond the expected one
};

[[nodiscard]] const char* to_string(Discrepancy::Kind k) noexcept;

struct Comparison {
    std::uint64_t expected_rows{0};
    std::uint64_t observed_rows{0};
    std::uint64_t matched{0};         // expected rows found exactly once
    std::uint64_t missing{0};         // expected rows not found
    std::uint64_t duplicated{0};      // copies of expected rows beyond one
    std::uint64_t mismatched{0};      // expected rows whose key holds other values
    std::uint64_t unexpected{0};      // rows of in-scope keys beyond the above
    std::uint64_t unstable_keys{0};   // keys the two reads saw differently
    std::vector<Discrepancy> listed;  // the first list_limit, stable keys only
};

// Compares the expected rows with two reads of the destination, keyed by the
// `key` columns. A key whose two reads differ is unstable: counted, not
// compared. Expected keys must be unique (the caller checks).
[[nodiscard]] Comparison compare(const std::vector<Row>& expected,
                                 const std::vector<std::size_t>& key,
                                 const std::vector<Row>& first,
                                 const std::vector<Row>& second,
                                 std::size_t list_limit);

// ---- running it --------------------------------------------------------------

struct Options {
    std::uint64_t checkpoint{0};   // 0: the bundle's selected checkpoint
    std::uint64_t sink_op{0};      // 0: the only sink with evidence
    std::optional<JobId> job;      // when given, the bundle's job must be it
    std::vector<std::string> key;  // the key columns, required
    std::string database;          // when given, must be the evidence's
    std::string table;
    ReadBounds bounds;
    std::size_t list_limit{20};
    // Pause between the two reads, so that a write landing in the scope while
    // it is read shows as instability rather than as a verdict.
    std::chrono::milliseconds settle{std::chrono::seconds{1}};
    // The scope from the run's start through the checkpoint (load_scope's
    // whole_run).
    bool from_run_start{false};
    // A reference artifact (reference.hpp) to audit against; empty: none.
    std::string reference;
    std::uint64_t max_reference_bytes{std::uint64_t{1} << 30};
    // Selected keys reads the rows of the keys the scope names; complete
    // table also reads the whole table, which the operator declares holds
    // this one bounded run's writes and nothing else, and requires it to hold
    // exactly the expected rows.
    enum class Coverage : std::uint8_t { SelectedKeys, CompleteTable };
    Coverage coverage{Coverage::SelectedKeys};
    std::uint64_t max_table_rows{1000000};
};

// What a destination comparison covered.
struct Coverage {
    std::string mode;  // "selected-keys" or "complete-table"
    std::uint64_t keys_selected{0};
    bool complete_established{false};
    std::string complete_reason;  // why it is, or why not
    std::uint64_t table_rows_read{0};
};

// What the audit of a windowed job established about its windows.
struct WindowAudit {
    std::uint64_t final_windows{0};  // the reference's rows
    std::uint64_t open_windows{0};
    std::uint64_t undetermined_windows{0};
    std::uint64_t captured_left_out{0};     // captured rows of undetermined windows
    std::uint64_t destination_left_out{0};  // rows of them in a whole-table read
    // Per lineage checkpoint: the watermark the sink recorded at its barrier,
    // and the one the source's records allow there, as the reference computed it.
    struct Mark {
        std::uint64_t checkpoint{0};
        std::optional<std::int64_t> recorded_ms;
        std::optional<std::int64_t> source_bound_ms;
    };
    std::vector<Mark> watermarks;
    std::vector<std::string> semantics_differences;
    std::vector<std::string> early;  // windows received before the source allowed them to close
    config::JsonValue reference_semantics;
    config::JsonValue records;  // the reference's account of the records it read
};

// One relationship the audit found broken, named for what it implicates.
struct Finding {
    std::string relationship;  // "transformation", "delivery", "reference-destination", "coverage"
    std::string summary;
};

struct Report {
    std::string bundle;
    std::optional<std::string> refused;  // exit 2, with why
    Scope scope;
    std::optional<DestinationDescription> destination;
    std::vector<Check> checks;  // bundle.*, reconcile.scope, reconcile.destination, reconcile.rows
    std::optional<Comparison> comparison;
    std::vector<std::string> key;
    std::string observed_from;  // UTC, the first read
    std::string observed_to;    // UTC, the last
    std::string server_time_from;
    std::string server_time_to;
    std::vector<std::string> assumptions;
    std::vector<std::string> mutations_noted;
    // The audit, when a reference was given.
    std::optional<config::JsonValue> scope_document;
    std::optional<std::string> reference_path;
    bool reference_complete{false};
    std::vector<std::string> reference_incomplete;
    std::string reference_implementation;
    std::string reference_rows_sha256;
    std::uint64_t reference_rows{0};
    std::vector<std::string> reference_recorded;
    std::vector<std::string> reference_writer_verified;
    std::vector<std::string> reference_verified;
    std::optional<Comparison> reference_capture;      // reference vs captured input
    std::optional<Comparison> reference_destination;  // reference vs destination
    std::optional<Comparison> table_comparison;       // complete table vs expectation
    Coverage coverage;
    std::vector<Finding> findings;
    std::optional<WindowAudit> window_audit;  // a windowed job's audit

    // 0: every check passed. 1: one failed. 3: none failed, one is unavailable
    // or unsupported. 2: refused.
    [[nodiscard]] int exit_code() const;
    [[nodiscard]] std::string render_text() const;
    [[nodiscard]] std::string render_json() const;
};

// Opens the destination for a database and table: called once the evidence
// has named them (and the operator's --database and --table, when given, have
// matched it). Throws std::runtime_error when it cannot, which refuses the run.
using DestinationFactory = std::function<std::unique_ptr<Destination>(const std::string& database,
                                                                      const std::string& table)>;

// The whole reconciliation: the bundle's integrity, the scope, the
// destination's identity and form, and the comparison. A request that does
// not fit the bundle, or a destination that is not the one the evidence
// names, is refused. The destination is only read.
[[nodiscard]] Report run(const std::filesystem::path& bundle_dir,
                         const DestinationFactory& open,
                         const Options& options);

// The expected rows as NDJSON, one object of canonical cells per row keyed by
// column name, sorted: for an independent check of the expectation.
[[nodiscard]] std::string expected_ndjson(const Scope& scope);

}  // namespace clink::incident::reconcile
