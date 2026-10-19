#include "clink/connectors/postgres_cdc_source.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/connectors/pg_cdc_test_seam.hpp"
#include "clink/connectors/postgres_decoder.hpp"
#include "clink/connectors/postgres_tls.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "cdc_cursor.hpp"
#include "cdc_fault_points.hpp"
#include "cdc_row_encoder.hpp"

#ifdef CLINK_HAS_POSTGRES
#include <libpq-fe.h>
#include <poll.h>
#endif

namespace clink {

#ifdef CLINK_HAS_POSTGRES

namespace {

// All wire/text decoders live in clink::pg (postgres_decoder.hpp). The
// names are imported here so the rest of this file reads the same as
// before the extraction.
using pg::format_lsn;
using pg::lookup_builtin_type_name;
using pg::parse_test_decoding;
using pg::postgres_epoch_us_now;
using pg::read_be16;
using pg::read_be32;
using pg::read_be64;
using pg::read_cstring;
using pg::write_be64;

namespace cdc = clink::postgres::cdc;
using Clock = std::chrono::steady_clock;

constexpr const char* kLog = "source.postgres_cdc";
constexpr const char* kMetricsName = "postgres_cdc";

// =====================================================================
// pgoutput parser - binary protocol with relation + type cache.
// =====================================================================
//
// Message types handled:
//   B (Begin), C (Commit), R (Relation), I/U/D (row changes),
//   T (Truncate), O (Origin, ignored), Y (Type, populates user-type cache),
//   M (Logical Message, ignored).
//
// Streaming (S/E/c/A) and two-phase (P/p/K/b/r) messages only appear when the
// client asks for them in START_REPLICATION, which this source never does.
// Outside strict mode they are skipped as before; under strict mode one
// arriving is a protocol error, since the transaction it belongs to could
// not be told apart from a committed one.

// How one captured table's changes are decoded.
struct DecoderTable {
    // Strict mode's key, for telling a key-changing UPDATE apart.
    std::vector<std::string> key_columns;
    // Columns the consumer reads; empty = every column.
    std::set<std::string> needed;
    // Strict only: the consumer keeps the latest row of every key, so an
    // unchanged out-of-line value is marked (cdc::kUnchangedToastType) for it
    // to fill in, and a TRUNCATE of the table is an event it expands.
    bool keep_rows{false};
};

struct DecoderConfig {
    // The tables to capture by schema-qualified name ("public.orders"),
    // several when they share the slot; empty = every table.
    std::map<std::string, DecoderTable> tables;
    bool strict{false};
};

// One decoded pgoutput message.
struct Decoded {
    enum class Kind : std::uint8_t { None, Begin, Commit, Change };
    Kind kind{Kind::None};
    std::uint64_t final_lsn{0};  // Begin: the transaction's commit LSN
    std::uint64_t end_lsn{0};    // Commit: the end of its commit record
    std::int64_t xid{0};
    bool target{true};    // Change: it touches the captured table
    bool dropped{false};  // Change: it could not be decoded or represented
    std::string error;    // why, when dropped
    std::vector<CdcEvent> events;
};

class PgOutputState {
public:
    explicit PgOutputState(DecoderConfig cfg = {}) : cfg_(std::move(cfg)) {}

    // Count of I/U/D change events that could NOT be decoded and were dropped
    // (unknown relation, missing/truncated tuple). Surfaced by the source as
    // dropped_events_total so this otherwise-silent data loss is observable.
    // Benign skips (begin/commit/relation metadata) are NOT counted.
    std::uint64_t dropped_data_events{0};

    // A new walsender session describes every relation again before using it.
    // The declared columns already seen stay seen.
    void reset_relations() { relations_.clear(); }

    void set_tables(std::map<std::string, DecoderTable> tables) { cfg_.tables = std::move(tables); }

    Decoded on_message(std::string_view payload, const std::string& lsn) {
        Decoded d;
        if (payload.empty()) {
            return d;
        }
        const char type = payload[0];
        const std::string_view rest = payload.substr(1);
        switch (type) {
            case 'B':
                return on_begin(rest, lsn);
            case 'C':
                return on_commit(rest, lsn);
            case 'R':
                on_relation(rest);
                return d;
            case 'Y':
                on_type(rest);
                return d;
            case 'I':
                return on_insert(rest, lsn);
            case 'U':
                return on_update(rest, lsn);
            case 'D':
                return on_delete(rest, lsn);
            case 'T':
                return on_truncate(rest, lsn);
            case 'O':  // Origin - replicated origin name; ignored
            case 'M':  // Logical Message (pg_logical_emit_message); ignored
                return d;
            default:
                if (cfg_.strict) {
                    throw std::runtime_error(
                        std::string{"postgres_cdc: unexpected pgoutput message '"} + type +
                        "' (streamed and two-phase transactions are not requested by this source)");
                }
                return d;
        }
    }

private:
    struct Column {
        std::uint8_t flags{};  // bit 0 = part of replica identity key
        std::string name;
        std::uint32_t type_oid{};
        std::int32_t type_modifier{};
    };
    struct Relation {
        std::uint32_t id{};
        std::string ns;
        std::string name;
        char replica_identity{'d'};
        std::vector<Column> columns;
        [[nodiscard]] std::string qualified() const { return ns + "." + name; }
    };
    // One column of a decoded TupleData block, with its kind byte ('n' null,
    // 'u' unchanged TOAST, 't' text, 'b' binary).
    struct Cell {
        CdcField field;
        char kind{'n'};
    };

    DecoderConfig cfg_;
    std::int64_t current_xid_{0};
    std::unordered_map<std::uint32_t, Relation> relations_;
    // Per captured table, the declared columns some Relation message of it has
    // had. One missing from a later message was dropped or renamed; one
    // missing before it was ever seen was added after the change that lacks it.
    std::map<std::string, std::set<std::string>> seen_;
    // User-defined type names from Y messages: oid -> "ns.name". Built-in
    // types are not sent as Y messages; resolve those via
    // lookup_builtin_type_name().
    std::unordered_map<std::uint32_t, std::string> user_types_;

    // The rules for a relation this decoder captures, or nullptr. With no
    // tables configured every relation is captured, every column read.
    [[nodiscard]] const DecoderTable* target_of(const Relation& rel) const {
        static const DecoderTable kEveryColumn{};
        if (cfg_.tables.empty()) {
            return &kEveryColumn;
        }
        const auto it = cfg_.tables.find(rel.qualified());
        return it == cfg_.tables.end() ? nullptr : &it->second;
    }
    [[nodiscard]] bool is_target(const Relation& rel) const { return target_of(rel) != nullptr; }
    [[nodiscard]] bool needed(const Relation& rel, const std::string& column) const {
        const auto* t = target_of(rel);
        return t != nullptr && (t->needed.empty() || t->needed.count(column) != 0);
    }

    Decoded drop_(std::string why) {
        ++dropped_data_events;
        Decoded d;
        d.kind = Decoded::Kind::Change;
        d.dropped = true;
        d.error = std::move(why);
        return d;
    }

    std::string resolve_type_name(std::uint32_t oid) const {
        if (auto it = user_types_.find(oid); it != user_types_.end()) {
            return it->second;
        }
        if (const char* b = lookup_builtin_type_name(oid); b != nullptr) {
            return b;
        }
        return {};  // unknown - leave field type empty
    }

    void on_type(std::string_view rest) {
        if (rest.size() < 4) {
            return;
        }
        const std::uint32_t oid = read_be32(rest.data());
        rest.remove_prefix(4);
        const std::string ns = read_cstring(rest);
        const std::string name = read_cstring(rest);
        if (ns.empty() && name.empty()) {
            return;
        }
        user_types_[oid] = ns.empty() ? name : (ns + "." + name);
    }

    Decoded on_begin(std::string_view rest, const std::string& lsn) {
        // final_lsn (8) + commit_ts (8) + xid (4)
        if (rest.size() < 20) {
            throw std::runtime_error("postgres_cdc: malformed pgoutput Begin message at " + lsn);
        }
        Decoded d;
        d.kind = Decoded::Kind::Begin;
        d.final_lsn = read_be64(rest.data());
        d.xid = static_cast<std::int64_t>(read_be32(rest.data() + 16));
        current_xid_ = d.xid;
        CdcEvent ev;
        ev.op = CdcEvent::Op::Begin;
        ev.lsn = lsn;
        ev.xid = d.xid;
        d.events.push_back(std::move(ev));
        return d;
    }

    Decoded on_commit(std::string_view rest, const std::string& lsn) {
        // flags (1) + commit_lsn (8) + end_lsn (8) + commit_ts (8)
        if (rest.size() < 25) {
            throw std::runtime_error("postgres_cdc: malformed pgoutput Commit message at " + lsn);
        }
        Decoded d;
        d.kind = Decoded::Kind::Commit;
        d.end_lsn = read_be64(rest.data() + 9);
        d.xid = current_xid_;
        CdcEvent ev;
        ev.op = CdcEvent::Op::Commit;
        ev.lsn = lsn;
        if (cfg_.strict) {
            ev.xid = current_xid_;
        }
        d.events.push_back(std::move(ev));
        return d;
    }

    Decoded on_truncate(std::string_view rest, const std::string& lsn) {
        // Format: nrelations (4) + flags (1) + relation_id[] (4 × n).
        if (rest.size() < 5) {
            return drop_("malformed TRUNCATE message at " + lsn);
        }
        const std::uint32_t n = read_be32(rest.data());
        rest.remove_prefix(5);
        if (rest.size() < std::size_t{4} * n) {
            return drop_("malformed TRUNCATE message at " + lsn);
        }
        Decoded d;
        d.kind = Decoded::Kind::Change;
        d.target = cfg_.tables.empty();
        CdcEvent ev;
        ev.op = CdcEvent::Op::Truncate;
        ev.lsn = lsn;
        // We pick the first relation as the event's "table"; if the
        // TRUNCATE batched several, the rest are added as values entries
        // so users still see them.
        std::vector<const Relation*> targets;
        for (std::uint32_t i = 0; i < n; ++i) {
            const std::uint32_t rel_id = read_be32(rest.data() + (static_cast<std::size_t>(i) * 4));
            auto it = relations_.find(rel_id);
            const std::string tbl = (it != relations_.end()) ? it->second.qualified()
                                                             : ("oid=" + std::to_string(rel_id));
            if (it != relations_.end() && is_target(it->second)) {
                d.target = true;
                targets.push_back(&it->second);
            }
            if (i == 0) {
                ev.table = tbl;
            } else {
                ev.values.push_back(
                    CdcField{.name = "also", .value = tbl, .type = "", .is_null = false});
            }
        }
        if (!d.target) {
            return d;
        }
        if (cfg_.strict) {
            // One Truncate event per captured table it names; the consumer of
            // each holds its rows and deletes every one.
            for (const auto* rel : targets) {
                if (!target_of(*rel)->keep_rows) {
                    return drop_("TRUNCATE of " + rel->qualified() + " at " + lsn +
                                 " cannot be expressed as row changes without the rows; declare "
                                 "the table with keep_rows='true', capture it again from a new "
                                 "slot with a fresh snapshot, or leave truncate out of the "
                                 "publication's publish list");
                }
            }
            for (const auto* rel : targets) {
                CdcEvent t;
                t.op = CdcEvent::Op::Truncate;
                t.lsn = lsn;
                t.table = rel->qualified();
                t.xid = current_xid_;
                d.events.push_back(std::move(t));
            }
            return d;
        }
        d.events.push_back(std::move(ev));
        return d;
    }

    void on_relation(std::string_view rest) {
        if (rest.size() < 4) {
            return;
        }
        Relation rel;
        rel.id = read_be32(rest.data());
        rest.remove_prefix(4);
        rel.ns = read_cstring(rest);
        rel.name = read_cstring(rest);
        if (rest.empty()) {
            return;
        }
        rel.replica_identity = rest[0];
        rest.remove_prefix(1);
        if (rest.size() < 2) {
            return;
        }
        const std::uint16_t ncols = read_be16(rest.data());
        rest.remove_prefix(2);
        rel.columns.reserve(ncols);
        for (std::uint16_t i = 0; i < ncols; ++i) {
            if (rest.empty()) {
                return;
            }
            Column c;
            c.flags = static_cast<std::uint8_t>(rest[0]);
            rest.remove_prefix(1);
            c.name = read_cstring(rest);
            if (rest.size() < 8) {
                return;
            }
            c.type_oid = read_be32(rest.data());
            c.type_modifier = static_cast<std::int32_t>(read_be32(rest.data() + 4));
            rest.remove_prefix(8);
            rel.columns.push_back(std::move(c));
        }
        if (const auto* t = target_of(rel); t != nullptr) {
            for (const auto& c : rel.columns) {
                if (t->needed.count(c.name) != 0) {
                    seen_[rel.qualified()].insert(c.name);
                }
            }
        }
        relations_[rel.id] = std::move(rel);
    }

    // Decode a TupleData block. `truncated` is set if the tuple ran short of
    // its declared column count or a column value overran the buffer - the
    // caller must then DROP the change rather than emit a partial
    // (column-missing) event, which would be silent data corruption.
    std::vector<Cell> parse_tuple(std::string_view& cursor,
                                  const Relation& rel,
                                  bool& truncated) const {
        std::vector<Cell> out;
        if (cursor.size() < 2) {
            truncated = true;
            return out;
        }
        const std::uint16_t ncols = read_be16(cursor.data());
        cursor.remove_prefix(2);
        out.reserve(ncols);
        for (std::uint16_t i = 0; i < ncols; ++i) {
            if (cursor.empty()) {
                truncated = true;  // fewer columns present than declared
                break;
            }
            Cell cell;
            cell.kind = cursor[0];
            cursor.remove_prefix(1);
            if (i < rel.columns.size()) {
                cell.field.name = rel.columns[i].name;
                cell.field.type = resolve_type_name(rel.columns[i].type_oid);
            }
            switch (cell.kind) {
                case 'n':
                    cell.field.is_null = true;
                    break;
                case 'u':
                    // Unchanged TOASTed value: the change does not carry it.
                    break;
                case 't':
                case 'b': {
                    if (cursor.size() < 4) {
                        truncated = true;
                        return out;
                    }
                    const std::uint32_t len = read_be32(cursor.data());
                    cursor.remove_prefix(4);
                    if (cursor.size() < len) {
                        truncated = true;
                        return out;
                    }
                    cell.field.value = std::string{cursor.substr(0, len)};
                    cursor.remove_prefix(len);
                    break;
                }
                default:
                    // Unknown column-kind byte - bail out to avoid running
                    // off the end of the buffer.
                    truncated = true;
                    return out;
            }
            out.push_back(std::move(cell));
        }
        return out;
    }

    static std::vector<CdcField> fields_of(std::vector<Cell> cells) {
        std::vector<CdcField> out;
        out.reserve(cells.size());
        for (auto& c : cells) {
            out.push_back(std::move(c.field));
        }
        return out;
    }

    // Under strict mode every needed column must be in the relation: a column
    // dropped or renamed after the job was declared would otherwise read as
    // NULL in every later change. A needed column the relation lacks and no
    // earlier relation had predates the change: it was added later (open()
    // found it in the table), which only a job that keeps rows can complete,
    // from the rows it holds. Such columns are returned in `predating`.
    [[nodiscard]] std::optional<std::string> missing_needed(
        const Relation& rel,
        const std::string& lsn,
        std::vector<std::string>* predating = nullptr) const {
        const auto* t = target_of(rel);
        if (!cfg_.strict || t == nullptr || t->needed.empty()) {
            return std::nullopt;
        }
        const auto seen = seen_.find(rel.qualified());
        std::string later;
        for (const auto& want : t->needed) {
            const bool present = std::any_of(rel.columns.begin(),
                                             rel.columns.end(),
                                             [&](const Column& c) { return c.name == want; });
            if (present) {
                continue;
            }
            if (seen != seen_.end() && seen->second.count(want) != 0) {
                return "table " + rel.qualified() + " no longer has column " + want +
                       " (dropped or renamed since the job was declared)";
            }
            if (t->keep_rows && predating != nullptr) {
                predating->push_back(want);
            } else {
                later += (later.empty() ? "" : ", ") + want;
            }
        }
        if (!later.empty()) {
            return "the change to " + rel.qualified() + " at " + lsn + " predates column(s) " +
                   later +
                   ", which this job declares; declare the table with keep_rows='true' to take "
                   "their values from the rows the job keeps, or start the job afresh";
        }
        return std::nullopt;
    }

    // A column the change predates, passed on as unchanged for the consumer
    // that keeps rows to fill in.
    static CdcField predating_field(const std::string& name) {
        return CdcField{.name = name,
                        .value = "",
                        .type = std::string{cdc::kUnchangedToastType},
                        .is_null = false};
    }

    CdcEvent make_event(CdcEvent::Op op,
                        const Relation& rel,
                        const std::string& lsn,
                        std::vector<CdcField> values) const {
        CdcEvent ev;
        ev.op = op;
        ev.lsn = lsn;
        ev.table = rel.qualified();
        if (cfg_.strict) {
            ev.xid = current_xid_;
        }
        ev.values = std::move(values);
        return ev;
    }

    Decoded on_insert(std::string_view rest, const std::string& lsn) {
        if (rest.size() < 5) {
            return drop_("truncated INSERT message at " + lsn);
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        if (rest[0] != 'N') {
            return drop_("INSERT at " + lsn + " carries no new tuple");
        }
        rest.remove_prefix(1);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_("INSERT at " + lsn + " names relation " + std::to_string(rel_id) +
                         ", which no Relation message described");
        }
        const Relation& rel = it->second;
        Decoded d;
        d.kind = Decoded::Kind::Change;
        if (!is_target(rel)) {
            d.target = false;
            return d;
        }
        bool truncated = false;
        auto cells = parse_tuple(rest, rel, truncated);
        if (truncated) {
            return drop_("INSERT into " + rel.qualified() + " at " + lsn +
                         " has a truncated tuple");
        }
        std::vector<std::string> predating;
        if (auto missing = missing_needed(rel, lsn, &predating)) {
            return drop_(*missing);
        }
        if (cfg_.strict) {
            // An UPDATE that brings a row into a publication's row filter is
            // published as an INSERT, and under a replica identity other than
            // FULL an out-of-line value it left unchanged is not in it.
            std::vector<CdcField> values;
            values.reserve(cells.size());
            for (auto& c : cells) {
                if (c.kind == 'u') {
                    if (!needed(rel, c.field.name)) {
                        continue;
                    }
                    return drop_("INSERT into " + rel.qualified() + " at " + lsn +
                                 " (an UPDATE that brought the row into the publication's row "
                                 "filter) does not carry column " +
                                 c.field.name +
                                 ", left unchanged and stored out of line (TOAST). Run ALTER "
                                 "TABLE " +
                                 rel.qualified() + " REPLICA IDENTITY FULL so that it does");
                }
                values.push_back(std::move(c.field));
            }
            for (const auto& name : predating) {
                values.push_back(predating_field(name));
            }
            d.events.push_back(make_event(CdcEvent::Op::Insert, rel, lsn, std::move(values)));
            return d;
        }
        d.events.push_back(make_event(CdcEvent::Op::Insert, rel, lsn, fields_of(std::move(cells))));
        return d;
    }

    Decoded on_update(std::string_view rest, const std::string& lsn) {
        if (rest.size() < 5) {
            return drop_("truncated UPDATE message at " + lsn);
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_("UPDATE at " + lsn + " names relation " + std::to_string(rel_id) +
                         ", which no Relation message described");
        }
        const Relation& rel = it->second;
        Decoded d;
        d.kind = Decoded::Kind::Change;
        if (!is_target(rel)) {
            d.target = false;
            return d;
        }
        // Optional 'K' (key only) or 'O' (whole old row) image before 'N'.
        char old_kind = 0;
        std::vector<Cell> old_cells;
        if (!rest.empty() && (rest[0] == 'K' || rest[0] == 'O')) {
            old_kind = rest[0];
            rest.remove_prefix(1);
            bool old_trunc = false;
            old_cells = parse_tuple(rest, rel, old_trunc);
            if (old_trunc) {
                // A truncated OLD image leaves the cursor at an arbitrary
                // offset, so the NEW image can no longer be located: a stray
                // 'N' byte would let a structurally-valid-but-WRONG NEW tuple
                // be emitted uncounted. Treat it as unrecoverable framing loss
                // and drop rather than emit corruption.
                return drop_("UPDATE of " + rel.qualified() + " at " + lsn +
                             " has a truncated old tuple");
            }
        }
        if (rest.empty() || rest[0] != 'N') {
            return drop_("UPDATE of " + rel.qualified() + " at " + lsn + " carries no new tuple");
        }
        rest.remove_prefix(1);
        bool truncated = false;
        auto cells = parse_tuple(rest, rel, truncated);
        if (truncated) {
            return drop_("UPDATE of " + rel.qualified() + " at " + lsn + " has a truncated tuple");
        }
        std::vector<std::string> predating;
        if (auto missing = missing_needed(rel, lsn, &predating)) {
            return drop_(*missing);
        }

        // Unchanged out-of-line (TOAST) values: the new tuple marks them 'u'
        // and carries nothing. A whole old row ('O', REPLICA IDENTITY FULL)
        // holds them, inlined, so they are taken from there.
        std::vector<CdcField> values;
        values.reserve(cells.size());
        for (std::size_t i = 0; i < cells.size(); ++i) {
            Cell& c = cells[i];
            if (c.kind == 'u') {
                if (old_kind == 'O' && i < old_cells.size() && old_cells[i].kind != 'u') {
                    c.field.value = old_cells[i].field.value;
                    c.field.is_null = old_cells[i].field.is_null;
                } else if (cfg_.strict) {
                    if (!needed(rel, c.field.name)) {
                        continue;  // not read by the consumer: leave it out
                    }
                    if (target_of(rel)->keep_rows) {
                        // The consumer fills it in from the row it holds.
                        c.field.type = std::string{cdc::kUnchangedToastType};
                        values.push_back(std::move(c.field));
                        continue;
                    }
                    return drop_(
                        "UPDATE of " + rel.qualified() + " at " + lsn + " left column " +
                        c.field.name +
                        " unchanged and stored out of line (TOAST), and the change does "
                        "not carry its value under this replica identity. Run ALTER TABLE " +
                        rel.qualified() +
                        " REPLICA IDENTITY FULL so that updates carry the old row, or declare "
                        "the table with keep_rows='true' to fill it in from the row the job "
                        "keeps");
                }
            }
            values.push_back(std::move(c.field));
        }
        for (const auto& name : predating) {
            values.push_back(predating_field(name));
        }

        if (cfg_.strict && old_kind != 0 && key_changed(rel, old_cells, values)) {
            // A key-changing UPDATE: the old key's row is gone and the new
            // key's row is the update.
            d.events.push_back(
                make_event(CdcEvent::Op::Delete, rel, lsn, fields_of(std::move(old_cells))));
        }
        d.events.push_back(make_event(CdcEvent::Op::Update, rel, lsn, std::move(values)));
        return d;
    }

    [[nodiscard]] bool key_changed(const Relation& rel,
                                   const std::vector<Cell>& old_cells,
                                   const std::vector<CdcField>& values) const {
        const auto* t = target_of(rel);
        const auto is_key = [&](const std::string& name, std::size_t idx) {
            if (t != nullptr && !t->key_columns.empty()) {
                return std::find(t->key_columns.begin(), t->key_columns.end(), name) !=
                       t->key_columns.end();
            }
            return idx < rel.columns.size() && (rel.columns[idx].flags & 1U) != 0;
        };
        for (std::size_t i = 0; i < old_cells.size() && i < rel.columns.size(); ++i) {
            const std::string& name = rel.columns[i].name;
            if (!is_key(name, i)) {
                continue;
            }
            const auto nv = std::find_if(
                values.begin(), values.end(), [&](const CdcField& f) { return f.name == name; });
            if (nv == values.end()) {
                continue;
            }
            const CdcField& ov = old_cells[i].field;
            if (ov.is_null != nv->is_null || ov.value != nv->value) {
                return true;
            }
        }
        return false;
    }

    Decoded on_delete(std::string_view rest, const std::string& lsn) {
        if (rest.size() < 5) {
            return drop_("truncated DELETE message at " + lsn);
        }
        const std::uint32_t rel_id = read_be32(rest.data());
        rest.remove_prefix(4);
        if (rest.empty() || (rest[0] != 'K' && rest[0] != 'O')) {
            return drop_("DELETE at " + lsn + " carries no old tuple");
        }
        rest.remove_prefix(1);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            return drop_("DELETE at " + lsn + " names relation " + std::to_string(rel_id) +
                         ", which no Relation message described");
        }
        const Relation& rel = it->second;
        Decoded d;
        d.kind = Decoded::Kind::Change;
        if (!is_target(rel)) {
            d.target = false;
            return d;
        }
        bool truncated = false;
        auto cells = parse_tuple(rest, rel, truncated);
        if (truncated) {
            return drop_("DELETE from " + rel.qualified() + " at " + lsn +
                         " has a truncated tuple");
        }
        d.events.push_back(make_event(CdcEvent::Op::Delete, rel, lsn, fields_of(std::move(cells))));
        return d;
    }
};

// =====================================================================
// libpq plumbing: cancellable connects and commands.
// =====================================================================

struct ConnDeleter {
    void operator()(PGconn* c) const noexcept {
        if (c != nullptr) {
            PQfinish(c);
        }
    }
};
using ConnPtr = std::unique_ptr<PGconn, ConnDeleter>;

struct ResultDeleter {
    void operator()(PGresult* r) const noexcept {
        if (r != nullptr) {
            PQclear(r);
        }
    }
};
using ResultPtr = std::unique_ptr<PGresult, ResultDeleter>;

// cancel() was observed while waiting on the server.
class Cancelled : public std::runtime_error {
public:
    Cancelled() : std::runtime_error("postgres_cdc: cancelled") {}
};

constexpr std::chrono::milliseconds kWaitTick{100};

// Wait up to `timeout` for the socket to become readable (or writable).
// Returns false when it did not within the time.
bool wait_socket(PGconn* c, bool for_write, std::chrono::milliseconds timeout) {
    const int fd = PQsocket(c);
    if (fd < 0) {
        return true;  // let the caller's libpq call report the problem
    }
    pollfd p{};
    p.fd = fd;
    p.events = static_cast<short>(for_write ? POLLOUT : POLLIN);
    return ::poll(&p, 1, static_cast<int>(timeout.count())) != 0;
}

// Ask the server to cancel the running command without waiting for it to
// answer: PQcancel blocks until the server closes its side, which a hung
// server never does. The PGcancel object is independent of the connection.
void cancel_in_background(PGconn* c) {
    PGcancel* cn = PQgetCancel(c);
    if (cn == nullptr) {
        return;
    }
    std::thread([cn] {
        std::array<char, 256> err{};
        (void)PQcancel(cn, err.data(), static_cast<int>(err.size()));
        PQfreeCancel(cn);
    }).detach();
}

std::string quote_ident(std::string_view v) {
    std::string out = "\"";
    for (const char c : v) {
        if (c == '"') {
            out += "\"\"";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('"');
    return out;
}

std::string quote_literal(std::string_view v) {
    std::string out = "'";
    for (const char c : v) {
        if (c == '\'') {
            out += "''";
        } else {
            out.push_back(c);
        }
    }
    out.push_back('\'');
    return out;
}

// Connect, observing `cancel` and `timeout`. Keyword order matters: values
// before "dbname" are defaults the user's connection string overrides, values
// after it are forced (PQconnectStartParams with expand_dbname).
ConnPtr connect_cancellable(const PostgresCdcSource::Options& o,
                            bool replication,
                            const std::atomic<bool>& cancel,
                            const std::string& purpose) {
    const std::string app = "clink_cdc_" + o.slot_name;
    std::vector<const char*> keys = {"application_name", "options", "dbname", "client_encoding"};
    std::vector<const char*> vals = {
        app.c_str(),
        "-c TimeZone=UTC -c DateStyle=ISO,MDY -c IntervalStyle=postgres "
        "-c extra_float_digits=3",
        o.conninfo.c_str(),
        "UTF8"};
    if (replication) {
        keys.push_back("replication");
        vals.push_back("database");
    }
    keys.push_back(nullptr);
    vals.push_back(nullptr);
    PGconn* raw = PQconnectStartParams(keys.data(), vals.data(), /*expand_dbname=*/1);
    if (raw == nullptr) {
        throw std::runtime_error("postgres_cdc: " + purpose +
                                 ": out of memory starting a connection");
    }
    ConnPtr conn{raw};
    if (PQstatus(raw) == CONNECTION_BAD) {
        throw std::runtime_error("postgres_cdc: " + purpose +
                                 " connection failed: " + PQerrorMessage(raw));
    }
    const auto deadline = Clock::now() + o.connect_timeout;
    PostgresPollingStatusType st = PGRES_POLLING_WRITING;
    while (st != PGRES_POLLING_OK) {
        if (st == PGRES_POLLING_FAILED) {
            throw std::runtime_error("postgres_cdc: " + purpose +
                                     " connection failed: " + PQerrorMessage(raw));
        }
        while (!wait_socket(raw, st == PGRES_POLLING_WRITING, kWaitTick)) {
            if (cancel.load()) {
                throw Cancelled();
            }
            if (Clock::now() >= deadline) {
                throw std::runtime_error("postgres_cdc: " + purpose +
                                         " connection did not complete within " +
                                         std::to_string(o.connect_timeout.count()) + " ms");
            }
        }
        if (cancel.load()) {
            throw Cancelled();
        }
        st = PQconnectPoll(raw);
    }
    return conn;
}

// Run one command, observing `cancel` (and `limit` when set). Returns the
// last result; a COPY result is returned as soon as it arrives, since the
// connection is then in copy mode.
ResultPtr run_command(PGconn* c,
                      const std::string& sql,
                      const std::atomic<bool>& cancel,
                      std::optional<std::chrono::milliseconds> limit,
                      const std::string& what,
                      const std::function<void(std::chrono::milliseconds)>& waiting = {}) {
    if (PQsendQuery(c, sql.c_str()) != 1) {
        throw std::runtime_error("postgres_cdc: " + what + ": " + PQerrorMessage(c));
    }
    const auto started = Clock::now();
    ResultPtr last;
    while (true) {
        while (PQisBusy(c) == 0) {
            PGresult* r = PQgetResult(c);
            if (r == nullptr) {
                return last;
            }
            const auto st = PQresultStatus(r);
            if (st == PGRES_COPY_BOTH || st == PGRES_COPY_OUT || st == PGRES_COPY_IN) {
                return ResultPtr{r};
            }
            last.reset(r);
        }
        if (cancel.load()) {
            cancel_in_background(c);
            throw Cancelled();
        }
        const auto waited =
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started);
        if (limit && waited >= *limit) {
            cancel_in_background(c);
            throw std::runtime_error("postgres_cdc: " + what + " did not finish within " +
                                     std::to_string(limit->count()) + " ms");
        }
        if (waiting) {
            waiting(waited);
        }
        if (wait_socket(c, false, kWaitTick) && PQconsumeInput(c) == 0) {
            throw std::runtime_error("postgres_cdc: " + what + ": " + PQerrorMessage(c));
        }
    }
}

// run_command, then require a command or tuples result.
ResultPtr run_ok(PGconn* c,
                 const std::string& sql,
                 const std::atomic<bool>& cancel,
                 std::optional<std::chrono::milliseconds> limit,
                 const std::string& what) {
    ResultPtr r = run_command(c, sql, cancel, limit, what);
    const auto st = r ? PQresultStatus(r.get()) : PGRES_FATAL_ERROR;
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = r ? PQresultErrorMessage(r.get()) : PQerrorMessage(c);
        throw std::runtime_error("postgres_cdc: " + what + " failed: " + err);
    }
    return r;
}

constexpr std::chrono::milliseconds kCatalogLimit{std::chrono::seconds{30}};

std::string cell(const PGresult* r, int row, int col) {
    if (PQgetisnull(r, row, col) != 0) {
        return {};
    }
    return {PQgetvalue(r, row, col), static_cast<std::size_t>(PQgetlength(r, row, col))};
}

// What the server says about a replication slot.
struct SlotInfo {
    bool exists{false};
    std::string slot_type;
    std::string plugin;
    std::string database;
    std::string current_database;
    std::string wal_status;
    bool active{false};
    std::uint64_t confirmed_flush{0};
};

SlotInfo read_slot(PGconn* admin, const std::string& slot, const std::atomic<bool>& cancel) {
    const bool has_wal_status = PQserverVersion(admin) >= 130000;
    const std::string sql =
        "SELECT slot_type, coalesce(plugin, ''), coalesce(database, ''), active, "
        "coalesce(confirmed_flush_lsn::text, ''), " +
        std::string{has_wal_status ? "coalesce(wal_status, '')" : "''"} +
        ", current_database() FROM pg_replication_slots WHERE slot_name = " + quote_literal(slot);
    ResultPtr r = run_ok(admin, sql, cancel, kCatalogLimit, "reading replication slot " + slot);
    SlotInfo s;
    if (PQntuples(r.get()) == 0) {
        ResultPtr db = run_ok(
            admin, "SELECT current_database()", cancel, kCatalogLimit, "reading the database name");
        s.current_database = cell(db.get(), 0, 0);
        return s;
    }
    s.exists = true;
    s.slot_type = cell(r.get(), 0, 0);
    s.plugin = cell(r.get(), 0, 1);
    s.database = cell(r.get(), 0, 2);
    s.active = cell(r.get(), 0, 3) == "t";
    if (const auto lsn = cdc::parse_lsn(cell(r.get(), 0, 4))) {
        s.confirmed_flush = *lsn;
    }
    s.wal_status = cell(r.get(), 0, 5);
    s.current_database = cell(r.get(), 0, 6);
    return s;
}

// Split a publication_names value ("a, \"B\"") into bare names.
std::vector<std::string> split_publications(const std::string& csv) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < csv.size()) {
        while (i < csv.size() && (csv[i] == ' ' || csv[i] == ',')) {
            ++i;
        }
        if (i >= csv.size()) {
            break;
        }
        std::string name;
        if (csv[i] == '"') {
            ++i;
            while (i < csv.size()) {
                if (csv[i] == '"') {
                    if (i + 1 < csv.size() && csv[i + 1] == '"') {
                        name.push_back('"');
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                name.push_back(csv[i++]);
            }
        } else {
            while (i < csv.size() && csv[i] != ',' && csv[i] != ' ') {
                name.push_back(
                    static_cast<char>(std::tolower(static_cast<unsigned char>(csv[i++]))));
            }
        }
        if (!name.empty()) {
            out.push_back(std::move(name));
        }
    }
    return out;
}

// Build a Standby Status Update payload and send it via the COPY stream.
// Layout (PostgreSQL protocol, Standby status update): 'r' + written +
// flushed + applied + clientTime + replyRequested. For a logical slot the
// server confirms the flushed position only; it is sent exactly as a
// completed checkpoint recorded it.
bool send_standby_status_update(PGconn* conn,
                                std::uint64_t written,
                                std::uint64_t flushed,
                                bool reply_requested) {
    std::array<char, 1 + 8 + 8 + 8 + 8 + 1> msg{};
    msg[0] = 'r';
    write_be64(msg.data() + 1, std::max(written, flushed));
    write_be64(msg.data() + 9, flushed);
    write_be64(msg.data() + 17, flushed);
    write_be64(msg.data() + 25, static_cast<std::uint64_t>(postgres_epoch_us_now()));
    msg[33] = reply_requested ? 1 : 0;
    if (PQputCopyData(conn, msg.data(), static_cast<int>(msg.size())) != 1) {
        return false;
    }
    return PQflush(conn) == 0;
}

constexpr const char* kLegacyLsnKey = "__postgres_cdc_lsn__";
constexpr const char* kCursorKey = "__postgres_cdc_cursor__";
// The declared columns a checkpoint was taken with, so a restore can tell a
// column declared since.
constexpr const char* kColumnsKey = "__postgres_cdc_columns__";

// What a checkpoint records of the tables it was taken with: one line per
// table, its name, a tab and its declared columns ("name:TYPE;...").
std::string declared_record(const std::vector<PostgresCdcSource::Options::Table>& tables) {
    std::string out;
    for (const auto& t : tables) {
        out += (out.empty() ? "" : "\n") + t.table + "\t";
        bool first = true;
        for (const auto& [name, type] : t.declared_columns) {
            out += (first ? "" : ";") + name + ":" + type;
            first = false;
        }
    }
    return out;
}

// What a restore declares that its checkpoint did not.
struct DeclaredSince {
    // Data columns of a recorded table declared now and not then, or with
    // another type, as "table.column".
    std::vector<std::string> columns;
    // The tables those belong to that do not keep rows: the replayed changes
    // that predate a column could not be completed.
    std::vector<std::string> without_kept_rows;
    // Tables the checkpoint did not capture at all.
    std::vector<std::string> tables;
};

DeclaredSince declared_since(const std::string& recorded,
                             const std::vector<PostgresCdcSource::Options::Table>& tables) {
    std::map<std::string, std::map<std::string, std::string>> before;
    std::size_t pos = 0;
    while (pos < recorded.size()) {
        const auto nl = recorded.find('\n', pos);
        const std::string line =
            recorded.substr(pos, nl == std::string::npos ? std::string::npos : nl - pos);
        pos = nl == std::string::npos ? recorded.size() : nl + 1;
        const auto tab = line.find('\t');
        if (tab == std::string::npos) {
            continue;
        }
        auto& cols = before[line.substr(0, tab)];
        std::size_t p = tab + 1;
        while (p < line.size()) {
            const auto semi = line.find(';', p);
            const std::string entry =
                line.substr(p, semi == std::string::npos ? std::string::npos : semi - p);
            p = semi == std::string::npos ? line.size() : semi + 1;
            if (const auto colon = entry.find(':'); colon != std::string::npos) {
                cols[entry.substr(0, colon)] = entry.substr(colon + 1);
            }
        }
    }
    DeclaredSince out;
    for (const auto& t : tables) {
        const auto rec = before.find(t.table);
        if (rec == before.end()) {
            out.tables.push_back(t.table);
            continue;
        }
        bool added = false;
        for (const auto& [name, type] : t.declared_columns) {
            if (cdc::is_metadata_column(name)) {
                continue;
            }
            const auto it = rec->second.find(name);
            if (it == rec->second.end() || it->second != type) {
                out.columns.push_back(t.table + "." + name);
                added = true;
            }
        }
        if (added && !t.consumer_keeps_rows) {
            out.without_kept_rows.push_back(t.table);
        }
    }
    return out;
}

std::string joined(const std::vector<std::string>& items) {
    std::string out;
    for (const auto& i : items) {
        out += (out.empty() ? "" : ", ") + i;
    }
    return out;
}
constexpr const char* kSnapshotCursor = "clink_cdc_snapshot";

}  // namespace

// Test-only seam (declared in pg_cdc_test_seam.hpp). Defined here so it can reach
// the anonymous-namespace PgOutputState. Drives the decoder over raw pgoutput
// message payloads and reports the decoded events + the drop count, so the F1
// silent-data-loss accounting is unit-testable without a live Postgres.
namespace pg_cdc_testing {

PgDecodeResult decode_pgoutput_messages(const std::vector<std::string>& messages) {
    return decode_pgoutput_messages(messages, PgDecodeConfig{});
}

PgDecodeResult decode_pgoutput_messages(const std::vector<std::string>& messages,
                                        const PgDecodeConfig& config) {
    DecoderConfig cfg;
    if (!config.table.empty()) {
        DecoderTable t;
        t.key_columns = config.key_columns;
        t.needed =
            std::set<std::string>(config.needed_columns.begin(), config.needed_columns.end());
        t.keep_rows = config.consumer_keeps_rows;
        cfg.tables.emplace(config.table, std::move(t));
    }
    cfg.strict = config.strict;
    PgOutputState state(std::move(cfg));
    PgDecodeResult result;
    for (const auto& msg : messages) {
        Decoded d = state.on_message(msg, "0/0");
        for (auto& ev : d.events) {
            result.events.push_back(std::move(ev));
        }
        if (d.dropped) {
            result.errors.push_back(d.error);
        }
    }
    result.dropped = state.dropped_data_events;
    return result;
}

}  // namespace pg_cdc_testing

// =====================================================================
// PostgresCdcSource implementation
// =====================================================================

struct PostgresCdcSource::Impl {
    Options opts;
    std::atomic<bool> cancel{false};
    // open() returned early on cancel(); produce() then ends the stream.
    bool opened_cancelled{false};

    PgOutputState pgoutput;
    std::uint64_t last_dropped{0};  // pgoutput.dropped_data_events at last surfacing

    enum class Phase : std::uint8_t { Idle, Snapshot, Stream };
    Phase phase{Phase::Idle};
    std::atomic<bool> snapshot_active{false};

    // ---- streaming ----
    ConnPtr repl;
    bool streaming{false};
    cdc::TxnCursor cursor;
    // The Begin marker of the transaction in progress was emitted before a
    // restart; it is not emitted again.
    std::uint64_t received_lsn{0};
    Clock::time_point last_inbound{Clock::now()};
    Clock::time_point last_status_send{Clock::now()};
    std::string start_position_lsn;
    bool tls_reported{false};

    // ---- snapshot ----
    ConnPtr snap;
    std::size_t snap_query{0};
    bool snap_cursor_open{false};
    bool snapshot_exact{false};
    std::uint64_t snapshot_rows{0};
    std::uint64_t snapshot_from{0};  // the slot position streaming starts at afterwards
    Clock::time_point snapshot_started{Clock::now()};

    // ---- convergence ----
    // A fresh snapshot's WAL position: changes before it replay what that
    // snapshot already holds (cdc::CursorState::converge_until). 0 = none.
    std::atomic<std::uint64_t> converge_until{0};

    // ---- restore ----
    std::optional<cdc::CursorState> restored;
    // The declared columns the restored checkpoint was taken with
    // ("name:TYPE;..."), when it recorded them.
    std::optional<std::string> restored_columns;
    // The captured tables' snapshot queries, for a backfill.
    std::vector<PostgresCdcSnapshotQuery> table_snapshots;

    // ---- acknowledgement ----
    cdc::AckLedger ledger;
    std::atomic<std::uint64_t> confirmed{0};  // last flush position sent

    // ---- reconnect ----
    std::optional<Clock::time_point> reconnect_deadline;
    Clock::time_point next_reconnect{Clock::now()};
    std::chrono::milliseconds reconnect_backoff{200};
    std::string lost_reason;
};

bool PostgresCdcSource::is_real_implementation() {
    return true;
}

PostgresCdcSource::PostgresCdcSource(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

PostgresCdcSource::~PostgresCdcSource() = default;

namespace {

void validate_options(const PostgresCdcSource::Options& o) {
    if (o.plugin != "test_decoding" && o.plugin != "pgoutput") {
        throw std::runtime_error("PostgresCdcSource: unsupported plugin '" + o.plugin +
                                 "' (supported: test_decoding, pgoutput)");
    }
    if (o.plugin == "pgoutput" && o.publication_names.empty()) {
        throw std::runtime_error("PostgresCdcSource: pgoutput requires Options::publication_names");
    }
    if (o.slot_name.empty()) {
        throw std::runtime_error("PostgresCdcSource: slot_name is required");
    }
    if (o.strict && o.plugin != "pgoutput") {
        throw std::runtime_error("PostgresCdcSource: strict decoding needs plugin pgoutput");
    }
    if (o.consumer_keeps_rows && !o.strict) {
        throw std::runtime_error("postgres_cdc: consumer_keeps_rows needs strict decoding");
    }
    if (o.strict && o.proto_version != 1) {
        throw std::runtime_error(
            "PostgresCdcSource: strict decoding needs pgoutput protocol version 1");
    }
    for (const auto& t : o.tables) {
        if (t.table.empty() || t.key_columns.empty()) {
            throw std::runtime_error(
                "PostgresCdcSource: each of `tables` needs a table and its primary key columns");
        }
        if (t.consumer_keeps_rows && !o.strict) {
            throw std::runtime_error("postgres_cdc: consumer_keeps_rows needs strict decoding");
        }
    }
    if (!o.table.empty() && o.key_columns.empty()) {
        throw std::runtime_error("PostgresCdcSource: capturing table " + o.table +
                                 " needs its primary key columns (key_columns)");
    }
    if (o.snapshot_batch_rows == 0 || o.max_batch_events == 0) {
        throw std::runtime_error(
            "PostgresCdcSource: snapshot_batch_rows and max_batch_events must be positive");
    }
}

// The server and role can serve logical replication at all.
void check_server(PGconn* admin, const std::atomic<bool>& cancel) {
    ResultPtr r = run_ok(admin,
                         "SELECT current_setting('wal_level'), "
                         "(SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = "
                         "current_user), current_user",
                         cancel,
                         kCatalogLimit,
                         "reading server settings");
    const std::string wal_level = cell(r.get(), 0, 0);
    if (wal_level != "logical") {
        throw std::runtime_error("postgres_cdc: the server runs with wal_level=" + wal_level +
                                 "; logical replication needs wal_level=logical (it takes a "
                                 "server restart to change)");
    }
    if (cell(r.get(), 0, 1) != "t") {
        throw std::runtime_error("postgres_cdc: role " + cell(r.get(), 0, 2) +
                                 " may not start replication; grant it with ALTER ROLE " +
                                 cell(r.get(), 0, 2) +
                                 " REPLICATION (or connect as a role that has it)");
    }
}

// The single-table checks. Returns the table's canonical "schema.name".
// What open() learns about the captured table.
struct TableCheck {
    std::string canonical;  // schema.name as the decoder sees it
    std::string quoted;     // the same, quoted for a query
    // The row filter the publications apply to every change, which the
    // snapshot must apply too; nullopt when the rows are not filtered.
    std::optional<std::string> row_filter;
};

TableCheck check_table(PGconn* admin,
                       const PostgresCdcSource::Options& o,
                       const std::atomic<bool>& cancel) {
    const auto refuse = [&](const std::string& why) -> std::runtime_error {
        return std::runtime_error("postgres_cdc: table " + o.table + ": " + why);
    };
    ResultPtr t = run_ok(admin,
                         "SELECT c.oid, n.nspname, c.relname, c.relreplident, c.relkind, "
                         "has_table_privilege(c.oid, 'SELECT') FROM pg_class c JOIN pg_namespace n "
                         "ON n.oid = c.relnamespace WHERE c.oid = to_regclass(" +
                             quote_literal(o.table) + ")",
                         cancel,
                         kCatalogLimit,
                         "reading table " + o.table);
    if (PQntuples(t.get()) == 0) {
        throw refuse("does not exist, or is not visible to this role");
    }
    const std::string oid = cell(t.get(), 0, 0);
    const std::string canonical = cell(t.get(), 0, 1) + "." + cell(t.get(), 0, 2);
    const std::string replident = cell(t.get(), 0, 3);
    const std::string relkind = cell(t.get(), 0, 4);
    // A partitioned table is captured through its root: a publication that
    // publishes it with publish_via_partition_root sends every partition's
    // changes under the root's name and columns, and the snapshot reads the
    // root, which reads every partition.
    const bool partitioned = relkind == "p";
    if (partitioned && PQserverVersion(admin) < 130000) {
        throw refuse(
            "is partitioned, and publishing a partitioned table through its root needs "
            "PostgreSQL 13 or later");
    }
    if (!partitioned && relkind != "r") {
        throw refuse("is not an ordinary or a partitioned table");
    }
    if (o.enable_initial_snapshot && cell(t.get(), 0, 5) != "t") {
        throw refuse(
            "this role may not SELECT from it, which the initial snapshot needs; GRANT "
            "SELECT ON " +
            canonical + " TO the role");
    }

    // Columns.
    ResultPtr cols = run_ok(admin,
                            "SELECT a.attname, t.typname, a.attstorage FROM pg_attribute a JOIN "
                            "pg_type t ON t.oid = a.atttypid WHERE a.attrelid = " +
                                oid + " AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                            cancel,
                            kCatalogLimit,
                            "reading the columns of " + canonical);
    std::map<std::string, std::pair<std::string, std::string>>
        by_name;  // name -> (typname, storage)
    for (int i = 0; i < PQntuples(cols.get()); ++i) {
        by_name[cell(cols.get(), i, 0)] = {cell(cols.get(), i, 1), cell(cols.get(), i, 2)};
    }
    std::vector<std::string> toastable;
    std::string declared_spec;
    for (const auto& [name, sql_type] : o.declared_columns) {
        if (cdc::is_metadata_column(name)) {
            continue;
        }
        const auto it = by_name.find(name);
        if (it == by_name.end()) {
            throw refuse("has no column " + name);
        }
        declared_spec += name + ":" + sql_type + ";";
        if (it->second.second != "p") {
            toastable.push_back(name);
        }
    }
    if (!declared_spec.empty()) {
        for (const auto& plan : cdc::plan_columns(declared_spec)) {
            if (auto problem = cdc::type_pair_problem(by_name[plan.name].first, plan)) {
                throw std::runtime_error("postgres_cdc: table " + canonical + ": " + *problem);
            }
        }
    }

    // Primary key, and a replica identity that carries it.
    ResultPtr pk = run_ok(admin,
                          "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = "
                          "i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = " +
                              oid + " AND i.indisprimary",
                          cancel,
                          kCatalogLimit,
                          "reading the primary key of " + canonical);
    std::set<std::string> pk_cols;
    for (int i = 0; i < PQntuples(pk.get()); ++i) {
        pk_cols.insert(cell(pk.get(), i, 0));
    }
    const std::set<std::string> want(o.key_columns.begin(), o.key_columns.end());
    const auto list = [](const std::set<std::string>& s) {
        std::string out;
        for (const auto& c : s) {
            out += (out.empty() ? "" : ", ") + c;
        }
        return out;
    };
    if (pk_cols.empty()) {
        throw refuse("has no primary key; the change stream is keyed by it");
    }
    if (pk_cols != want) {
        throw refuse("its primary key is (" + list(pk_cols) + ") but the job declares (" +
                     list(want) + ")");
    }
    // The replica identity that carries the key. A partitioned table's changes
    // happen in its partitions, so each partition's identity is the one that
    // decides what an update or delete carries; the root's own is not used.
    struct Identity {
        std::string oid;
        std::string name;
        std::string replident;
    };
    std::vector<Identity> identities;
    if (partitioned) {
        ResultPtr leaves =
            run_ok(admin,
                   "SELECT c.oid, n.nspname || '.' || c.relname, c.relreplident, c.relkind FROM "
                   "pg_partition_tree(" +
                       oid +
                       ") p JOIN pg_class c ON c.oid = p.relid JOIN pg_namespace n ON n.oid = "
                       "c.relnamespace WHERE p.isleaf ORDER BY 2",
                   cancel,
                   kCatalogLimit,
                   "reading the partitions of " + canonical);
        for (int i = 0; i < PQntuples(leaves.get()); ++i) {
            const std::string leaf = cell(leaves.get(), i, 1);
            if (cell(leaves.get(), i, 3) != "r") {
                throw refuse("partition " + leaf +
                             " is not an ordinary table (a foreign table, for one), and its "
                             "changes are not published");
            }
            identities.push_back(
                Identity{cell(leaves.get(), i, 0), leaf, cell(leaves.get(), i, 2)});
        }
    } else {
        identities.push_back(Identity{oid, canonical, replident});
    }
    std::vector<std::string> not_full;
    for (const auto& id : identities) {
        if (id.replident == "n") {
            throw refuse((partitioned ? "partition " + id.name + " has" : std::string{"has"}) +
                         " REPLICA IDENTITY NOTHING, so its updates and deletes do not identify "
                         "their row; set REPLICA IDENTITY DEFAULT or FULL" +
                         (partitioned ? " on " + id.name : std::string{}));
        }
        if (id.replident == "i") {
            ResultPtr ri =
                run_ok(admin,
                       "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = "
                       "i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = " +
                           id.oid + " AND i.indisreplident",
                       cancel,
                       kCatalogLimit,
                       "reading the replica identity of " + id.name);
            std::set<std::string> ri_cols;
            for (int i = 0; i < PQntuples(ri.get()); ++i) {
                ri_cols.insert(cell(ri.get(), i, 0));
            }
            if (ri_cols != pk_cols) {
                throw refuse((partitioned ? "the replica identity index of partition " + id.name
                                          : std::string{"its replica identity index"}) +
                             " (" + list(ri_cols) +
                             ") is not the primary key, so deletes would not carry the key; set "
                             "REPLICA IDENTITY DEFAULT or FULL");
            }
        }
        if (id.replident != "f") {
            not_full.push_back(id.name);
        }
    }
    if (o.strict && !o.consumer_keeps_rows && !not_full.empty() && !toastable.empty()) {
        std::string names;
        for (const auto& c : toastable) {
            names += (names.empty() ? "" : ", ") + c;
        }
        std::string tables;
        for (const auto& n : not_full) {
            tables += (tables.empty() ? "" : ", ") + n;
        }
        clink::log::warn(kLog,
                         "table " + canonical + " has columns that may be stored out of line (" +
                             names + ") and the replica identity of " + tables +
                             " is not FULL: an UPDATE that leaves such a value unchanged does "
                             "not carry it, and the source stops on it rather than emit the row "
                             "without it. ALTER TABLE ... REPLICA IDENTITY FULL avoids that");
    }

    // Publications.
    const auto pubs = split_publications(o.publication_names);
    std::string pub_list;
    for (const auto& p : pubs) {
        pub_list += (pub_list.empty() ? "" : ", ") + quote_literal(p);
    }
    if (pubs.empty()) {
        throw refuse("no publication is named");
    }
    const bool has_via_root = PQserverVersion(admin) >= 130000;
    ResultPtr pr = run_ok(admin,
                          "SELECT pubname, pubinsert, pubupdate, pubdelete" +
                              std::string{has_via_root ? ", pubviaroot" : ""} +
                              " FROM pg_publication WHERE pubname IN (" + pub_list + ")",
                          cancel,
                          kCatalogLimit,
                          "reading publications");
    std::map<std::string, std::array<bool, 3>> found;
    std::map<std::string, bool> via_root;
    for (int i = 0; i < PQntuples(pr.get()); ++i) {
        found[cell(pr.get(), i, 0)] = {
            cell(pr.get(), i, 1) == "t", cell(pr.get(), i, 2) == "t", cell(pr.get(), i, 3) == "t"};
        via_root[cell(pr.get(), i, 0)] = has_via_root && cell(pr.get(), i, 4) == "t";
    }
    for (const auto& p : pubs) {
        if (found.count(p) == 0) {
            throw refuse(
                "publication " + p + " does not exist; CREATE PUBLICATION " + p + " FOR TABLE " +
                canonical +
                (partitioned ? " WITH (publish_via_partition_root = true)" : std::string{}));
        }
    }
    if (partitioned) {
        // A publication that sends a partition's changes under the
        // partition's own name (publish_via_partition_root off, or the
        // partition published on its own) sends changes the source, which
        // follows the root, would never see.
        ResultPtr own = run_ok(
            admin,
            "SELECT pt.pubname, pt.schemaname || '.' || pt.tablename FROM pg_publication_tables "
            "pt JOIN pg_partition_tree(" +
                oid +
                ") p ON p.relid = (quote_ident(pt.schemaname) || '.' || "
                "quote_ident(pt.tablename))::regclass WHERE p.relid <> " +
                oid + " AND pt.pubname IN (" + pub_list + ") ORDER BY 1, 2 LIMIT 1",
            cancel,
            kCatalogLimit,
            "reading how the partitions of " + canonical + " are published");
        if (PQntuples(own.get()) > 0) {
            const std::string pub = cell(own.get(), 0, 0);
            throw refuse("publication " + pub + " publishes partition " + cell(own.get(), 0, 1) +
                         " under its own name, so its changes would not reach the source, which "
                         "follows the partitioned table; publish " +
                         canonical + " itself with publish_via_partition_root: ALTER PUBLICATION " +
                         pub + " SET (publish_via_partition_root = true)");
        }
    }
    const bool has_column_lists = PQserverVersion(admin) >= 150000;
    ResultPtr member = run_ok(
        admin,
        "SELECT pubname" + std::string{has_column_lists ? ", attnames::text, rowfilter" : ""} +
            " FROM pg_publication_tables WHERE pubname IN (" + pub_list +
            ") AND schemaname = " + quote_literal(cell(t.get(), 0, 1)) +
            " AND tablename = " + quote_literal(cell(t.get(), 0, 2)),
        cancel,
        kCatalogLimit,
        "reading publication membership");
    if (PQntuples(member.get()) == 0) {
        throw refuse("is in none of the publications (" + o.publication_names +
                     "); ALTER PUBLICATION <name> ADD TABLE " + canonical +
                     (partitioned ? ", with publish_via_partition_root = true" : std::string{}));
    }
    // Each publication that contains the table, with its row filter ("" for
    // none).
    std::vector<std::pair<std::string, std::string>> members;
    for (int i = 0; i < PQntuples(member.get()); ++i) {
        if (partitioned && !via_root[cell(member.get(), i, 0)]) {
            const std::string pub = cell(member.get(), i, 0);
            throw refuse("publication " + pub +
                         " does not set publish_via_partition_root, so the partitions' changes "
                         "would arrive under their own names; ALTER PUBLICATION " +
                         pub + " SET (publish_via_partition_root = true)");
        }
        members.emplace_back(cell(member.get(), i, 0),
                             has_column_lists ? cell(member.get(), i, 2) : std::string{});
        if (has_column_lists) {
            const std::string attnames = cell(member.get(), i, 1);
            for (const auto& [name, sql_type] : o.declared_columns) {
                (void)sql_type;
                if (cdc::is_metadata_column(name)) {
                    continue;
                }
                // attnames is "{a,b}"; a missing column list publishes every column.
                const bool listed = attnames.find("{" + name + ",") != std::string::npos ||
                                    attnames.find("," + name + ",") != std::string::npos ||
                                    attnames.find("," + name + "}") != std::string::npos ||
                                    attnames == "{" + name + "}";
                if (!attnames.empty() && !listed) {
                    throw refuse("publication " + cell(member.get(), i, 0) +
                                 " does not publish column " + name);
                }
            }
        }
    }
    // The row filter PostgreSQL applies to each action: the filters of the
    // publications that publish it, ORed, or none when one of them has none.
    // The snapshot can apply one filter, so all three must agree; otherwise a
    // row could be published by one action and never by the one that would
    // remove it.
    static constexpr std::array<const char*, 3> kAction = {"insert", "update", "delete"};
    std::array<std::optional<std::set<std::string>>, 3> filter_of;
    for (std::size_t k = 0; k < 3; ++k) {
        bool published = false;
        bool unfiltered = false;
        std::set<std::string> filters;
        for (const auto& [pub, filter] : members) {
            if (!found[pub][k]) {
                continue;
            }
            published = true;
            if (filter.empty()) {
                unfiltered = true;
            } else {
                filters.insert(filter);
            }
        }
        if (!published) {
            throw refuse(std::string{"no publication of it publishes "} + kAction[k] +
                         "s, so those changes would never arrive; set publish = 'insert, update, "
                         "delete' on the publication");
        }
        if (!unfiltered) {
            filter_of[k] = std::move(filters);
        }
    }
    const auto render = [](const std::optional<std::set<std::string>>& f) {
        if (!f) {
            return std::string{"no filter"};
        }
        std::string out;
        for (const auto& e : *f) {
            out += (out.empty() ? "(" : " OR (") + e + ")";
        }
        return out;
    };
    if (filter_of[0] != filter_of[1] || filter_of[1] != filter_of[2]) {
        throw refuse("its publications filter its rows differently for inserts (" +
                     render(filter_of[0]) + "), updates (" + render(filter_of[1]) +
                     ") and deletes (" + render(filter_of[2]) +
                     "), so a row could be published and its removal not; give every "
                     "publication that contains it the same publish list, or the same row "
                     "filter");
    }
    TableCheck out;
    out.canonical = canonical;
    out.quoted = quote_ident(cell(t.get(), 0, 1)) + "." + quote_ident(cell(t.get(), 0, 2));
    if (filter_of[0]) {
        out.row_filter = render(filter_of[0]);
    }
    return out;
}

// The slot can serve a resume from `resume_lsn`.
void check_slot(const SlotInfo& slot,
                const PostgresCdcSource::Options& o,
                std::optional<std::uint64_t> resume_lsn) {
    if (!slot.exists) {
        throw cdc::HistoryUnavailable(
            "postgres_cdc: replication slot " + o.slot_name + " does not exist" +
            (resume_lsn ? ", but this job's checkpoint resumes from " + format_lsn(*resume_lsn) +
                              " in it. It was dropped, or the job now points at another server. "
                              "Recreating it would skip every change since that position, so the "
                              "source refuses; start the job from scratch (a fresh slot and "
                              "snapshot) once the downstream copy is reset"
                        : std::string{}));
    }
    if (slot.slot_type != "logical") {
        throw cdc::HistoryUnavailable("postgres_cdc: replication slot " + o.slot_name + " is a " +
                                      slot.slot_type + " slot, not a logical one");
    }
    if (slot.plugin != o.plugin) {
        throw cdc::HistoryUnavailable("postgres_cdc: replication slot " + o.slot_name +
                                      " decodes with " + slot.plugin + ", not " + o.plugin);
    }
    if (!slot.database.empty() && slot.database != slot.current_database) {
        throw cdc::HistoryUnavailable("postgres_cdc: replication slot " + o.slot_name +
                                      " belongs to database " + slot.database + ", not " +
                                      slot.current_database);
    }
    if (slot.wal_status == "lost") {
        throw cdc::HistoryUnavailable(
            "postgres_cdc: replication slot " + o.slot_name +
            " has lost WAL it needs (wal_status=lost, usually max_slot_wal_keep_size). The "
            "changes it no longer holds cannot be read again; drop the slot and start the job "
            "from scratch once the downstream copy is reset");
    }
    if (resume_lsn && slot.confirmed_flush > *resume_lsn) {
        throw cdc::HistoryUnavailable(
            "postgres_cdc: replication slot " + o.slot_name + " is confirmed at " +
            format_lsn(slot.confirmed_flush) + ", beyond the position this job resumes from (" +
            format_lsn(*resume_lsn) +
            "). PostgreSQL would silently start at the later position and skip the changes in "
            "between, so the source refuses. Something else consumed from the slot, or it was "
            "advanced by hand; start the job from scratch once the downstream copy is reset");
    }
}

}  // namespace

// ---------------------------------------------------------------------
// open
// ---------------------------------------------------------------------

void PostgresCdcSource::open() {
    auto& s = *impl_;
    validate_options(s.opts);
    // The tables captured: `tables`, or `table` on its own.
    if (s.opts.tables.empty() && !s.opts.table.empty()) {
        s.opts.tables.push_back(Options::Table{.table = s.opts.table,
                                               .key_columns = s.opts.key_columns,
                                               .declared_columns = s.opts.declared_columns,
                                               .consumer_keeps_rows = s.opts.consumer_keeps_rows});
    }
    DecoderConfig cfg;
    cfg.strict = s.opts.strict;
    s.pgoutput = PgOutputState(std::move(cfg));

    try {
        ConnPtr admin = connect_cancellable(s.opts, false, s.cancel, "postgres_cdc_source");
        clink::connectors::pg::assert_no_silent_downgrade(
            admin.get(), s.opts.conninfo, "postgres_cdc_source");
        s.tls_reported = true;
        check_server(admin.get(), s.cancel);
        if (!s.opts.tables.empty()) {
            std::map<std::string, DecoderTable> decoded;
            std::optional<std::string> filtered;
            const std::string first_declared = s.opts.tables.front().table;
            for (auto& t : s.opts.tables) {
                Options one = s.opts;
                one.table = t.table;
                one.key_columns = t.key_columns;
                one.declared_columns = t.declared_columns;
                one.consumer_keeps_rows = t.consumer_keeps_rows;
                const TableCheck table = check_table(admin.get(), one, s.cancel);
                DecoderTable rules;
                rules.key_columns = t.key_columns;
                rules.keep_rows = t.consumer_keeps_rows;
                // The table's snapshot: its declared columns (every column
                // when none are declared), filtered as the publications filter
                // its changes. The initial snapshot and a backfill read it.
                std::string select_list;
                for (const auto& [name, type] : t.declared_columns) {
                    (void)type;
                    if (!cdc::is_metadata_column(name)) {
                        rules.needed.insert(name);
                        select_list += (select_list.empty() ? "" : ", ") + quote_ident(name);
                    }
                }
                if (!decoded.emplace(table.canonical, std::move(rules)).second) {
                    throw std::runtime_error("postgres_cdc: table " + table.canonical +
                                             " is captured twice by one source");
                }
                s.table_snapshots.push_back(PostgresCdcSnapshotQuery{
                    .table = table.canonical,
                    .query = "SELECT " + (select_list.empty() ? std::string{"*"} : select_list) +
                             " FROM " + table.quoted +
                             (table.row_filter ? " WHERE " + *table.row_filter : std::string{})});
                if (table.row_filter && !filtered) {
                    filtered = table.canonical + ", filtered by " + *table.row_filter;
                }
                t.table = table.canonical;
            }
            s.pgoutput.set_tables(std::move(decoded));
            if (s.opts.enable_initial_snapshot && s.opts.initial_snapshot.empty()) {
                s.opts.initial_snapshot = s.table_snapshots;
            } else if (filtered && s.opts.enable_initial_snapshot) {
                throw std::runtime_error(
                    "postgres_cdc: table " + *filtered +
                    ", which a snapshot query given in initial_snapshot does not apply; leave "
                    "initial_snapshot empty and the source reads the table with that filter");
            }
            for (auto& q : s.opts.initial_snapshot) {
                if (q.table == first_declared) {
                    q.table = s.opts.tables.front().table;
                }
            }
            s.opts.table = s.opts.tables.front().table;
        }
        const SlotInfo slot = read_slot(admin.get(), s.opts.slot_name, s.cancel);
        const bool want_snapshot =
            s.opts.enable_initial_snapshot && !s.opts.initial_snapshot.empty();
        std::string plan;

        if (s.restored) {
            const auto& r = *s.restored;
            check_slot(slot, s.opts, r.resume_lsn);
            s.confirmed.store(slot.confirmed_flush);
            std::optional<std::pair<std::uint64_t, std::uint64_t>> interrupted;
            if (r.in_flight) {
                interrupted = std::make_pair(r.txn_id, r.txn_changes);
            }
            s.cursor.start(r.resume_lsn, interrupted);
            // Columns, or tables, declared since the checkpoint: the rows
            // already delivered lack them, so the tables are read again (a
            // backfill) before the stream resumes. The kept rows complete the
            // replayed changes that predate a column; without them the
            // restore stops.
            DeclaredSince since;
            if (s.opts.strict && s.restored_columns) {
                since = declared_since(*s.restored_columns, s.opts.tables);
            } else if (s.opts.strict) {
                clink::log::info(kLog,
                                 "the checkpoint does not record the columns it was taken "
                                 "with; a column declared since is not read for the rows "
                                 "already delivered");
            }
            if (!since.without_kept_rows.empty()) {
                throw std::runtime_error(
                    "postgres_cdc: column(s) " + joined(since.columns) +
                    " are declared since the checkpoint this job restores, and the rows already "
                    "delivered do not have them. Declare " +
                    joined(since.without_kept_rows) +
                    " with keep_rows='true', which reads the table again for them on restore, "
                    "or start the job afresh with a new slot and snapshot");
            }
            const bool backfill = !since.columns.empty() || !since.tables.empty();
            const std::string added_list =
                joined(since.columns) +
                (since.columns.empty() || since.tables.empty() ? "" : ", ") + joined(since.tables);
            if ((r.phase == cdc::Phase::Snapshot || backfill) && s.opts.initial_snapshot.empty()) {
                s.opts.initial_snapshot = s.table_snapshots;
            }
            if ((r.phase == cdc::Phase::Snapshot || backfill) && !s.opts.initial_snapshot.empty()) {
                admin.reset();
                s.snapshot_from = r.resume_lsn;
                begin_snapshot_("");
                plan = backfill ? "restored at " + format_lsn(r.resume_lsn) + "; " + added_list +
                                      " declared since the checkpoint, so the table is read "
                                      "again (a backfill, fresh), streaming from " +
                                      format_lsn(r.resume_lsn) + " afterwards"
                                : "restored mid-snapshot; snapshot taken again (fresh), "
                                  "streaming from " +
                                      format_lsn(r.resume_lsn) + " afterwards";
            } else {
                admin.reset();
                s.phase = Impl::Phase::Stream;
                plan =
                    "resume from checkpoint at " + format_lsn(r.resume_lsn) +
                    (r.in_flight ? " (inside transaction " + format_lsn(r.txn_id) + ", " +
                                       std::to_string(r.txn_changes) + " changes already emitted)"
                                 : std::string{});
            }
        } else if (!slot.exists) {
            if (!s.opts.create_slot) {
                check_slot(slot, s.opts, std::nullopt);
            }
            admin.reset();
            ConnPtr creator = connect_cancellable(s.opts, true, s.cancel, "replication");
            const std::string create_sql =
                "CREATE_REPLICATION_SLOT " + quote_ident(s.opts.slot_name) + " LOGICAL " +
                s.opts.plugin + (want_snapshot ? " EXPORT_SNAPSHOT" : " NOEXPORT_SNAPSHOT");
            const auto waiting = [&s, logged = std::chrono::milliseconds{0}](
                                     std::chrono::milliseconds waited) mutable {
                if (waited - logged >= std::chrono::seconds{10}) {
                    logged = waited;
                    clink::log::info(kLog,
                                     "creating replication slot " + s.opts.slot_name + ": waited " +
                                         std::to_string(waited.count() / 1000) +
                                         " s for transactions open on the server to finish; the "
                                         "slot is consistent only once they do");
                }
            };
            ResultPtr r = run_command(creator.get(),
                                      create_sql,
                                      s.cancel,
                                      std::nullopt,
                                      "CREATE_REPLICATION_SLOT",
                                      waiting);
            if (!r || PQresultStatus(r.get()) != PGRES_TUPLES_OK) {
                const std::string err =
                    r ? PQresultErrorMessage(r.get()) : PQerrorMessage(creator.get());
                throw std::runtime_error("postgres_cdc: CREATE_REPLICATION_SLOT failed: " + err);
            }
            std::string consistent_point;
            std::string snapshot_name;
            for (int c = 0; c < PQnfields(r.get()); ++c) {
                const std::string_view name = PQfname(r.get(), c);
                if (name == "consistent_point") {
                    consistent_point = cell(r.get(), 0, c);
                } else if (name == "snapshot_name") {
                    snapshot_name = cell(r.get(), 0, c);
                }
            }
            const auto cp = cdc::parse_lsn(consistent_point);
            if (!cp) {
                throw std::runtime_error(
                    "postgres_cdc: CREATE_REPLICATION_SLOT returned no consistent point");
            }
            s.confirmed.store(*cp);
            s.cursor.start(*cp, std::nullopt);
            clink::log::info(kLog,
                             "created replication slot " + s.opts.slot_name +
                                 " at consistent point " + consistent_point);
            CLINK_FAULT_POINT(cdc::points::kAfterSlotCreated);
            if (want_snapshot) {
                s.snapshot_from = *cp;
                // The exported snapshot lives only while `creator` runs no
                // other command, so it is imported before anything else.
                begin_snapshot_(snapshot_name);
                plan = "new slot; snapshot bound to the slot's export, streaming from " +
                       consistent_point + " afterwards";
            } else {
                s.phase = Impl::Phase::Stream;
                plan = "new slot; streaming from " + consistent_point;
            }
        } else {
            check_slot(slot, s.opts, std::nullopt);
            admin.reset();
            s.confirmed.store(slot.confirmed_flush);
            s.cursor.start(slot.confirmed_flush, std::nullopt);
            if (want_snapshot) {
                s.snapshot_from = slot.confirmed_flush;
                begin_snapshot_("");
                plan = "existing slot; snapshot taken fresh, streaming from the slot's position " +
                       format_lsn(slot.confirmed_flush) +
                       " afterwards (changes the snapshot already holds are replayed after it)";
            } else {
                s.phase = Impl::Phase::Stream;
                plan = "existing slot; streaming from its position " +
                       format_lsn(slot.confirmed_flush);
            }
        }
        std::vector<std::string> names;
        for (const auto& t : s.opts.tables) {
            names.push_back(t.table);
        }
        clink::log::info(
            kLog,
            "postgres cdc source open: slot=" + s.opts.slot_name + " plugin=" + s.opts.plugin +
                " table=" + (names.empty() ? std::string{"(all published)"} : joined(names)) +
                " plan: " + plan);
        if (s.phase == Impl::Phase::Stream) {
            start_or_retry_();
        }
    } catch (const Cancelled&) {
        s.snap.reset();
        s.repl.reset();
        s.opened_cancelled = true;
    }
}

// Open the snapshot transaction and its first cursor. `snapshot_name` set
// imports the slot's exported snapshot (the exact boundary); empty takes a
// fresh repeatable-read snapshot.
void PostgresCdcSource::begin_snapshot_(const std::string& snapshot_name) {
    auto& s = *impl_;
    s.snap = connect_cancellable(s.opts, false, s.cancel, "snapshot");
    run_ok(s.snap.get(),
           "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
           s.cancel,
           kCatalogLimit,
           "starting the snapshot transaction");
    s.snapshot_exact = !snapshot_name.empty();
    if (s.snapshot_exact) {
        run_ok(s.snap.get(),
               "SET TRANSACTION SNAPSHOT " + quote_literal(snapshot_name),
               s.cancel,
               kCatalogLimit,
               "importing the slot's snapshot");
        s.converge_until.store(0);
    } else {
        // The first statement takes the snapshot, and the WAL insert position
        // read in it is past the commit of every transaction it can see: the
        // stream has replayed all of them once it is past this position.
        ResultPtr w = run_ok(s.snap.get(),
                             "SELECT pg_current_wal_insert_lsn()::text",
                             s.cancel,
                             kCatalogLimit,
                             "taking the snapshot");
        const auto lsn = cdc::parse_lsn(cell(w.get(), 0, 0));
        if (!lsn) {
            throw std::runtime_error(
                "postgres_cdc: could not read the WAL position of the snapshot");
        }
        s.converge_until.store(*lsn);
    }
    s.snap_query = 0;
    s.snapshot_rows = 0;
    s.snapshot_started = Clock::now();
    run_ok(s.snap.get(),
           std::string{"DECLARE "} + kSnapshotCursor + " NO SCROLL CURSOR FOR " +
               s.opts.initial_snapshot[0].query,
           s.cancel,
           kCatalogLimit,
           "opening the snapshot query");
    s.snap_cursor_open = true;
    s.phase = Impl::Phase::Snapshot;
    s.snapshot_active.store(true);
}

// ---------------------------------------------------------------------
// streaming
// ---------------------------------------------------------------------

void PostgresCdcSource::start_streaming_() {
    auto& s = *impl_;
    if (!s.repl) {
        s.repl = connect_cancellable(s.opts, true, s.cancel, "replication");
        if (!s.tls_reported) {
            clink::connectors::pg::assert_no_silent_downgrade(
                s.repl.get(), s.opts.conninfo, "postgres_cdc_source");
            s.tls_reported = true;
        }
    }
    const std::uint64_t from = s.cursor.resume_lsn();
    std::string sql =
        "START_REPLICATION SLOT " + quote_ident(s.opts.slot_name) + " LOGICAL " + format_lsn(from);
    if (s.opts.plugin == "pgoutput") {
        sql += " (proto_version " + quote_literal(std::to_string(s.opts.proto_version)) +
               ", publication_names " + quote_literal(s.opts.publication_names) + ")";
    }
    ResultPtr r =
        run_command(s.repl.get(), sql, s.cancel, s.opts.connect_timeout, "START_REPLICATION");
    if (!r || PQresultStatus(r.get()) != PGRES_COPY_BOTH) {
        const std::string err = r ? PQresultErrorMessage(r.get()) : PQerrorMessage(s.repl.get());
        s.repl.reset();
        if (err.find("does not exist") != std::string::npos ||
            err.find("can no longer get changes") != std::string::npos ||
            err.find("has already been removed") != std::string::npos ||
            err.find("invalidated") != std::string::npos) {
            throw cdc::HistoryUnavailable("postgres_cdc: START_REPLICATION from " +
                                          format_lsn(from) + " refused: " + err);
        }
        throw std::runtime_error("postgres_cdc: START_REPLICATION from " + format_lsn(from) +
                                 " failed: " + err);
    }
    s.streaming = true;
    s.phase = Impl::Phase::Stream;
    s.start_position_lsn = format_lsn(from);
    s.pgoutput.reset_relations();
    s.last_inbound = Clock::now();
    s.last_status_send = Clock::now();
    CLINK_FAULT_POINT(cdc::points::kStreamStarted);
}

// Start streaming now, or - when the failure is one a retry can cure, such
// as the slot still held by the session of a consumer that just died - hand
// over to the bounded reconnect loop in produce().
void PostgresCdcSource::start_or_retry_() {
    try {
        start_streaming_();
    } catch (const cdc::HistoryUnavailable&) {
        throw;
    } catch (const Cancelled&) {
        throw;
    } catch (const std::exception& e) {
        connection_lost_(e.what());
    }
}

void PostgresCdcSource::connection_lost_(const std::string& reason) {
    auto& s = *impl_;
    s.repl.reset();
    s.streaming = false;
    s.lost_reason = reason;
    clink::metrics::connector::error_inc(kMetricsName, "source");
    if (s.opts.reconnect_timeout.count() == 0) {
        throw std::runtime_error("postgres_cdc: replication connection lost (" + reason +
                                 ") and reconnect_timeout is 0");
    }
    if (!s.reconnect_deadline) {
        s.reconnect_deadline = Clock::now() + s.opts.reconnect_timeout;
        s.next_reconnect = Clock::now();
        s.reconnect_backoff = std::chrono::milliseconds{200};
        clink::log::warn(kLog,
                         "replication connection lost (" + reason + "); resuming from " +
                             format_lsn(s.cursor.resume_lsn()) + " within " +
                             std::to_string(s.opts.reconnect_timeout.count()) + " ms");
    }
}

bool PostgresCdcSource::reconnect_step_() {
    auto& s = *impl_;
    const auto now = Clock::now();
    if (now < s.next_reconnect) {
        std::this_thread::sleep_for(std::min<Clock::duration>(s.next_reconnect - now, kWaitTick));
        return false;
    }
    if (s.reconnect_deadline && now >= *s.reconnect_deadline) {
        throw std::runtime_error(
            "postgres_cdc: lost the replication connection and could not resume "
            "within " +
            std::to_string(s.opts.reconnect_timeout.count()) + " ms; last error: " + s.lost_reason);
    }
    try {
        // The slot is checked before every resume: PostgreSQL would start a
        // slot confirmed past our position at its own position instead.
        ConnPtr admin = connect_cancellable(s.opts, false, s.cancel, "postgres_cdc_source");
        const SlotInfo slot = read_slot(admin.get(), s.opts.slot_name, s.cancel);
        admin.reset();
        check_slot(slot, s.opts, s.cursor.resume_lsn());
        s.cursor.start(s.cursor.resume_lsn(), s.cursor.interrupted());
        start_streaming_();
    } catch (const cdc::HistoryUnavailable&) {
        throw;
    } catch (const Cancelled&) {
        return false;
    } catch (const std::exception& e) {
        s.repl.reset();
        s.lost_reason = e.what();
        s.next_reconnect = Clock::now() + s.reconnect_backoff;
        s.reconnect_backoff = std::min(s.reconnect_backoff * 2, std::chrono::milliseconds{5000});
        return false;
    }
    clink::log::info(kLog,
                     "replication resumed at " + s.start_position_lsn + " after: " + s.lost_reason);
    s.reconnect_deadline.reset();
    return true;
}

bool PostgresCdcSource::produce(Emitter<CdcEvent>& out) {
    auto& s = *impl_;
    if (this->cancelled() || s.cancel.load() || s.opened_cancelled) {
        return false;
    }
    try {
        if (s.phase == Impl::Phase::Snapshot) {
            return produce_snapshot_(out);
        }
        return produce_stream_(out);
    } catch (const Cancelled&) {
        return false;
    }
}

bool PostgresCdcSource::produce_snapshot_(Emitter<CdcEvent>& out) {
    auto& s = *impl_;
    const auto& q = s.opts.initial_snapshot[s.snap_query];
    ResultPtr r = run_ok(
        s.snap.get(),
        "FETCH FORWARD " + std::to_string(s.opts.snapshot_batch_rows) + " FROM " + kSnapshotCursor,
        s.cancel,
        std::nullopt,
        "reading the snapshot of " + q.table);
    const int nrows = PQntuples(r.get());
    if (nrows == 0) {
        run_ok(s.snap.get(),
               std::string{"CLOSE "} + kSnapshotCursor,
               s.cancel,
               kCatalogLimit,
               "closing the snapshot query");
        s.snap_cursor_open = false;
        if (++s.snap_query < s.opts.initial_snapshot.size()) {
            run_ok(s.snap.get(),
                   std::string{"DECLARE "} + kSnapshotCursor + " NO SCROLL CURSOR FOR " +
                       s.opts.initial_snapshot[s.snap_query].query,
                   s.cancel,
                   kCatalogLimit,
                   "opening the snapshot query");
            s.snap_cursor_open = true;
            return true;
        }
        run_ok(s.snap.get(), "COMMIT", s.cancel, kCatalogLimit, "ending the snapshot transaction");
        s.snap.reset();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                              s.snapshot_started);
        clink::log::info(kLog,
                         "snapshot complete: " + std::to_string(s.snapshot_rows) + " rows in " +
                             std::to_string(ms.count()) + " ms (" +
                             (s.snapshot_exact ? "exact" : "fresh") + "); streaming from " +
                             format_lsn(s.cursor.resume_lsn()));
        s.snapshot_active.store(false);
        s.phase = Impl::Phase::Stream;
        CLINK_FAULT_POINT(cdc::points::kSnapshotDone);
        start_or_retry_();
        return true;
    }
    const int ncols = PQnfields(r.get());
    std::vector<std::string> names;
    std::vector<std::string> types;
    names.reserve(static_cast<std::size_t>(ncols));
    types.reserve(static_cast<std::size_t>(ncols));
    for (int c = 0; c < ncols; ++c) {
        names.emplace_back(PQfname(r.get(), c));
        const char* nm = lookup_builtin_type_name(static_cast<std::uint32_t>(PQftype(r.get(), c)));
        types.emplace_back(nm != nullptr ? nm : "");
    }
    const std::string lsn = format_lsn(s.snapshot_from);
    Batch<CdcEvent> batch;
    std::uint64_t bytes = 0;
    for (int row = 0; row < nrows; ++row) {
        CdcEvent ev;
        ev.op = CdcEvent::Op::Insert;
        ev.table = q.table;
        ev.lsn = lsn;
        ev.values.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            CdcField f;
            f.name = names[static_cast<std::size_t>(c)];
            f.type = types[static_cast<std::size_t>(c)];
            if (PQgetisnull(r.get(), row, c) != 0) {
                f.is_null = true;
            } else {
                f.value = cell(r.get(), row, c);
                bytes += f.value.size();
            }
            ev.values.push_back(std::move(f));
        }
        batch.emplace(std::move(ev));
    }
    s.snapshot_rows += static_cast<std::uint64_t>(nrows);
    out.emit_data(std::move(batch));
    clink::metrics::connector::records_in_inc(kMetricsName, static_cast<std::uint64_t>(nrows));
    clink::metrics::connector::bytes_in_inc(kMetricsName, bytes);
    CLINK_FAULT_POINT(cdc::points::kSnapshotBatch);
    return true;
}

bool PostgresCdcSource::produce_stream_(Emitter<CdcEvent>& out) {
    auto& s = *impl_;
    if (!s.streaming) {
        if (!reconnect_step_()) {
            return true;
        }
    }

    Batch<CdcEvent> batch;
    std::uint64_t bytes = 0;
    std::string fail_after_emit;
    bool waited = false;

    const auto take = [&](std::vector<CdcEvent>& events) {
        for (auto& ev : events) {
            batch.emplace(std::move(ev));
        }
    };

    while (batch.size() < s.opts.max_batch_events && s.streaming) {
        if (s.cancel.load()) {
            break;
        }
        char* buffer = nullptr;
        const int n = PQgetCopyData(s.repl.get(), &buffer, /*async=*/1);
        if (n == 0) {
            if (waited || !batch.empty()) {
                break;
            }
            waited = true;
            if (wait_socket(s.repl.get(), false, s.opts.poll_interval) &&
                PQconsumeInput(s.repl.get()) == 0) {
                connection_lost_(PQerrorMessage(s.repl.get()));
            }
            continue;
        }
        if (n == -1) {
            // The server ended the COPY stream: a shutdown, a terminated
            // walsender, a failover. Never the end of the change stream.
            ResultPtr res{PQgetResult(s.repl.get())};
            const std::string why = res ? PQresultErrorMessage(res.get()) : std::string{};
            connection_lost_("the server ended the replication stream" +
                             (why.empty() ? std::string{} : ": " + why));
            break;
        }
        if (n == -2) {
            connection_lost_(PQerrorMessage(s.repl.get()));
            break;
        }
        std::unique_ptr<char, void (*)(void*)> owned{buffer, PQfreemem};
        s.last_inbound = Clock::now();
        const char type_byte = buffer[0];
        if (type_byte == 'w' && n >= 25) {
            // XLogData: dataStart (8), walEnd (8), sendTime (8), payload.
            const std::uint64_t data_start = read_be64(buffer + 1);
            s.received_lsn = std::max(s.received_lsn, data_start);
            const std::string_view payload(buffer + 25, static_cast<std::size_t>(n) - 25);
            bytes += payload.size();
            const std::string lsn = format_lsn(data_start);
            if (s.opts.plugin == "pgoutput") {
                Decoded d = s.pgoutput.on_message(payload, lsn);
                switch (d.kind) {
                    case Decoded::Kind::Begin:
                        if (s.cursor.on_begin(d.final_lsn)) {
                            take(d.events);
                        }
                        break;
                    case Decoded::Kind::Commit:
                        s.cursor.on_commit(d.end_lsn);
                        take(d.events);
                        break;
                    case Decoded::Kind::Change:
                        if (!d.target) {
                            break;
                        }
                        if (!s.cursor.on_change()) {
                            break;  // emitted before the restart
                        }
                        if (d.dropped) {
                            if (s.opts.decode_error_policy == DecodeErrorPolicy::Fail) {
                                fail_after_emit = d.error;
                            } else {
                                clink::log::warn(kLog, "dropped an undecodable change: " + d.error);
                            }
                            break;
                        }
                        take(d.events);
                        break;
                    case Decoded::Kind::None:
                        break;
                }
            } else {
                CdcEvent ev = parse_test_decoding(payload, lsn);
                const bool target = s.opts.table.empty() || ev.table == s.opts.table;
                switch (ev.op) {
                    case CdcEvent::Op::Begin:
                        if (s.cursor.on_begin(static_cast<std::uint64_t>(ev.xid))) {
                            batch.emplace(std::move(ev));
                        }
                        break;
                    case CdcEvent::Op::Commit:
                        // test_decoding's COMMIT arrives at the end of the
                        // commit record, which is where a resume starts.
                        s.cursor.on_commit(data_start);
                        batch.emplace(std::move(ev));
                        break;
                    default:
                        if (target && s.cursor.on_change()) {
                            batch.emplace(std::move(ev));
                        }
                        break;
                }
            }
            if (!fail_after_emit.empty()) {
                break;
            }
        } else if (type_byte == 'k' && n >= 18) {
            // PrimaryKeepalive: walEnd (8), sendTime (8), replyRequested (1).
            const std::uint64_t wal_end = read_be64(buffer + 1);
            s.received_lsn = std::max(s.received_lsn, wal_end);
            s.cursor.on_keepalive(wal_end);
            if (buffer[17] != 0) {
                send_status_(/*reply_requested=*/false);
            }
        }
    }

    if (!batch.empty()) {
        const auto sz = batch.size();
        out.emit_data(std::move(batch));
        clink::metrics::connector::records_in_inc(kMetricsName, sz);
        CLINK_FAULT_POINT(cdc::points::kStreamBatch);
    }
    if (bytes > 0) {
        clink::metrics::connector::bytes_in_inc(kMetricsName, bytes);
    }
    // Surface any change events the decoder had to DROP (unknown relation /
    // truncated tuple) - otherwise-silent data loss is counted + flagged.
    if (s.pgoutput.dropped_data_events > s.last_dropped) {
        const std::uint64_t delta = s.pgoutput.dropped_data_events - s.last_dropped;
        s.last_dropped = s.pgoutput.dropped_data_events;
        clink::metrics::connector::dropped_events_inc(kMetricsName, delta);
        clink::metrics::connector::error_inc(kMetricsName, "source");
    }
    if (!fail_after_emit.empty()) {
        // Fail policy: never move past the event. The job restarts from its
        // last checkpoint, which this event lies after, and meets it again.
        throw std::runtime_error("postgres_cdc: " + fail_after_emit + " (on_decode_error=fail)");
    }
    if (const auto w = s.converge_until.load(); w != 0 && s.cursor.resume_lsn() >= w) {
        s.converge_until.store(0);
        clink::log::info(kLog,
                         "the stream has passed the snapshot taken at " + format_lsn(w) +
                             ": every change from here on is newer than it");
    }
    // received_lsn doubles as a consumer-lag-proxy: how far behind WAL
    // the source is. There's no server-side current LSN cheaply
    // available from libpq's replication protocol, so we publish the
    // received LSN as the gauge and dashboards diff against a separate
    // server probe.
    clink::metrics::connector::consumer_lag_set(kMetricsName,
                                                static_cast<std::int64_t>(s.received_lsn));

    if (!s.streaming) {
        return true;
    }
    const auto now = Clock::now();
    if (s.ledger.confirmable() > s.confirmed.load()) {
        send_status_(/*reply_requested=*/false);
    } else if (s.opts.standby_status_interval.count() > 0 &&
               now - s.last_status_send >= s.opts.standby_status_interval) {
        // Ask for an answer: on a quiet stream the answer is the only sign the
        // connection still works.
        send_status_(/*reply_requested=*/true);
    }
    if (s.streaming && s.opts.standby_status_interval.count() > 0) {
        const auto silent_for =
            std::max<Clock::duration>(std::chrono::seconds{30}, s.opts.standby_status_interval * 3);
        if (now - s.last_inbound > silent_for) {
            connection_lost_(
                "no message from the server for " +
                std::to_string(
                    std::chrono::duration_cast<std::chrono::seconds>(silent_for).count()) +
                " s");
        }
    }
    return true;
}

void PostgresCdcSource::send_status_(bool reply_requested) {
    auto& s = *impl_;
    if (!s.streaming || !s.repl) {
        return;
    }
    const std::uint64_t before = s.confirmed.load();
    const std::uint64_t flush = std::max(before, s.ledger.confirmable());
    if (!send_standby_status_update(s.repl.get(), s.received_lsn, flush, reply_requested)) {
        connection_lost_(std::string{"could not send a status update: "} +
                         PQerrorMessage(s.repl.get()));
        return;
    }
    s.last_status_send = Clock::now();
    if (flush > before) {
        s.confirmed.store(flush);
        clink::log::debug(
            kLog, "confirmed " + format_lsn(flush) + " to replication slot " + s.opts.slot_name);
        CLINK_FAULT_POINT(cdc::points::kWalConfirmed);
    }
}

void PostgresCdcSource::cancel() {
    Source<CdcEvent>::cancel();
    impl_->cancel.store(true);
}

std::string PostgresCdcSource::start_position() const {
    return impl_ != nullptr ? impl_->start_position_lsn : std::string{};
}

bool PostgresCdcSource::snapshot_in_progress() const noexcept {
    return impl_ != nullptr && impl_->snapshot_active.load();
}

std::uint64_t PostgresCdcSource::confirmed_lsn() const noexcept {
    return impl_ != nullptr ? impl_->confirmed.load() : 0;
}

std::uint64_t PostgresCdcSource::converging_until() const noexcept {
    return impl_ != nullptr ? impl_->converge_until.load() : 0;
}

std::vector<std::string> PostgresCdcSource::captured_tables() const {
    std::vector<std::string> out;
    if (impl_ != nullptr) {
        for (const auto& t : impl_->opts.tables) {
            out.push_back(t.table);
        }
    }
    return out;
}

void PostgresCdcSource::close() {
    if (impl_ == nullptr) {
        return;
    }
    impl_->snap.reset();
    impl_->repl.reset();
    impl_->streaming = false;
    impl_->snapshot_active.store(false);
    // Auto-drop the slot if requested. Done after the session ends so the
    // server has released the slot.
    if (impl_->opts.drop_slot_on_close) {
        try {
            drop_slot();
        } catch (...) {
            // Best-effort on close - swallow and let the runner's error
            // bookkeeping note any leak via metrics.
        }
    }
}

// ---------------------------------------------------------------------
// checkpoints
// ---------------------------------------------------------------------

void PostgresCdcSource::snapshot_offset(StateBackend& backend,
                                        OperatorId op_id,
                                        CheckpointId ckpt) {
    auto& s = *impl_;
    const auto phase = s.phase == Impl::Phase::Snapshot ? cdc::Phase::Snapshot : cdc::Phase::Stream;
    cdc::CursorState state = s.cursor.state(phase);
    state.converge_until = s.converge_until.load();
    const auto bytes = cdc::encode_cursor(state);
    backend.put_operator_state(op_id,
                               StateBackend::KeyView{kCursorKey, std::strlen(kCursorKey)},
                               StateBackend::ValueView{bytes.data(), bytes.size()});
    if (s.opts.strict) {
        const std::string columns = declared_record(s.opts.tables);
        backend.put_operator_state(op_id,
                                   StateBackend::KeyView{kColumnsKey, std::strlen(kColumnsKey)},
                                   StateBackend::ValueView{columns.data(), columns.size()});
    }
    s.ledger.record(ckpt.value(), state.resume_lsn);
    CLINK_FAULT_POINT(cdc::points::kOffsetRecorded);
}

bool PostgresCdcSource::restore_offset(StateBackend& backend, OperatorId op_id) {
    auto& s = *impl_;
    if (auto v = backend.get_operator_state(
            op_id, StateBackend::KeyView{kCursorKey, std::strlen(kCursorKey)})) {
        std::vector<char> bytes;
        bytes.reserve(v->size());
        for (const std::byte b : *v) {
            bytes.push_back(static_cast<char>(b));
        }
        auto state = cdc::decode_cursor(bytes);
        if (!state) {
            throw std::runtime_error(
                "postgres_cdc: the checkpoint holds a source position this "
                "release cannot read (encoding version " +
                std::to_string(bytes.empty() ? 0 : static_cast<std::uint8_t>(bytes[0])) + ")");
        }
        s.restored = *state;
        if (auto c = backend.get_operator_state(
                op_id, StateBackend::KeyView{kColumnsKey, std::strlen(kColumnsKey)})) {
            std::string columns;
            columns.reserve(c->size());
            for (const std::byte b : *c) {
                columns.push_back(static_cast<char>(b));
            }
            s.restored_columns = std::move(columns);
        }
        seed_from_restore_();
        return true;
    }
    // A checkpoint from a release that recorded the received LSN alone. It is
    // a safe resume position (every transaction committed before it had been
    // received) but may sit inside a transaction, which is then replayed from
    // its start.
    auto v = backend.get_operator_state(
        op_id, StateBackend::KeyView{kLegacyLsnKey, std::strlen(kLegacyLsnKey)});
    if (!v.has_value() || v->size() < 8) {
        return false;
    }
    std::uint64_t restored = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        restored |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*v)[i])) << (i * 8);
    }
    cdc::CursorState state;
    state.phase = cdc::Phase::Stream;
    state.resume_lsn = restored;
    s.restored = state;
    seed_from_restore_();
    clink::log::info(kLog,
                     "restored a position recorded by an earlier release (" + format_lsn(restored) +
                         "); a transaction interrupted by that checkpoint is replayed whole");
    return true;
}

// The restored position is the position until open() moves on from it, so a
// checkpoint taken before then records it unchanged.
void PostgresCdcSource::seed_from_restore_() {
    auto& s = *impl_;
    const auto& r = *s.restored;
    std::optional<std::pair<std::uint64_t, std::uint64_t>> interrupted;
    if (r.in_flight) {
        interrupted = std::make_pair(r.txn_id, r.txn_changes);
    }
    s.cursor.start(r.resume_lsn, interrupted);
    s.phase = r.phase == cdc::Phase::Snapshot ? Impl::Phase::Snapshot : Impl::Phase::Stream;
    s.converge_until.store(r.converge_until);
}

void PostgresCdcSource::notify_checkpoint_complete(CheckpointId ckpt) {
    impl_->ledger.complete(ckpt.value());
}

void PostgresCdcSource::notify_checkpoint_aborted(CheckpointId ckpt) {
    impl_->ledger.abort(ckpt.value());
}

void PostgresCdcSource::drop_slot() {
    std::atomic<bool> never{false};
    ConnPtr admin = connect_cancellable(impl_->opts, false, never, "drop_slot");

    // Postgres briefly reports the slot as "in use" right after the
    // session that held it disconnects; retry a few times before giving
    // up. This is the same pattern Debezium uses for slot teardown.
    const std::string sql =
        "SELECT pg_drop_replication_slot(" + quote_literal(impl_->opts.slot_name) + ")";
    constexpr int attempts = 10;
    std::string last_err;
    for (int i = 0; i < attempts; ++i) {
        ResultPtr r{PQexec(admin.get(), sql.c_str())};
        const auto status = PQresultStatus(r.get());
        if (status == PGRES_TUPLES_OK || status == PGRES_COMMAND_OK) {
            return;
        }
        last_err = PQerrorMessage(admin.get());

        // Slot doesn't exist → treat as success (idempotent).
        if (last_err.find("does not exist") != std::string::npos) {
            return;
        }
        if (last_err.find("is active") == std::string::npos) {
            // Some other error - bail out, no point retrying.
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
    }
    throw std::runtime_error("PostgresCdcSource::drop_slot: " + last_err);
}

#else  // !CLINK_HAS_POSTGRES

struct PostgresCdcSource::Impl {};

bool PostgresCdcSource::is_real_implementation() {
    return false;
}

PostgresCdcSource::PostgresCdcSource(Options /*opts*/) {
    throw std::runtime_error(
        "PostgresCdcSource: built without libpq. Install postgresql or libpq "
        "and reconfigure with CLINK_WITH_POSTGRES=ON.");
}

PostgresCdcSource::~PostgresCdcSource() = default;
void PostgresCdcSource::open() {}
bool PostgresCdcSource::produce(Emitter<CdcEvent>& /*out*/) {
    return false;
}
void PostgresCdcSource::cancel() {
    Source<CdcEvent>::cancel();
}
std::string PostgresCdcSource::start_position() const {
    return {};
}
bool PostgresCdcSource::snapshot_in_progress() const noexcept {
    return false;
}
std::uint64_t PostgresCdcSource::confirmed_lsn() const noexcept {
    return 0;
}
std::uint64_t PostgresCdcSource::converging_until() const noexcept {
    return 0;
}
std::vector<std::string> PostgresCdcSource::captured_tables() const {
    return {};
}
void PostgresCdcSource::close() {}
void PostgresCdcSource::drop_slot() {}
void PostgresCdcSource::snapshot_offset(StateBackend&, OperatorId, CheckpointId) {}
bool PostgresCdcSource::restore_offset(StateBackend&, OperatorId) {
    return false;
}
void PostgresCdcSource::notify_checkpoint_complete(CheckpointId) {}
void PostgresCdcSource::notify_checkpoint_aborted(CheckpointId) {}

#endif

}  // namespace clink
