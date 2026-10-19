#include "clink/sql/cdc_keep_rows.hpp"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/config/interned_name.hpp"
#include "clink/config/json.hpp"
#include "clink/core/codec.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/sql/row_kind.hpp"

namespace clink::sql {

namespace {

constexpr const char* kLog = "sql.cdc_keep_rows";

// Rows per emitted batch while a truncate turns every held row into a delete.
constexpr std::size_t kEmitChunk = 1024;

// The CDC contract's change metadata. A delete in append mode takes these
// from the delete, and everything else from the row it removed.
constexpr std::array<std::string_view, 6> kMetadata = {
    "__op", "__lsn", "__version", "__deleted", "__table", "__xid"};

std::uint64_t version_of(const Row& row) {
    const auto it = row.values.find(kCdcVersionField);
    if (it == row.values.end() || !it->second.is_number()) {
        return 0;
    }
    const auto v = it->second.as_int();
    return v < 0 ? 0 : static_cast<std::uint64_t>(v);
}

// The row as it is held and emitted: without the stage's input fields and
// without a changelog kind.
Row image_of(const Row& in) {
    Row out = in;
    for (const auto name : {kCdcKindField,
                            kCdcVersionField,
                            kCdcUnchangedField,
                            kCdcConvergingField,
                            kRowKindField}) {
        out.values.erase(std::string{name});
    }
    return out;
}

std::string text_of(const Row& row) {
    std::string s;
    clink::config::JsonValue{to_json_object(row.values)}.serialize_kind_preserving_into(s);
    return s;
}

}  // namespace

CdcKeepRowsOperator::CdcKeepRowsOperator(std::vector<std::string> key_columns,
                                         Mode mode,
                                         bool emit_unheld_deletes)
    : key_columns_(std::move(key_columns)), mode_(mode), emit_unheld_deletes_(emit_unheld_deletes) {
    if (key_columns_.empty()) {
        throw std::runtime_error(
            "cdc_keep_rows_row: 'key_columns' is required: the rows are kept by the table's "
            "primary key");
    }
}

KeyedState<std::string, Row> CdcKeepRowsOperator::rows_() {
    return this->runtime()->template keyed_state<std::string, Row>(
        "cdc_rows", clink::string_codec(), row_json_codec());
}

// Built like the GROUP BY key: each key column's JSON form joined on '\x1f'.
std::string CdcKeepRowsOperator::key_of_(const Row& row) const {
    std::string key;
    for (std::size_t i = 0; i < key_columns_.size(); ++i) {
        if (i > 0) {
            key += '\x1f';
        }
        const auto it = row.values.find(key_columns_[i]);
        if (it != row.values.end() && !it->second.is_null()) {
            it->second.serialize_into(key);
        }
    }
    return key;
}

void CdcKeepRowsOperator::process(const StreamElement<Row>& element, Emitter<Row>& out) {
    if (element.is_watermark()) {
        this->on_watermark(element.as_watermark(), out);
        return;
    }
    if (!element.is_data()) {
        this->on_barrier(element.as_barrier(), out);
        return;
    }
    auto kv = rows_();
    Batch<Row> batch;
    const auto flush = [&] {
        if (!batch.empty()) {
            out.emit_data(std::move(batch));
            batch = Batch<Row>{};
        }
    };
    for (const auto& rec : element.as_data()) {
        const auto kind = rec.value().values.find(kCdcKindField);
        if (kind != rec.value().values.end() && kind->second.is_string() &&
            kind->second.as_string() == "truncate") {
            flush();
            truncate_(rec, kv, out);
            continue;
        }
        apply_(rec, kv, batch);
        if (batch.size() >= kEmitChunk) {
            flush();
        }
    }
    flush();
}

void CdcKeepRowsOperator::apply_(const Record<Row>& rec,
                                 KeyedState<std::string, Row>& kv,
                                 Batch<Row>& out) {
    const Row& in = rec.value();
    const auto kind_it = in.values.find(kCdcKindField);
    if (kind_it == in.values.end() || !kind_it->second.is_string()) {
        throw std::runtime_error(
            "cdc_keep_rows_row: a row without __cdc_kind reached the stage, which keeps rows "
            "only for a CDC table declared with keep_rows='true'");
    }
    const std::string& kind = kind_it->second.as_string();
    const std::uint64_t version = version_of(in);
    const auto emit = [&](Row row, std::string_view row_kind) {
        if (mode_ == Mode::Upsert) {
            set_row_kind(row, row_kind);
        }
        if (const auto ts = rec.event_time()) {
            out.emplace(std::move(row), *ts);
        } else {
            out.emplace(std::move(row));
        }
    };

    const std::string key = key_of_(in);
    std::optional<Row> held = kv.get(key);
    const std::uint64_t held_version = held ? version_of(*held) : 0;
    if (held && version > 0 && held_version > 0 && version <= held_version) {
        // Only a replay delivers a change no newer than the row it produced.
        ++stale_;
        if (!warned_stale_) {
            warned_stale_ = true;
            clink::log::warn(kLog,
                             "dropped a change at WAL position " + std::to_string(version) +
                                 " that is no newer than the row held for its key (" +
                                 std::to_string(held_version) +
                                 "); later ones are counted, not logged");
        }
        return;
    }

    if (kind == "delete") {
        if (!held) {
            moved_.reset();
            if (!emit_unheld_deletes_) {
                ++unheld_dropped_;
                return;
            }
            emit(image_of(in), kRowKindDelete);
            return;
        }
        Row old = image_of(*held);
        Row gone = old;
        if (mode_ == Mode::Append) {
            for (const auto name : kMetadata) {
                if (const auto it = in.values.find(name); it != in.values.end()) {
                    gone.values[clink::config::InternedName{name}] = it->second;
                }
            }
        }
        kv.erase(key);
        moved_ = std::make_pair(version, std::move(old));
        emit(std::move(gone), kRowKindDelete);
        return;
    }
    if (kind != "snapshot" && kind != "insert" && kind != "update") {
        throw std::runtime_error("cdc_keep_rows_row: unknown __cdc_kind '" + kind + "'");
    }
    if (kind == "update" && !held) {
        // Before a fresh snapshot's position, an update of a row the snapshot
        // does not have is of a row deleted before it was read; that delete
        // follows.
        const auto c = in.values.find(kCdcConvergingField);
        if (c != in.values.end() && c->second.is_bool() && c->second.as_bool()) {
            moved_.reset();
            ++converging_skips_;
            return;
        }
    }

    Row next = image_of(in);
    if (const auto u = in.values.find(kCdcUnchangedField);
        u != in.values.end() && u->second.is_array() && !u->second.as_array().empty()) {
        // An UPDATE left these out-of-line values unchanged without carrying
        // them: they are the held row's, or the old key's when the same change
        // moved the row to this key.
        const Row* from = nullptr;
        if (held) {
            from = &*held;
        } else if (moved_ && moved_->first == version) {
            from = &moved_->second;
        }
        if (from == nullptr) {
            std::string named;
            for (const auto& k : key_columns_) {
                named += (named.empty() ? "" : ", ") + k + "=" +
                         in.get_string(k).value_or(std::string{"NULL"});
            }
            throw std::runtime_error(
                "cdc_keep_rows_row: the change at WAL position " + std::to_string(version) +
                " to the row (" + named +
                ") left a column unchanged and stored out of line without carrying it, and the "
                "job holds no row for that key to take it from. The row reached the table in a "
                "way the change stream does not carry (a partition attached, rows restored "
                "outside the slot); capture the table again from a new slot");
        }
        for (const auto& c : u->second.as_array()) {
            if (!c.is_string()) {
                continue;
            }
            const auto it = from->values.find(c.as_string());
            next.values[clink::config::InternedName{c.as_string()}] =
                it != from->values.end() ? it->second : clink::config::JsonValue{};
        }
    }
    moved_.reset();

    Row stored = next;
    stored.values[clink::config::InternedName{kCdcVersionField}] =
        clink::config::JsonValue{static_cast<std::int64_t>(std::max(version, held_version))};
    if (mode_ == Mode::Upsert) {
        if (held) {
            Row old = image_of(*held);
            if (text_of(old) != text_of(next)) {
                emit(std::move(old), kRowKindUpdateBefore);
                emit(std::move(next), kRowKindUpdateAfter);
            }
        } else {
            emit(std::move(next), kRowKindInsert);
        }
    } else {
        emit(std::move(next), kRowKindInsert);
    }
    kv.put(key, stored);
}

void CdcKeepRowsOperator::truncate_(const Record<Row>& rec,
                                    KeyedState<std::string, Row>& kv,
                                    Emitter<Row>& out) {
    const Row& in = rec.value();
    const std::uint64_t version = version_of(in);
    Batch<Row> batch;
    std::vector<std::string> removed;
    std::uint64_t kept = 0;
    kv.scan([&](const std::string& key, const Row& row) {
        // A row changed after the truncate is only met in a replay; it stays.
        if (const auto v = version_of(row); v > 0 && version > 0 && v >= version) {
            ++kept;
            return;
        }
        removed.push_back(key);
        Row gone = image_of(row);
        if (mode_ == Mode::Append) {
            for (const auto name : kMetadata) {
                if (const auto it = in.values.find(name); it != in.values.end()) {
                    gone.values[clink::config::InternedName{name}] = it->second;
                }
            }
        } else {
            set_row_kind(gone, kRowKindDelete);
        }
        if (const auto ts = rec.event_time()) {
            batch.emplace(std::move(gone), *ts);
        } else {
            batch.emplace(std::move(gone));
        }
        if (batch.size() >= kEmitChunk) {
            out.emit_data(std::move(batch));
            batch = Batch<Row>{};
        }
    });
    if (!batch.empty()) {
        out.emit_data(std::move(batch));
    }
    for (const auto& key : removed) {
        kv.erase(key);
    }
    moved_.reset();
    clink::log::info(
        kLog,
        "TRUNCATE at WAL position " + std::to_string(version) + ": deleted " +
            std::to_string(removed.size()) + " rows" +
            (kept > 0 ? ", kept " + std::to_string(kept) + " changed after it" : std::string{}));
}

}  // namespace clink::sql
