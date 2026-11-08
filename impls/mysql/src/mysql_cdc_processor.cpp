// The MySQL CDC source's event processing: table maps and the definitions
// rows are decoded with, transaction boundaries, row decoding, DDL on captured
// tables, and the decode-error policy. See mysql_cdc_processor.hpp.

#include "clink/mysql/mysql_cdc_processor.hpp"

#include <utility>

#include "clink/connectors/cdc_json.hpp"

namespace clink::mysql {

namespace {

constexpr int kXaPrepareEvent = 38;
constexpr int kTransactionPayloadEvent = 40;
constexpr std::uint8_t kRealTypeEnum = 247;
constexpr std::uint8_t kRealTypeSet = 248;
constexpr std::uint8_t kStringType = 254;
constexpr std::uint8_t kYearType = 13;

std::string hex(const std::uint8_t* p, std::size_t n) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out.push_back(kDigits[p[i] >> 4]);
        out.push_back(kDigits[p[i] & 0x0F]);
    }
    return out;
}

// Every one of the first `n` bits set.
bool all_bits(const std::vector<std::uint8_t>& bitmap, std::size_t n) {
    if (bitmap.size() < (n + 7) / 8) {
        return false;
    }
    for (std::size_t i = 0; i < n; ++i) {
        if ((bitmap[i / 8] & (1U << (i % 8))) == 0) {
            return false;
        }
    }
    return true;
}

// The binlog types the SIGNEDNESS bitmap gives a bit to, as MySQL's
// is_numeric_type defines them. YEAR takes one (always set: it is stored
// unsigned), which the live suite checks against MySQL 8.0 and 8.4 by
// decoding unsigned columns that follow a YEAR.
bool numeric_type(std::uint8_t t) {
    switch (t) {
        case 1:    // TINY
        case 2:    // SHORT
        case 9:    // INT24
        case 3:    // LONG
        case 8:    // LONGLONG
        case 4:    // FLOAT
        case 5:    // DOUBLE
        case 0:    // DECIMAL
        case 246:  // NEWDECIMAL
        case kYearType:
            return true;
        default:
            return false;
    }
}

// The real type of a column: ENUM and SET travel as STRING with the real type
// in the first metadata byte.
std::uint8_t real_type(const ColumnMeta& m) {
    if (m.type == kStringType && (m.m0 == kRealTypeEnum || m.m0 == kRealTypeSet)) {
        return m.m0;
    }
    return m.type;
}

RowOp row_op(int type) {
    switch (type) {
        case 24:
        case 31:
        case 167:
        case 170:
            return RowOp::Update;
        case 25:
        case 32:
        case 168:
        case 171:
            return RowOp::Delete;
        default:
            return RowOp::Insert;
    }
}

}  // namespace

const char* decode_fault_name(DecodeFault f) {
    switch (f) {
        case DecodeFault::UnsupportedEvent:
            return "unsupported event";
        case DecodeFault::CorruptPayload:
            return "corrupt row payload";
        case DecodeFault::MissingTableMap:
            return "row event without its TABLE_MAP";
        case DecodeFault::SchemaMismatch:
            return "schema mismatch";
        case DecodeFault::PartialRowImage:
            return "partial row image";
        case DecodeFault::UnsupportedStatement:
            return "change not logged as rows";
    }
    return "decode fault";
}

BinlogEventProcessor::BinlogEventProcessor(ProcessorOptions opts) : opts_(std::move(opts)) {
    for (const auto& t : opts_.tables) {
        filter_.insert(t);
    }
}

void BinlogEventProcessor::start(std::string file,
                                 std::uint64_t pos,
                                 const std::map<std::string, std::string>& definitions,
                                 std::string unrecorded) {
    file_ = std::move(file);
    pos_ = pos;
    committed_file_ = file_;
    committed_pos_ = pos_;
    tracker_ = TransactionTracker{};
    tables_.clear();
    defs_.clear();
    for (const auto& [key, text] : definitions) {
        Definition d;
        if (!text.empty() && text.front() == kUnknownDefinition) {
            d.unknown = text.substr(1);
        } else if (auto cols = parse_schema_fingerprint(text)) {
            d.columns = std::move(*cols);
        } else {
            d.unknown = "its recorded definition is malformed";
        }
        defs_.emplace(key, std::move(d));
    }
    unrecorded_ = std::move(unrecorded);
    all_unknown_.clear();
    xa_group_ = false;
}

bool BinlogEventProcessor::captured(const std::string& db, const std::string& table) const {
    return filter_.empty() || filter_.count(db + "." + table) > 0 || filter_.count(table) > 0;
}

std::string BinlogEventProcessor::location() const {
    return file_ + ":" + std::to_string(pos_);
}

void BinlogEventProcessor::fault(DecodeFault f,
                                 const std::string& why,
                                 const std::string& table,
                                 std::string payload) {
    if (opts_.policy == MysqlDecodeErrorPolicy::Fail) {
        throw MysqlCdcDecodeError(
            std::string{"mysql_cdc: "} + decode_fault_name(f) + " at " + location() +
                (table.empty() ? std::string{} : " in " + table) + ": " + why +
                ". on_decode_error='fail' stops here; on_decode_error='drop' would skip the event "
                "and lose its changes",
            f);
    }
    dropped_.push_back(DroppedEvent{f, why, table, std::move(payload), location()});
}

void BinlogEventProcessor::fatal(const std::string& why) const {
    throw MysqlCdcDecodeError("mysql_cdc: " + why + " (at " + location() + ")", std::nullopt);
}

std::vector<DroppedEvent> BinlogEventProcessor::take_dropped() {
    std::vector<DroppedEvent> out;
    out.swap(dropped_);
    return out;
}

std::map<std::string, std::string> BinlogEventProcessor::definitions() const {
    std::map<std::string, std::string> out;
    for (const auto& [key, d] : defs_) {
        if (!d.columns.empty()) {
            out.emplace(key, schema_fingerprint(d.columns));
        }
    }
    // An unknown definition is recorded only for its reason: a table the
    // checkpoint does not list is unknown after a restore as well. So when the
    // row's limit is near, the reasons go first.
    for (const auto& [key, d] : defs_) {
        if (d.columns.empty() && out.size() < kMaxCheckpointDefinitions) {
            out.emplace(key, std::string(1, kUnknownDefinition) + d.unknown);
        }
    }
    return out;
}

const std::string* BinlogEventProcessor::unknown_definition(const std::string& key) const {
    if (!all_unknown_.empty()) {
        return &all_unknown_;
    }
    const auto it = defs_.find(key);
    if (it == defs_.end()) {
        return &unrecorded_;
    }
    return it->second.columns.empty() ? &it->second.unknown : nullptr;
}

void BinlogEventProcessor::forget(const std::string& key, const std::string& why) {
    auto& d = defs_[key];
    d.columns.clear();
    d.unknown = why;
}

void BinlogEventProcessor::mark_boundary() {
    if (!tracker_.open()) {
        committed_file_ = file_;
        committed_pos_ = pos_;
    }
}

void BinlogEventProcessor::process(const BinlogEvent& ev, std::vector<std::string>& out) {
    std::string why;
    const auto cls = classify_binlog_event(ev.type, &why);
    if (cls == BinlogEventClass::Rotate) {
        if (!ev.rotate_file.empty()) {
            file_ = ev.rotate_file;
        }
        pos_ = ev.rotate_pos > 0 ? ev.rotate_pos : 4;
    } else if (ev.next_pos > 0) {
        pos_ = ev.next_pos;
    }
    switch (cls) {
        case BinlogEventClass::TransactionStart:
            tracker_.on_gtid(ev.mariadb_gtid && !ev.gtid_standalone);
            xid_ = ev.gtid_seq;
            break;
        case BinlogEventClass::Query:
            on_query(ev);
            break;
        case BinlogEventClass::Xid:
            tracker_.on_end();
            xa_group_ = false;
            break;
        case BinlogEventClass::TableMap:
            on_table_map(ev);
            break;
        case BinlogEventClass::Rows:
            on_rows(ev, out);
            break;
        case BinlogEventClass::Rotate:
        case BinlogEventClass::Heartbeat:
        case BinlogEventClass::Stop:
        case BinlogEventClass::Informational:
            break;
        case BinlogEventClass::Unsupported:
            if (xa_group_ && ev.type == kXaPrepareEvent) {
                // The XA transaction's group ends at its prepare; a row change
                // inside it was a fault when it came.
                tracker_.on_end();
                xa_group_ = false;
                break;
            }
            fault(DecodeFault::UnsupportedEvent,
                  why + " (event type " + std::to_string(ev.type) + ")",
                  "",
                  "");
            // A compressed payload holds a whole transaction, its commit
            // included, and closes the group its GTID event opened.
            if (ev.type == kTransactionPayloadEvent) {
                tracker_.on_end();
            }
            break;
        case BinlogEventClass::Fatal:
            fatal(why);
    }
    mark_boundary();
}

void BinlogEventProcessor::on_table_map(const BinlogEvent& ev) {
    tracker_.on_rows();
    const auto key = ev.db + "." + ev.table;
    TableState st;
    st.filtered = !captured(ev.db, ev.table);
    if (st.filtered) {
        tables_[ev.table_id] = std::move(st);
        return;
    }
    st.metas = parse_table_metadata(ev.column_types.data(),
                                    static_cast<std::uint32_t>(ev.column_types.size()),
                                    ev.metadata.data(),
                                    ev.metadata.size());
    st.schema.db = ev.db;
    st.schema.table = ev.table;
    const auto n = st.metas.size();
    // Signedness from the TABLE_MAP itself, when it carries the bitmap.
    std::vector<std::optional<bool>> event_unsigned(n);
    if (!ev.signedness.empty()) {
        std::size_t bit = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (!numeric_type(st.metas[i].type)) {
                continue;
            }
            if (bit / 8 < ev.signedness.size()) {
                event_unsigned[i] = (ev.signedness[bit / 8] & (0x80U >> (bit % 8))) != 0;
            }
            ++bit;
        }
    }
    const auto unusable = [&](const std::string& why) {
        fault(DecodeFault::SchemaMismatch, why, key, "");
        st.unusable = why;
        tables_[ev.table_id] = std::move(st);
    };
    const auto def_it = defs_.find(key);
    const Definition* def =
        unknown_definition(key) == nullptr && def_it != defs_.end() ? &def_it->second : nullptr;

    if (!ev.column_names.empty()) {
        // binlog_row_metadata=FULL: the event names its columns and lists its
        // ENUM and SET members, so its rows decode with the definition they
        // were written with.
        if (ev.column_names.size() != n) {
            unusable("its TABLE_MAP names " + std::to_string(ev.column_names.size()) +
                     " columns and has " + std::to_string(n));
            return;
        }
        std::size_t next_enum = 0;
        std::size_t next_set = 0;
        for (std::size_t i = 0; i < n; ++i) {
            CdcColumn c;
            c.name = ev.column_names[i];
            const auto rt = real_type(st.metas[i]);
            if (rt == kRealTypeEnum || rt == kRealTypeSet) {
                const auto& lists = rt == kRealTypeEnum ? ev.enum_values : ev.set_values;
                auto& next = rt == kRealTypeEnum ? next_enum : next_set;
                if (!lists || next >= lists->size()) {
                    unusable(
                        "its TABLE_MAP names its columns but not the members of ENUM or SET "
                        "column " +
                        c.name);
                    return;
                }
                c.enum_set_labels = (*lists)[next++];
            }
            if (event_unsigned[i]) {
                c.is_unsigned = *event_unsigned[i];
            } else if (numeric_type(st.metas[i].type)) {
                // No bitmap: the definition in force, when it names this column here.
                if (def == nullptr || i >= def->columns.size() || def->columns[i].name != c.name) {
                    unusable("its TABLE_MAP gives no signedness for numeric column " + c.name +
                             ", and the source knows no definition naming it there");
                    return;
                }
                c.is_unsigned = def->columns[i].is_unsigned;
            }
            st.schema.columns.push_back(std::move(c));
        }
        tables_[ev.table_id] = std::move(st);
        return;
    }

    // binlog_row_metadata=MINIMAL: the definition in force at this coordinate.
    if (def == nullptr) {
        const std::string* why = unknown_definition(key);
        fatal("a TABLE_MAP of captured table " + key +
              " carries no column names (binlog_row_metadata=MINIMAL), and the source does not "
              "know the table's definition here: " +
              (why != nullptr ? *why : std::string{"no definition was recorded for it"}) +
              ". information_schema describes the table as it is now, not as it was when the "
              "event was written, so the source does not decode the event with it. Restart the "
              "source from a new snapshot; with binlog_row_metadata=FULL on the server every "
              "event carries its own definition");
    }
    const auto& cols = def->columns;
    if (cols.size() != n) {
        unusable("the binlog writes " + std::to_string(n) +
                 " columns and the definition in force has " + std::to_string(cols.size()));
        return;
    }
    for (std::size_t i = 0; i < n; ++i) {
        if (!binlog_type_matches(st.metas[i].type, st.metas[i].m0, cols[i].data_type)) {
            unusable("column " + std::to_string(i + 1) + " (" + cols[i].name + ") is " +
                     cols[i].data_type +
                     " in the definition in force but another type in the binlog (binlog type " +
                     std::to_string(st.metas[i].type) + ")");
            return;
        }
        // YEAR's bit is always set and says nothing a decode needs.
        if (event_unsigned[i] && st.metas[i].type != kYearType &&
            *event_unsigned[i] != cols[i].is_unsigned) {
            unusable("column " + cols[i].name + " is " +
                     (cols[i].is_unsigned ? "unsigned" : "signed") +
                     " in the definition in force but " +
                     (*event_unsigned[i] ? "unsigned" : "signed") + " in the binlog");
            return;
        }
    }
    for (const auto& spec : cols) {
        CdcColumn c;
        c.name = spec.name;
        c.is_unsigned = spec.is_unsigned;
        c.enum_set_labels = spec.labels;
        st.schema.columns.push_back(std::move(c));
    }
    tables_[ev.table_id] = std::move(st);
}

void BinlogEventProcessor::on_rows(const BinlogEvent& ev, std::vector<std::string>& out) {
    tracker_.on_rows();
    const auto payload = [&] { return hex(ev.row_data, ev.row_data_size); };
    const auto it = tables_.find(ev.table_id);
    if (it == tables_.end()) {
        fault(DecodeFault::MissingTableMap,
              "a row event for table id " + std::to_string(ev.table_id) +
                  " with no TABLE_MAP before it in this stream",
              "",
              payload());
        return;
    }
    auto& st = it->second;
    const auto key = st.schema.qualified();
    if (st.filtered) {
        ++filtered_;
        return;
    }
    if (xa_group_) {
        fault(DecodeFault::UnsupportedStatement,
              "a row change inside an XA transaction: XA PREPARE logs it before the transaction "
              "commits or rolls back, so it cannot be passed on as a change that happened",
              key,
              payload());
        return;
    }
    if (!st.unusable.empty()) {
        fault(DecodeFault::SchemaMismatch, st.unusable, key, payload());
        return;
    }
    const auto n = st.metas.size();
    if (ev.column_count != n) {
        fault(DecodeFault::CorruptPayload,
              "the row event has " + std::to_string(ev.column_count) +
                  " columns and its TABLE_MAP " + std::to_string(n),
              key,
              payload());
        return;
    }
    const auto partial = [&] {
        fault(DecodeFault::PartialRowImage,
              "the row event does not carry every column (binlog_row_image is not FULL for the "
              "session that wrote it), so its rows cannot be passed on whole",
              key,
              payload());
    };
    if (!all_bits(ev.present_columns, n)) {
        partial();
        return;
    }
    const auto op = row_op(ev.type);
    const bool v1_update = is_v1_update_rows_event(ev.type);
    if (op == RowOp::Update) {
        std::vector<std::uint8_t> after;
        if (v1_update) {
            after = ev.update_present_columns;
        } else {
            const auto bytes = (n + 7) / 8;
            if (ev.row_data_size < bytes) {
                fault(DecodeFault::CorruptPayload,
                      "the update ends inside its after-image column bitmap",
                      key,
                      payload());
                return;
            }
            after.assign(ev.row_data, ev.row_data + bytes);
        }
        if (!all_bits(after, n)) {
            partial();
            return;
        }
    }
    if (ev.row_data == nullptr || ev.row_data_size == 0) {
        return;  // no rows, so no change
    }
    auto res = decode_rows_payload(ev.row_data,
                                   ev.row_data_size,
                                   op,
                                   st.metas,
                                   st.schema,
                                   location(),
                                   xid_,
                                   /*skip_update_bitmap=*/op == RowOp::Update && !v1_update);
    if (res.dropped > 0 || res.events.empty()) {
        fault(DecodeFault::CorruptPayload,
              "the event's row payload does not decode with its TABLE_MAP (" +
                  std::to_string(res.events.size()) +
                  " row(s) decoded before it failed); none of the event's rows is passed on",
              key,
              payload());
        return;
    }
    for (const auto& cev : res.events) {
        if (auto js = clink::cdc::cdc_event_to_json_row(cev)) {
            out.push_back(std::move(*js));
        }
    }
}

void BinlogEventProcessor::on_query(const BinlogEvent& ev) {
    using K = QueryStatement::Kind;
    const auto q = classify_query(ev.query, ev.query_db);
    switch (q.kind) {
        case K::Begin:
            tracker_.on_begin();
            return;
        case K::Commit:
        case K::Rollback:
            tracker_.on_end();
            xa_group_ = false;
            return;
        case K::InTransaction:
            (void)tracker_.on_statement();
            return;
        case K::XaStart:
            // Its rows are faults when they come (on_rows), so an XA transaction
            // that changes no captured table passes.
            xa_group_ = true;
            tracker_.on_begin();
            return;
        case K::Ddl:
            on_ddl(q, ev);
            (void)tracker_.on_statement();
            return;
        case K::Dml: {
            std::string hit;
            if (!q.targets_known) {
                hit = "(tables the source could not name)";
            }
            for (const auto& t : q.targets) {
                if (captured(t.db, t.table)) {
                    hit = t.db + "." + t.table;
                    break;
                }
            }
            if (!hit.empty()) {
                fault(DecodeFault::UnsupportedStatement,
                      "a statement-logged change (binlog_format is not ROW for the session that "
                      "made it), whose rows are not in the binlog",
                      hit,
                      ev.query);
            }
            (void)tracker_.on_statement();
            return;
        }
        case K::Other:
            (void)tracker_.on_statement();
            return;
    }
}

void BinlogEventProcessor::on_ddl(const QueryStatement& q, const BinlogEvent& ev) {
    using E = QueryStatement::Effect;
    const auto at = location();
    for (const auto& t : q.targets) {
        if (t.effect == E::DropDatabase) {
            bool hit = filter_.empty();
            for (const auto& entry : filter_) {
                hit =
                    hit || entry.rfind(t.db + ".", 0) == 0 || entry.find('.') == std::string::npos;
            }
            if (hit) {
                fault(DecodeFault::UnsupportedStatement,
                      "DROP DATABASE removes the rows of captured tables without row events",
                      t.db + ".*",
                      ev.query);
                for (auto& [key, d] : defs_) {
                    if (key.rfind(t.db + ".", 0) == 0) {
                        d.columns.clear();
                        d.unknown = "its database was dropped at " + at;
                    }
                }
            }
            continue;
        }
        if (!captured(t.db, t.table)) {
            continue;
        }
        const auto key = t.db + "." + t.table;
        switch (t.effect) {
            case E::Truncate:
                fault(DecodeFault::UnsupportedStatement,
                      "rows were removed or replaced without row events (TRUNCATE, or a "
                      "partition or tablespace dropped, truncated, exchanged, discarded or "
                      "imported), so the changes cannot be passed on",
                      key,
                      ev.query);
                break;
            case E::Drop:
                fault(DecodeFault::UnsupportedStatement,
                      "DROP TABLE removes the table's rows without row events",
                      key,
                      ev.query);
                forget(key, "it was dropped at " + at);
                break;
            case E::Rename:
                fault(DecodeFault::UnsupportedStatement,
                      "the table was renamed away: its rows leave the captured name without row "
                      "events",
                      key,
                      ev.query);
                forget(key, "it was renamed away at " + at);
                break;
            case E::Create:
                forget(key, "it was created, or renamed into its name, at " + at);
                break;
            case E::Alter:
                if (t.columns) {
                    forget(key, "a DDL statement at " + at + " may have changed its columns");
                }
                break;
            case E::Write:
            case E::DropDatabase:
                break;
        }
    }
    if (!q.targets_known) {
        all_unknown_ = "a DDL statement at " + at + " changed tables the source could not name";
    }
}

}  // namespace clink::mysql
