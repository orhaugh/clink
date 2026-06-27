// Postgres factory registrations.
//
// Contains:
//   * StringPostgresCdcSource - adapter that serialises each CdcEvent
//     as a JSON line so it's addressable on the "string" channel.
//   * StringPostgresSource    - adapter that joins each row's column
//     values with a configurable delimiter.
//   * clink::postgres::install() - registers postgres_text_source
//     and postgres_cdc_text_source with the supplied PluginRegistry.
//     Callers invoke explicitly after ensure_built_ins_registered().

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/cdc_json.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_row.hpp"
#include "clink/connectors/postgres_source.hpp"
#include "clink/connectors/postgres_sql.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/postgres/cdc_event_codec.hpp"
#include "clink/postgres/install.hpp"
#include "clink/postgres/postgres_row_codec.hpp"

namespace clink::postgres {

namespace {

// Split a comma-separated option value, trimming spaces, dropping empties.
std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::size_t i = 0;
    while (i < s.size()) {
        std::size_t j = s.find(',', i);
        if (j == std::string::npos) {
            j = s.size();
        }
        std::size_t b = i;
        std::size_t e = j;
        while (b < e && s[b] == ' ') {
            ++b;
        }
        while (e > b && s[e - 1] == ' ') {
            --e;
        }
        if (e > b) {
            out.push_back(s.substr(b, e - b));
        }
        i = j + 1;
    }
    return out;
}

// Adapter that turns a PostgresCdcSource into a Source<std::string>,
// serialising each CdcEvent as a JSON-shaped line:
//   {"op":"insert","table":"public.users","lsn":"0/16E2A38","xid":42,
//    "values":{"id":"1","name":"alice"}}
// Sinks downstream can parse the JSON or do regex-based extraction; the
// goal here is to make the CDC stream consumable through the same
// string-channel submission path as kafka_text / file_text.
class StringPostgresCdcSource final : public Source<std::string> {
public:
    // json=false: the nested {"op","table","lsn","xid","values":{...}} line
    //   (postgres_cdc_text_source) - back-compat.
    // json=true:  M5 Row-path emission - one FLAT JSON object per data-change
    //   event (insert/update/delete) with the changed columns at the top level
    //   (so json_string_to_row maps them by name) plus CDC metadata under reserved
    //   __op / __table / __lsn / __xid keys. begin/commit/truncate/unknown are
    //   skipped (transaction markers carry no row to map).
    explicit StringPostgresCdcSource(PostgresCdcSource::Options opts, bool json = false)
        : inner_(std::move(opts)), json_(json) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override {
        Source<std::string>::cancel();
        inner_.cancel();
    }

    bool produce(Emitter<std::string>& out) override {
        const bool json = json_;
        Emitter<CdcEvent> forwarder(
            Emitter<CdcEvent>::Forward([&out, json](StreamElement<CdcEvent> e) -> bool {
                if (e.is_data()) {
                    Batch<std::string> b;
                    for (const auto& r : e.as_data()) {
                        if (!json) {
                            b.emplace(serialize(r.value()));
                            continue;
                        }
                        // Row path: flat JSON per data-change event; skip markers.
                        if (auto row = clink::pgcdc::cdc_event_to_json_row(r.value())) {
                            b.emplace(std::move(*row));
                        }
                    }
                    return out.emit_data(std::move(b));
                }
                if (e.is_watermark()) {
                    return out.emit_watermark(e.as_watermark());
                }
                return out.emit_barrier(e.as_barrier());
            }));
        return inner_.produce(forwarder);
    }

    // #57: forward source-replay to the inner source so the string/SQL path
    // inherits whatever offset replay the inner connector implements (symmetry
    // with the data path; the inner CDC LSN replay is a connector follow-up).
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override {
        inner_.snapshot_offset(backend, op_id, ckpt_id);
    }
    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        return inner_.restore_offset(backend, op_id);
    }

    std::string name() const override {
        return json_ ? "postgres_cdc_source" : "postgres_cdc_text_source";
    }

private:
    static std::string escape(std::string_view s) {
        std::string out;
        out.reserve(s.size() + 2);
        out += '"';
        for (char c : s) {
            switch (c) {
                case '"':
                    out += "\\\"";
                    break;
                case '\\':
                    out += "\\\\";
                    break;
                case '\n':
                    out += "\\n";
                    break;
                case '\r':
                    out += "\\r";
                    break;
                case '\t':
                    out += "\\t";
                    break;
                default:
                    out += c;
            }
        }
        out += '"';
        return out;
    }

    static std::string serialize(const CdcEvent& ev) {
        std::string out;
        out += "{\"op\":";
        out += escape(clink::pgcdc::cdc_op_name(ev.op));
        out += ",\"table\":";
        out += escape(ev.table);
        out += ",\"lsn\":";
        out += escape(ev.lsn);
        out += ",\"xid\":";
        out += std::to_string(ev.xid);
        out += ",\"values\":{";
        for (std::size_t i = 0; i < ev.values.size(); ++i) {
            if (i > 0) {
                out += ',';
            }
            out += escape(ev.values[i].name);
            out += ':';
            if (ev.values[i].is_null) {
                out += "null";
            } else {
                out += escape(ev.values[i].value);
            }
        }
        out += "}}";
        return out;
    }

    PostgresCdcSource inner_;
    bool json_{false};
};

// Render a PostgresRow as a single-line JSON object keyed by column name (M3),
// so json_string_to_row can map it to a multi-column Row table. Values are text
// (the bridge coerces per the declared column types); a SQL NULL cell emits JSON
// null (M5: PostgresRow::is_null, populated from PQgetisnull). Column names are
// always populated by the source (PQfname); if they are ever absent we fail loud
// rather than emit positional keys that the by-name bridge would silently map to
// all-NULL.
std::string postgres_row_to_json(const PostgresRow& r) {
    const auto& vs = r.values();
    const auto names = r.column_names();
    if (!names || names->size() != vs.size()) {
        throw std::runtime_error("postgres_source: column names unavailable for row->JSON");
    }
    clink::config::JsonObject obj;
    for (std::size_t i = 0; i < vs.size(); ++i) {
        obj[(*names)[i]] =
            r.is_null(i) ? clink::config::JsonValue{} : clink::config::JsonValue{vs[i]};
    }
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

// Adapter that turns a PostgresSource into a Source<std::string>. json=false:
// legacy delimiter-joined string (postgres_text_source). json=true: a JSON object
// keyed by column name (postgres_source, M3) for the Row path.
class StringPostgresSource final : public Source<std::string> {
public:
    StringPostgresSource(PostgresSource::Options opts, std::string delim, bool json = false)
        : inner_(std::move(opts)), delim_(std::move(delim)), json_(json) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override { Source<std::string>::cancel(); }

    bool produce(Emitter<std::string>& out) override {
        const auto& delim = delim_;
        const bool json = json_;
        Emitter<PostgresRow> forwarder(Emitter<PostgresRow>::Forward(
            [&out, &delim, json](StreamElement<PostgresRow> e) -> bool {
                if (e.is_data()) {
                    Batch<std::string> b;
                    for (const auto& r : e.as_data()) {
                        if (json) {
                            b.emplace(postgres_row_to_json(r.value()));
                            continue;
                        }
                        std::string joined;
                        const auto& vs = r.value().values();
                        for (std::size_t i = 0; i < vs.size(); ++i) {
                            if (i > 0) {
                                joined += delim;
                            }
                            joined += vs[i];
                        }
                        b.emplace(std::move(joined));
                    }
                    return out.emit_data(std::move(b));
                }
                if (e.is_watermark()) {
                    return out.emit_watermark(e.as_watermark());
                }
                return out.emit_barrier(e.as_barrier());
            }));
        return inner_.produce(forwarder);
    }

    // #57: forward source-replay to the inner source (symmetry with the data
    // path; the inner bounded-query replay is a connector follow-up).
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override {
        inner_.snapshot_offset(backend, op_id, ckpt_id);
    }
    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        return inner_.restore_offset(backend, op_id);
    }

    std::string name() const override { return json_ ? "postgres_source" : "postgres_text_source"; }

private:
    PostgresSource inner_;
    std::string delim_;
    bool json_{false};
};

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    using clink::plugin::BuildContext;

    // Register the typed channels for PostgresRow and CdcEvent so
    // pipelines can carry full row/event records through the cluster
    // without flattening to a delimiter-joined or JSON-encoded
    // std::string at the connector boundary. The typed channels
    // preserve column names, type names, null indicators, and
    // transaction metadata end-to-end.
    reg.register_type<PostgresRow>(std::string{kChannelPostgresRow}, postgres_row_codec());
    reg.register_type<CdcEvent>(std::string{kChannelPostgresCdcEvent}, cdc_event_codec());

    // postgres_row_source: SELECT rows emitted as typed PostgresRow.
    // Same options as postgres_text_source except no `delim` - the
    // typed channel keeps columns separate, so downstream operators
    // address them by name via PostgresRow::at("column_name").
    reg.register_source<PostgresRow>(
        "postgres_row_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<PostgresRow>> {
            PostgresSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.query = ctx.param_or("query");
            opts.batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_row_source: 'conninfo' is required");
            }
            if (opts.query.empty()) {
                throw std::runtime_error("postgres_row_source: 'query' is required");
            }
            return std::make_shared<PostgresSource>(std::move(opts));
        });

    // postgres_cdc_event_source: subscribe to a logical replication
    // slot and emit each CdcEvent as a typed record. Same options as
    // postgres_cdc_text_source. Downstream operators see the full
    // event (op/table/lsn/xid + typed per-column CdcFields including
    // type names and is_null sentinels) instead of a JSON line.
    reg.register_source<CdcEvent>(
        "postgres_cdc_event_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<CdcEvent>> {
            PostgresCdcSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.slot_name = ctx.param_or("slot_name");
            opts.plugin = ctx.param_or("plugin", "test_decoding");
            opts.publication_names = ctx.param_or("publication_names");
            opts.create_slot = ctx.param_or("create_slot", "true") == "true";
            opts.drop_slot_on_close = ctx.param_or("drop_slot_on_close", "false") == "true";
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_cdc_event_source: 'conninfo' is required");
            }
            if (opts.slot_name.empty()) {
                throw std::runtime_error("postgres_cdc_event_source: 'slot_name' is required");
            }
            if (opts.plugin == "pgoutput" && opts.publication_names.empty()) {
                throw std::runtime_error(
                    "postgres_cdc_event_source: 'publication_names' is required when "
                    "plugin='pgoutput'");
            }
            return std::make_shared<PostgresCdcSource>(std::move(opts));
        });

    // postgres_cdc_text_source: subscribe to a logical replication slot
    // and emit each CdcEvent as a JSON line. params:
    //   conninfo (required), slot_name (required)
    //   plugin (default "test_decoding"): "test_decoding" or "pgoutput"
    //   publication_names (required when plugin == "pgoutput")
    //   create_slot (default "true")
    //   drop_slot_on_close (default "false")
    reg.register_source<std::string>(
        "postgres_cdc_text_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PostgresCdcSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.slot_name = ctx.param_or("slot_name");
            opts.plugin = ctx.param_or("plugin", "test_decoding");
            opts.publication_names = ctx.param_or("publication_names");
            opts.create_slot = ctx.param_or("create_slot", "true") == "true";
            opts.drop_slot_on_close = ctx.param_or("drop_slot_on_close", "false") == "true";
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_cdc_text_source: 'conninfo' is required");
            }
            if (opts.slot_name.empty()) {
                throw std::runtime_error("postgres_cdc_text_source: 'slot_name' is required");
            }
            if (opts.plugin == "pgoutput" && opts.publication_names.empty()) {
                throw std::runtime_error(
                    "postgres_cdc_text_source: 'publication_names' is required when "
                    "plugin='pgoutput'");
            }
            return std::make_shared<StringPostgresCdcSource>(std::move(opts));
        });

    // postgres_cdc_source (M5): the same logical-replication CDC stream, but each
    // insert/update/delete is a FLAT JSON object - the changed columns at the top
    // level (NULL cells -> JSON null) plus __op/__table/__lsn/__xid metadata - so
    // json_string_to_row drives a multi-column Row pipeline. Transaction markers
    // (begin/commit/truncate) are skipped. Same params as postgres_cdc_text_source.
    reg.register_source<std::string>(
        "postgres_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PostgresCdcSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.slot_name = ctx.param_or("slot_name");
            opts.plugin = ctx.param_or("plugin", "test_decoding");
            opts.publication_names = ctx.param_or("publication_names");
            opts.create_slot = ctx.param_or("create_slot", "true") == "true";
            opts.drop_slot_on_close = ctx.param_or("drop_slot_on_close", "false") == "true";
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_cdc_source: 'conninfo' is required");
            }
            if (opts.slot_name.empty()) {
                throw std::runtime_error("postgres_cdc_source: 'slot_name' is required");
            }
            if (opts.plugin == "pgoutput" && opts.publication_names.empty()) {
                throw std::runtime_error(
                    "postgres_cdc_source: 'publication_names' is required when plugin='pgoutput'");
            }
            return std::make_shared<StringPostgresCdcSource>(std::move(opts), /*json=*/true);
        });

    // postgres_text_source: SELECT rows joined with `delim` (default "|").
    // params:
    //   conninfo (required): libpq connection string.
    //   query (required): SELECT statement (no trailing ';').
    //   delim (default "|"): column-value separator.
    //   batch_size (default 256): rows per produce() batch.
    reg.register_source<std::string>(
        "postgres_text_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PostgresSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.query = ctx.param_or("query");
            opts.batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_text_source: 'conninfo' is required");
            }
            if (opts.query.empty()) {
                throw std::runtime_error("postgres_text_source: 'query' is required");
            }
            return std::make_shared<StringPostgresSource>(std::move(opts),
                                                          ctx.param_or("delim", "|"));
        });

    // postgres_source (M3): the same SELECT, but each row is a JSON object keyed by
    // column name on the string channel - bridged to a multi-column Row table via
    // json_string_to_row. The newer pattern (cf. mysql_source); the delimited
    // postgres_text_source is kept for back-compat.
    reg.register_source<std::string>(
        "postgres_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            PostgresSource::Options opts;
            opts.conninfo = ctx.param_or("conninfo");
            opts.query = ctx.param_or("query");
            opts.batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 256));
            if (opts.conninfo.empty()) {
                throw std::runtime_error("postgres_source: 'conninfo' is required");
            }
            if (opts.query.empty()) {
                throw std::runtime_error("postgres_source: 'query' is required");
            }
            return std::make_shared<StringPostgresSource>(std::move(opts), "|", /*json=*/true);
        });

    // postgres_sink (M4): batched multi-row INSERT of JSON-object rows. At-least-
    // once (effectively-once with on_conflict='update' + conflict_columns). The
    // newer string-channel + JSON pattern (cf. mysql_sink), distinct from the
    // older vector<string>+$N PostgresSink. params:
    //   conninfo (required)
    //   table (required)
    //   columns                   - comma-separated projection; on the SQL path it
    //       defaults to the table's declared schema (schema_columns).
    //   on_conflict ("" [plain] | "update" | "nothing")
    //   conflict_columns          - comma-separated ON CONFLICT target (required
    //       when on_conflict is set)
    //   update_columns            - comma-separated DO UPDATE SET list (empty = all
    //       non-conflict columns)
    //   batch_records (default 1000), max_bytes (0=off), linger_ms (0=off)
    reg.register_sink<std::string>(
        "postgres_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PostgresJsonSinkOptions o;
            o.conninfo = ctx.param_or("conninfo");
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                // SQL Row path: fall back to the declared table schema so the user
                // need not repeat columns= matching the DDL.
                o.columns = pgsql::columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.on_conflict = ctx.param_or("on_conflict", "");
            o.conflict_columns = split_csv(ctx.param_or("conflict_columns", ""));
            o.update_columns = split_csv(ctx.param_or("update_columns", ""));
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_bytes = static_cast<std::size_t>(ctx.param_int64_or("max_bytes", 0));
            o.max_age = std::chrono::milliseconds{ctx.param_int64_or("linger_ms", 0)};
            o.name = "postgres_sink";
            return std::make_shared<PostgresJsonSink>(std::move(o));
        });
}

}  // namespace clink::postgres
