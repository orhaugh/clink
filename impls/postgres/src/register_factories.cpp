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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/connectors/cdc_json.hpp"
#include "clink/connectors/postgres_cdc_source.hpp"
#include "clink/connectors/postgres_json_sink.hpp"
#include "clink/connectors/postgres_json_sink_2pc.hpp"
#include "clink/connectors/postgres_json_upsert_sink.hpp"
#include "clink/connectors/postgres_row.hpp"
#include "clink/connectors/postgres_source.hpp"
#include "clink/connectors/postgres_sql.hpp"
#include "clink/core/record.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/postgres/cdc_event_codec.hpp"
#include "clink/postgres/install.hpp"
#include "clink/postgres/postgres_row_codec.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "cdc_row_encoder.hpp"

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
    // encoder set (json=true with changelog_mode): the SQL CDC contract - typed
    //   values for the declared columns, refused when a value cannot be held
    //   exactly, and __row_kind in upsert mode (cdc_row_encoder.hpp).
    explicit StringPostgresCdcSource(PostgresCdcSource::Options opts,
                                     bool json = false,
                                     std::optional<cdc::RowEncoder> encoder = std::nullopt)
        : inner_(opts),
          json_(json),
          encoder_(std::move(encoder)),
          policy_(opts.decode_error_policy) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override {
        Source<std::string>::cancel();
        inner_.cancel();
    }

    bool produce(Emitter<std::string>& out) override {
        const bool json = json_;
        // One produce() call emits rows of one phase, so the phase before the
        // call is the phase of its rows.
        const bool snapshot_rows = inner_.snapshot_in_progress();
        Emitter<CdcEvent> forwarder(
            Emitter<CdcEvent>::Forward([&](StreamElement<CdcEvent> e) -> bool {
                if (e.is_data()) {
                    Batch<std::string> b;
                    for (const auto& r : e.as_data()) {
                        if (!json) {
                            b.emplace(serialize(r.value()));
                            continue;
                        }
                        if (encoder_) {
                            if (auto row = encode_(r.value(), snapshot_rows)) {
                                b.emplace(std::move(*row));
                            }
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

    // The checkpoint hooks reach the inner source, which records its position
    // and confirms the slot only once the checkpoint is complete.
    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId ckpt_id) override {
        inner_.snapshot_offset(backend, op_id, ckpt_id);
    }
    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        return inner_.restore_offset(backend, op_id);
    }
    void notify_checkpoint_complete(CheckpointId ckpt_id) override {
        inner_.notify_checkpoint_complete(ckpt_id);
    }
    void notify_checkpoint_aborted(CheckpointId ckpt_id) override {
        inner_.notify_checkpoint_aborted(ckpt_id);
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

    // A value the contract cannot carry follows on_decode_error: fail stops
    // the job on it (the restart meets it again, it is never skipped), drop
    // counts it and leaves the row out.
    std::optional<std::string> encode_(const CdcEvent& ev, bool snapshot_row) const {
        try {
            return encoder_->encode(ev, snapshot_row);
        } catch (const cdc::EncodeError& e) {
            clink::metrics::connector::error_inc("postgres_cdc", "source");
            if (policy_ == PostgresCdcSource::DecodeErrorPolicy::Fail) {
                throw std::runtime_error(std::string{"postgres_cdc: "} + e.what() +
                                         " (on_decode_error=fail)");
            }
            clink::metrics::connector::dropped_events_inc("postgres_cdc", 1);
            clink::log::warn(
                "source.postgres_cdc",
                std::string{"dropped a change the SQL contract cannot carry: "} + e.what());
            return std::nullopt;
        }
    }

    PostgresCdcSource inner_;
    bool json_{false};
    std::optional<cdc::RowEncoder> encoder_;
    PostgresCdcSource::DecodeErrorPolicy policy_;
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

// ---- CDC source options --------------------------------------------------

// A boolean option: 'true' or 'false', nothing else. A typo would otherwise
// read as the default with no word said.
bool bool_option(const clink::plugin::BuildContext& ctx,
                 const std::string& factory,
                 const std::string& key,
                 bool fallback) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    if (it->second == "true") {
        return true;
    }
    if (it->second == "false") {
        return false;
    }
    throw std::runtime_error(factory + ": '" + key + "' must be 'true' or 'false' (got '" +
                             it->second + "')");
}

std::int64_t int_option(const clink::plugin::BuildContext& ctx,
                        const std::string& factory,
                        const std::string& key,
                        std::int64_t fallback,
                        std::int64_t min) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    std::size_t used = 0;
    std::int64_t v = 0;
    try {
        v = std::stoll(it->second, &used);
    } catch (const std::exception&) {
        used = 0;
    }
    if (used == 0 || used != it->second.size() || v < min) {
        throw std::runtime_error(factory + ": '" + key + "' must be an integer of at least " +
                                 std::to_string(min) + " (got '" + it->second + "')");
    }
    return v;
}

std::string domain_option(const clink::plugin::BuildContext& ctx,
                          const std::string& factory,
                          const std::string& key,
                          const std::string& fallback,
                          const std::vector<std::string>& allowed) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    if (std::find(allowed.begin(), allowed.end(), it->second) != allowed.end()) {
        return it->second;
    }
    std::string list;
    for (const auto& a : allowed) {
        list += (list.empty() ? "'" : ", '") + a + "'";
    }
    throw std::runtime_error(factory + ": '" + key + "' must be one of " + list + " (got '" +
                             it->second + "')");
}

std::string quote_ident(std::string_view v) {
    std::string out = "\"";
    for (const char c : v) {
        out += c == '"' ? std::string{"\"\""} : std::string{c};
    }
    out += '"';
    return out;
}

struct CdcConfig {
    PostgresCdcSource::Options source;
    std::optional<cdc::ChangelogMode> changelog_mode;
    std::vector<cdc::ColumnPlan> plan;
};

// The options every CDC factory takes. `sql_contract` admits changelog_mode,
// which selects the SQL CDC contract (postgres_cdc_source only):
//
//   conninfo, slot_name            required
//   plugin                         test_decoding | pgoutput (pgoutput under the contract)
//   publication_names              required with pgoutput
//   create_slot                    true | false (default true)
//   drop_slot_on_close             true | false (default false)
//   on_decode_error                drop | fail (default drop; fail under the contract)
//   table                          the schema-qualified table to capture
//   primary_key                    its key columns, required with table
//   snapshot_mode                  initial | never (default never; initial under
//                                  the contract); initial needs table
//   changelog_mode                 upsert | append
//   snapshot_batch_rows            rows per snapshot fetch (default 1000)
//   poll_interval_ms               (default 50)
//   status_interval_ms             (default 10000; 0 sends only what is due)
//   connect_timeout_ms             (default 30000)
//   reconnect_timeout_ms           (default 60000; 0 fails at once)
//   declared_types                 the SQL table's columns, set by the planner
CdcConfig cdc_config(const clink::plugin::BuildContext& ctx,
                     const std::string& factory,
                     bool sql_contract) {
    CdcConfig cfg;
    auto& o = cfg.source;
    o.conninfo = ctx.param_or("conninfo");
    o.slot_name = ctx.param_or("slot_name");
    if (o.conninfo.empty()) {
        throw std::runtime_error(factory + ": 'conninfo' is required");
    }
    if (o.slot_name.empty()) {
        throw std::runtime_error(factory + ": 'slot_name' is required");
    }
    if (ctx.params.count("changelog_mode") != 0) {
        if (!sql_contract) {
            throw std::runtime_error(factory +
                                     ": 'changelog_mode' applies to postgres_cdc_source (the SQL "
                                     "source) only");
        }
        cfg.changelog_mode =
            domain_option(ctx, factory, "changelog_mode", "", {"upsert", "append"}) == "upsert"
                ? cdc::ChangelogMode::Upsert
                : cdc::ChangelogMode::Append;
    }
    const bool contract = cfg.changelog_mode.has_value();
    o.plugin = domain_option(ctx,
                             factory,
                             "plugin",
                             contract ? "pgoutput" : "test_decoding",
                             {"test_decoding", "pgoutput"});
    if (contract && o.plugin != "pgoutput") {
        throw std::runtime_error(factory + ": changelog_mode needs plugin='pgoutput'");
    }
    o.publication_names = ctx.param_or("publication_names");
    if (o.plugin == "pgoutput" && o.publication_names.empty()) {
        throw std::runtime_error(factory +
                                 ": 'publication_names' is required when plugin='pgoutput'");
    }
    o.create_slot = bool_option(ctx, factory, "create_slot", true);
    o.drop_slot_on_close = bool_option(ctx, factory, "drop_slot_on_close", false);
    o.decode_error_policy =
        domain_option(
            ctx, factory, "on_decode_error", contract ? "fail" : "drop", {"drop", "fail"}) == "fail"
            ? PostgresCdcSource::DecodeErrorPolicy::Fail
            : PostgresCdcSource::DecodeErrorPolicy::Drop;
    o.table = ctx.param_or("table");
    o.key_columns = split_csv(ctx.param_or("primary_key"));
    const std::string snapshot_mode = domain_option(
        ctx, factory, "snapshot_mode", contract ? "initial" : "never", {"initial", "never"});
    if (contract && o.table.empty()) {
        throw std::runtime_error(factory +
                                 ": changelog_mode needs 'table', the schema-qualified PostgreSQL "
                                 "table to capture");
    }
    if (!o.table.empty() && o.key_columns.empty()) {
        throw std::runtime_error(factory + ": capturing table " + o.table +
                                 " needs its primary key (PRIMARY KEY in the table definition, or "
                                 "primary_key='col,...')");
    }
    if (snapshot_mode == "initial" && o.table.empty()) {
        throw std::runtime_error(factory +
                                 ": snapshot_mode='initial' needs 'table', the table to read the "
                                 "existing rows from");
    }
    std::string select_list = "*";
    if (contract) {
        try {
            cfg.plan = cdc::plan_columns(ctx.param_or("declared_types"));
        } catch (const cdc::EncodeError& e) {
            throw std::runtime_error(factory + ": " + e.what());
        }
        select_list.clear();
        for (const auto& col : cfg.plan) {
            o.declared_columns.emplace_back(col.name, col.sql_type);
            if (!cdc::is_metadata_column(col.name)) {
                select_list += (select_list.empty() ? "" : ", ") + quote_ident(col.name);
            }
        }
        if (select_list.empty()) {
            throw std::runtime_error(factory + ": the table declares no columns to capture");
        }
        for (const auto& key : o.key_columns) {
            const bool declared =
                std::any_of(cfg.plan.begin(), cfg.plan.end(), [&](const cdc::ColumnPlan& c) {
                    return c.name == key;
                });
            if (!declared) {
                throw std::runtime_error(factory + ": primary key column " + key +
                                         " is not a declared column");
            }
        }
        o.strict = true;
    }
    o.snapshot_batch_rows =
        static_cast<std::size_t>(int_option(ctx, factory, "snapshot_batch_rows", 1000, 1));
    o.poll_interval =
        std::chrono::milliseconds{int_option(ctx, factory, "poll_interval_ms", 50, 1)};
    o.standby_status_interval =
        std::chrono::milliseconds{int_option(ctx, factory, "status_interval_ms", 10000, 0)};
    o.connect_timeout =
        std::chrono::milliseconds{int_option(ctx, factory, "connect_timeout_ms", 30000, 1)};
    o.reconnect_timeout =
        std::chrono::milliseconds{int_option(ctx, factory, "reconnect_timeout_ms", 60000, 0)};
    if (snapshot_mode == "initial") {
        o.enable_initial_snapshot = true;
        o.initial_snapshot.push_back(PostgresCdcSnapshotQuery{
            .table = o.table, .query = "SELECT " + select_list + " FROM " + o.table});
    }
    return cfg;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "postgres",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"libpq"},
        .runtime_dependencies = {"postgresql server"},
        .formats = {"json", "text", "cdc-event"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = true,
        .offset_model = clink::connectors::OffsetModel::Lsn,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"password", "md5", "scram-sha-256", "trust"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout", "statement_timeout"},
        .available_in_sql = true,
        .limitations = {"CDC needs a logical replication slot, confirmed only by completed "
                        "checkpoints: WAL is retained while the job is stopped or not "
                        "checkpointing",
                        "CDC captures one table per source (changelog_mode), with no schema "
                        "evolution, partitioned tables or publication row filters",
                        "a CDC snapshot interrupted by a failure is taken again and converges; "
                        "it does not resume",
                        "the plain sink is at-least-once - use postgres_2pc or the upsert sink"},
    });

    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "postgres_2pc",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"libpq"},
        .runtime_dependencies = {"postgresql server with max_prepared_transactions > 0"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::ExactlyOnceTwoPhaseCommit,
        .transactional = true,
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"password", "md5", "scram-sha-256", "trust"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout", "statement_timeout"},
        .available_in_sql = true,
        .limitations = {"requires max_prepared_transactions > 0 on the server; a prepared "
                        "transaction that is never resolved holds locks and pins WAL "
                        "indefinitely",
                        "the prepared-transaction id must be unique per subtask and stable "
                        "across restarts for recovery to find it"},
        .required_options_for_exactly_once = {"table"},
    });

    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "postgres_upsert",
        .version = "1",
        .is_source = false,
        .is_sink = true,
        .build_dependencies = {"libpq"},
        .runtime_dependencies = {"postgresql server"},
        .formats = {"json"},
        .boundedness = clink::connectors::Boundedness::Unbounded,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        .delivery = clink::connectors::DeliveryGuarantee::EffectivelyOnceIdempotent,
        .transactional = false,
        .idempotency_key_option = "primary_key",
        .schema_evolution = false,
        .partition_discovery = false,
        .auth_methods = {"password", "md5", "scram-sha-256", "trust"},
        .tls = true,
        .backpressure = true,
        .retries = true,
        .timeout_options = {"connect_timeout", "statement_timeout"},
        .available_in_sql = true,
        .limitations = {"duplicates collapse only if primary_key really is unique for the row; "
                        "clink cannot verify the target schema"},
    });

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
            auto cfg = cdc_config(ctx, "postgres_cdc_event_source", /*sql_contract=*/false);
            return std::make_shared<PostgresCdcSource>(std::move(cfg.source));
        });

    // postgres_cdc_text_source: subscribe to a logical replication slot
    // and emit each CdcEvent as a JSON line. Options: see cdc_config.
    reg.register_source<std::string>(
        "postgres_cdc_text_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            auto cfg = cdc_config(ctx, "postgres_cdc_text_source", /*sql_contract=*/false);
            return std::make_shared<StringPostgresCdcSource>(std::move(cfg.source));
        });

    // postgres_cdc_source: the SQL frontend's CDC source (connector='postgres',
    // mode='cdc'). With changelog_mode set, the SQL CDC contract: one table,
    // keyed by its primary key, typed values, an initial snapshot by default,
    // on_decode_error=fail by default, and either a keyed changelog
    // (__row_kind, 'upsert') or one appended row per change ('append').
    // Without it, the earlier flat JSON per data-change event (changed
    // columns at the top level plus __op/__table/__lsn/__xid), unchanged.
    reg.register_source<std::string>(
        "postgres_cdc_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            auto cfg = cdc_config(ctx, "postgres_cdc_source", /*sql_contract=*/true);
            if (cfg.changelog_mode) {
                return std::make_shared<StringPostgresCdcSource>(
                    std::move(cfg.source),
                    /*json=*/true,
                    cdc::RowEncoder(std::move(cfg.plan), *cfg.changelog_mode));
            }
            return std::make_shared<StringPostgresCdcSource>(std::move(cfg.source), /*json=*/true);
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

    // postgres_2pc_sink: EXACTLY-ONCE via two-phase commit (PREPARE TRANSACTION /
    // COMMIT PREPARED). Selected by the SQL path for delivery_guarantee=
    // 'exactly_once'. Rows since the last barrier are INSERTed into an open
    // transaction, PREPAREd at the barrier under a deterministic global id, and
    // COMMIT PREPAREd once the checkpoint is globally durable. Requires the
    // server's max_prepared_transactions > 0. params:
    //   conninfo (required), table (required)
    //   columns  - comma-separated projection; on the SQL Row path defaults to
    //              the declared table schema (schema_columns)
    //   batch_records (default 1000), max_bytes (0=off)
    reg.register_sink<std::string>(
        "postgres_2pc_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PostgresJsonSink2PCOptions o;
            o.conninfo = ctx.param_or("conninfo");
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                o.columns = pgsql::columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.subtask_idx = ctx.subtask_idx;
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.max_bytes = static_cast<std::size_t>(ctx.param_int64_or("max_bytes", 0));
            o.name = "postgres_2pc_sink";
            auto sink = std::make_shared<PostgresJsonSink2PC>(std::move(o));
            if (auto cg = ctx.param_or("commit_group", ""); !cg.empty()) {
                sink->set_commit_group(cg);
            }
            return sink;
        });

    // postgres_upsert_sink: changelog-aware, mode='upsert'. Maintains a table by
    // PRIMARY KEY - insert/update_after upsert (INSERT ON CONFLICT DO UPDATE),
    // delete/update_before DELETE by key. Effectively-once on the sink table for a
    // stable PK (keyed idempotent ops). Lets a retracting SQL query maintain a
    // Postgres table. params:
    //   conninfo (required), table (required)
    //   columns       - full projection; on the SQL Row path defaults to schema_columns
    //   key_columns   - the PRIMARY KEY (conflict target + delete key); on the SQL
    //                   path threaded from the table's PRIMARY KEY
    //   batch_records (default 1000)
    reg.register_sink<std::string>(
        "postgres_upsert_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            PostgresJsonUpsertSinkOptions o;
            o.conninfo = ctx.param_or("conninfo");
            o.table = ctx.param_or("table");
            o.columns = split_csv(ctx.param_or("columns", ""));
            if (o.columns.empty()) {
                o.columns = pgsql::columns_from_schema(ctx.param_or("schema_columns", ""));
            }
            o.key_columns = split_csv(ctx.param_or("key_columns", ""));
            o.batch_records = static_cast<std::size_t>(ctx.param_int64_or("batch_records", 1000));
            o.name = "postgres_upsert_sink";
            return std::make_shared<PostgresJsonUpsertSink>(std::move(o));
        });
}

}  // namespace clink::postgres
