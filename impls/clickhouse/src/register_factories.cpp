// ClickHouse factory registration.

#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "clink/clickhouse/clickhouse_row_codec.hpp"
#include "clink/clickhouse/install.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/clickhouse_row.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/connectors/clickhouse_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

#include "native/register_native.hpp"

namespace clink::clickhouse {

namespace {

// Render a ClickHouseRow as a single-line JSON object keyed by column name (M2),
// so json_string_to_row can map it to a multi-column Row table. Values are text
// (the bridge coerces per the declared column types); a Nullable NULL cell emits
// JSON null (M5: ClickHouseRow::is_null, populated from ColumnNullable::IsNull).
// Column names are always populated by the source (block.GetColumnName); if they
// are ever absent we fail loud rather than emit positional keys that the by-name
// bridge would silently map to all-NULL.
std::string clickhouse_row_to_json(const ClickHouseRow& r) {
    const auto& vs = r.values();
    const auto names = r.column_names();
    if (!names || names->size() != vs.size()) {
        throw std::runtime_error("clickhouse_source: column names unavailable for row->JSON");
    }
    clink::config::JsonObject obj;
    for (std::size_t i = 0; i < vs.size(); ++i) {
        obj[(*names)[i]] =
            r.is_null(i) ? clink::config::JsonValue{} : clink::config::JsonValue{vs[i]};
    }
    return clink::config::JsonValue{std::move(obj)}.serialize(0);
}

// StringClickHouseSource adapts a ClickHouseSource<ClickHouseRow> onto
// the "string" channel for pipelines submitted through the legacy
// text-only submission path. Each row's cell values are joined with a
// configurable delimiter (default '|', matching postgres_text_source).
class StringClickHouseSource final : public Source<std::string> {
public:
    // json=false: legacy delimiter-joined string (clickhouse_text_source).
    // json=true:  a JSON object keyed by column name (clickhouse_source, M2) for
    //             the Row path via json_string_to_row.
    StringClickHouseSource(ClickHouseSource::Options opts, std::string delim, bool json = false)
        : inner_(std::move(opts)), delim_(std::move(delim)), json_(json) {}

    void open() override { inner_.open(); }
    void close() override { inner_.close(); }
    void cancel() override { Source<std::string>::cancel(); }

    bool produce(Emitter<std::string>& out) override {
        const auto& delim = delim_;
        const bool json = json_;
        Emitter<ClickHouseRow> forwarder(Emitter<ClickHouseRow>::Forward(
            [&out, &delim, json](StreamElement<ClickHouseRow> e) -> bool {
                if (e.is_data()) {
                    Batch<std::string> b;
                    for (const auto& r : e.as_data()) {
                        if (json) {
                            b.emplace(clickhouse_row_to_json(r.value()));
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

    std::string name() const override {
        return json_ ? "clickhouse_source" : "clickhouse_text_source";
    }

private:
    ClickHouseSource inner_;
    std::string delim_;
    bool json_{false};
};

// A clickhouse_sink integer option: the whole value must be the number, from 1
// to `max`. param_int64_or falls back to the default on garbage, and the casts
// it fed truncated, so a typo passed unnoticed, batch_rows='0' flushed on every
// row and port='70000' connected to port 4464.
// batch_rows and batch_interval_ms take any positive value, because the
// Stable builder has always passed any value through and a job that ran must
// keep running. port and the timeouts are bounded: a value past those bounds
// never worked (a truncated port, an infinite poll).
constexpr std::int64_t kNoUpperBound = std::numeric_limits<std::int64_t>::max();

std::int64_t sink_integer_param(const clink::plugin::BuildContext& ctx,
                                const std::string& key,
                                std::int64_t fallback,
                                std::int64_t max) {
    const auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    const std::string value = clink::plugin::BuildContext::resolve_secret(it->second);
    const char* first = value.data();
    const char* last = first + value.size();
    std::int64_t parsed = 0;
    const auto [end, ec] = std::from_chars(first, last, parsed);
    const bool whole = !value.empty() && end == last;
    const bool too_large = whole && ((ec == std::errc{} && parsed > max) ||
                                     (ec == std::errc::result_out_of_range && value[0] != '-'));
    if (too_large) {
        throw std::runtime_error("clickhouse_sink: " + key + " must be at most " +
                                 std::to_string(max) + " (got '" + it->second + "')");
    }
    if (!whole || ec != std::errc{} || parsed < 1) {
        throw std::runtime_error("clickhouse_sink: " + key + " must be a positive integer (got '" +
                                 it->second + "')");
    }
    return parsed;
}

// tsv, json and jsoneachrow, in any case. Any other value keeps the TSV it has
// always meant: the builder's format() takes any string, and a job that ran
// must keep running. The sink names the value in a warning at open instead.
void apply_sink_format(const std::string& value, ClickHouseSink::Options& opts) {
    std::string lower = value;
    for (char& c : lower) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (lower == "json" || lower == "jsoneachrow") {
        opts.format = ClickHouseSink::Format::JSONEachRow;
        return;
    }
    opts.format = ClickHouseSink::Format::TSV;
    if (lower != "tsv") {
        opts.unrecognised_format = value;
    }
}

// Shared options parser used by every source factory below. Centralised
// so a "host=..." typo fails the same way no matter which channel
// flavour the job graph picked.
ClickHouseSource::Options parse_source_options(const clink::plugin::BuildContext& ctx,
                                               const std::string& op_label) {
    ClickHouseSource::Options opts;
    opts.host = ctx.param_or("host", "localhost");
    opts.port = static_cast<std::uint16_t>(ctx.param_int64_or("port", 9000));
    opts.database = ctx.param_or("database", "default");
    opts.user = ctx.param_or("user", "default");
    opts.password = ctx.param_or("password", "");
    opts.query = ctx.param_or("query");
    opts.batch_size = static_cast<std::size_t>(ctx.param_int64_or("batch_size", 1024));
    if (opts.query.empty()) {
        throw std::runtime_error(op_label + ": 'query' is required");
    }
    return opts;
}

}  // namespace

void install(clink::plugin::PluginRegistry& reg) {
    clink::connectors::declare_connector(clink::connectors::ConnectorCapabilities{
        .name = "clickhouse",
        .version = "1",
        .is_source = true,
        .is_sink = true,
        .build_dependencies = {"clickhouse-cpp"},
        .runtime_dependencies = {"clickhouse server (native protocol, port 9000)"},
        .formats = {"tsv", "json"},
        // The source runs a query to completion; the sink streams.
        .boundedness = clink::connectors::Boundedness::Either,
        .replayable = false,
        .offset_model = clink::connectors::OffsetModel::None,
        .checkpoint_integrated = true,
        // Batched INSERTs flushed at the barrier. Each carries a deduplication
        // token of its own, so a replay re-inserts the tail since the last
        // checkpoint rather than matching what landed before the restart.
        .delivery = clink::connectors::DeliveryGuarantee::AtLeastOnce,
        .transactional = false,
        .auth_methods = {"none", "password"},
        .tls = false,
        .backpressure = true,
        .retries = false,
        .timeout_options =
            {"connect_timeout_ms", "send_timeout_ms", "receive_timeout_ms", "batch_interval_ms"},
        .available_in_sql = true,
        .limitations = {"query source re-reads from the start on restart (no offset state)"},
    });

    using clink::plugin::BuildContext;

    // Register the typed channel for ClickHouseRow so pipelines can
    // carry full rows (column names + types + stringified values) end
    // to end through the cluster without flattening to a delimiter-
    // joined std::string at the connector boundary.
    reg.register_type<ClickHouseRow>(std::string{kChannelClickHouseRow}, clickhouse_row_codec());

    // clickhouse_sink: inserts string records into a ClickHouse table, each
    // record one row. params:
    //   host (default "localhost"), port (default 9000, 1 to 65535)
    //   database (default "default"), table (required)
    //   user (default "default"), password (default "")
    //   format ("tsv", or "json" / "jsoneachrow" for JSONEachRow, in any case;
    //     default "tsv"; any other value is sent as TSV with a warning at open)
    //   batch_rows (default 1000, 1 to 2^31-1)
    //   batch_interval_ms (default 1000, 1 to 3600000)
    //   connect_timeout_ms (default 5000), send_timeout_ms and
    //     receive_timeout_ms (default 30000 each), 1 to 600000
    // Keys it does not know are ignored: the SQL planner puts its own on the op.
    reg.register_sink<std::string>(
        "clickhouse_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            ClickHouseSink::Options opts;
            opts.host = ctx.param_or("host", "localhost");
            opts.port = static_cast<std::uint16_t>(sink_integer_param(ctx, "port", 9000, 65535));
            opts.database = ctx.param_or("database", "default");
            opts.table = ctx.param_or("table");
            opts.user = ctx.param_or("user", "default");
            opts.password = ctx.param_or("password", "");
            if (opts.table.empty()) {
                throw std::runtime_error("clickhouse_sink: 'table' is required");
            }
            apply_sink_format(ctx.param_or("format", "tsv"), opts);
            opts.batch_rows = static_cast<std::size_t>(
                sink_integer_param(ctx, "batch_rows", 1000, kNoUpperBound));
            opts.batch_interval = std::chrono::milliseconds{
                sink_integer_param(ctx, "batch_interval_ms", 1000, kNoUpperBound)};
            opts.connect_timeout = std::chrono::milliseconds{
                sink_integer_param(ctx, "connect_timeout_ms", 5000, 600000)};
            opts.send_timeout = std::chrono::milliseconds{
                sink_integer_param(ctx, "send_timeout_ms", 30000, 600000)};
            opts.receive_timeout = std::chrono::milliseconds{
                sink_integer_param(ctx, "receive_timeout_ms", 30000, 600000)};
            return std::make_shared<ClickHouseSink>(std::move(opts));
        });

    // clickhouse_row_source: SELECT rows emitted as typed ClickHouseRow
    // records. Downstream operators address columns by index or by name
    // (row.at("col_name")). params:
    //   host (default "localhost"), port (default 9000)
    //   database (default "default"), user (default "default")
    //   password (default "")
    //   query (required) - the SELECT statement to execute
    //   batch_size (default 1024) - rows emitted per produce() call
    reg.register_source<ClickHouseRow>(
        "clickhouse_row_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<ClickHouseRow>> {
            auto opts = parse_source_options(ctx, "clickhouse_row_source");
            return std::make_shared<ClickHouseSource>(std::move(opts));
        });

    // clickhouse_text_source: same SELECT as clickhouse_row_source but
    // each row is flattened to a single std::string with columns joined
    // by `delim` (default "|") - for jobs submitted through the
    // string-only channel.
    reg.register_source<std::string>(
        "clickhouse_text_source",
        [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            auto opts = parse_source_options(ctx, "clickhouse_text_source");
            const auto delim = ctx.param_or("delim", "|");
            return std::make_shared<StringClickHouseSource>(std::move(opts), delim);
        });

    // clickhouse_source (M2): the same SELECT, but each row is a JSON object keyed
    // by column name on the string channel - bridged to a multi-column Row table
    // via json_string_to_row. The newer pattern (cf. mysql_source); the delimited
    // clickhouse_text_source is kept for back-compat.
    reg.register_source<std::string>(
        "clickhouse_source", [](const BuildContext& ctx) -> std::shared_ptr<Source<std::string>> {
            auto opts = parse_source_options(ctx, "clickhouse_source");
            return std::make_shared<StringClickHouseSource>(std::move(opts), "|", /*json=*/true);
        });

    // clickhouse_native_sink and its clickhouse_native record. A build whose
    // client is too old for it still registers the factory, which refuses by
    // name.
    native::register_native(reg);
}

}  // namespace clink::clickhouse
