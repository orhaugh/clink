// ClickHouse factory registration.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/clickhouse/clickhouse_row_codec.hpp"
#include "clink/clickhouse/install.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/clickhouse_row.hpp"
#include "clink/connectors/clickhouse_sink.hpp"
#include "clink/connectors/clickhouse_source.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::clickhouse {

namespace {

// Render a ClickHouseRow as a single-line JSON object keyed by column name (M2),
// so json_string_to_row can map it to a multi-column Row table. Values are text
// (the bridge coerces per the declared column types).
// LIMITATION: ClickHouseRow carries no null indicator, so a NULL cell arrives as
// "" and is emitted as an empty JSON string, NOT JSON null (the bridge then
// coerces "" per the column type). Threading a null bit through ClickHouseRow is
// the M5 follow-up. Column names are always populated by the source
// (block.GetColumnName); if they are ever absent we fail loud rather than emit
// positional keys that the by-name bridge would silently map to all-NULL.
std::string clickhouse_row_to_json(const ClickHouseRow& r) {
    const auto& vs = r.values();
    const auto names = r.column_names();
    if (!names || names->size() != vs.size()) {
        throw std::runtime_error("clickhouse_source: column names unavailable for row->JSON");
    }
    clink::config::JsonObject obj;
    for (std::size_t i = 0; i < vs.size(); ++i) {
        obj[(*names)[i]] = clink::config::JsonValue{vs[i]};
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
    using clink::plugin::BuildContext;

    // Register the typed channel for ClickHouseRow so pipelines can
    // carry full rows (column names + types + stringified values) end
    // to end through the cluster without flattening to a delimiter-
    // joined std::string at the connector boundary.
    reg.register_type<ClickHouseRow>(std::string{kChannelClickHouseRow}, clickhouse_row_codec());

    // clickhouse_sink: inserts string records into a ClickHouse table.
    // Each record is one row (TSV by default) or one JSON object
    // (when format="jsoneachrow"). params:
    //   host (default "localhost"), port (default 9000)
    //   database (default "default"), table (required)
    //   user (default "default"), password (default "")
    //   format ("tsv" or "jsoneachrow"; default "tsv")
    //   batch_rows (default 1000)
    //   batch_interval_ms (default 1000)
    reg.register_sink<std::string>(
        "clickhouse_sink", [](const BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
            ClickHouseSink::Options opts;
            opts.host = ctx.param_or("host", "localhost");
            opts.port = static_cast<std::uint16_t>(ctx.param_int64_or("port", 9000));
            opts.database = ctx.param_or("database", "default");
            opts.table = ctx.param_or("table");
            opts.user = ctx.param_or("user", "default");
            opts.password = ctx.param_or("password", "");
            if (opts.table.empty()) {
                throw std::runtime_error("clickhouse_sink: 'table' is required");
            }
            const auto fmt = ctx.param_or("format", "tsv");
            if (fmt == "jsoneachrow" || fmt == "JSONEachRow") {
                opts.format = ClickHouseSink::Format::JSONEachRow;
            } else {
                opts.format = ClickHouseSink::Format::TSV;
            }
            opts.batch_rows = static_cast<std::size_t>(ctx.param_int64_or("batch_rows", 1000));
            opts.batch_interval =
                std::chrono::milliseconds{ctx.param_int64_or("batch_interval_ms", 1000)};
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
}

}  // namespace clink::clickhouse
