#include "native/native_sink.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <arrow/api.h>

#include "clink/metrics/counter.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "native/errors.hpp"
#include "native/intake.hpp"
#include "native/metrics.hpp"
#include "native/native_sink_core.hpp"
#include "native/row_arrow.hpp"
#include "native/types.hpp"
#include "native/writer.hpp"

namespace clink::clickhouse::native {

namespace {

std::string tagged(const char* metric,
                   std::uint64_t op_id,
                   std::string_view key,
                   std::string_view value) {
    std::string out(metric);
    out += "{op_id=\"";
    out += std::to_string(op_id);
    out += "\",";
    out += key;
    out += "=\"";
    out += value;
    out += "\"}";
    return out;
}

std::vector<SqlColumn> parse_columns(const SinkOptions& options) {
    if (options.endpoints.empty()) {
        throw NativeSinkError(code::kOptionInvalid,
                              "clickhouse_native_sink: the options name no endpoint");
    }
    if (options.table.empty()) {
        throw NativeSinkError(code::kOptionInvalid,
                              "clickhouse_native_sink: option 'table' must not be empty");
    }
    if (options.sql_column_types.empty()) {
        throw NativeSinkError(code::kOptionInvalid,
                              "clickhouse_native_sink: the native sink is built from SQL; a "
                              "Dag-direct job must pass sql_column_types");
    }
    return parse_sql_column_types(options.sql_column_types);
}

}  // namespace

struct NativeSink::Impl {
    Impl(SinkOptions o, TransportFactory f)
        : core(o, parse_columns(o), InputKind::SqlTable, std::move(f)) {}

    // Registers the decline series at open, as the core registers its own,
    // so every series shows from the start.
    void cache(const RuntimeContext* ctx);
    // The plan for a sidecar of this schema, or nullptr once the decline is
    // counted and, the first time for the schema, logged.
    [[nodiscard]] const IntakePlan* intake_for(const std::shared_ptr<arrow::Schema>& schema);

    // Made during the core's open, after the column plan accepted the
    // declared columns and before the writer starts.
    std::unique_ptr<RowArrowBuilder> builder;

    // The last sidecar schema on_data_columnar saw and what compile_intake
    // made of it: a stream keeps one schema, so it compiles once.
    std::shared_ptr<arrow::Schema> intake_schema;
    IntakeResult intake{IntakeDecline::EventTime};
    std::unordered_set<std::string> declines_logged;
    // Null without a context.
    std::array<Counter*, 3> declined{};  // by IntakeDecline

    // Declared last, so it goes first: its destructor aborts a writer that
    // was never closed.
    SinkCore core;
};

void NativeSink::Impl::cache(const RuntimeContext* ctx) {
    if (ctx == nullptr || ctx->metrics() == nullptr) {
        return;
    }
    MetricsRegistry* metrics = ctx->metrics();
    const std::uint64_t op_id = ctx->operator_id().value();
    for (const IntakeDecline reason :
         {IntakeDecline::EventTime, IntakeDecline::UnsupportedType, IntakeDecline::DuplicateName}) {
        declined.at(static_cast<std::size_t>(reason)) = &metrics->counter(
            tagged(metric::kColumnarDeclinedTotal, op_id, "reason", to_string(reason)));
    }
}

const IntakePlan* NativeSink::Impl::intake_for(const std::shared_ptr<arrow::Schema>& schema) {
    if (!intake_schema || (intake_schema != schema && !intake_schema->Equals(*schema))) {
        intake = compile_intake(*schema, core.columns());
        intake_schema = schema;
        if (const auto* reason = std::get_if<IntakeDecline>(&intake)) {
            std::string fields;
            for (const auto& f : schema->fields()) {
                fields += (fields.empty() ? "" : ", ") + f->name() + ": " + f->type()->ToString();
            }
            if (declines_logged.insert(fields).second) {
                core.log(LogSeverity::Info,
                         "clickhouse native sink: subtask=" + core.subtask() +
                             " takes columnar batches of this schema through their rows (" +
                             to_string(*reason) + "): " + fields);
            }
        }
    }
    if (const auto* plan = std::get_if<IntakePlan>(&intake)) {
        return plan;
    }
    if (Counter* c = declined.at(static_cast<std::size_t>(std::get<IntakeDecline>(intake)))) {
        c->increment();
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// NativeSink

NativeSink::NativeSink(SinkOptions options, TransportFactory factory)
    : impl_(std::make_unique<Impl>(std::move(options), std::move(factory))) {}

NativeSink::~NativeSink() = default;

void NativeSink::open() {
    Impl& s = *impl_;
    const RuntimeContext* ctx = runtime();
    s.cache(ctx);
    // The builder is made once the column plan has accepted the columns, and
    // before the writer starts, so a failure to make it leaves no writer.
    s.core.open(ctx, id(), name(), [&s] {
        s.builder = std::make_unique<RowArrowBuilder>(s.core.columns());
    });
}

void NativeSink::on_data(const Batch<sql::Row>& batch) {
    Impl& s = *impl_;
    s.core.require_writer("on_data");
    s.core.drain_released();
    if (batch.empty()) {
        return;
    }
    try {
        std::shared_ptr<arrow::RecordBatch> rows;
        try {
            // Built here, on the task thread, so the per-row JSON work overlaps
            // the writer's network I/O.
            rows = s.builder->build(batch);
        } catch (const ConversionError& e) {
            s.core.conversion_failed(e);
            throw;
        }
        s.core.submit(std::move(rows), Carrier::Row, false);
    } catch (...) {
        // Every call that throws fails the task, and the runner then skips
        // both closes: stop the writer now, which also logs its summary.
        s.core.abort();
        throw;
    }
}

void NativeSink::on_data(Batch<sql::Row>&& batch) {
    on_data(static_cast<const Batch<sql::Row>&>(batch));
}

bool NativeSink::on_data_columnar(const Batch<sql::Row>& batch) {
    Impl& s = *impl_;
    s.core.require_writer("on_data_columnar");
    s.core.drain_released();
    const std::shared_ptr<arrow::RecordBatch>& sidecar = batch.arrow();
    if (!sidecar) {
        return false;
    }
    if (batch.empty()) {
        return true;
    }
    if (sidecar->num_rows() != static_cast<std::int64_t>(batch.size())) {
        // A count the sidecar does not bear out is no layout this intake
        // reads; on_data checks the rows it materialises against it.
        return false;
    }
    try {
        const IntakePlan* plan = s.intake_for(sidecar->schema());
        if (plan == nullptr) {
            // Nothing is reserved or sent yet, so the row path can take it.
            return false;
        }
        std::shared_ptr<arrow::RecordBatch> rows;
        std::size_t reused = 0;
        try {
            rows = s.builder->build_columnar(*sidecar, *plan, &reused);
        } catch (const ConversionError& e) {
            s.core.conversion_failed(e);
            throw;
        }
        s.core.submit(std::move(rows), Carrier::Columnar, reused > 0);
    } catch (...) {
        s.core.abort();
        throw;
    }
    return true;
}

void NativeSink::on_barrier(CheckpointBarrier barrier) {
    impl_->core.barrier(barrier);
}

void NativeSink::flush() {
    impl_->core.flush();
}

void NativeSink::close() {
    impl_->core.close();
}

void NativeSink::close_cancelled() {
    impl_->core.close_cancelled();
}

}  // namespace clink::clickhouse::native
