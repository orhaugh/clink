// Typed structs into ClickHouse over the native protocol.
//
// make_clickhouse_native_sink<T> builds the native sink for a Dag-direct job,
// and register_clickhouse_native_sink<T> registers it as a factory for a
// plugin job. Rows of T become Arrow chunks through an ArrowBatcher<T> (the
// one CLINK_FIELDS derives, by default), and the sink takes its column types
// from that batcher's schema, so the options are the native sink's own less
// sql_column_types. The engine's event_time column is dropped: field 0 of the
// batcher's schema, when it is an int64 named event_time, is always taken for
// it. CLINK_FIELDS batchers put it there, so a struct field of that name is an
// ordinary column; a hand-written batcher must put the engine's column first,
// or give its own field 0 another name.
//
//   struct Trade { std::int64_t id; std::uint32_t qty; std::string venue; };
//   CLINK_FIELDS(Trade, id, qty, venue)
//
//   auto sink = clink::clickhouse::make_clickhouse_native_sink<Trade>(
//       {{"host", "clickhouse.internal"}, {"database", "markets"}, {"table", "trades"}});
//   dag.add_sink<Trade>(handle, sink);
//
// At-least-once, as the SQL native sink is: the sink holds no state and needs
// no uid, takes aligned checkpoint barriers only, and must be the only sink on
// its chain. Options, refusals and logs are those of docs/connectors/
// clickhouse.md. The sink is defined in the clink::clickhouse library, so a
// job module that uses it links clink::clickhouse. On a build without the
// native sink every helper here still compiles, and building the sink throws
// clickhouse.native_unavailable.
//
// Internal tier: the helper and its options may still change.

#pragma once

#ifdef CLINK_HAS_ARROW

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <utility>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/columnar_batcher.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"

namespace clink::clickhouse {

namespace detail {

// The type-independent native sink behind NativeSinkOf<T>: the options, the
// column plan from the batcher's schema, the open, the writer, the barrier
// and the closes. Every call is made on the task thread.
class NativeArrowSinkCore {
public:
    // Parses `options` and derives the columns from `schema` (its event_time
    // column excluded). Throws the sink's own refusal, a std::runtime_error
    // whose what() starts "[clickhouse.<code>]", for an option it cannot
    // take, and clickhouse.native_unavailable on a build without the native
    // sink. Nothing connects until open().
    NativeArrowSinkCore(std::shared_ptr<arrow::Schema> schema,
                        std::map<std::string, std::string> options,
                        std::uint32_t subtask_idx,
                        std::uint32_t parallelism);
    ~NativeArrowSinkCore();
    NativeArrowSinkCore(const NativeArrowSinkCore&) = delete;
    NativeArrowSinkCore& operator=(const NativeArrowSinkCore&) = delete;
    NativeArrowSinkCore(NativeArrowSinkCore&&) = delete;
    NativeArrowSinkCore& operator=(NativeArrowSinkCore&&) = delete;

    // Validates the target and starts the writer. A null `ctx` runs the sink
    // unaccounted and names it by `id` and `name`.
    void open(const RuntimeContext* ctx, OperatorId id, const std::string& name);
    // Queues a chunk of the schema's columns, event_time already dropped. A
    // chunk with no rows sends nothing. A null chunk stands for one the
    // batcher could not build: the writer is stopped and the call throws.
    void write(std::shared_ptr<arrow::RecordBatch> rows);
    // Returns only once every row written before the barrier is in the table.
    void barrier(CheckpointBarrier barrier);
    void flush();
    void close();
    void close_cancelled() noexcept;

    struct Impl;

private:
    std::unique_ptr<Impl> impl_;
};

}  // namespace detail

// The native sink for rows of T. Built by the helpers below.
template <typename T>
class NativeSinkOf final : public Sink<T> {
public:
    NativeSinkOf(ArrowBatcher<T> batcher,
                 std::shared_ptr<arrow::Schema> schema,
                 std::map<std::string, std::string> options,
                 std::uint32_t subtask_idx,
                 std::uint32_t parallelism)
        : batcher_(std::move(batcher)),
          core_(std::move(schema), std::move(options), subtask_idx, parallelism) {}

    void open() override { core_.open(this->runtime(), this->id(), name()); }

    // Builds the chunk here, on the task thread. Field 0, when it is an int64
    // named event_time, is taken for the engine's column and dropped.
    void on_data(const Batch<T>& batch) override {
        if (batch.empty()) {
            return;
        }
        std::shared_ptr<arrow::RecordBatch> rows;
        try {
            rows = batcher_.build(batch);
            if (rows && rows->num_columns() > 0 &&
                clink::detail::is_event_time_field(*rows->schema()->field(0))) {
                auto dropped = rows->RemoveColumn(0);
                rows = dropped.ok() ? std::move(dropped).ValueUnsafe() : nullptr;
            }
        } catch (...) {
            // A throw fails the task, and the runner then skips both closes:
            // stop the writer now, which also logs its summary.
            core_.close_cancelled();
            throw;
        }
        core_.write(std::move(rows));
    }

    void on_barrier(CheckpointBarrier barrier) override { core_.barrier(barrier); }
    void flush() override { core_.flush(); }
    void close() override { core_.close(); }
    void close_cancelled() override { core_.close_cancelled(); }

    // The guarantee rests on on_barrier preceding the checkpoint's ack, which
    // holds only while this sink owns its chain's checkpoint, so
    // Dag::add_sink keeps it the only sink on its chain.
    [[nodiscard]] bool gates_checkpoint_ack() const noexcept override { return true; }
    [[nodiscard]] std::string name() const override { return "clickhouse_native_sink"; }

private:
    ArrowBatcher<T> batcher_;
    detail::NativeArrowSinkCore core_;
};

// The sink for rows of T, built by `batcher`, with the native sink's options
// (sql_column_types excepted). `subtask_idx` and `parallelism` say which
// subtask this is, for the open report and the INSERT's log_comment. Throws
// std::invalid_argument when the batcher has no schema or no build function,
// and the sink's refusal for an option it cannot take.
template <typename T>
std::shared_ptr<Sink<T>> make_clickhouse_native_sink(ArrowBatcher<T> batcher,
                                                     std::map<std::string, std::string> options,
                                                     std::uint32_t subtask_idx = 0,
                                                     std::uint32_t parallelism = 1) {
    if (!batcher.schema) {
        throw std::invalid_argument(
            "make_clickhouse_native_sink: the ArrowBatcher has no schema function");
    }
    if (!batcher.build) {
        throw std::invalid_argument(
            "make_clickhouse_native_sink: the ArrowBatcher has no build function");
    }
    std::shared_ptr<arrow::Schema> schema = batcher.schema();
    if (!schema) {
        throw std::invalid_argument(
            "make_clickhouse_native_sink: the ArrowBatcher's schema is null");
    }
    return std::make_shared<NativeSinkOf<T>>(
        std::move(batcher), std::move(schema), std::move(options), subtask_idx, parallelism);
}

// The sink for a CLINK_FIELDS struct, through the batcher its declaration
// derives.
template <HasArrowFields T>
std::shared_ptr<Sink<T>> make_clickhouse_native_sink(std::map<std::string, std::string> options) {
    return make_clickhouse_native_sink<T>(make_columnar_arrow_batcher<T>(), std::move(options));
}

// A factory for PluginRegistry::register_sink<T>: each subtask's sink takes
// the op's params as its options.
template <HasArrowFields T>
std::function<std::shared_ptr<Sink<T>>(const plugin::BuildContext&)>
clickhouse_native_sink_factory() {
    return [](const plugin::BuildContext& ctx) -> std::shared_ptr<Sink<T>> {
        return make_clickhouse_native_sink<T>(
            make_columnar_arrow_batcher<T>(), ctx.params, ctx.subtask_idx, ctx.parallelism);
    };
}

// Registers the sink for T under `op_type`, by default
// clickhouse_native_sink_<channel>, where <channel> is the name T was
// registered under. Keep the clickhouse_native_sink prefix on any other name:
// the delivery-guarantee gate finds the sink's at-least-once record by it.
// T must be registered first (register_type<T>()); this does not register it.
template <HasArrowFields T>
void register_clickhouse_native_sink(plugin::PluginRegistry& registry, std::string op_type = {}) {
    if (op_type.empty()) {
        const std::string channel = registry.type_registry().channel_for_typeid(typeid(T).name());
        if (channel.empty()) {
            throw std::runtime_error(
                "register_clickhouse_native_sink<T>: T is not registered; call "
                "register_type<T>() first");
        }
        op_type = "clickhouse_native_sink_" + channel;
    }
    registry.register_sink<T>(op_type, clickhouse_native_sink_factory<T>());
}

}  // namespace clink::clickhouse

#endif  // CLINK_HAS_ARROW
