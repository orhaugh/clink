// The non-template half of clink/clickhouse/native_sink.hpp. Built whatever
// client the build found: with the native sink it drives a SinkCore, and
// without it the constructor refuses clickhouse.native_unavailable, so the
// installed header never depends on which.

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/clickhouse/native_sink.hpp"

#include "native/errors.hpp"
#include "native/register_native.hpp"

#if defined(CLINK_CLICKHOUSE_NATIVE)
#include "native/column_plan.hpp"
#include "native/insert_transport.hpp"
#include "native/native_sink_core.hpp"
#include "native/sink_options.hpp"
#include "native/types.hpp"
#endif

namespace clink::clickhouse::detail {

namespace native = clink::clickhouse::native;

#if defined(CLINK_CLICKHOUSE_NATIVE)

namespace {

native::SinkOptions typed_options(const std::map<std::string, std::string>& options,
                                  std::uint32_t subtask_idx,
                                  std::uint32_t parallelism) {
    try {
        return native::parse_typed_sink_options(options, subtask_idx, parallelism);
    } catch (const native::NativeSinkError& e) {
        native::count_factory_refusal(e.code());
        throw;
    }
}

}  // namespace

struct NativeArrowSinkCore::Impl {
    Impl(const arrow::Schema& schema,
         const std::map<std::string, std::string>& options,
         std::uint32_t subtask_idx,
         std::uint32_t parallelism)
        : core(typed_options(options, subtask_idx, parallelism),
               native::columns_from_arrow_schema(schema),
               native::InputKind::TypedStruct,
               native::current_transport_factory()) {}

    native::SinkCore core;
};

NativeArrowSinkCore::NativeArrowSinkCore(std::shared_ptr<arrow::Schema> schema,
                                         std::map<std::string, std::string> options,
                                         std::uint32_t subtask_idx,
                                         std::uint32_t parallelism) {
    if (!schema) {
        throw std::invalid_argument("make_clickhouse_native_sink: the schema is null");
    }
    impl_ = std::make_unique<Impl>(*schema, options, subtask_idx, parallelism);
}

NativeArrowSinkCore::~NativeArrowSinkCore() = default;

void NativeArrowSinkCore::open(const RuntimeContext* ctx, OperatorId id, const std::string& name) {
    impl_->core.open(ctx, id, name);
}

void NativeArrowSinkCore::write(std::shared_ptr<arrow::RecordBatch> rows) {
    native::SinkCore& core = impl_->core;
    core.require_writer("on_data");
    core.drain_released();
    if (!rows) {
        // Every call that throws fails the task, and the runner then skips
        // both closes: stop the writer now, which also logs its summary.
        core.abort();
        throw std::runtime_error(
            "clickhouse native sink: the ArrowBatcher could not build a chunk");
    }
    if (rows->num_rows() == 0) {
        return;
    }
    try {
        // The batcher built a fresh chunk for this batch alone, so nothing
        // the writer holds shares the input's buffers.
        core.submit(std::move(rows), native::Carrier::Row, false);
    } catch (...) {
        core.abort();
        throw;
    }
}

void NativeArrowSinkCore::barrier(CheckpointBarrier barrier) {
    impl_->core.barrier(barrier);
}

void NativeArrowSinkCore::flush() {
    impl_->core.flush();
}

void NativeArrowSinkCore::close() {
    impl_->core.close();
}

void NativeArrowSinkCore::close_cancelled() noexcept {
    impl_->core.close_cancelled();
}

#else  // !CLINK_CLICKHOUSE_NATIVE

// Never made: the constructor refuses before it would be.
struct NativeArrowSinkCore::Impl {};

NativeArrowSinkCore::NativeArrowSinkCore(std::shared_ptr<arrow::Schema> /*schema*/,
                                         std::map<std::string, std::string> /*options*/,
                                         std::uint32_t /*subtask_idx*/,
                                         std::uint32_t /*parallelism*/) {
    native::count_factory_refusal(native::code::kNativeUnavailable);
    throw native::NativeSinkError(native::code::kNativeUnavailable,
                                  native::native_unavailable_message());
}

NativeArrowSinkCore::~NativeArrowSinkCore() = default;

void NativeArrowSinkCore::open(const RuntimeContext* /*ctx*/,
                               OperatorId /*id*/,
                               const std::string& /*name*/) {}

void NativeArrowSinkCore::write(std::shared_ptr<arrow::RecordBatch> /*rows*/) {}

void NativeArrowSinkCore::barrier(CheckpointBarrier /*barrier*/) {}

void NativeArrowSinkCore::flush() {}

void NativeArrowSinkCore::close() {}

void NativeArrowSinkCore::close_cancelled() noexcept {}

#endif  // CLINK_CLICKHOUSE_NATIVE

}  // namespace clink::clickhouse::detail
