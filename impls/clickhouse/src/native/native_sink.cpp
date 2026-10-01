#include "native/native_sink.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

struct NativeSink::Impl {};

NativeSink::NativeSink(SinkOptions /*options*/, TransportFactory /*factory*/) {
    not_implemented("NativeSink");
}

NativeSink::~NativeSink() = default;

void NativeSink::open() {
    not_implemented("NativeSink::open");
}

void NativeSink::on_data(const Batch<sql::Row>& /*batch*/) {
    not_implemented("NativeSink::on_data");
}

void NativeSink::on_data(Batch<sql::Row>&& /*batch*/) {
    not_implemented("NativeSink::on_data");
}

void NativeSink::on_barrier(CheckpointBarrier /*barrier*/) {
    not_implemented("NativeSink::on_barrier");
}

void NativeSink::flush() {
    not_implemented("NativeSink::flush");
}

void NativeSink::close() {
    not_implemented("NativeSink::close");
}

void NativeSink::close_cancelled() {}

}  // namespace clink::clickhouse::native
