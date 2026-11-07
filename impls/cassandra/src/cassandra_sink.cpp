#include "clink/cassandra/cassandra_sink.hpp"

#include <string>
#include <utility>

#include "sink_core.hpp"

namespace clink::cassandra {

struct CassandraSink::Impl {
    explicit Impl(detail::SinkCoreOptions o) : core(std::move(o)) {}
    detail::SinkCore core;
};

CassandraSink::CassandraSink(Options opts)
    : impl_(std::make_unique<Impl>(detail::checked_options(opts.name,
                                                           opts.conn,
                                                           opts.keyspace,
                                                           opts.table,
                                                           {},
                                                           /*keyed=*/false,
                                                           opts.max_in_flight,
                                                           opts.max_in_flight_bytes,
                                                           std::move(opts.service)))) {}

CassandraSink::~CassandraSink() = default;

void CassandraSink::open() {
    impl_->core.open(runtime());
}

void CassandraSink::on_data(const Batch<std::string>& batch) {
    auto& core = impl_->core;
    core.check_usable();
    if (batch.empty()) {
        return;
    }
    core.reach("before_write");
    auto* rt = runtime();
    for (const auto& rec : batch) {
        CassandraWrite w;
        w.kind = CassandraWrite::Kind::Insert;
        w.json = rec.value();
        const std::size_t bytes = w.json.size();
        core.submit(rt, std::move(w), bytes);
    }
}

void CassandraSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();
}

void CassandraSink::flush() {
    auto& core = impl_->core;
    core.check_usable();
    if (core.in_flight() == 0) {
        return;
    }
    core.reach("in_flight");
    core.drain(runtime());
    core.reach("after_write");
}

void CassandraSink::close() {
    impl_->core.close();
}

std::string CassandraSink::name() const {
    return impl_->core.name();
}

bool CassandraSink::failed() const {
    return impl_->core.failed();
}

std::size_t CassandraSink::in_flight() const {
    return impl_->core.in_flight();
}

}  // namespace clink::cassandra
