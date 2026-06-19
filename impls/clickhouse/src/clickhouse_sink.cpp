#include "clink/connectors/clickhouse_sink.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"

#ifdef CLINK_HAS_CLICKHOUSE
#include <clickhouse/client.h>
#endif

namespace clink {

#ifdef CLINK_HAS_CLICKHOUSE

struct ClickHouseSink::Impl {
    Options opts;
    std::unique_ptr<clickhouse::Client> client;
    std::vector<std::string> buffer;
    std::chrono::steady_clock::time_point last_flush{std::chrono::steady_clock::now()};
};

bool ClickHouseSink::is_real_implementation() {
    return true;
}

ClickHouseSink::ClickHouseSink(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

ClickHouseSink::~ClickHouseSink() = default;

void ClickHouseSink::open() {
    clickhouse::ClientOptions co;
    co.SetHost(impl_->opts.host)
        .SetPort(impl_->opts.port)
        .SetDefaultDatabase(impl_->opts.database)
        .SetUser(impl_->opts.user)
        .SetPassword(impl_->opts.password);
    impl_->client = std::make_unique<clickhouse::Client>(co);
}

namespace {

void flush_buffer(clickhouse::Client& client,
                  const std::string& db,
                  const std::string& table,
                  ClickHouseSink::Format format,
                  std::vector<std::string>& buffer) {
    if (buffer.empty()) {
        return;
    }
    // Concatenate the rows into a single payload separated by newlines.
    std::string body;
    body.reserve(buffer.size() * 32);
    for (const auto& r : buffer) {
        body.append(r);
        body.push_back('\n');
    }
    const std::string fmt = (format == ClickHouseSink::Format::TSV) ? "TSV" : "JSONEachRow";
    const std::string query = "INSERT INTO " + db + "." + table + " FORMAT " + fmt + "\n" + body;
    client.Execute(query);
    buffer.clear();
}

}  // namespace

void ClickHouseSink::on_data(const Batch<std::string>& batch) {
    std::uint64_t bytes_written = 0;
    for (const auto& r : batch) {
        bytes_written += r.value().size();
        impl_->buffer.push_back(r.value());
        if (impl_->buffer.size() >= impl_->opts.batch_rows) {
            const auto t0 = std::chrono::steady_clock::now();
            try {
                flush_buffer(*impl_->client,
                             impl_->opts.database,
                             impl_->opts.table,
                             impl_->opts.format,
                             impl_->buffer);
            } catch (...) {
                clink::metrics::connector::error_inc("clickhouse");
                throw;
            }
            const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            clink::metrics::connector::commit_latency_observe("clickhouse",
                                                              static_cast<std::uint64_t>(dt));
            impl_->last_flush = std::chrono::steady_clock::now();
        }
    }
    const auto now = std::chrono::steady_clock::now();
    if (now - impl_->last_flush >= impl_->opts.batch_interval && !impl_->buffer.empty()) {
        const auto t0 = std::chrono::steady_clock::now();
        try {
            flush_buffer(*impl_->client,
                         impl_->opts.database,
                         impl_->opts.table,
                         impl_->opts.format,
                         impl_->buffer);
        } catch (...) {
            clink::metrics::connector::error_inc("clickhouse");
            throw;
        }
        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        clink::metrics::connector::commit_latency_observe("clickhouse",
                                                          static_cast<std::uint64_t>(dt));
        impl_->last_flush = now;
    }
    clink::metrics::connector::records_out_inc("clickhouse", batch.size());
    clink::metrics::connector::bytes_out_inc("clickhouse", bytes_written);
}

void ClickHouseSink::flush() {
    if (impl_ && impl_->client) {
        flush_buffer(*impl_->client,
                     impl_->opts.database,
                     impl_->opts.table,
                     impl_->opts.format,
                     impl_->buffer);
    }
}

void ClickHouseSink::close() {
    flush();
    if (impl_) {
        impl_->client.reset();
    }
}

#else

struct ClickHouseSink::Impl {};

bool ClickHouseSink::is_real_implementation() {
    return false;
}

ClickHouseSink::ClickHouseSink(Options /*opts*/) {
    throw std::runtime_error(
        "ClickHouseSink: built without clickhouse-cpp. Install it and "
        "reconfigure cmake - find_package(clickhouse-cpp) must succeed.");
}

ClickHouseSink::~ClickHouseSink() = default;
void ClickHouseSink::open() {}
void ClickHouseSink::on_data(const Batch<std::string>& /*batch*/) {}
void ClickHouseSink::flush() {}
void ClickHouseSink::close() {}

#endif

}  // namespace clink
