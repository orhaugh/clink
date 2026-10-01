#include "clink/connectors/clickhouse_sink.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "native/sql_text.hpp"

#ifdef CLINK_HAS_CLICKHOUSE
#include <clickhouse/client.h>
#endif

namespace clink {

namespace {

using Clock = std::chrono::steady_clock;

constexpr const char* kConnector = "clickhouse";

std::uint64_t nanos_since(Clock::time_point start) {
    const auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
    return ns > 0 ? static_cast<std::uint64_t>(ns) : 0;
}

const char* format_name(ClickHouseSink::Format format) {
    return format == ClickHouseSink::Format::JSONEachRow ? "JSONEachRow" : "TSV";
}

}  // namespace

struct ClickHouseSink::Impl {
    Options opts;
#ifdef CLINK_HAS_CLICKHOUSE
    std::unique_ptr<::clickhouse::Client> client;
#endif
    // Drawn at open, so no token of this run can repeat one of an earlier run,
    // whose batches may already sit in the target's deduplication log.
    std::optional<clickhouse::native::TokenSource> tokens;
    bool connected{false};
    std::vector<std::string> buffer;
    std::uint64_t buffered_bytes{0};
    Clock::time_point last_flush{Clock::now()};
};

#ifdef CLINK_HAS_CLICKHOUSE

bool ClickHouseSink::is_real_implementation() {
    return true;
}

ClickHouseSink::ClickHouseSink(Options opts) : impl_(std::make_unique<Impl>()) {
    impl_->opts = std::move(opts);
}

void ClickHouseSink::connect() {
    const auto& o = impl_->opts;
    ::clickhouse::ClientOptions co;
    co.SetHost(o.host)
        .SetPort(o.port)
        .SetDefaultDatabase(o.database)
        .SetUser(o.user)
        .SetPassword(o.password)
        .SetConnectionConnectTimeout(o.connect_timeout)
        .SetConnectionSendTimeout(o.send_timeout)
        .SetConnectionRecvTimeout(o.receive_timeout);
    impl_->client = std::make_unique<::clickhouse::Client>(co);
}

void ClickHouseSink::send_insert(const std::string& statement) {
    if (!impl_->client) {
        throw std::logic_error("clickhouse_sink: no connection to send the INSERT on");
    }
    impl_->client->Execute(statement);
}

#else

bool ClickHouseSink::is_real_implementation() {
    return false;
}

ClickHouseSink::ClickHouseSink(Options /*opts*/) {
    throw std::runtime_error(
        "ClickHouseSink: built without clickhouse-cpp. Install it and "
        "reconfigure cmake - find_package(clickhouse-cpp) must succeed.");
}

void ClickHouseSink::connect() {}
void ClickHouseSink::send_insert(const std::string& /*statement*/) {}

#endif

ClickHouseSink::~ClickHouseSink() = default;

const ClickHouseSink::Options& ClickHouseSink::options() const noexcept {
    return impl_->opts;
}

void ClickHouseSink::open() {
    impl_->connected = false;
    impl_->tokens.emplace(clickhouse::native::TokenSource::random());
    if (!impl_->opts.unrecognised_format.empty()) {
        warn("format '" + impl_->opts.unrecognised_format +
             "' is not one of tsv, json or jsoneachrow; the rows are sent as TSV");
    }
    connect();
    impl_->connected = true;
    impl_->last_flush = Clock::now();
}

void ClickHouseSink::on_data(const Batch<std::string>& batch) {
    auto& im = *impl_;
    for (const auto& r : batch) {
        im.buffered_bytes += r.value().size();
        im.buffer.push_back(r.value());
        if (im.buffer.size() >= im.opts.batch_rows) {
            flush_with_metrics();
        }
    }
    if (!im.buffer.empty() && Clock::now() - im.last_flush >= im.opts.batch_interval) {
        flush_with_metrics();
    }
}

void ClickHouseSink::flush_with_metrics() {
    auto& im = *impl_;
    if (im.buffer.empty()) {
        return;
    }
    if (!im.connected) {
        throw std::logic_error("clickhouse_sink: rows are buffered but open() has not completed");
    }
    // A fresh token for every statement, a resend after a failure included. A
    // resend may carry rows that arrived since the failure, and if the first
    // attempt did land, the server would drop the whole resend under the old
    // token as its duplicate, those rows with it.
    const auto token = im.tokens->next();
    std::string statement = clickhouse::native::legacy_insert_prefix(
        im.opts.database, im.opts.table, token, format_name(im.opts.format));
    statement.reserve(statement.size() + 1 + im.buffered_bytes + im.buffer.size());
    statement.push_back('\n');
    for (const auto& row : im.buffer) {
        statement.append(row);
        statement.push_back('\n');
    }
    const auto start = Clock::now();
    try {
        send_insert(statement);
    } catch (...) {
        metrics::connector::error_inc(kConnector);
        throw;
    }
    // Counted only now: rows the server has not acknowledged were not written,
    // and a failed flush keeps them buffered for the next attempt.
    metrics::connector::commit_latency_observe(kConnector, nanos_since(start));
    metrics::connector::records_out_inc(kConnector, im.buffer.size());
    metrics::connector::bytes_out_inc(kConnector, im.buffered_bytes);
    im.buffer.clear();
    im.buffered_bytes = 0;
    im.last_flush = Clock::now();
}

void ClickHouseSink::flush() {
    flush_with_metrics();
}

void ClickHouseSink::close() {
    flush();
    impl_->connected = false;
#ifdef CLINK_HAS_CLICKHOUSE
    impl_->client.reset();
#endif
}

void ClickHouseSink::warn(const std::string& message) const {
    if (const auto* rt = runtime(); rt != nullptr) {
        rt->log_warn(message);
        return;
    }
    clink::logging::op_log(nullptr, LogSeverity::Warn, name(), message);
}

}  // namespace clink
