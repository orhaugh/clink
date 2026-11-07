// The Cassandra sinks' shared core: open and the table check, the bounded window
// of writes in flight, timestamps, the failed state, cancellation and metrics.

#include "sink_core.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/logging.hpp"

namespace clink::cassandra::detail {

namespace {

constexpr char kLabel[] = "cassandra";
constexpr char kLogComponent[] = "cassandra.sink";
// How long one wait for a write lasts before the task's cancel signal is read.
constexpr std::chrono::microseconds kTick{50'000};

clink::Counter& submitted_counter() {
    static auto& c = clink::MetricsRegistry::global().counter(
        clink::metrics::connector_metric_name("writes_submitted_total", kLabel, "sink"));
    return c;
}

clink::Gauge& in_flight_gauge() {
    static auto& g = clink::MetricsRegistry::global().gauge(
        clink::metrics::connector_metric_name("writes_in_flight", kLabel, "sink"));
    return g;
}

// Not quoted(): libstdc++'s std::quoted would win it by argument-dependent lookup.
std::string cql_quoted(const std::string& name) {
    return "\"" + name + "\"";
}

std::string names(const std::vector<CassandraColumnMeta>& cols) {
    std::string out;
    for (const auto& c : cols) {
        out += (out.empty() ? "" : ", ") + c.name;
    }
    return out.empty() ? "none" : out;
}

std::string described(const CassandraOutcome& o) {
    std::string out = o.error.empty() ? std::string{"error"} : o.error;
    if (!o.message.empty()) {
        out += ": " + redact_cassandra_message(o.message);
    }
    return out;
}

const char* meaning(CassandraFault f) {
    switch (f) {
        case CassandraFault::NotApplied:
            return "no replica wrote it: the server refused it first, or no host could be "
                   "reached to send it";
        case CassandraFault::Ambiguous:
            return "it may or may not have been applied";
        case CassandraFault::Permanent:
            return "the server refused the request itself, and would again";
        case CassandraFault::Cancelled:
            return "the task was cancelled";
        case CassandraFault::None:
            break;
    }
    return "";
}

}  // namespace

SinkCoreOptions checked_options(const std::string& name,
                                const CassandraConnParams& conn,
                                const std::string& keyspace,
                                const std::string& table,
                                const std::vector<std::string>& key_columns,
                                bool keyed,
                                std::size_t max_in_flight,
                                std::size_t max_in_flight_bytes,
                                std::shared_ptr<CassandraWriteService> service) {
    const auto refuse = [&name](const std::string& why) {
        throw std::runtime_error(name + ": " + why);
    };
    const auto identifier =
        [&refuse](const char* what, const std::string& value, std::size_t max_length) {
            if (value.empty()) {
                refuse(std::string{"'"} + what + "' is required");
            }
            std::string why;
            auto id = cql_identifier(value, max_length, &why);
            if (!id) {
                refuse(std::string{"'"} + what + "' (" + value +
                       ") is not a name the sink accepts: " + why);
            }
            return *id;
        };
    SinkCoreOptions o;
    o.name = name;
    o.conn = conn;
    o.keyspace = identifier("keyspace", keyspace, kCqlMaxTableNameLength);
    o.table = identifier("table", table, kCqlMaxTableNameLength);
    if (keyed) {
        if (key_columns.empty()) {
            refuse("'key_columns' (the table's primary key) is required");
        }
        for (const auto& k : key_columns) {
            o.key_columns.push_back(identifier("key_columns", k, kCqlMaxColumnNameLength));
        }
    } else if (!key_columns.empty()) {
        refuse("'key_columns' applies to the upsert sink only");
    }
    if (conn.contact_points.find_first_not_of(" ,") == std::string::npos) {
        refuse("'contact_points' names no host");
    }
    if (conn.port < 1 || conn.port > 65535) {
        refuse("'port' must be 1 to 65535; it is " + std::to_string(conn.port));
    }
    if (conn.connect_timeout_ms < 1 || conn.connect_timeout_ms > kMaxTimeoutMs) {
        refuse("'connect_timeout_ms' must be 1 to " + std::to_string(kMaxTimeoutMs) + "; it is " +
               std::to_string(conn.connect_timeout_ms));
    }
    if (conn.request_timeout_ms < 1 || conn.request_timeout_ms > kMaxTimeoutMs) {
        refuse("'request_timeout_ms' must be 1 to " + std::to_string(kMaxTimeoutMs) + "; it is " +
               std::to_string(conn.request_timeout_ms));
    }
    if (conn.username.empty() != conn.password.empty()) {
        refuse(
            "'username' and 'password' go together: set both, or neither for a cluster "
            "without authentication");
    }
    if (max_in_flight < 1 || max_in_flight > kMaxInFlightLimit) {
        refuse("'max_in_flight' must be 1 to " + std::to_string(kMaxInFlightLimit) + "; it is " +
               std::to_string(max_in_flight));
    }
    if (max_in_flight_bytes < kMinBytesBound || max_in_flight_bytes > kMaxBytesBound) {
        refuse("'max_in_flight_bytes' must be " + std::to_string(kMinBytesBound) + " to " +
               std::to_string(kMaxBytesBound) + "; it is " + std::to_string(max_in_flight_bytes));
    }
    o.max_in_flight = max_in_flight;
    o.max_in_flight_bytes = max_in_flight_bytes;
    o.service = std::move(service);
    return o;
}

SinkCore::SinkCore(SinkCoreOptions opts) : opts_(std::move(opts)) {}

SinkCore::~SinkCore() {
    close();
}

void SinkCore::open(RuntimeContext* rt) {
    if (open_ && !failure_) {
        throw std::runtime_error(opts_.name +
                                 ": open() on a sink that is already open; close() "
                                 "it first");
    }
    close();
    failure_.reset();
    key_.clear();
    last_timestamp_ = 0;
    submitted_ = 0;
    acknowledged_ = 0;
    service_ = opts_.service ? opts_.service : make_cassandra_driver_service();

    const auto cancelled = [rt] { return rt != nullptr && rt->cancel_requested(); };
    const std::string where =
        opts_.conn.contact_points + " port " + std::to_string(opts_.conn.port);
    const auto refuse = [this](const std::string& why) {
        service_->close();
        throw std::runtime_error(opts_.name + ": " + why);
    };

    if (const auto o = service_->connect(opts_.conn, cancelled); !o.ok()) {
        refuse("cannot connect to " + where + ": " + described(o));
    }
    const auto meta = service_->describe_table(opts_.keyspace, opts_.table);
    if (!meta.error.ok()) {
        refuse("cannot read the schema: " + described(meta.error));
    }
    if (!meta.keyspace_found) {
        refuse("keyspace '" + opts_.keyspace +
               "' does not exist (the sink does not create keyspaces or tables)");
    }
    if (!meta.table_found) {
        refuse("table '" + opts_.keyspace + "." + opts_.table +
               "' does not exist (the sink does not create keyspaces or tables)");
    }

    if (!opts_.key_columns.empty()) {
        std::vector<CassandraColumnMeta> table_key = meta.partition_key;
        table_key.insert(table_key.end(), meta.clustering_key.begin(), meta.clustering_key.end());
        std::set<std::string> given;
        for (const auto& k : opts_.key_columns) {
            if (!given.insert(k).second) {
                refuse("key_columns names '" + k + "' twice");
            }
        }
        std::set<std::string> wanted;
        for (const auto& c : table_key) {
            wanted.insert(c.name);
        }
        if (given != wanted) {
            std::string list;
            for (const auto& k : opts_.key_columns) {
                list += (list.empty() ? "" : ", ") + k;
            }
            refuse("key_columns (" + list + ") must name the table's whole primary key, its " +
                   "partition key (" + names(meta.partition_key) + ") and clustering columns (" +
                   names(meta.clustering_key) +
                   "): a delete by part of it would remove other rows, and records differing "
                   "only outside it would be taken for one row");
        }
        for (const auto& c : table_key) {
            const auto type = cql_key_type(c.type);
            if (!type) {
                refuse("the key column '" + c.name + "' is of type " + c.type +
                       ", which the upsert sink does not accept in a key (frozen collections, "
                       "tuples, user-defined and custom types are not supported)");
            }
            key_.push_back(CassandraKeyColumn{c.name, *type});
        }
    }

    const std::string table = cql_quoted(opts_.keyspace) + "." + cql_quoted(opts_.table);
    const std::string insert_cql = "INSERT INTO " + table + " JSON ?";
    std::string delete_cql;
    if (!key_.empty()) {
        delete_cql = "DELETE FROM " + table + " WHERE ";
        for (std::size_t i = 0; i < key_.size(); ++i) {
            delete_cql += (i == 0 ? "" : " AND ") + cql_quoted(key_[i].name) + " = fromJson(?)";
        }
    }
    if (const auto o = service_->prepare(insert_cql, delete_cql, cancelled); !o.ok()) {
        refuse("cannot prepare the sink's statements for " + opts_.keyspace + "." + opts_.table +
               ": " + described(o));
    }
    open_ = true;
    clink::log::info(kLogComponent,
                     opts_.name + ": writing to " + opts_.keyspace + "." + opts_.table + " at " +
                         std::string{cassandra_consistency_name(opts_.conn.consistency)} +
                         " (in flight at most " + std::to_string(opts_.max_in_flight) +
                         " writes, " + std::to_string(opts_.max_in_flight_bytes) + " bytes)");
}

void SinkCore::check_usable() const {
    if (failure_) {
        throw std::runtime_error(opts_.name +
                                 ": the sink failed earlier and dropped writes, so it refuses to "
                                 "write or acknowledge a barrier (" +
                                 *failure_ + ")");
    }
    if (!open_) {
        throw std::runtime_error(opts_.name + ": the sink is not open");
    }
}

void SinkCore::submit(RuntimeContext* rt, CassandraWrite write, std::size_t bytes) {
    check_usable();
    while (!window_.empty() && settle_oldest(std::chrono::microseconds{0})) {
    }
    while (window_.size() >= opts_.max_in_flight ||
           (!window_.empty() && window_bytes_ + bytes > opts_.max_in_flight_bytes)) {
        if (!settle_oldest(kTick)) {
            check_cancel(rt, "waiting for room among the writes in flight");
        }
    }
    write.timestamp_us = next_timestamp();
    const std::uint64_t ticket = service_->submit(write);
    window_.push_back(InFlight{ticket, bytes, std::chrono::steady_clock::now()});
    window_bytes_ += bytes;
    ++submitted_;
    submitted_counter().increment();
    in_flight_gauge().add(1);
}

void SinkCore::drain(RuntimeContext* rt) {
    check_usable();
    if (window_.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    while (!window_.empty()) {
        if (!settle_oldest(kTick)) {
            check_cancel(rt, "waiting for the writes in flight");
        }
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

bool SinkCore::settle_oldest(std::chrono::microseconds wait) {
    const InFlight front = window_.front();
    const auto outcome = service_->poll(front.ticket, wait);
    if (!outcome) {
        return false;
    }
    service_->release(front.ticket);
    window_.pop_front();
    window_bytes_ -= front.bytes;
    in_flight_gauge().add(-1);
    if (!outcome->ok()) {
        fail("a write was not acknowledged (" + described(*outcome) + "; " +
             meaning(outcome->fault) + ")");
    }
    ++acknowledged_;
    clink::metrics::connector::records_out_inc(kLabel, 1);
    clink::metrics::connector::bytes_out_inc(kLabel, front.bytes);
    return true;
}

void SinkCore::check_cancel(RuntimeContext* rt, const char* doing) {
    if (rt != nullptr && rt->cancel_requested()) {
        fail(std::string{"cancelled while "} + doing);
    }
}

void SinkCore::fail(const std::string& why, std::size_t dropped_buffered) {
    const std::size_t dropped = window_.size();
    release_window();
    std::string message = why;
    if (dropped > 0) {
        message += "; " + std::to_string(dropped) +
                   " write(s) still in flight are given up with this sink instance, their "
                   "outcome unknown";
    }
    dropped_buffered = std::max(dropped_buffered, buffered_);
    buffered_ = 0;
    if (dropped_buffered > 0) {
        message += "; " + std::to_string(dropped_buffered) +
                   " buffered row(s) not yet written are dropped with it";
    }
    message += ". Since it opened, the sink submitted " + std::to_string(submitted_) +
               " write(s) and the server acknowledged " + std::to_string(acknowledged_) +
               ". It accepts nothing more, and the job's restart replays every record after the "
               "last completed checkpoint.";
    failure_ = message;
    clink::metrics::connector::error_inc(kLabel, "sink");
    clink::log::error(kLogComponent, opts_.name + ": " + message);
    throw std::runtime_error(opts_.name + ": " + message);
}

void SinkCore::reach(const char* suffix) const {
    (void)CLINK_FAULT_POINT(opts_.name + "." + suffix);
}

void SinkCore::close() noexcept {
    if (!window_.empty()) {
        try {
            clink::log::warn(kLogComponent,
                             opts_.name + ": closing with " + std::to_string(window_.size()) +
                                 " write(s) in flight; their outcomes are not awaited, and no "
                                 "barrier after the last one acknowledged covers them");
        } catch (...) {  // NOLINT(bugprone-empty-catch) a log line must not stop the close
        }
        release_window();
    }
    if (service_ && open_) {
        service_->close();  // a failed open has closed it already
    }
    open_ = false;
}

std::int64_t SinkCore::next_timestamp() {
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    last_timestamp_ = std::max<std::int64_t>(now, last_timestamp_ + 1);
    return last_timestamp_;
}

void SinkCore::release_window() noexcept {
    for (const auto& w : window_) {
        service_->release(w.ticket);
    }
    in_flight_gauge().add(-static_cast<std::int64_t>(window_.size()));
    window_.clear();
    window_bytes_ = 0;
}

}  // namespace clink::cassandra::detail
