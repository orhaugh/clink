// The MySQL sinks' shared core. See mysql_sink_core.hpp.

#include "clink/mysql/mysql_sink_core.hpp"

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "fault_points.hpp"

namespace clink::mysql {

namespace {

constexpr const char* kLabel = "mysql";
constexpr const char* kLogComponent = "mysql.sink";
constexpr unsigned kLockWaitTimeout = 1205;  // ER_LOCK_WAIT_TIMEOUT
constexpr unsigned kDeadlock = 1213;         // ER_LOCK_DEADLOCK
// Room max_allowed_packet keeps for the packet header and anything else.
constexpr std::uint64_t kPacketSlack = 1024;

// MySQL 8.0's defaults with strict mode for every table, so a value the column
// cannot hold fails the statement rather than land converted.
constexpr std::string_view kSqlMode =
    "STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
    "NO_ENGINE_SUBSTITUTION";

}  // namespace

// Ends the statement in flight once the task is cancelled, so a cancelled task
// stops at once rather than waiting out a lock wait. One thread per sink,
// asleep unless a statement is armed.
class SinkCore::Watch {
public:
    Watch(MysqlSinkServer& server, CancelSignal cancel)
        : server_(server), cancel_(std::move(cancel)), thread_([this] { run(); }) {}
    ~Watch() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }
    Watch(const Watch&) = delete;
    Watch& operator=(const Watch&) = delete;

    void arm() {
        {
            const std::lock_guard<std::mutex> lock(mu_);
            armed_ = true;
        }
        cv_.notify_all();
    }
    // True when the watch ended the statement armed last.
    bool disarm() {
        const std::lock_guard<std::mutex> lock(mu_);
        armed_ = false;
        return cancelled_;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stop_) {
            if (!armed_ || cancelled_) {
                cv_.wait(lock, [this] { return stop_ || (armed_ && !cancelled_); });
                continue;
            }
            cv_.wait_for(lock, std::chrono::milliseconds{20});
            if (stop_ || !armed_ || !cancel_.requested()) {
                continue;
            }
            cancelled_ = true;
            lock.unlock();
            server_.cancel();
            lock.lock();
        }
    }

    MysqlSinkServer& server_;
    CancelSignal cancel_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool cancelled_{false};
    std::thread thread_;
};

SinkCore::SinkCore(SinkCoreOptions o) : opts_(std::move(o)) {
    if (!opts_.server_factory) {
        opts_.server_factory = [](const ConnectOptions& c) { return connect_mysql_sink_server(c); };
    }
}

SinkCore::~SinkCore() {
    close();
}

void SinkCore::open(RuntimeContext* rt) {
    close();
    failure_.clear();
    rt_ = rt;
    auto conn = opts_.conn;
    // A statement waits for locks at most lock_wait_timeout; a reply later than
    // that by a margin means the server or the network went away.
    const auto io = opts_.lock_wait_timeout + std::chrono::seconds{60};
    conn.read_timeout = io;
    conn.write_timeout = io;
    server_ = opts_.server_factory(conn);
    const auto set = [&](const std::string& sql) {
        if (auto e = server_->exec(sql)) {
            throw std::runtime_error(opts_.name + ": setting the session up failed (" + sql +
                                     "): " + describe_(*e));
        }
    };
    std::string mode{kSqlMode};
    if (opts_.target.mode == SinkMode::Changelog) {
        // A key of 0 in an AUTO_INCREMENT column is that key, not a new one.
        mode += ",NO_AUTO_VALUE_ON_ZERO";
    }
    set("SET SESSION time_zone = '+00:00', sql_mode = '" + mode + "'");
    const auto wait = std::to_string(opts_.lock_wait_timeout.count());
    set("SET SESSION innodb_lock_wait_timeout = " + wait + ", lock_wait_timeout = " + wait);
    schema_ = server_->describe(opts_.target.table);
    const auto problems = check_target(opts_.target, schema_);
    if (!problems.empty()) {
        std::string why;
        for (const auto& p : problems) {
            why += (why.empty() ? "" : "; ") + p;
        }
        server_.reset();
        throw std::runtime_error(opts_.name + ": cannot write to " + opts_.target.table + ": " +
                                 why);
    }
    const auto packet = server_->max_allowed_packet();
    statement_bytes_ = static_cast<std::size_t>(std::min<std::uint64_t>(
        opts_.max_statement_bytes, packet > kPacketSlack ? packet - kPacketSlack : packet));
    if (rt_ != nullptr) {
        watch_ = std::make_unique<Watch>(*server_, rt_->cancel_signal());
    }
    open_ = true;
}

void SinkCore::close() noexcept {
    watch_.reset();
    if (server_ && in_transaction_) {
        rollback_();
    }
    in_transaction_ = false;
    server_.reset();
    open_ = false;
}

void SinkCore::require_not_failed(std::string_view what) const {
    if (!failure_.empty()) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} +
                                 " refused: the sink failed earlier (" + failure_ +
                                 ") and has not been opened again, so nothing more is written");
    }
}

void SinkCore::require_usable(std::string_view what) const {
    require_not_failed(what);
    if (!open_) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} + " before open()");
    }
}

void SinkCore::fail(const std::string& why) {
    failure_ = why;
    clink::metrics::connector::error_inc(kLabel, "sink");
    throw std::runtime_error(opts_.name + ": " + why);
}

std::string SinkCore::escape(std::string_view value) {
    return server_->escape(value);
}

bool SinkCore::cancel_requested_() const noexcept {
    return rt_ != nullptr && rt_->cancel_requested();
}

std::optional<ServerError> SinkCore::exec_(std::string_view sql) {
    if (watch_) {
        watch_->arm();
    }
    auto e = server_->exec(sql);
    if (watch_ && watch_->disarm()) {
        // The watch ended the statement: it is unusable now, and so is the
        // connection.
        if (!e) {
            e = ServerError{2013, "HY000", "cancelled"};
        }
        e->message = "the task was cancelled";
        e->code = 2013;
    }
    return e;
}

void SinkCore::rollback_() noexcept {
    try {
        (void)server_->exec("ROLLBACK");
    } catch (...) {
    }
    in_transaction_ = false;
}

std::string SinkCore::describe_(const ServerError& e) const {
    return "MySQL error " + std::to_string(e.code) +
           (e.sqlstate.empty() ? std::string{} : " (" + e.sqlstate + ")") + ": " +
           redact_server_message(e.message);
}

std::string SinkCore::left_behind_() const {
    return schema_.transactional ? "Nothing of this flush was committed"
                                 : "What the flush's statements wrote before it stays: the "
                                   "table's engine has no transactions";
}

void SinkCore::statement_failed_(const ServerError& e, std::string_view during) {
    const bool lost = e.lost();
    if (!lost) {
        rollback_();
    }
    in_transaction_ = false;
    fail("writing to " + opts_.target.table + " failed at " + std::string{during} + ": " +
         describe_(e) + ". " + left_behind_() +
         "; the job restarts from its last checkpoint and writes its records again");
}

void SinkCore::pause_(std::chrono::milliseconds d) {
    const auto until = std::chrono::steady_clock::now() + d;
    while (std::chrono::steady_clock::now() < until) {
        if (cancel_requested_()) {
            fail("cancelled while waiting to write to " + opts_.target.table + " again (" +
                 last_retry_ + ")");
        }
        std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(
            std::chrono::milliseconds{20}, until - std::chrono::steady_clock::now()));
    }
}

SinkCore::Attempt SinkCore::attempt_(const std::vector<std::string>& statements) {
    if (auto e = exec_("START TRANSACTION")) {
        statement_failed_(*e, "START TRANSACTION");
    }
    in_transaction_ = true;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        const auto during =
            "statement " + std::to_string(i + 1) + " of " + std::to_string(statements.size());
        if (auto e = exec_(statements[i])) {
            // Only a rollback makes trying again safe: without transactions the
            // statements before this one stay written.
            if ((e->code == kDeadlock || e->code == kLockWaitTimeout) && schema_.transactional &&
                !cancel_requested_()) {
                rollback_();
                last_retry_ = describe_(*e) + " at " + during;
                return Attempt::Retry;
            }
            statement_failed_(*e, during);
        }
        const auto warnings = server_->warnings();
        if (!warnings.empty()) {
            rollback_();
            std::string listed;
            for (const auto& w : warnings) {
                listed += (listed.empty() ? "" : "; ") + w.level + " " + std::to_string(w.code) +
                          ": " + redact_server_message(w.message);
            }
            fail("writing to " + opts_.target.table + " raised " + std::to_string(warnings.size()) +
                 " warning(s) at " + during + " (" + listed +
                 "): a value the declared column does not hold as written. " + left_behind_());
        }
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeCommit);
    if (auto e = exec_("COMMIT")) {
        in_transaction_ = false;
        if (e->lost()) {
            fail("the reply to the COMMIT of a flush to " + opts_.target.table + " was lost (" +
                 describe_(*e) +
                 "): the flush may or may not have been committed. The job restarts from its "
                 "last checkpoint and writes its records again, so a committed flush is written "
                 "twice by an append sink and rewritten by key otherwise");
        }
        fail("the server refused the COMMIT of a flush to " + opts_.target.table + " (" +
             describe_(*e) +
             "): nothing of it was committed; the job restarts from its last checkpoint and "
             "writes its records again");
    }
    in_transaction_ = false;
    return Attempt::Done;
}

void SinkCore::write(const std::vector<std::string>& statements,
                     std::size_t records,
                     std::size_t bytes) {
    require_usable("a write");
    if (statements.empty()) {
        return;
    }
    const auto t0 = std::chrono::steady_clock::now();
    try {
        for (int attempt = 0;; ++attempt) {
            if (cancel_requested_()) {
                fail("cancelled before writing to " + opts_.target.table);
            }
            if (attempt_(statements) == Attempt::Done) {
                break;
            }
            if (attempt >= opts_.max_retries) {
                fail("writing to " + opts_.target.table + " gave up after " +
                     std::to_string(opts_.max_retries) + " retries (" + last_retry_ +
                     "). Nothing of this flush was committed; the job restarts from its last "
                     "checkpoint and writes its records again");
            }
            const auto backoff = std::chrono::milliseconds{
                std::min<long long>(5000, 100LL << std::min(attempt, 10))};
            clink::log::warn(kLogComponent,
                             opts_.name + ": " + last_retry_ + "; trying the flush again in " +
                                 std::to_string(backoff.count()) + " ms (retry " +
                                 std::to_string(attempt + 1) + " of " +
                                 std::to_string(opts_.max_retries) + ")");
            pause_(backoff);
        }
    } catch (const std::exception& e) {
        // Whatever ended the write fails the sink, after rolling back what it
        // left open.
        if (failure_.empty()) {
            if (in_transaction_) {
                rollback_();
            }
            fail("writing to " + opts_.target.table + " failed: " + e.what());
        }
        throw;
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::records_out_inc(kLabel, records);
    clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

}  // namespace clink::mysql
