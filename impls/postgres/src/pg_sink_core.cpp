// The PostgreSQL sinks' shared core. See pg_sink_core.hpp.

#include "clink/postgres/pg_sink_core.hpp"

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

namespace clink::postgres {

namespace {

constexpr const char* kLabel = "postgres";
constexpr const char* kLogComponent = "postgres.sink";
// Retried: the transaction was rolled back and nothing of it stays.
constexpr std::string_view kDeadlock = "40P01";
constexpr std::string_view kSerialization = "40001";
constexpr std::string_view kLockNotAvailable = "55P03";
// ON CONFLICT DO UPDATE was asked to change one row twice in a statement.
constexpr std::string_view kCardinality = "21000";
constexpr std::string_view kQueryCanceled = "57014";

bool retryable(const PgError& e) {
    return e.sqlstate == kDeadlock || e.sqlstate == kSerialization ||
           e.sqlstate == kLockNotAvailable;
}

}  // namespace

// Ends the statement in flight once the task is cancelled, so a cancelled task
// stops at once rather than waiting out a lock. One thread per sink, asleep
// unless a statement is armed.
class PgSinkCore::Watch {
public:
    Watch(PgSinkServer& server, CancelSignal cancel)
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

    PgSinkServer& server_;
    CancelSignal cancel_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool cancelled_{false};
    std::thread thread_;
};

PgSinkCore::PgSinkCore(PgSinkCoreOptions o) : opts_(std::move(o)) {
    if (!opts_.server_factory) {
        opts_.server_factory = [](const std::string& conninfo,
                                  const PgConnectSettings& settings,
                                  const std::string& who) {
            return connect_pg_sink_server(conninfo, settings, who);
        };
    }
}

PgSinkCore::~PgSinkCore() {
    close();
}

void PgSinkCore::open(RuntimeContext* rt) {
    close();
    failure_.clear();
    rt_ = rt;
    server_ = opts_.server_factory(opts_.conninfo, opts_.settings, opts_.name);
    const auto set = [&](const std::string& sql) {
        if (const auto o = server_->exec(sql); o.error) {
            const auto why = describe_(*o.error);
            server_.reset();
            throw std::runtime_error(opts_.name + ": setting the session up failed (" + sql +
                                     "): " + why);
        }
    };
    const auto wait = std::to_string(opts_.lock_wait_timeout.count());
    set("SET TIME ZONE 'UTC'");
    set("SET client_encoding = 'UTF8'");
    set("SET standard_conforming_strings = on");
    set("SET lock_timeout = '" + wait + "s'");
    set("SET statement_timeout = '" + std::to_string(opts_.lock_wait_timeout.count() + 60) + "s'");
    if (!opts_.target.table.empty()) {
        try {
            table_ = server_->describe(opts_.target.table);
        } catch (const std::exception& e) {
            server_.reset();
            throw std::runtime_error(opts_.name + ": " + e.what());
        }
        const auto problems = check_pg_target(opts_.target, table_);
        if (!problems.empty()) {
            std::string why;
            for (const auto& p : problems) {
                why += (why.empty() ? "" : "; ") + p;
            }
            server_.reset();
            throw std::runtime_error(opts_.name + ": cannot write to " + opts_.target.table + ": " +
                                     why);
        }
    }
    if (rt_ != nullptr) {
        watch_ = std::make_unique<Watch>(*server_, rt_->cancel_signal());
    }
    open_ = true;
}

void PgSinkCore::close() noexcept {
    watch_.reset();
    if (server_ && in_transaction_) {
        rollback_();
    }
    in_transaction_ = false;
    server_.reset();
    open_ = false;
}

void PgSinkCore::require_not_failed(std::string_view what) const {
    if (!failure_.empty()) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} +
                                 " refused: the sink failed earlier (" + failure_ +
                                 ") and has not been opened again, so nothing more is written");
    }
}

void PgSinkCore::require_usable(std::string_view what) const {
    require_not_failed(what);
    if (!open_) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} + " before open()");
    }
}

void PgSinkCore::fail(const std::string& why) {
    failure_ = why;
    clink::metrics::connector::error_inc(kLabel, "sink");
    throw std::runtime_error(opts_.name + ": " + why);
}

std::string PgSinkCore::escape(std::string_view value) {
    return server_->escape(value);
}

bool PgSinkCore::cancel_requested_() const noexcept {
    return rt_ != nullptr && rt_->cancel_requested();
}

PgOutcome PgSinkCore::exec_(std::string_view sql) {
    return exec_(PgStatement{std::string{sql}, std::nullopt});
}

PgOutcome PgSinkCore::exec_(const PgStatement& s) {
    if (watch_) {
        watch_->arm();
    }
    auto o = s.params ? server_->exec_params(s.sql, *s.params) : server_->exec(s.sql);
    if (watch_ && watch_->disarm()) {
        // The watch asked the server to end the statement; whatever it
        // answered, the task was cancelled.
        PgError e;
        e.sqlstate = std::string{kQueryCanceled};
        e.message = "the task was cancelled";
        e.lost = o.error ? o.error->lost : false;
        o.error = std::move(e);
    }
    return o;
}

void PgSinkCore::rollback_() noexcept {
    try {
        (void)server_->exec("ROLLBACK");
    } catch (...) {
    }
    in_transaction_ = false;
}

std::string PgSinkCore::describe_(const PgError& e) const {
    return "PostgreSQL error" + (e.sqlstate.empty() ? std::string{} : " " + e.sqlstate) + ": " +
           redact_pg_message(e.message);
}

void PgSinkCore::statement_failed_(const PgError& e, std::string_view during) {
    if (!e.lost) {
        rollback_();
    }
    in_transaction_ = false;
    fail("writing to " +
         (opts_.target.table.empty() ? std::string{"the server"} : opts_.target.table) +
         " failed at " + std::string{during} + ": " + describe_(e) +
         ". Nothing of this flush was committed; the job restarts from its last checkpoint "
         "and writes its records again");
}

void PgSinkCore::pause_(std::chrono::milliseconds d) {
    const auto until = std::chrono::steady_clock::now() + d;
    while (std::chrono::steady_clock::now() < until) {
        if (cancel_requested_()) {
            fail("cancelled while waiting to write again (" + last_retry_ + ")");
        }
        std::this_thread::sleep_for(std::min<std::chrono::steady_clock::duration>(
            std::chrono::milliseconds{20}, until - std::chrono::steady_clock::now()));
    }
}

PgSinkCore::Attempt PgSinkCore::attempt_(const std::vector<PgStatement>& statements,
                                         bool one_row_each) {
    if (const auto o = exec_("BEGIN"); o.error) {
        statement_failed_(*o.error, "BEGIN");
    }
    in_transaction_ = true;
    for (std::size_t i = 0; i < statements.size(); ++i) {
        const auto during =
            "statement " + std::to_string(i + 1) + " of " + std::to_string(statements.size());
        const auto o = exec_(statements[i]);
        if (!o.error) {
            continue;
        }
        const auto& e = *o.error;
        if (!e.lost && !cancel_requested_()) {
            if (retryable(e)) {
                rollback_();
                last_retry_ = describe_(e) + " at " + during;
                return Attempt::Retry;
            }
            if (e.sqlstate == kCardinality && !one_row_each) {
                rollback_();
                return Attempt::OneRowEach;
            }
        }
        statement_failed_(e, during);
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeCommit);
    const auto c = exec_("COMMIT");
    in_transaction_ = false;
    if (c.error) {
        if (c.error->lost || c.error->sqlstate == kQueryCanceled) {
            fail("the reply to the COMMIT of a flush was lost (" + describe_(*c.error) +
                 "): the flush may or may not have been committed. The job restarts from its "
                 "last checkpoint and writes its records again, so a committed flush is written "
                 "twice by an append sink and rewritten by key otherwise");
        }
        fail("the server refused the COMMIT of a flush (" + describe_(*c.error) +
             "): nothing of it was committed; the job restarts from its last checkpoint and "
             "writes its records again");
    }
    if (c.tag != "COMMIT") {
        fail("the server answered the COMMIT of a flush with " + c.tag +
             ": nothing of it was committed; the job restarts from its last checkpoint and "
             "writes its records again");
    }
    return Attempt::Done;
}

void PgSinkCore::write(const Plan& plan, std::size_t records, std::size_t bytes) {
    require_usable("a write");
    const auto t0 = std::chrono::steady_clock::now();
    try {
        bool one_row_each = false;
        auto statements = plan(false);
        if (statements.empty()) {
            return;
        }
        for (int attempt = 0;;) {
            if (cancel_requested_()) {
                fail("cancelled before writing");
            }
            const auto a = attempt_(statements, one_row_each);
            if (a == Attempt::Done) {
                break;
            }
            if (a == Attempt::OneRowEach) {
                one_row_each = true;
                statements = plan(true);
                continue;
            }
            if (attempt >= opts_.max_retries) {
                fail("writing gave up after " + std::to_string(opts_.max_retries) + " retries (" +
                     last_retry_ +
                     "). Nothing of this flush was committed; the job restarts from its last "
                     "checkpoint and writes its records again");
            }
            const auto backoff = std::chrono::milliseconds{
                std::min<long long>(5000, 100LL << std::min(attempt, 10))};
            ++attempt;
            clink::log::warn(kLogComponent,
                             opts_.name + ": " + last_retry_ + "; trying the flush again in " +
                                 std::to_string(backoff.count()) + " ms (retry " +
                                 std::to_string(attempt) + " of " +
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
            fail(std::string{"writing failed: "} + e.what());
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

}  // namespace clink::postgres
