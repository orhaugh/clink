// The Redis sinks' shared core. See redis_sink_core.hpp.

#include "clink/redis/redis_sink_core.hpp"

#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"

#include "fault_points.hpp"

namespace clink::redis {

namespace {

constexpr const char* kLabel = "redis";

class RealRedisServer final : public RedisSinkServer {
public:
    explicit RealRedisServer(const ConnectOptions& o) : conn_(o) {}

    void append(const std::vector<std::string>& args) override { conn_.append(args); }

    RedisReplyView reply() override {
        if (lost_) {
            return RedisReplyView{RedisReplyView::Kind::Lost, lost_why_};
        }
        try {
            const Reply r = conn_.get_reply();
            if (r.is_error()) {
                return RedisReplyView{RedisReplyView::Kind::Error, r.error_text()};
            }
            return RedisReplyView{};
        } catch (const std::exception& e) {
            lost_ = true;
            lost_why_ = e.what();
            return RedisReplyView{RedisReplyView::Kind::Lost, lost_why_};
        }
    }

    void cancel() noexcept override { conn_.shutdown_socket(); }

private:
    Connection conn_;
    bool lost_{false};
    std::string lost_why_;
};

}  // namespace

std::unique_ptr<RedisSinkServer> connect_redis_sink_server(const ConnectOptions& o) {
    return std::make_unique<RealRedisServer>(o);
}

// Shuts the socket once the task is cancelled, so a cancelled flush stops at
// once rather than waiting out the command timeout. One thread per sink, asleep
// unless a flush is armed.
class RedisSinkCore::Watch {
public:
    Watch(RedisSinkServer& server, CancelSignal cancel)
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
    // True when the watch shut the socket while armed.
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

    RedisSinkServer& server_;
    CancelSignal cancel_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_{false};
    bool armed_{false};
    bool cancelled_{false};
    std::thread thread_;
};

RedisSinkCore::RedisSinkCore(RedisSinkCoreOptions o) : opts_(std::move(o)) {
    if (opts_.conn.command_timeout.count() <= 0) {
        opts_.conn.command_timeout = kDefaultCommandTimeout;
    }
    if (!opts_.server_factory) {
        opts_.server_factory = [](const ConnectOptions& c) { return connect_redis_sink_server(c); };
    }
}

RedisSinkCore::~RedisSinkCore() {
    close();
}

void RedisSinkCore::open(RuntimeContext* rt) {
    close();
    failure_.clear();
    rt_ = rt;
    server_ = opts_.server_factory(opts_.conn);
    if (rt_ != nullptr) {
        watch_ = std::make_unique<Watch>(*server_, rt_->cancel_signal());
    }
    open_ = true;
}

void RedisSinkCore::close() noexcept {
    watch_.reset();
    server_.reset();
    open_ = false;
}

void RedisSinkCore::require_not_failed(std::string_view what) const {
    if (!failure_.empty()) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} +
                                 " refused: the sink failed earlier (" + failure_ +
                                 ") and has not been opened again, so nothing more is written");
    }
}

void RedisSinkCore::require_usable(std::string_view what) const {
    require_not_failed(what);
    if (!open_) {
        throw std::runtime_error(opts_.name + ": " + std::string{what} + " before open()");
    }
}

void RedisSinkCore::fail(const std::string& why) {
    failure_ = why;
    clink::metrics::connector::error_inc(kLabel, "sink");
    throw std::runtime_error(opts_.name + ": " + why);
}

bool RedisSinkCore::cancel_requested_() const noexcept {
    return rt_ != nullptr && rt_->cancel_requested();
}

void RedisSinkCore::write(const std::vector<std::vector<std::string>>& commands,
                          std::size_t records,
                          std::size_t bytes) {
    require_usable("a write");
    if (commands.empty()) {
        return;
    }
    if (cancel_requested_()) {
        fail("cancelled before writing to " + opts_.target);
    }
    (void)CLINK_FAULT_POINT(points::kSinkBeforeFlush);
    const auto t0 = std::chrono::steady_clock::now();
    try {
        for (const auto& c : commands) {
            server_->append(c);
        }
    } catch (const std::exception& e) {
        // Queued on this side only: nothing was sent.
        fail("queueing a flush to " + opts_.target + " failed (" + e.what() +
             "); nothing of it was sent");
    }
    if (watch_) {
        watch_->arm();
    }
    std::size_t applied = 0;
    std::size_t refused = 0;
    std::string first_refusal;
    for (std::size_t i = 0; i < commands.size(); ++i) {
        const auto r = server_->reply();
        if (r.kind == RedisReplyView::Kind::Ok) {
            ++applied;
            continue;
        }
        if (r.kind == RedisReplyView::Kind::Error) {
            if (refused++ == 0) {
                first_refusal = r.text;
            }
            continue;
        }
        const bool cancelled = watch_ && watch_->disarm();
        const auto unknown = commands.size() - i;
        fail((cancelled ? std::string{"the task was cancelled"}
                        : "the connection to Redis failed (" + r.text + ")") +
             " with " + std::to_string(applied) + " of " + std::to_string(commands.size()) +
             " commands of a flush to " + opts_.target + " applied" +
             (refused > 0 ? ", " + std::to_string(refused) + " refused" : std::string{}) +
             ", and the other " + std::to_string(unknown) +
             " may or may not have been. The job restarts from its last checkpoint and writes "
             "its records again");
    }
    if (watch_) {
        (void)watch_->disarm();
    }
    if (refused > 0) {
        fail("Redis refused " + std::to_string(refused) + " of " + std::to_string(commands.size()) +
             " commands of a flush to " + opts_.target + " (first: " + first_refusal +
             "); the other " + std::to_string(applied) +
             " were applied. The job restarts from its last checkpoint and writes its records "
             "again");
    }
    const auto dt =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    clink::metrics::connector::records_out_inc(kLabel, records);
    clink::metrics::connector::bytes_out_inc(kLabel, bytes);
    clink::metrics::connector::commit_latency_observe(kLabel, static_cast<std::uint64_t>(dt));
}

}  // namespace clink::redis
