#pragma once

// What the Redis sinks share: the connection, its command timeout and the
// cancel watch, the failure state, and a pipelined write that reads every reply
// and says what was applied when one is refused or lost.
//
// Writes. A flush sends its commands pipelined and reads every reply. A command
// the server refuses (an error reply) does not stop the others, which Redis
// runs regardless; the flush then fails, saying how many were applied. A reply
// that does not come (the connection lost, the command timeout passed, the task
// cancelled) leaves the commands from there on unknown: each may or may not
// have run. Either way the sink fails, and the job restarts from its last
// checkpoint and writes its records again. A reply means the server applied the
// command in memory; persistence (appendfsync) and replication are the
// server's, and the sink waits for neither.
//
// Failure state. A failure fails the sink: every later record, barrier and
// flush is refused, an empty flush included, until open() is called again.
// close() releases the connection and never throws.
//
// Bounds. A command waits at most the connection's command timeout (30 s
// unless set), and when the task's cancel is requested during a flush a watch
// shuts the socket, so the flush fails at once.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "clink/redis/redis_client.hpp"
#include "clink/runtime/runtime_context.hpp"

namespace clink::redis {

// A pipelined command's reply, as the sinks judge it.
struct RedisReplyView {
    enum class Kind : std::uint8_t { Ok, Error, Lost };
    Kind kind{Kind::Ok};
    std::string text;  // the server's error, or why no reply came
};

// The sinks' connection, behind an interface so their failure paths can be
// tested against a scripted server.
class RedisSinkServer {
public:
    virtual ~RedisSinkServer() = default;
    // Queues a command; nothing is sent until the first reply is read.
    virtual void append(const std::vector<std::string>& args) = 0;
    // The next queued command's reply. Lost once the connection has failed.
    virtual RedisReplyView reply() = 0;
    // Ends the read or write in flight. Safe to call from another thread.
    virtual void cancel() noexcept = 0;
};

using RedisSinkServerFactory =
    std::function<std::unique_ptr<RedisSinkServer>(const ConnectOptions&)>;

// A server over hiredis. Throws when the connection cannot be made.
[[nodiscard]] std::unique_ptr<RedisSinkServer> connect_redis_sink_server(const ConnectOptions& o);

struct RedisSinkCoreOptions {
    ConnectOptions conn;  // a command_timeout of 0 means kDefaultCommandTimeout
    std::string name;
    std::string target;                     // what the errors name: "stream orders"
    RedisSinkServerFactory server_factory;  // unset: a real server
};

class RedisSinkCore {
public:
    static constexpr std::chrono::milliseconds kDefaultCommandTimeout{30000};

    explicit RedisSinkCore(RedisSinkCoreOptions o);
    ~RedisSinkCore();
    RedisSinkCore(const RedisSinkCore&) = delete;
    RedisSinkCore& operator=(const RedisSinkCore&) = delete;

    // Connects (AUTH, SELECT, TLS as the options say). Clears a failure.
    void open(RuntimeContext* rt);
    void close() noexcept;

    // Throws unless the sink is open and has not failed.
    void require_usable(std::string_view what) const;
    // Throws when the sink has failed (an unopened sink passes).
    void require_not_failed(std::string_view what) const;
    // Fails the sink and throws `why`.
    [[noreturn]] void fail(const std::string& why);
    [[nodiscard]] bool failed() const noexcept { return !failure_.empty(); }

    // Sends `commands` pipelined and reads every reply; fails the sink, saying
    // what was applied, unless every command succeeded.
    void write(const std::vector<std::vector<std::string>>& commands,
               std::size_t records,
               std::size_t bytes);

private:
    class Watch;

    [[nodiscard]] bool cancel_requested_() const noexcept;

    RedisSinkCoreOptions opts_;
    std::unique_ptr<RedisSinkServer> server_;
    std::unique_ptr<Watch> watch_;
    RuntimeContext* rt_{nullptr};
    std::string failure_;
    bool open_{false};
};

}  // namespace clink::redis
