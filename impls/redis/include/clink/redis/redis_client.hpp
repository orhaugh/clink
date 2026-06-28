#pragma once

// Minimal RAII wrapper over hiredis' synchronous API, shared by the Redis
// Streams source and sink. Two concerns it centralises:
//   1. reply lifecycle - every redisReply* must be freeReplyObject'd exactly
//      once (the #1 hiredis leak); Reply is the RAII owner.
//   2. connection lifecycle - the redisContext* is owned, AUTH/SELECT are run
//      at connect, and a NULL reply (dead connection) throws so the caller can
//      surface it (sink rethrows -> job replays; source treats it as fatal and
//      restarts, recovering via the consumer-group PEL).
//
// Compiled only where hiredis is found (CLINK_HAS_REDIS); the whole impls/redis
// module is dep-gated on it.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <hiredis/hiredis.h>

#ifdef CLINK_REDIS_TLS
#include <mutex>

#include <hiredis/hiredis_ssl.h>
#endif

namespace clink::redis {

// Owns a redisReply*. Move-only; frees exactly once.
class Reply {
public:
    Reply() = default;
    explicit Reply(redisReply* r) : r_(r) {}
    Reply(const Reply&) = delete;
    Reply& operator=(const Reply&) = delete;
    Reply(Reply&& o) noexcept : r_(o.r_) { o.r_ = nullptr; }
    Reply& operator=(Reply&& o) noexcept {
        if (this != &o) {
            reset();
            r_ = o.r_;
            o.r_ = nullptr;
        }
        return *this;
    }
    ~Reply() { reset(); }

    void reset() {
        if (r_ != nullptr) {
            freeReplyObject(r_);
            r_ = nullptr;
        }
    }
    redisReply* get() const noexcept { return r_; }
    redisReply* operator->() const noexcept { return r_; }
    explicit operator bool() const noexcept { return r_ != nullptr; }

    bool is_error() const noexcept { return r_ != nullptr && r_->type == REDIS_REPLY_ERROR; }
    bool is_nil() const noexcept { return r_ != nullptr && r_->type == REDIS_REPLY_NIL; }
    std::string error_text() const {
        return (r_ != nullptr && r_->str != nullptr) ? std::string(r_->str, r_->len)
                                                     : std::string{};
    }

private:
    redisReply* r_{nullptr};
};

struct ConnectOptions {
    std::string host{"localhost"};
    std::uint16_t port{6379};
    std::string username;  // ACL username (Redis 6+); empty = legacy AUTH <password>
    std::string password;  // empty = no AUTH
    int db{0};             // SELECT <db>; 0 = default, no SELECT
    std::chrono::milliseconds connect_timeout{5000};
    std::chrono::milliseconds command_timeout{0};  // 0 = block (set > BLOCK ms for the source)
    // TLS (requires the build to find libhiredis_ssl, i.e. CLINK_REDIS_TLS). When
    // tls is true the connection is wrapped in TLS before AUTH (credentials are
    // encrypted). tls_ca/cert/key are optional PEM paths (CA for server verify,
    // cert+key for mutual TLS). tls_sni overrides the SNI/verify hostname (default
    // = host). tls_verify=false skips server-cert verification (self-signed dev).
    bool tls{false};
    std::string tls_ca;
    std::string tls_cert;
    std::string tls_key;
    std::string tls_sni;
    bool tls_verify{true};
};

// Owns a redisContext. Move-only. Connects (+ AUTH/SELECT) in the ctor and
// throws on any failure, so a constructed Connection is always usable.
class Connection {
public:
    explicit Connection(const ConnectOptions& o) {
        const timeval ct = to_timeval_(o.connect_timeout);
        ctx_ = redisConnectWithTimeout(o.host.c_str(), o.port, ct);
        if (ctx_ == nullptr) {
            throw std::runtime_error("redis: redisConnect returned null (out of memory)");
        }
        if (ctx_->err != 0) {
            std::string err = ctx_->errstr;
            redisFree(ctx_);
            ctx_ = nullptr;
            throw std::runtime_error("redis: connect to " + o.host + ":" + std::to_string(o.port) +
                                     " failed: " + err);
        }
        if (o.command_timeout.count() > 0) {
            const timeval cmdt = to_timeval_(o.command_timeout);
            redisSetTimeout(ctx_, cmdt);
        }
        if (o.tls) {
            secure_(o);  // wrap in TLS before AUTH so credentials are encrypted
        }
        if (!o.password.empty()) {
            Reply r = o.username.empty() ? command({"AUTH", o.password})
                                         : command({"AUTH", o.username, o.password});
            require_ok_(r, "AUTH");
        }
        if (o.db != 0) {
            Reply r = command({"SELECT", std::to_string(o.db)});
            require_ok_(r, "SELECT");
        }
    }

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& o) noexcept : ctx_(o.ctx_), ssl_ctx_(o.ssl_ctx_) {
        o.ctx_ = nullptr;
        o.ssl_ctx_ = nullptr;
    }
    Connection& operator=(Connection&& o) noexcept {
        if (this != &o) {
            free_();
            ctx_ = o.ctx_;
            ssl_ctx_ = o.ssl_ctx_;
            o.ctx_ = nullptr;
            o.ssl_ctx_ = nullptr;
        }
        return *this;
    }
    ~Connection() { free_(); }

    // Binary-safe single command. Throws on a NULL reply (the connection is then
    // dead - ctx_->err is set). A protocol-level error reply (e.g. WRONGTYPE) is
    // returned as a Reply with is_error() true for the caller to inspect.
    Reply command(const std::vector<std::string>& args) {
        if (ctx_ == nullptr) {
            throw std::runtime_error("redis: command on a closed connection");
        }
        auto [argv, lens] = argv_(args);
        void* raw = redisCommandArgv(ctx_, static_cast<int>(args.size()), argv.data(), lens.data());
        if (raw == nullptr) {
            throw std::runtime_error("redis: command '" + cmd_name_(args) +
                                     "' failed: " + std::string(ctx_->errstr));
        }
        return Reply{static_cast<redisReply*>(raw)};
    }

    // Pipeline: queue a command without reading its reply. Pair each append()
    // with one get_reply() in order.
    void append(const std::vector<std::string>& args) {
        if (ctx_ == nullptr) {
            throw std::runtime_error("redis: append on a closed connection");
        }
        auto [argv, lens] = argv_(args);
        if (redisAppendCommandArgv(ctx_, static_cast<int>(args.size()), argv.data(), lens.data()) !=
            REDIS_OK) {
            throw std::runtime_error("redis: append '" + cmd_name_(args) +
                                     "' failed: " + std::string(ctx_->errstr));
        }
    }

    Reply get_reply() {
        if (ctx_ == nullptr) {
            throw std::runtime_error("redis: get_reply on a closed connection");
        }
        void* raw = nullptr;
        if (redisGetReply(ctx_, &raw) != REDIS_OK || raw == nullptr) {
            throw std::runtime_error("redis: get_reply failed: " + std::string(ctx_->errstr));
        }
        return Reply{static_cast<redisReply*>(raw)};
    }

    bool ok() const noexcept { return ctx_ != nullptr && ctx_->err == 0; }

private:
    static timeval to_timeval_(std::chrono::milliseconds ms) {
        timeval tv{};
        tv.tv_sec = static_cast<decltype(tv.tv_sec)>(ms.count() / 1000);
        tv.tv_usec = static_cast<decltype(tv.tv_usec)>((ms.count() % 1000) * 1000);
        return tv;
    }
    static std::pair<std::vector<const char*>, std::vector<std::size_t>> argv_(
        const std::vector<std::string>& args) {
        std::vector<const char*> argv;
        std::vector<std::size_t> lens;
        argv.reserve(args.size());
        lens.reserve(args.size());
        for (const auto& a : args) {
            argv.push_back(a.data());
            lens.push_back(a.size());
        }
        return {std::move(argv), std::move(lens)};
    }
    static std::string cmd_name_(const std::vector<std::string>& args) {
        return args.empty() ? std::string{} : args.front();
    }
    static void require_ok_(const Reply& r, const char* what) {
        if (r.is_error()) {
            throw std::runtime_error(std::string("redis: ") + what + " failed: " + r.error_text());
        }
    }
    // Wrap the (already TCP-connected) context in TLS before any command runs.
    void secure_(const ConnectOptions& o) {
#ifdef CLINK_REDIS_TLS
        static std::once_flag init_once;
        std::call_once(init_once, [] { redisInitOpenSSL(); });
        redisSSLOptions sopt{};
        sopt.cacert_filename = o.tls_ca.empty() ? nullptr : o.tls_ca.c_str();
        sopt.capath = nullptr;
        sopt.cert_filename = o.tls_cert.empty() ? nullptr : o.tls_cert.c_str();
        sopt.private_key_filename = o.tls_key.empty() ? nullptr : o.tls_key.c_str();
        const std::string& sni = o.tls_sni.empty() ? o.host : o.tls_sni;
        sopt.server_name = sni.empty() ? nullptr : sni.c_str();
        sopt.verify_mode = o.tls_verify ? REDIS_SSL_VERIFY_PEER : REDIS_SSL_VERIFY_NONE;
        redisSSLContextError ssl_err = REDIS_SSL_CTX_NONE;
        redisSSLContext* sctx = redisCreateSSLContextWithOptions(&sopt, &ssl_err);
        if (sctx == nullptr) {
            const std::string e = redisSSLContextGetError(ssl_err);
            redisFree(ctx_);
            ctx_ = nullptr;
            throw std::runtime_error("redis: TLS context init failed: " + e);
        }
        ssl_ctx_ = sctx;
        if (redisInitiateSSLWithContext(ctx_, sctx) != REDIS_OK) {
            const std::string e = ctx_->errstr;
            free_();  // frees ctx_ + ssl_ctx_
            throw std::runtime_error("redis: TLS handshake failed: " + e);
        }
#else
        (void)o;
        redisFree(ctx_);
        ctx_ = nullptr;
        throw std::runtime_error(
            "redis: tls=true but this build has no TLS support (rebuild with libhiredis_ssl)");
#endif
    }

    void free_() {
        if (ctx_ != nullptr) {
            redisFree(ctx_);
            ctx_ = nullptr;
        }
#ifdef CLINK_REDIS_TLS
        if (ssl_ctx_ != nullptr) {
            redisFreeSSLContext(static_cast<redisSSLContext*>(ssl_ctx_));
            ssl_ctx_ = nullptr;
        }
#endif
    }

    redisContext* ctx_{nullptr};
    void* ssl_ctx_{nullptr};  // redisSSLContext* (type-erased to avoid the TLS header here)
};

}  // namespace clink::redis
