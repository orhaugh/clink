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
#include <memory>
#include <mutex>

#include <arpa/inet.h>
#include <hiredis/hiredis_ssl.h>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
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
    // cert+key for mutual TLS). tls_sni is the name sent as SNI and the one the
    // server's certificate must carry (default = host). tls_verify=false skips
    // server-cert verification (self-signed dev); tls_verify_hostname=false keeps
    // the chain check but accepts a certificate that names another host.
    bool tls{false};
    std::string tls_ca;
    std::string tls_cert;
    std::string tls_key;
    std::string tls_sni;
    bool tls_verify{true};
    bool tls_verify_hostname{true};
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
    Connection(Connection&& o) noexcept : ctx_(o.ctx_) { o.ctx_ = nullptr; }
    Connection& operator=(Connection&& o) noexcept {
        if (this != &o) {
            free_();
            ctx_ = o.ctx_;
            o.ctx_ = nullptr;
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
        const std::string& name = o.tls_sni.empty() ? o.host : o.tls_sni;
        SSL* ssl = nullptr;
        try {
            ssl = tls_session_(o, name);
        } catch (...) {
            free_();
            throw;
        }
        if (redisInitiateSSL(ctx_, ssl) != REDIS_OK) {
            // hiredis owns the session only once the handshake succeeds.
            std::string e = ctx_->errstr;
            const long verified = SSL_get_verify_result(ssl);
            if (o.tls_verify && o.tls_verify_hostname &&
                (verified == X509_V_ERR_HOSTNAME_MISMATCH ||
                 verified == X509_V_ERR_IP_ADDRESS_MISMATCH)) {
                e = "the server's certificate does not match the host " + name + " (" +
                    certificate_names_(ssl) +
                    "). Connect by a name or address the certificate carries (tls_sni sets the "
                    "name checked), reissue it with " +
                    name +
                    " in its subjectAltName, or turn the hostname check off with "
                    "tls_verify_hostname=false.";
            }
            SSL_free(ssl);
            free_();
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
    }

#ifdef CLINK_REDIS_TLS
    [[noreturn]] static void tls_init_failed_(redisSSLContextError e) {
        throw std::runtime_error(std::string("redis: TLS context init failed: ") +
                                 redisSSLContextGetError(e));
    }

    // The session for one connection, built here rather than by
    // redisCreateSSLContextWithOptions: hiredis sends SNI but never checks that
    // the certificate names the host, and the check has to be set on the
    // session before the handshake. Otherwise the settings, and the errors,
    // are hiredis's own: TLS 1.2 or later, the CAs in tls_ca or else the
    // default paths, and the client certificate when one is given.
    static SSL* tls_session_(const ConnectOptions& o, const std::string& name) {
        if (o.tls_cert.empty() != o.tls_key.empty()) {
            tls_init_failed_(REDIS_SSL_CTX_CERT_KEY_REQUIRED);
        }
        // Each session holds a reference to its context, so this one is
        // released on return.
        const std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> ctx(
            SSL_CTX_new(TLS_client_method()), &SSL_CTX_free);
        if (!ctx) {
            tls_init_failed_(REDIS_SSL_CTX_CREATE_FAILED);
        }
        SSL_CTX_set_min_proto_version(ctx.get(), TLS1_2_VERSION);
        SSL_CTX_set_verify(ctx.get(), o.tls_verify ? SSL_VERIFY_PEER : SSL_VERIFY_NONE, nullptr);
        if (!o.tls_ca.empty()) {
            if (SSL_CTX_load_verify_locations(ctx.get(), o.tls_ca.c_str(), nullptr) != 1) {
                tls_init_failed_(REDIS_SSL_CTX_CA_CERT_LOAD_FAILED);
            }
        } else if (SSL_CTX_set_default_verify_paths(ctx.get()) != 1) {
            tls_init_failed_(REDIS_SSL_CTX_CLIENT_DEFAULT_CERT_FAILED);
        }
        if (!o.tls_cert.empty()) {
            if (SSL_CTX_use_certificate_chain_file(ctx.get(), o.tls_cert.c_str()) != 1) {
                tls_init_failed_(REDIS_SSL_CTX_CLIENT_CERT_LOAD_FAILED);
            }
            if (SSL_CTX_use_PrivateKey_file(ctx.get(), o.tls_key.c_str(), SSL_FILETYPE_PEM) != 1) {
                tls_init_failed_(REDIS_SSL_CTX_PRIVATE_KEY_LOAD_FAILED);
            }
        }
        std::unique_ptr<SSL, decltype(&SSL_free)> ssl(SSL_new(ctx.get()), &SSL_free);
        if (!ssl) {
            tls_init_failed_(REDIS_SSL_CTX_CREATE_FAILED);
        }
        if (!name.empty()) {
            // SSL_set_tlsext_host_name, spelt out: the macro casts C-style, which
            // warns in every translation unit that includes this header.
            SSL_ctrl(ssl.get(),
                     SSL_CTRL_SET_TLSEXT_HOSTNAME,
                     TLSEXT_NAMETYPE_host_name,
                     const_cast<char*>(name.c_str()));
        }
        if (o.tls_verify && o.tls_verify_hostname) {
            require_name_(ssl.get(), name);
        }
        return ssl.release();
    }

    // The address `host` spells when it is an IPv4 or IPv6 literal, without an
    // IPv6 zone ("fe80::1%en0"); empty when it is a name.
    static std::string ip_literal_(const std::string& host) {
        in_addr v4{};
        if (::inet_pton(AF_INET, host.c_str(), &v4) == 1) {
            return host;
        }
        const std::string unzoned = host.substr(0, host.find('%'));
        in6_addr v6{};
        if (::inet_pton(AF_INET6, unzoned.c_str(), &v6) == 1) {
            return unzoned;
        }
        return {};
    }

    // Make the handshake check that the certificate names `name`: a name
    // against its DNS subjectAltNames (or, with none, its subject CN), a
    // wildcard only as a whole left-most label; an address against its IP
    // address entries.
    static void require_name_(SSL* ssl, const std::string& name) {
        X509_VERIFY_PARAM* param = SSL_get0_param(ssl);
        const std::string ip = ip_literal_(name);
        int ok = 0;
        if (!ip.empty()) {
            ok = X509_VERIFY_PARAM_set1_ip_asc(param, ip.c_str());
        } else {
            X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
            ok = X509_VERIFY_PARAM_set1_host(param, name.c_str(), name.size());
        }
        if (ok != 1) {
            throw std::runtime_error("redis: cannot check the server's certificate against " +
                                     name);
        }
    }

    // What the server's certificate names, for the error a mismatch throws:
    // its subjectAltName DNS and IP entries, and its subject CN when it has no
    // DNS names. Read from the peer's chain, which OpenSSL 3 keeps after a
    // failed verify and 1.1.1 does not.
    static std::string certificate_names_(SSL* ssl) {
        STACK_OF(X509)* chain = SSL_get_peer_cert_chain(ssl);
        if (chain == nullptr || sk_X509_num(chain) == 0) {
            return "what it names could not be read";
        }
        X509* cert = sk_X509_value(chain, 0);
        std::string names;
        bool any_dns = false;
        const auto add = [&names](const std::string& n) {
            names += (names.empty() ? "" : ", ") + n;
        };
        auto* sans = static_cast<GENERAL_NAMES*>(
            X509_get_ext_d2i(cert, NID_subject_alt_name, nullptr, nullptr));
        if (sans != nullptr) {
            for (int i = 0; i < sk_GENERAL_NAME_num(sans); ++i) {
                const GENERAL_NAME* gn = sk_GENERAL_NAME_value(sans, i);
                if (gn->type == GEN_DNS) {
                    const ASN1_STRING* a = gn->d.dNSName;
                    add("DNS:" +
                        std::string(reinterpret_cast<const char*>(ASN1_STRING_get0_data(a)),
                                    static_cast<std::size_t>(ASN1_STRING_length(a))));
                    any_dns = true;
                } else if (gn->type == GEN_IPADD) {
                    const ASN1_OCTET_STRING* a = gn->d.iPAddress;
                    const int len = ASN1_STRING_length(a);
                    char text[INET6_ADDRSTRLEN] = {};
                    const int family = len == 4 ? AF_INET : (len == 16 ? AF_INET6 : AF_UNSPEC);
                    if (family != AF_UNSPEC &&
                        ::inet_ntop(family, ASN1_STRING_get0_data(a), text, sizeof(text)) !=
                            nullptr) {
                        add(std::string{"IP:"} + text);
                    }
                }
            }
            GENERAL_NAMES_free(sans);
        }
        if (!any_dns) {
            char cn[256] = {};
            if (X509_NAME_get_text_by_NID(
                    X509_get_subject_name(cert), NID_commonName, cn, sizeof(cn)) > 0) {
                add(std::string{"CN="} + cn);
            }
        }
        return names.empty() ? "it names nothing" : "it names " + names;
    }
#endif

    redisContext* ctx_{nullptr};
};

}  // namespace clink::redis
