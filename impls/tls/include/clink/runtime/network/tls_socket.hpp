#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace clink::network {

// TLS-wrapped socket helpers. Compiled when CLINK_HAS_OPENSSL is set;
// otherwise every method throws on construction so callers fail fast at
// link time and the rest of the runtime keeps working without OpenSSL.
//
// Two contexts:
//   TlsServerContext - holds the cert + private key the server presents.
//   TlsClientContext - holds the CA cert(s) used to verify the server.
//
// Both are reference-counted via shared_ptr; multiple concurrent TLS
// sockets can share one context safely (OpenSSL serialises internally).
//
// mTLS: pass a CA path to TlsServerContext::set_client_ca_path() and the
// server will require + verify a client certificate. The client side
// supplies its own cert/key via TlsClientContext::set_client_cert(...).

class TlsServerContext {
public:
    TlsServerContext(const std::string& cert_path, const std::string& key_path);
    ~TlsServerContext();

    TlsServerContext(const TlsServerContext&) = delete;
    TlsServerContext& operator=(const TlsServerContext&) = delete;
    TlsServerContext(TlsServerContext&&) = delete;
    TlsServerContext& operator=(TlsServerContext&&) = delete;

    // Require + verify the client's certificate against this CA file.
    // No-op if not called → server-only TLS (no client auth).
    void set_client_ca_path(const std::string& ca_path);

    void* native_handle() const noexcept;  // SSL_CTX*; void* avoids leaking <openssl/ssl.h>

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class TlsClientContext {
public:
    explicit TlsClientContext(const std::string& ca_path);
    ~TlsClientContext();

    TlsClientContext(const TlsClientContext&) = delete;
    TlsClientContext& operator=(const TlsClientContext&) = delete;
    TlsClientContext(TlsClientContext&&) = delete;
    TlsClientContext& operator=(TlsClientContext&&) = delete;

    // Optionally present a client cert (mTLS).
    void set_client_cert(const std::string& cert_path, const std::string& key_path);

    void* native_handle() const noexcept;

    static bool is_real_implementation();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// A TLS-wrapped TCP socket. Send/recv go through OpenSSL's record layer.
// Owns the underlying fd and tears it down on destruct.
class TlsSocket {
public:
    static TlsSocket accept(int listener_fd, const TlsServerContext& ctx);
    static TlsSocket connect(const std::string& host,
                             std::uint16_t port,
                             const TlsClientContext& ctx);

    TlsSocket() = default;
    ~TlsSocket();

    TlsSocket(const TlsSocket&) = delete;
    TlsSocket& operator=(const TlsSocket&) = delete;
    TlsSocket(TlsSocket&& other) noexcept;
    TlsSocket& operator=(TlsSocket&& other) noexcept;

    bool send_all(const std::byte* buf, std::size_t len);
    bool recv_all(std::byte* buf, std::size_t len);
    void shutdown_write();
    void close();

    bool is_open() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::network
