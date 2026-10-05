#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace clink::network {

class AcceptWake;

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

    // The most server handshake steps that may compute at once across every
    // socket accepted with this context. A step is one SSL_accept call, where
    // the handshake's CPU goes (key exchange, the certificate signature,
    // client-certificate verification); the slot is held for the step only,
    // never while waiting on the client, so a client that stalls its
    // handshake holds none. Bounds what a flood of handshakes, each cheap for
    // the client and costly for the server, can take from the rest of the
    // process. A step that cannot get a slot before its handshake deadline
    // fails the handshake. A handshake waiting for a slot gives up within a
    // few milliseconds once its socket is shut down (or reset by the peer) or
    // its TlsAcceptOptions::wake is woken, and checks again once it has the
    // slot, before the step computes, so one abandoned while it waited holds
    // neither its thread nor a slot's CPU. Default: half the hardware
    // threads, at least one.
    // Zero is treated as one. Set before the context is in use.
    void set_max_concurrent_handshake_steps(std::size_t n);

    void* native_handle() const noexcept;  // SSL_CTX*; void* avoids leaking <openssl/ssl.h>

    static bool is_real_implementation();

private:
    friend class TlsSocket;  // takes a handshake slot for each step
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

// Bounds on the server side of a handshake. The defaults bound nothing, which
// is only right for a caller that owns the client too (a test, a loopback
// pair): a server that accepts from a network must set a deadline, or one
// client that connects and never finishes the handshake holds the accepting
// thread for ever.
struct TlsAcceptOptions {
    // Deadline for the whole handshake, from the TCP accept to its end; zero
    // or less means none. A client that sends its ClientHello a byte at a
    // time cannot stretch it.
    std::chrono::milliseconds handshake_timeout{0};
    // Abandons the handshake at once when woken, so the owner of an accept
    // thread can stop it without waiting out the deadline. Optional. Shutting
    // the socket down from another thread abandons it too, whether it is
    // waiting on the client or for a handshake slot.
    const AcceptWake* wake{nullptr};
};

// How long a TLS client connect may take unless its caller says otherwise.
inline constexpr std::chrono::milliseconds kDefaultTlsConnectTimeout{5000};

// Bounds on the client side of a connect. Unlike TlsAcceptOptions, the
// default is bounded: a server whose TCP connection completes and that never
// answers the ClientHello would otherwise hold the connecting thread for ever.
// The kernel completes the connection into the listen backlog whatever the
// process is doing, so a frozen server, or one whose accepting thread is
// stuck, looks exactly like that; so does a stalled middlebox, or a server not
// speaking TLS that waits for more than the ClientHello.
struct TlsConnectOptions {
    // Deadline for the whole connect, from the host resolving: the TCP
    // connect and the handshake share it. Zero or less means none, which only
    // a caller that owns the server too should choose. Name resolution is not
    // covered (see NetworkSocket::connect_within).
    std::chrono::milliseconds connect_timeout{kDefaultTlsConnectTimeout};
};

// A TLS-wrapped TCP socket. Send/recv go through OpenSSL's record layer.
// Owns the underlying fd and tears it down on destruct.
class TlsSocket {
public:
    // Accept one connection on `listener_fd` and run the server handshake.
    // Throws std::runtime_error when the TCP accept fails, the handshake
    // fails, the handshake deadline passes, or the wake abandons it; the
    // connection is closed either way. On a non-blocking listener with
    // nothing left to accept (the connection went before it was accepted)
    // it returns an unopened socket instead, so the caller can go back to
    // waiting. The returned socket is blocking.
    static TlsSocket accept(int listener_fd,
                            const TlsServerContext& ctx,
                            const TlsAcceptOptions& opts);
    // No deadline and no wake: see TlsAcceptOptions.
    static TlsSocket accept(int listener_fd, const TlsServerContext& ctx);
    // The handshake half of accept, on a socket the caller has already
    // accepted (blocking, as NetworkSocket::accept_one returns it), which
    // this takes ownership of. The deadline runs from the call. Throws, with
    // the socket closed, on the same failures as accept.
    static TlsSocket handshake_accepted(int fd,
                                        const TlsServerContext& ctx,
                                        const TlsAcceptOptions& opts);
    // Connect to host:port and run the client handshake, verifying the server
    // against the CAs in `ctx`, all within opts.connect_timeout. Throws
    // std::runtime_error, naming the stage, when the host does not resolve,
    // the TCP connect fails, the handshake fails, or the deadline passes
    // first; the connection is closed either way. The returned socket is
    // blocking.
    static TlsSocket connect(const std::string& host,
                             std::uint16_t port,
                             const TlsClientContext& ctx,
                             const TlsConnectOptions& opts);
    // With the default deadline, kDefaultTlsConnectTimeout.
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
    // Shut the socket itself down for reading (shutdown(SHUT_RD)), so a read
    // blocked in another thread returns and fails. Frees nothing: unlike
    // close(), it is safe while another thread is inside recv_all.
    void shutdown_read();
    // As shutdown_read, in both directions, so a blocked write fails too.
    void interrupt();
    // Frees the session and closes the socket. Never while another thread
    // may be using the socket: wake it with shutdown_read or interrupt, and
    // close once it has let go.
    void close();

    // Bound how long one read may block (SO_RCVTIMEO); zero clears it. A
    // read that times out fails, as a read from a closed peer does. False
    // when the socket is not open or the option cannot be set.
    bool set_recv_timeout(std::chrono::milliseconds timeout);
    // Bound how long one write may block (SO_SNDTIMEO); zero clears it. A
    // write that times out fails, and the session is not to be used again.
    // False when the socket is not open or the option cannot be set.
    bool set_send_timeout(std::chrono::milliseconds timeout);

    bool is_open() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::network
