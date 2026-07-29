#pragma once

// A blocking WebSocket client for the connector's produce() thread: connect
// (with a deadline), complete the RFC 6455 opening handshake, then poll() -
// which reads whatever arrives within a bounded block, answers pings with
// pongs internally, and hands back complete data messages. Single-threaded
// by design: every method is called from the one thread that owns the
// source, so there is no locking anywhere.
//
// ws:// speaks over a plain TCP socket. wss:// requires the impl to have
// been built with OpenSSL (CLINK_WEBSOCKET_TLS); without it, open() on a
// wss:// URL fails with an error that says exactly that. All socket and TLS
// detail lives behind the pImpl in ws_client.cpp, so this header stays free
// of platform and OpenSSL includes.

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "clink/websocket/ws_protocol.hpp"

namespace clink::websocket {

struct WsClientOptions {
    std::string url;  // ws://host[:port]/path or wss://...
    std::chrono::milliseconds open_timeout{10'000};
    // Verify the server certificate + hostname on wss:// (tls_verify='false'
    // in SQL). Ignored for ws://.
    bool tls_verify{true};
    std::size_t max_message_bytes{64 * 1024 * 1024};
    std::string name{"websocket"};  // error-message prefix
};

class WsClient {
public:
    explicit WsClient(WsClientOptions opts);
    ~WsClient();

    WsClient(const WsClient&) = delete;
    WsClient& operator=(const WsClient&) = delete;

    // Resolve, connect, TLS-handshake (wss://), and complete the opening
    // handshake, all within open_timeout. Throws std::runtime_error with the
    // failing stage in the message.
    void open();

    [[nodiscard]] bool connected() const noexcept;

    // Send one masked text frame (subscription messages, application data).
    // Throws on a send failure; the caller treats that as a lost connection.
    void send_text(const std::string& payload);

    // Send a masked ping (keepalive). Throws on a send failure.
    void send_ping(const std::string& payload = {});

    struct PollResult {
        std::vector<std::string> texts;   // complete text messages, in order
        std::size_t binaries_skipped{0};  // binary messages seen (not delivered)
        bool closed{false};               // the server closed (close frame or EOF); the
                                          // close reply, when owed, was already sent
    };

    // Read whatever arrives within `block`, answering pings internally.
    // Returns early once at least one message (or the close) is decoded.
    // Throws on a socket/protocol error; the connection is dead afterwards.
    PollResult poll(std::chrono::milliseconds block);

    // Best-effort close handshake (send a close frame, ignore failures),
    // then release the socket. Idempotent.
    void close();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace clink::websocket
