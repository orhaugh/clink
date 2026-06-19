#pragma once

// Factory functions producing TLS-wrapped Connections. Implementation
// lives in impls/tls so clink_core itself doesn't link OpenSSL.
// Callers that want TLS link clink_tls and call these factories;
// callers that don't get plain TCP from clink_core unchanged.
//
// Both functions take std::shared_ptr to the context so multiple
// concurrent connections can share one SSL_CTX (OpenSSL serialises
// internally). nullptr context = no TLS, falls back to plain.

#include <memory>
#include <string>

#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/network/tls_socket.hpp"

namespace clink::network {

// Accept a TLS connection on the given listener fd. Performs the TCP
// accept + TLS handshake; throws std::runtime_error on handshake
// failure. Returns the Connection owning the accepted socket.
std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx);

// Connect to host:port over TLS, verifying against the CAs in `ctx`.
// Returns nullptr on TCP failure; throws on TLS handshake failure.
std::unique_ptr<Connection> connect_tls_connection(const std::string& host,
                                                   std::uint16_t port,
                                                   std::shared_ptr<TlsClientContext> ctx);

}  // namespace clink::network
