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
// accept + TLS handshake, bounded by `opts` (see TlsAcceptOptions); throws
// std::runtime_error on a failed, timed-out or abandoned handshake. Returns
// the Connection owning the accepted socket, or nullptr when a non-blocking
// listener had nothing left to accept.
std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx,
                                                  const TlsAcceptOptions& opts);

// As above with no handshake deadline and no wake. Only for a caller that
// owns the client too: a server that accepts from a network passes options.
std::unique_ptr<Connection> accept_tls_connection(int listener_fd,
                                                  std::shared_ptr<TlsServerContext> ctx);

// The handshake half of accept_tls_connection, on a socket the caller has
// already accepted, which this takes ownership of: the form for a server
// that accepts on one thread and handshakes on others, as the coordinator
// does. Throws, with the socket closed, on a failed, timed-out or abandoned
// handshake.
std::unique_ptr<Connection> handshake_accepted_tls_connection(int fd,
                                                              std::shared_ptr<TlsServerContext> ctx,
                                                              const TlsAcceptOptions& opts);

// Connect to host:port over TLS, verifying against the CAs in `ctx`.
// Returns nullptr on TCP failure; throws on TLS handshake failure.
std::unique_ptr<Connection> connect_tls_connection(const std::string& host,
                                                   std::uint16_t port,
                                                   std::shared_ptr<TlsClientContext> ctx);

}  // namespace clink::network
