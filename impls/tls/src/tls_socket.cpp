#include "clink/runtime/network/tls_socket.hpp"

#include <stdexcept>

#include "clink/runtime/network/network_socket.hpp"

#ifdef CLINK_HAS_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

namespace clink::network {

#ifdef CLINK_HAS_OPENSSL

namespace {

std::string ossl_last_error() {
    char buf[256] = {};
    ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
    return std::string{buf};
}

void init_openssl() {
    static const bool _ = [] {
        SSL_library_init();
        SSL_load_error_strings();
        OpenSSL_add_all_algorithms();
        return true;
    }();
    (void)_;
}

}  // namespace

// ---------- TlsServerContext ----------

struct TlsServerContext::Impl {
    SSL_CTX* ctx{nullptr};
};

bool TlsServerContext::is_real_implementation() {
    return true;
}

TlsServerContext::TlsServerContext(const std::string& cert_path, const std::string& key_path)
    : impl_(std::make_unique<Impl>()) {
    init_openssl();
    impl_->ctx = SSL_CTX_new(TLS_server_method());
    if (impl_->ctx == nullptr) {
        throw std::runtime_error("TlsServerContext: SSL_CTX_new failed: " + ossl_last_error());
    }
    SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION);
    if (SSL_CTX_use_certificate_file(impl_->ctx, cert_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: load cert failed: " + err);
    }
    if (SSL_CTX_use_PrivateKey_file(impl_->ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: load key failed: " + err);
    }
    if (SSL_CTX_check_private_key(impl_->ctx) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsServerContext: cert/key mismatch: " + err);
    }
}

TlsServerContext::~TlsServerContext() {
    if (impl_ && impl_->ctx != nullptr) {
        SSL_CTX_free(impl_->ctx);
    }
}

void TlsServerContext::set_client_ca_path(const std::string& ca_path) {
    if (SSL_CTX_load_verify_locations(impl_->ctx, ca_path.c_str(), nullptr) != 1) {
        throw std::runtime_error("TlsServerContext::set_client_ca_path: " + ossl_last_error());
    }
    SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
}

void* TlsServerContext::native_handle() const noexcept {
    return impl_->ctx;
}

// ---------- TlsClientContext ----------

struct TlsClientContext::Impl {
    SSL_CTX* ctx{nullptr};
};

bool TlsClientContext::is_real_implementation() {
    return true;
}

TlsClientContext::TlsClientContext(const std::string& ca_path) : impl_(std::make_unique<Impl>()) {
    init_openssl();
    impl_->ctx = SSL_CTX_new(TLS_client_method());
    if (impl_->ctx == nullptr) {
        throw std::runtime_error("TlsClientContext: SSL_CTX_new failed: " + ossl_last_error());
    }
    SSL_CTX_set_min_proto_version(impl_->ctx, TLS1_2_VERSION);
    if (SSL_CTX_load_verify_locations(impl_->ctx, ca_path.c_str(), nullptr) != 1) {
        const auto err = ossl_last_error();
        SSL_CTX_free(impl_->ctx);
        impl_->ctx = nullptr;
        throw std::runtime_error("TlsClientContext: load CA failed: " + err);
    }
    SSL_CTX_set_verify(impl_->ctx, SSL_VERIFY_PEER, nullptr);
}

TlsClientContext::~TlsClientContext() {
    if (impl_ && impl_->ctx != nullptr) {
        SSL_CTX_free(impl_->ctx);
    }
}

void TlsClientContext::set_client_cert(const std::string& cert_path, const std::string& key_path) {
    if (SSL_CTX_use_certificate_file(impl_->ctx, cert_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: cert: " + ossl_last_error());
    }
    if (SSL_CTX_use_PrivateKey_file(impl_->ctx, key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: key: " + ossl_last_error());
    }
    if (SSL_CTX_check_private_key(impl_->ctx) != 1) {
        throw std::runtime_error("TlsClientContext::set_client_cert: mismatch");
    }
}

void* TlsClientContext::native_handle() const noexcept {
    return impl_->ctx;
}

// ---------- TlsSocket ----------

struct TlsSocket::Impl {
    int fd{-1};
    SSL* ssl{nullptr};
};

TlsSocket::~TlsSocket() {
    close();
}

TlsSocket::TlsSocket(TlsSocket&& other) noexcept = default;
TlsSocket& TlsSocket::operator=(TlsSocket&& other) noexcept = default;

TlsSocket TlsSocket::accept(int listener_fd, const TlsServerContext& ctx) {
    const int fd = NetworkSocket::accept_one(listener_fd);
    if (fd < 0) {
        throw std::runtime_error("TlsSocket::accept: TCP accept failed");
    }
    TlsSocket out;
    out.impl_ = std::make_unique<Impl>();
    out.impl_->fd = fd;
    out.impl_->ssl =
        SSL_new(static_cast<SSL_CTX*>(const_cast<TlsServerContext&>(ctx).native_handle()));
    if (out.impl_->ssl == nullptr) {
        NetworkSocket::close(fd);
        throw std::runtime_error("TlsSocket::accept: SSL_new failed");
    }
    SSL_set_fd(out.impl_->ssl, fd);
    if (SSL_accept(out.impl_->ssl) != 1) {
        const auto err = ossl_last_error();
        SSL_free(out.impl_->ssl);
        NetworkSocket::close(fd);
        out.impl_.reset();
        throw std::runtime_error("TlsSocket::accept: handshake failed: " + err);
    }
    return out;
}

TlsSocket TlsSocket::connect(const std::string& host,
                             std::uint16_t port,
                             const TlsClientContext& ctx) {
    const int fd = NetworkSocket::connect_to(host, port);
    if (fd < 0) {
        throw std::runtime_error("TlsSocket::connect: TCP connect failed");
    }
    TlsSocket out;
    out.impl_ = std::make_unique<Impl>();
    out.impl_->fd = fd;
    out.impl_->ssl =
        SSL_new(static_cast<SSL_CTX*>(const_cast<TlsClientContext&>(ctx).native_handle()));
    if (out.impl_->ssl == nullptr) {
        NetworkSocket::close(fd);
        throw std::runtime_error("TlsSocket::connect: SSL_new failed");
    }
    SSL_set_fd(out.impl_->ssl, fd);
    SSL_set_tlsext_host_name(out.impl_->ssl, host.c_str());
    if (SSL_connect(out.impl_->ssl) != 1) {
        const auto err = ossl_last_error();
        SSL_free(out.impl_->ssl);
        NetworkSocket::close(fd);
        out.impl_.reset();
        throw std::runtime_error("TlsSocket::connect: handshake failed: " + err);
    }
    return out;
}

bool TlsSocket::send_all(const std::byte* buf, std::size_t len) {
    if (!impl_ || impl_->ssl == nullptr) {
        return false;
    }
    while (len > 0) {
        const int n = SSL_write(impl_->ssl, buf, static_cast<int>(len));
        if (n <= 0) {
            return false;
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool TlsSocket::recv_all(std::byte* buf, std::size_t len) {
    if (!impl_ || impl_->ssl == nullptr) {
        return false;
    }
    while (len > 0) {
        const int n = SSL_read(impl_->ssl, buf, static_cast<int>(len));
        if (n <= 0) {
            return false;
        }
        buf += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

void TlsSocket::shutdown_write() {
    if (impl_ && impl_->ssl != nullptr) {
        SSL_shutdown(impl_->ssl);
    }
}

void TlsSocket::close() {
    if (!impl_) {
        return;
    }
    if (impl_->ssl != nullptr) {
        SSL_free(impl_->ssl);
        impl_->ssl = nullptr;
    }
    if (impl_->fd >= 0) {
        NetworkSocket::close(impl_->fd);
        impl_->fd = -1;
    }
    impl_.reset();
}

bool TlsSocket::is_open() const noexcept {
    return impl_ && impl_->ssl != nullptr;
}

#else  // !CLINK_HAS_OPENSSL

struct TlsServerContext::Impl {};
struct TlsClientContext::Impl {};
struct TlsSocket::Impl {};

bool TlsServerContext::is_real_implementation() {
    return false;
}
bool TlsClientContext::is_real_implementation() {
    return false;
}

TlsServerContext::TlsServerContext(const std::string&, const std::string&) {
    throw std::runtime_error(
        "TlsServerContext: built without OpenSSL. Install OpenSSL and "
        "reconfigure cmake.");
}
TlsServerContext::~TlsServerContext() = default;
void TlsServerContext::set_client_ca_path(const std::string&) {}
void* TlsServerContext::native_handle() const noexcept {
    return nullptr;
}

TlsClientContext::TlsClientContext(const std::string&) {
    throw std::runtime_error("TlsClientContext: built without OpenSSL.");
}
TlsClientContext::~TlsClientContext() = default;
void TlsClientContext::set_client_cert(const std::string&, const std::string&) {}
void* TlsClientContext::native_handle() const noexcept {
    return nullptr;
}

TlsSocket::~TlsSocket() = default;
TlsSocket::TlsSocket(TlsSocket&&) noexcept = default;
TlsSocket& TlsSocket::operator=(TlsSocket&&) noexcept = default;
TlsSocket TlsSocket::accept(int, const TlsServerContext&) {
    return {};
}
TlsSocket TlsSocket::connect(const std::string&, std::uint16_t, const TlsClientContext&) {
    return {};
}
bool TlsSocket::send_all(const std::byte*, std::size_t) {
    return false;
}
bool TlsSocket::recv_all(std::byte*, std::size_t) {
    return false;
}
void TlsSocket::shutdown_write() {}
void TlsSocket::close() {}
bool TlsSocket::is_open() const noexcept {
    return false;
}

#endif

}  // namespace clink::network
