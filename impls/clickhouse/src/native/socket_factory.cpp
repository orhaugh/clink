#include "native/socket_factory.hpp"

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <clickhouse/base/input.h>
#include <clickhouse/base/output.h>
#include <clickhouse/version.h>
#include <sys/socket.h>

#include "native/errors.hpp"

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
#include "native/vendor/clickhouse/base/sslsocket.h"

// The vendored header declares the client's TLS classes as 2.6.2 lays them
// out; upstream does not install it. Any other client must fail here rather
// than link a factory against a layout it does not have.
static_assert(CLICKHOUSE_CPP_VERSION_MAJOR == 2 && CLICKHOUSE_CPP_VERSION_MINOR == 6 &&
                  CLICKHOUSE_CPP_VERSION_PATCH == 2,
              "native/vendor/clickhouse/base/sslsocket.h is clickhouse-cpp 2.6.2's; re-vendor it "
              "from the client this build links");
#endif

namespace clink::clickhouse::native {

namespace {

[[noreturn]] void throw_aborted() {
    throw std::system_error(ECONNABORTED,
                            std::system_category(),
                            "clickhouse native sink: the connection was abandoned");
}

std::int64_t steady_ns(std::chrono::steady_clock::time_point t) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

// Checked before every read, write and flush.
void ensure_usable(const SocketControl& control) {
    if (control.poisoned()) {
        throw_aborted();
    }
    control.check_deadline();
}

class CountingInput final : public ::clickhouse::InputStream {
public:
    CountingInput(std::unique_ptr<::clickhouse::InputStream> inner,
                  std::shared_ptr<SocketControl> control)
        : inner_(std::move(inner)), control_(std::move(control)) {}

    bool Skip(std::size_t bytes) override {
        ensure_usable(*control_);
        return inner_->Skip(bytes);
    }

protected:
    std::size_t DoRead(void* buf, std::size_t len) override {
        ensure_usable(*control_);
        std::size_t n = 0;
        try {
            n = inner_->Read(buf, len);
        } catch (...) {
            // A poison shuts the fd down under a blocked recv, which then
            // fails as "closed" or as a TLS error. Report it as the abandon it
            // is, so the caller sees one signal whichever way it surfaced.
            if (control_->poisoned()) {
                throw_aborted();
            }
            throw;
        }
        control_->add_read(n);
        return n;
    }

private:
    std::unique_ptr<::clickhouse::InputStream> inner_;
    std::shared_ptr<SocketControl> control_;
};

class CountingOutput final : public ::clickhouse::OutputStream {
public:
    CountingOutput(std::unique_ptr<::clickhouse::OutputStream> inner,
                   std::shared_ptr<SocketControl> control)
        : inner_(std::move(inner)), control_(std::move(control)) {}

protected:
    void DoFlush() override {
        ensure_usable(*control_);
        try {
            inner_->Flush();
        } catch (...) {
            if (control_->poisoned()) {
                throw_aborted();
            }
            throw;
        }
    }

    std::size_t DoWrite(const void* data, std::size_t len) override {
        ensure_usable(*control_);
        if (len == 0) {
            return 0;
        }
        std::size_t n = 0;
        try {
            n = inner_->Write(data, len);
        } catch (...) {
            if (control_->poisoned()) {
                throw_aborted();
            }
            throw;
        }
        if (n == 0) {
            // BufferedOutput::DoFlush loops until its buffer is out, so a 0
            // returned for a non-empty write would spin there for ever.
            throw std::system_error(
                EIO,
                std::system_category(),
                "clickhouse native sink: the socket took 0 of " + std::to_string(len) + " bytes");
        }
        control_->add_written(n);
        return n;
    }

private:
    std::unique_ptr<::clickhouse::OutputStream> inner_;
    std::shared_ptr<SocketControl> control_;
};

class CountingSocket final : public ::clickhouse::SocketBase {
public:
    CountingSocket(std::unique_ptr<::clickhouse::SocketBase> inner,
                   std::shared_ptr<SocketControl> control,
                   int fd)
        : inner_(std::move(inner)), control_(std::move(control)), fd_(fd) {}

    ~CountingSocket() override {
        // Detach before the inner socket closes the fd, so that a poison on
        // another thread never shuts down a descriptor number the process has
        // since reused.
        if (fd_ >= 0) {
            control_->detach_fd(fd_);
        }
        inner_.reset();
    }

    CountingSocket(const CountingSocket&) = delete;
    CountingSocket& operator=(const CountingSocket&) = delete;

    std::unique_ptr<::clickhouse::InputStream> makeInputStream() const override {
        return std::make_unique<CountingInput>(inner_->makeInputStream(), control_);
    }

    std::unique_ptr<::clickhouse::OutputStream> makeOutputStream() const override {
        return std::make_unique<CountingOutput>(inner_->makeOutputStream(), control_);
    }

private:
    std::unique_ptr<::clickhouse::SocketBase> inner_;
    std::shared_ptr<SocketControl> control_;
    int fd_;
};

std::unique_ptr<::clickhouse::NonSecureSocketFactory> make_inner_factory(
    const ::clickhouse::ClientOptions& opts, bool tls) {
    if (!tls) {
        return std::make_unique<::clickhouse::NonSecureSocketFactory>();
    }
#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    if (!opts.ssl_options) {
        // SSLSocketFactory reads them unchecked.
        throw std::invalid_argument("CountingSocketFactory: TLS requested without SSL options");
    }
    return std::make_unique<::clickhouse::SSLSocketFactory>(opts);
#else
    (void)opts;
    throw NativeSinkError(code::kTlsUnavailable,
                          "clickhouse_native_sink: secure='true' is not available: this build of "
                          "the native sink was compiled without TLS support");
#endif
}

std::shared_ptr<SocketControl> require_control(std::shared_ptr<SocketControl> control) {
    if (!control) {
        throw std::invalid_argument("CountingSocketFactory: no SocketControl");
    }
    return control;
}

// handle_ is protected; naming it through a derived class is the one way to
// read it without changing the client. Never constructed.
struct SocketHandleAccess : ::clickhouse::Socket {
    static int get(const ::clickhouse::Socket& socket) noexcept {
        return socket.*(&SocketHandleAccess::handle_);
    }
};

}  // namespace

void SocketControl::poison() noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    poisoned_.store(true, std::memory_order_release);
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
    }
}

bool SocketControl::poisoned() const noexcept {
    return poisoned_.load(std::memory_order_acquire);
}

std::uint64_t SocketControl::bytes_written() const noexcept {
    return written_.load(std::memory_order_relaxed);
}

std::uint64_t SocketControl::bytes_read() const noexcept {
    return read_.load(std::memory_order_relaxed);
}

void SocketControl::set_deadline(
    std::optional<std::chrono::steady_clock::time_point> deadline) noexcept {
    if (!deadline) {
        deadline_ns_.store(0, std::memory_order_release);
        return;
    }
    // 0 means no deadline, so an instant that happens to encode as 0 is moved
    // by a nanosecond rather than silently clearing it.
    const std::int64_t ns = steady_ns(*deadline);
    deadline_ns_.store(ns == 0 ? 1 : ns, std::memory_order_release);
}

void SocketControl::attach_fd(int fd) noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    fd_ = fd;
    if (fd >= 0 && poisoned_.load(std::memory_order_acquire)) {
        ::shutdown(fd, SHUT_RDWR);
    }
}

void SocketControl::detach_fd(int fd) noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    if (fd_ == fd) {
        fd_ = -1;
    }
}

void SocketControl::add_written(std::size_t n) noexcept {
    written_.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
}

void SocketControl::add_read(std::size_t n) noexcept {
    read_.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
}

void SocketControl::check_deadline() const {
    const std::int64_t deadline = deadline_ns_.load(std::memory_order_acquire);
    if (deadline != 0 && steady_ns(std::chrono::steady_clock::now()) >= deadline) {
        throw std::system_error(ETIMEDOUT,
                                std::system_category(),
                                "clickhouse native sink: the attempt deadline has passed");
    }
}

CountingSocketFactory::CountingSocketFactory(const ::clickhouse::ClientOptions& opts,
                                             bool tls,
                                             std::shared_ptr<SocketControl> control)
    : inner_(make_inner_factory(opts, tls)), control_(require_control(std::move(control))) {}

CountingSocketFactory::CountingSocketFactory(
    std::unique_ptr<::clickhouse::NonSecureSocketFactory> inner,
    std::shared_ptr<SocketControl> control)
    : inner_(std::move(inner)), control_(require_control(std::move(control))) {
    if (!inner_) {
        throw std::invalid_argument("CountingSocketFactory: no inner factory");
    }
}

CountingSocketFactory::~CountingSocketFactory() = default;

std::unique_ptr<::clickhouse::SocketBase> CountingSocketFactory::connect(
    const ::clickhouse::ClientOptions& opts, const ::clickhouse::Endpoint& endpoint) {
    if (control_->poisoned()) {
        throw_aborted();
    }
    std::unique_ptr<::clickhouse::SocketBase> inner;
    try {
        inner = inner_->connect(opts, endpoint);
    } catch (...) {
        if (control_->poisoned()) {
            throw_aborted();
        }
        throw;
    }
    const auto* socket = dynamic_cast<const ::clickhouse::Socket*>(inner.get());
    const int fd = socket != nullptr ? socket_fd(*socket) : -1;
    // Wrapped before the fd is attached, so that on every way out of here,
    // the throw below included, the wrapper detaches the fd before the inner
    // socket closes it.
    auto wrapped = std::make_unique<CountingSocket>(std::move(inner), control_, fd);
    if (fd >= 0) {
        control_->attach_fd(fd);
    }
    // A poison that landed during the inner connect found no fd to shut down;
    // attach_fd has just done it, and the connect must not complete.
    if (control_->poisoned()) {
        throw_aborted();
    }
    return wrapped;
}

int socket_fd(const ::clickhouse::Socket& socket) noexcept {
    return SocketHandleAccess::get(socket);
}

}  // namespace clink::clickhouse::native
