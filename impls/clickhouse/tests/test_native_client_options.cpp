// make_client_options field by field, the result and header readers, and the
// real transport against a loopback peer that scripts just enough of the
// native protocol (the Hello exchange, a header block, a result block and
// EndOfStream) to put a real ::clickhouse::Client mid-INSERT without a server.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <clickhouse/block.h>
#include <clickhouse/client.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/exceptions.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "native/clickhouse_transport.hpp"
#include "native/default_ca.hpp"
#include "native/errors.hpp"
#include "native/insert_transport.hpp"
#include "native/sink_options.hpp"

namespace {

using namespace std::chrono_literals;
namespace native = clink::clickhouse::native;
using native::CaEnvironment;
using native::CaLocation;
using native::Compression;
using native::Endpoint;
using native::fallback_ca_location;
using native::make_clickhouse_transport;
using native::make_client_options;
using native::NativeSinkError;
using native::PathKind;
using native::SinkOptions;
using CoClock = std::chrono::steady_clock;

SinkOptions co_options(std::uint16_t port = 9000) {
    SinkOptions o;
    o.endpoints = {Endpoint{"127.0.0.1", port}};
    o.database = "analytics";
    o.table = "events";
    o.user = "writer";
    o.password = "s3cret";
    o.compression = Compression::None;
    o.connect_timeout = 2000ms;
    o.send_timeout = 5000ms;
    o.receive_timeout = 5000ms;
    return o;
}

// The refusal `f` throws; fails the test when it throws nothing.
template <typename F>
std::optional<NativeSinkError> co_refusal(F&& f) {
    try {
        f();
    } catch (const NativeSinkError& e) {
        return e;
    }
    return std::nullopt;
}

// The errno of a std::system_error from `f`; 0 when `f` does not throw.
template <typename F>
int co_system_errno(F&& f) {
    try {
        f();
    } catch (const std::system_error& e) {
        return e.code().value();
    }
    return 0;
}

int co_errno_of(const std::exception_ptr& failure) {
    try {
        std::rethrow_exception(failure);
    } catch (const std::system_error& e) {
        return e.code().value();
    } catch (...) {
        return -1;
    }
}

// --- a loopback peer that speaks a little of the native protocol ---

class CoFd {
public:
    explicit CoFd(int fd = -1) noexcept : fd_(fd) {}
    ~CoFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    CoFd(CoFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    CoFd& operator=(CoFd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }
    CoFd(const CoFd&) = delete;
    CoFd& operator=(const CoFd&) = delete;
    [[nodiscard]] int get() const noexcept { return fd_; }

private:
    int fd_;
};

bool co_readable(int fd, std::chrono::milliseconds wait) {
    pollfd p{};
    p.fd = fd;
    p.events = POLLIN;
    return ::poll(&p, 1, static_cast<int>(wait.count())) > 0;
}

class CoListener {
public:
    CoListener() : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {
        if (fd_.get() < 0) {
            throw std::system_error(errno, std::system_category(), "socket");
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::bind(fd_.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof addr) != 0 ||
            ::listen(fd_.get(), 8) != 0) {
            throw std::system_error(errno, std::system_category(), "bind or listen");
        }
        socklen_t len = sizeof addr;
        ::getsockname(fd_.get(), reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
    }
    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }
    [[nodiscard]] int fd() const noexcept { return fd_.get(); }
    [[nodiscard]] bool has_pending(std::chrono::milliseconds wait) const {
        return co_readable(fd_.get(), wait);
    }

private:
    CoFd fd_;
    std::uint16_t port_{0};
};

// The native protocol's numbers and lengths are LEB128 varints.
void co_put_varint(std::string& out, std::uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<char>((v & 0x7fU) | 0x80U));
        v >>= 7;
    }
    out.push_back(static_cast<char>(v));
}

void co_put_string(std::string& out, std::string_view s) {
    co_put_varint(out, s.size());
    out.append(s);
}

// 54401 carries the patch version in the Hello, and sits below the quota-key
// addendum and the per-column serialisation flag, which keeps the script
// short.
constexpr std::uint64_t kCoRevision = 54401;

std::string co_server_hello(std::string_view display_name,
                            std::uint64_t major,
                            std::uint64_t minor,
                            std::uint64_t patch) {
    std::string s;
    co_put_varint(s, 0);  // Hello
    co_put_string(s, "ClickHouse");
    co_put_varint(s, major);
    co_put_varint(s, minor);
    co_put_varint(s, kCoRevision);
    co_put_string(s, "UTC");
    co_put_string(s, display_name);
    co_put_varint(s, patch);
    return s;
}

struct CoColumn {
    std::string name;
    std::string type;
    std::string body;  // the encoded values of every row
};

std::string co_data_packet(const std::vector<CoColumn>& columns, std::uint64_t rows) {
    std::string s;
    co_put_varint(s, 1);   // Data
    co_put_string(s, "");  // temporary table name
    co_put_varint(s, 1);   // block info: is_overflows
    s.push_back('\0');
    co_put_varint(s, 2);  // block info: bucket_num, -1
    s.append(4, '\xff');
    co_put_varint(s, 0);  // end of block info
    co_put_varint(s, columns.size());
    co_put_varint(s, rows);
    for (const CoColumn& c : columns) {
        co_put_string(s, c.name);
        co_put_string(s, c.type);
        s += c.body;
    }
    return s;
}

std::string co_end_of_stream() {
    std::string s;
    co_put_varint(s, 5);
    return s;
}

// The empty Data block a client sends to end an INSERT's data: Data, no
// table name, the block info, 0 columns and 0 rows.
const std::string kCoEndOfData("\x02\x00\x01\x00\x02\xff\xff\xff\xff\x00\x00\x00", 12);

// One accepted connection, driven by a script on the server thread. Every
// read gives up after 5 s, so a broken test fails instead of hanging.
class CoPeer {
public:
    explicit CoPeer(CoFd fd) : fd_(std::move(fd)) {
        timeval tv{};
        tv.tv_sec = 5;
        ::setsockopt(fd_.get(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    }

    std::uint8_t read_byte() {
        unsigned char b = 0;
        const ssize_t n = ::recv(fd_.get(), &b, 1, 0);
        if (n != 1) {
            throw std::runtime_error(n == 0 ? "the client closed the connection"
                                            : "recv failed or timed out");
        }
        ++received_;
        return b;
    }

    std::uint64_t read_varint() {
        std::uint64_t v = 0;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            const std::uint8_t b = read_byte();
            v |= static_cast<std::uint64_t>(b & 0x7fU) << shift;
            if ((b & 0x80U) == 0) {
                return v;
            }
        }
        throw std::runtime_error("varint too long");
    }

    std::string read_string() {
        const std::uint64_t n = read_varint();
        std::string s;
        for (std::uint64_t i = 0; i < n; ++i) {
            s.push_back(static_cast<char>(read_byte()));
        }
        return s;
    }

    void send(std::string_view bytes) {
        while (!bytes.empty()) {
            const ssize_t n = ::send(fd_.get(), bytes.data(), bytes.size(), 0);
            if (n <= 0) {
                throw std::runtime_error("send failed");
            }
            bytes.remove_prefix(static_cast<std::size_t>(n));
            sent_ += static_cast<std::size_t>(n);
        }
    }

    // Everything the client sends until it has gone quiet for `quiet`.
    std::string read_until_quiet(std::chrono::milliseconds quiet = 200ms) {
        if (!co_readable(fd_.get(), 5000ms)) {
            throw std::runtime_error("the client sent nothing");
        }
        std::string out;
        char buf[4096];
        do {
            const ssize_t n = ::recv(fd_.get(), buf, sizeof buf, 0);
            if (n <= 0) {
                break;
            }
            out.append(buf, static_cast<std::size_t>(n));
            received_ += static_cast<std::size_t>(n);
        } while (co_readable(fd_.get(), quiet));
        return out;
    }

    // What arrives before the client's end of stream, or nullopt if the
    // stream is still open after `wait`.
    std::optional<std::string> read_until_eof(std::chrono::milliseconds wait = 3000ms) {
        std::string out;
        const auto until = CoClock::now() + wait;
        char buf[4096];
        while (true) {
            const auto left =
                std::chrono::duration_cast<std::chrono::milliseconds>(until - CoClock::now());
            if (left <= 0ms || !co_readable(fd_.get(), left)) {
                return std::nullopt;
            }
            const ssize_t n = ::recv(fd_.get(), buf, sizeof buf, 0);
            if (n <= 0) {
                return out;
            }
            out.append(buf, static_cast<std::size_t>(n));
            received_ += static_cast<std::size_t>(n);
        }
    }

    [[nodiscard]] std::size_t received() const noexcept { return received_; }
    [[nodiscard]] std::size_t sent() const noexcept { return sent_; }

private:
    CoFd fd_;
    std::size_t received_{0};
    std::size_t sent_{0};
};

struct CoClientHello {
    std::string client_name;
    std::string database;
    std::string user;
    std::string password;
};

CoClientHello co_read_client_hello(CoPeer& peer) {
    if (peer.read_varint() != 0) {
        throw std::runtime_error("expected the client's Hello");
    }
    CoClientHello hello;
    hello.client_name = peer.read_string();
    (void)peer.read_varint();  // client major
    (void)peer.read_varint();  // client minor
    (void)peer.read_varint();  // client revision
    hello.database = peer.read_string();
    hello.user = peer.read_string();
    hello.password = peer.read_string();
    return hello;
}

// Runs a script against the connections it accepts, on its own thread, and
// keeps the first failure, so a broken script fails the test rather than
// terminating the binary. Declare it before the transport it serves, so that
// the transport goes first and the script sees its end of stream.
class CoServer {
public:
    using Script = std::function<void(CoServer&)>;

    explicit CoServer(Script script) : thread_([this, s = std::move(script)] { run(s); }) {}
    ~CoServer() { join(); }
    CoServer(const CoServer&) = delete;
    CoServer& operator=(const CoServer&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return listener_.port(); }
    [[nodiscard]] Endpoint endpoint() const { return Endpoint{"127.0.0.1", listener_.port()}; }

    CoPeer accept() {
        if (!co_readable(listener_.fd(), 5000ms)) {
            throw std::runtime_error("no connection arrived");
        }
        return CoPeer(CoFd(::accept(listener_.fd(), nullptr, nullptr)));
    }

    // Waits for the script; error() is meaningful afterwards.
    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    [[nodiscard]] const std::string& error() const noexcept { return error_; }

private:
    void run(const Script& script) {
        try {
            script(*this);
        } catch (const std::exception& e) {
            error_ = e.what();
        }
    }

    CoListener listener_;
    std::string error_;
    std::thread thread_;
};

// The Hello exchange a successful connect needs.
CoClientHello co_handshake(CoPeer& peer) {
    CoClientHello hello = co_read_client_hello(peer);
    peer.send(co_server_hello("replica-7", 26, 8, 15));
    return hello;
}

const std::string kCoInsert = "INSERT INTO `analytics`.`events` (`id`, `name`) FORMAT Native";

// Waits until `writing` is set and the bytes `transport` has written have
// grown past `from` and then stood still for `quiet`; false when that has not
// happened within 5 s. A send with room in the socket buffers returns at once,
// so a writer whose count stands still that long is blocked in one.
bool co_writer_blocks(const native::InsertTransport& transport,
                      const std::atomic<bool>& writing,
                      std::uint64_t from,
                      std::chrono::milliseconds quiet) {
    const auto give_up = CoClock::now() + 5s;
    while (!writing && CoClock::now() < give_up) {
        std::this_thread::sleep_for(1ms);
    }
    std::uint64_t last = transport.counters().bytes_written;
    auto since = CoClock::now();
    while (CoClock::now() < give_up) {
        std::this_thread::sleep_for(10ms);
        const std::uint64_t count = transport.counters().bytes_written;
        if (count != last) {
            last = count;
            since = CoClock::now();
        } else if (count > from && CoClock::now() - since >= quiet) {
            return true;
        }
    }
    return false;
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
std::string co_temp_path(const std::string& name) {
    return (std::filesystem::temp_directory_path() /
            ("clink-native-co-" + std::to_string(::getpid()) + "-" + name))
        .string();
}

std::string co_temp_file(const std::string& name, const std::string& contents) {
    const std::string path = co_temp_path(name);
    std::ofstream(path) << contents;
    return path;
}

std::string co_temp_dir(const std::string& name) {
    const std::string path = co_temp_path(name);
    std::filesystem::create_directories(path);
    return path;
}

// What a secure connect with `tls` refuses, or empty when it refuses nothing.
// On the way, checks that it opened no socket and left the transport
// unconnected.
std::string co_ca_refusal(native::TlsOptions tls) {
    CoListener listener;
    SinkOptions o = co_options(listener.port());
    tls.enabled = true;
    o.tls = std::move(tls);
    // Bounds the handshake a connect that wrongly gets that far would wait in.
    o.receive_timeout = 300ms;
    const auto transport = make_clickhouse_transport(o);
    std::string refusal;
    try {
        transport->connect(Endpoint{"127.0.0.1", listener.port()});
    } catch (const NativeSinkError& e) {
        EXPECT_EQ(e.code(), native::code::kOptionInvalid) << e.what();
        refusal = e.what();
    } catch (const std::exception& e) {
        ADD_FAILURE() << "the connect failed without a refusal: " << e.what();
    }
    EXPECT_FALSE(transport->connected());
    EXPECT_FALSE(listener.has_pending(100ms)) << "the connect opened a socket";
    return refusal;
}
#endif

}  // namespace

// --- make_client_options ---

TEST(NativeClientOptions, SetsExactlyOneEndpointAndNoHost) {
    const auto opts = make_client_options(co_options(), Endpoint{"ch-2.internal", 9440});
    EXPECT_TRUE(opts.host.empty());
    ASSERT_EQ(opts.endpoints.size(), 1U);
    EXPECT_EQ(opts.endpoints[0].host, "ch-2.internal");
    EXPECT_EQ(opts.endpoints[0].port, 9440);
}

TEST(NativeClientOptions, UsesTheEndpointGivenNotTheFirstConfiguredOne) {
    SinkOptions o = co_options();
    o.endpoints = {Endpoint{"ch-1", 9000}, Endpoint{"ch-2", 9001}};
    const auto opts = make_client_options(o, o.endpoints[1]);
    ASSERT_EQ(opts.endpoints.size(), 1U);
    EXPECT_EQ(opts.endpoints[0].host, "ch-2");
    EXPECT_EQ(opts.endpoints[0].port, 9001);
}

TEST(NativeClientOptions, CarriesTheDatabaseUserAndPassword) {
    const auto opts = make_client_options(co_options(), Endpoint{"ch", 9000});
    EXPECT_EQ(opts.default_database, "analytics");
    EXPECT_EQ(opts.user, "writer");
    EXPECT_EQ(opts.password, "s3cret");
}

TEST(NativeClientOptions, RethrowsExceptionsAndNeverPingsOrRetriesASend) {
    const auto opts = make_client_options(co_options(), Endpoint{"ch", 9000});
    EXPECT_TRUE(opts.rethrow_exceptions);
    EXPECT_FALSE(opts.ping_before_query);
    EXPECT_EQ(opts.send_retries, 1U);
}

TEST(NativeClientOptions, MapsEachTimeoutToItsOwnField) {
    SinkOptions o = co_options();
    o.connect_timeout = 1234ms;
    o.send_timeout = 2345ms;
    o.receive_timeout = 3456ms;
    const auto opts = make_client_options(o, Endpoint{"ch", 9000});
    EXPECT_EQ(opts.connection_connect_timeout, 1234ms);
    EXPECT_EQ(opts.connection_send_timeout, 2345ms);
    EXPECT_EQ(opts.connection_recv_timeout, 3456ms);

    o.connect_timeout = 1ms;
    o.send_timeout = 1ms;
    o.receive_timeout = 1ms;
    const auto smallest = make_client_options(o, Endpoint{"ch", 9000});
    EXPECT_EQ(smallest.connection_connect_timeout, 1ms);
    EXPECT_EQ(smallest.connection_send_timeout, 1ms);
    EXPECT_EQ(smallest.connection_recv_timeout, 1ms);
}

TEST(NativeClientOptions, RefusesATimeoutTheClientWouldReadAsNever) {
    const std::vector<std::pair<std::chrono::milliseconds SinkOptions::*, std::string>> timeouts{
        {&SinkOptions::connect_timeout, "connect_timeout_ms"},
        {&SinkOptions::send_timeout, "send_timeout_ms"},
        {&SinkOptions::receive_timeout, "receive_timeout_ms"},
    };
    for (const auto& [field, key] : timeouts) {
        SinkOptions o = co_options();
        o.*field = 0ms;
        const auto refusal =
            co_refusal([&] { (void)make_client_options(o, Endpoint{"ch", 9000}); });
        ASSERT_TRUE(refusal.has_value()) << key;
        EXPECT_EQ(refusal->code(), native::code::kOptionInvalid);
        EXPECT_STREQ(refusal->what(),
                     ("[clickhouse.option_invalid] clickhouse_native_sink: option '" + key +
                      "' must be at least 1 ms; got 0")
                         .c_str());
    }
}

TEST(NativeClientOptions, MapsEachCompressionToTheClientsMethod) {
    SinkOptions o = co_options();
    o.compression = Compression::Lz4;
    EXPECT_EQ(make_client_options(o, Endpoint{"ch", 9000}).compression_method,
              ::clickhouse::CompressionMethod::LZ4);
    o.compression = Compression::Zstd;
    EXPECT_EQ(make_client_options(o, Endpoint{"ch", 9000}).compression_method,
              ::clickhouse::CompressionMethod::ZSTD);
    o.compression = Compression::None;
    EXPECT_EQ(make_client_options(o, Endpoint{"ch", 9000}).compression_method,
              ::clickhouse::CompressionMethod::None);
}

// --- the CA fallback ---

native::TlsOptions verified_tls() {
    native::TlsOptions tls;
    tls.enabled = true;
    tls.verify = true;
    return tls;
}

// OpenSSL compiled to look under /usr/local/ssl, as a static OpenSSL built
// elsewhere does.
CaEnvironment foreign_openssl() {
    CaEnvironment env;
    env.default_cert_file = "/usr/local/ssl/cert.pem";
    env.default_cert_dir = "/usr/local/ssl/certs";
    return env;
}

// A filesystem holding exactly `present`.
std::function<PathKind(const std::string&)> filesystem_of(
    std::vector<std::pair<std::string, PathKind>> present) {
    return [present = std::move(present)](const std::string& path) {
        for (const auto& [p, kind] : present) {
            if (p == path) {
                return kind;
            }
        }
        return PathKind::Missing;
    };
}

TEST(NativeCaFallback, ProbesInTheKafkaClientsOrder) {
    const auto& locations = native::standard_ca_locations();
    ASSERT_GE(locations.size(), 11U);
    EXPECT_EQ(locations.front(), "/etc/pki/tls/certs/ca-bundle.crt");
    const auto at = [&](const std::string& path) {
        return std::find(locations.begin(), locations.end(), path) - locations.begin();
    };
    // The Debian bundle comes before the Debian directory it is built from.
    EXPECT_LT(at("/etc/ssl/certs/ca-certificates.crt"), at("/etc/ssl/certs"));
    EXPECT_LT(at("/etc/ssl/certs"), static_cast<std::ptrdiff_t>(locations.size()));
}

TEST(NativeCaFallback, TakesTheFirstStandardLocationThatExists) {
    // AlmaLinux 8: the RHEL bundle, and OpenSSL's own directory is absent.
    const auto alma = fallback_ca_location(
        verified_tls(),
        foreign_openssl(),
        filesystem_of({{"/etc/pki/tls/certs/ca-bundle.crt", PathKind::File},
                       {"/etc/ssl/certs/ca-certificates.crt", PathKind::File}}));
    ASSERT_TRUE(alma.has_value());
    EXPECT_EQ(alma->path, "/etc/pki/tls/certs/ca-bundle.crt");
    EXPECT_FALSE(alma->directory);

    // Debian without the openssl package: the bundle ca-certificates writes.
    const auto debian =
        fallback_ca_location(verified_tls(),
                             foreign_openssl(),
                             filesystem_of({{"/etc/ssl/certs/ca-certificates.crt", PathKind::File},
                                            {"/etc/ssl/certs", PathKind::Directory}}));
    ASSERT_TRUE(debian.has_value());
    EXPECT_EQ(debian->path, "/etc/ssl/certs/ca-certificates.crt");
    EXPECT_FALSE(debian->directory);
}

TEST(NativeCaFallback, ReportsADirectoryAsOne) {
    const auto dir = fallback_ca_location(verified_tls(),
                                          foreign_openssl(),
                                          filesystem_of({{"/etc/ssl/certs", PathKind::Directory}}));
    ASSERT_TRUE(dir.has_value());
    EXPECT_EQ(dir->path, "/etc/ssl/certs");
    EXPECT_TRUE(dir->directory);
}

TEST(NativeCaFallback, NoneWhenNothingExists) {
    EXPECT_FALSE(
        fallback_ca_location(verified_tls(), foreign_openssl(), filesystem_of({})).has_value());
}

TEST(NativeCaFallback, NoneWhenOpenSslsOwnDefaultsExist) {
    const auto bundle = std::pair{std::string("/etc/pki/tls/certs/ca-bundle.crt"), PathKind::File};
    EXPECT_FALSE(
        fallback_ca_location(verified_tls(),
                             foreign_openssl(),
                             filesystem_of({{"/usr/local/ssl/cert.pem", PathKind::File}, bundle}))
            .has_value());
    EXPECT_FALSE(
        fallback_ca_location(verified_tls(),
                             foreign_openssl(),
                             filesystem_of({{"/usr/local/ssl/certs", PathKind::Directory}, bundle}))
            .has_value());
}

TEST(NativeCaFallback, NoneWhenTheEnvironmentNamesTheStore) {
    const auto fs = filesystem_of({{"/etc/pki/tls/certs/ca-bundle.crt", PathKind::File}});
    CaEnvironment file_env = foreign_openssl();
    file_env.cert_file_env = "/opt/ca.pem";
    EXPECT_FALSE(fallback_ca_location(verified_tls(), file_env, fs).has_value());

    CaEnvironment dir_env = foreign_openssl();
    dir_env.cert_dir_env = "/opt/ca.d";
    EXPECT_FALSE(fallback_ca_location(verified_tls(), dir_env, fs).has_value());

    // Set to nothing reads as unset.
    CaEnvironment empty_env = foreign_openssl();
    empty_env.cert_file_env = "";
    empty_env.cert_dir_env = "";
    EXPECT_TRUE(fallback_ca_location(verified_tls(), empty_env, fs).has_value());
}

TEST(NativeCaFallback, NoneWhenACaIsNamedOrNothingIsVerified) {
    const auto fs = filesystem_of({{"/etc/pki/tls/certs/ca-bundle.crt", PathKind::File}});
    native::TlsOptions file = verified_tls();
    file.ca_file = "/etc/clink/ca.pem";
    EXPECT_FALSE(fallback_ca_location(file, foreign_openssl(), fs).has_value());

    native::TlsOptions dir = verified_tls();
    dir.ca_dir = "/etc/clink/ca.d";
    EXPECT_FALSE(fallback_ca_location(dir, foreign_openssl(), fs).has_value());

    native::TlsOptions unverified = verified_tls();
    unverified.verify = false;
    EXPECT_FALSE(fallback_ca_location(unverified, foreign_openssl(), fs).has_value());

    native::TlsOptions off = verified_tls();
    off.enabled = false;
    EXPECT_FALSE(fallback_ca_location(off, foreign_openssl(), fs).has_value());
}

TEST(NativeClientOptions, LeavesTlsOffUnlessSecure) {
    const auto opts = make_client_options(co_options(), Endpoint{"ch", 9000});
    EXPECT_FALSE(opts.ssl_options.has_value());
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)

TEST(NativeClientOptions, SecureTurnsOnSniAndSkipsVerificationOnlyWhenTlsVerifyIsOff) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    o.tls.verify = true;
    const auto verified = make_client_options(o, Endpoint{"ch", 9440});
    ASSERT_TRUE(verified.ssl_options.has_value());
    EXPECT_TRUE(verified.ssl_options->use_sni);
    EXPECT_FALSE(verified.ssl_options->skip_verification);

    o.tls.verify = false;
    const auto unverified = make_client_options(o, Endpoint{"ch", 9440});
    ASSERT_TRUE(unverified.ssl_options.has_value());
    EXPECT_TRUE(unverified.ssl_options->use_sni);
    EXPECT_TRUE(unverified.ssl_options->skip_verification);
}

TEST(NativeClientOptions, MapsTheCaFileAndTheCaDirectory) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    o.tls.ca_file = "/etc/clink/ca.pem";
    o.tls.ca_dir = "/etc/clink/ca.d";
    const auto opts = make_client_options(o, Endpoint{"ch", 9440});
    ASSERT_TRUE(opts.ssl_options.has_value());
    EXPECT_EQ(opts.ssl_options->path_to_ca_files, std::vector<std::string>{"/etc/clink/ca.pem"});
    EXPECT_EQ(opts.ssl_options->path_to_ca_directory, "/etc/clink/ca.d");

    o.tls.ca_file.clear();
    o.tls.ca_dir.clear();
    const auto none = make_client_options(o, Endpoint{"ch", 9440});
    ASSERT_TRUE(none.ssl_options.has_value());
    EXPECT_TRUE(none.ssl_options->path_to_ca_files.empty());
    EXPECT_TRUE(none.ssl_options->path_to_ca_directory.empty());
}

TEST(NativeClientOptions, TurnsOffTheDefaultCaLocationsExactlyWhenACaIsNamed) {
    struct Case {
        std::string file;
        std::string dir;
        bool defaults;
    };
    const std::vector<Case> cases{
        {"", "", true},
        {"/ca.pem", "", false},
        {"", "/ca.d", false},
        {"/ca.pem", "/ca.d", false},
    };
    for (const Case& c : cases) {
        SinkOptions o = co_options();
        o.tls.enabled = true;
        o.tls.ca_file = c.file;
        o.tls.ca_dir = c.dir;
        const auto opts = make_client_options(o, Endpoint{"ch", 9440});
        ASSERT_TRUE(opts.ssl_options.has_value());
        EXPECT_EQ(opts.ssl_options->use_default_ca_locations, c.defaults)
            << "file='" << c.file << "' dir='" << c.dir << "'";
    }
}

TEST(NativeClientOptions, AddsTheFallbackCaAndKeepsTheDefaults) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    const auto file = make_client_options(
        o, Endpoint{"ch", 9440}, CaLocation{"/etc/pki/tls/certs/ca-bundle.crt", false});
    ASSERT_TRUE(file.ssl_options.has_value());
    EXPECT_EQ(file.ssl_options->path_to_ca_files,
              std::vector<std::string>{"/etc/pki/tls/certs/ca-bundle.crt"});
    EXPECT_TRUE(file.ssl_options->path_to_ca_directory.empty());
    EXPECT_TRUE(file.ssl_options->use_default_ca_locations);

    const auto dir =
        make_client_options(o, Endpoint{"ch", 9440}, CaLocation{"/etc/ssl/certs", true});
    ASSERT_TRUE(dir.ssl_options.has_value());
    EXPECT_TRUE(dir.ssl_options->path_to_ca_files.empty());
    EXPECT_EQ(dir.ssl_options->path_to_ca_directory, "/etc/ssl/certs");
    EXPECT_TRUE(dir.ssl_options->use_default_ca_locations);
}

TEST(NativeClientOptions, ANamedCaStillTheOnlyOneTrustedWithAFallbackOffered) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    o.tls.ca_file = "/etc/clink/ca.pem";
    const auto opts = make_client_options(
        o, Endpoint{"ch", 9440}, CaLocation{"/etc/pki/tls/certs/ca-bundle.crt", false});
    ASSERT_TRUE(opts.ssl_options.has_value());
    EXPECT_EQ(opts.ssl_options->path_to_ca_files, std::vector<std::string>{"/etc/clink/ca.pem"});
    EXPECT_FALSE(opts.ssl_options->use_default_ca_locations);
}

#else

TEST(NativeClientOptions, SecureRefusesTlsUnavailableOnABuildWithoutIt) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    const auto refusal = co_refusal([&] { (void)make_client_options(o, Endpoint{"ch", 9440}); });
    ASSERT_TRUE(refusal.has_value());
    EXPECT_EQ(refusal->code(), native::code::kTlsUnavailable);
}

#endif

// --- the header and result readers ---

TEST(NativeTransportBlocks, HeaderColumnsUseTheClientsTypeSpelling) {
    ::clickhouse::Block header;
    header.AppendColumn("id", std::make_shared<::clickhouse::ColumnInt64>());
    header.AppendColumn(
        "name", std::make_shared<::clickhouse::ColumnNullableT<::clickhouse::ColumnString>>());
    header.AppendColumn(
        "kind",
        std::make_shared<::clickhouse::ColumnLowCardinalityT<::clickhouse::ColumnString>>());
    header.AppendColumn("at", std::make_shared<::clickhouse::ColumnDateTime>("UTC"));

    const auto columns = native::header_columns(header);
    ASSERT_EQ(columns.size(), 4U);
    EXPECT_EQ(columns[0].name, "id");
    EXPECT_EQ(columns[0].type, "Int64");
    EXPECT_EQ(columns[1].name, "name");
    EXPECT_EQ(columns[1].type, "Nullable(String)");
    EXPECT_EQ(columns[2].name, "kind");
    EXPECT_EQ(columns[2].type, "LowCardinality(String)");
    EXPECT_EQ(columns[3].name, "at");
    EXPECT_EQ(columns[3].type, "DateTime('UTC')");
}

TEST(NativeTransportBlocks, ResultBlocksReadEveryTextColumnKindAndNullsAsEmpty) {
    auto plain = std::make_shared<::clickhouse::ColumnString>();
    plain->Append("a");
    plain->Append("b");
    auto nullable = std::make_shared<::clickhouse::ColumnNullableT<::clickhouse::ColumnString>>();
    nullable->Append(std::optional<std::string_view>("1"));
    nullable->Append(std::nullopt);
    auto low = std::make_shared<::clickhouse::ColumnLowCardinalityT<::clickhouse::ColumnString>>();
    low->Append("x");
    low->Append("x");
    auto low_nullable = std::make_shared<::clickhouse::ColumnLowCardinalityT<
        ::clickhouse::ColumnNullableT<::clickhouse::ColumnString>>>();
    low_nullable->Append(std::nullopt);
    low_nullable->Append(std::optional<std::string_view>("y"));

    ::clickhouse::Block block;
    block.AppendColumn("plain", plain);
    block.AppendColumn("nullable", nullable);
    block.AppendColumn("low", low);
    block.AppendColumn("low_nullable", low_nullable);

    native::ResultSet out;
    native::append_result_block(block, out);
    EXPECT_EQ(out.columns, (std::vector<std::string>{"plain", "nullable", "low", "low_nullable"}));
    EXPECT_EQ(out.rows,
              (std::vector<std::vector<std::string>>{{"a", "1", "x", ""}, {"b", "", "x", "y"}}));
}

TEST(NativeTransportBlocks, ResultRowsAccumulateAcrossBlocksAfterAnEmptyHeader) {
    ::clickhouse::Block header;
    header.AppendColumn("name", std::make_shared<::clickhouse::ColumnString>());
    auto first = std::make_shared<::clickhouse::ColumnString>();
    first->Append("async_insert");
    ::clickhouse::Block one;
    one.AppendColumn("name", first);
    auto second = std::make_shared<::clickhouse::ColumnString>();
    second->Append("max_threads");
    second->Append("log_comment");
    ::clickhouse::Block two;
    two.AppendColumn("name", second);

    native::ResultSet out;
    native::append_result_block(header, out);
    EXPECT_EQ(out.columns, std::vector<std::string>{"name"});
    EXPECT_TRUE(out.rows.empty());
    native::append_result_block(one, out);
    native::append_result_block(two, out);
    EXPECT_EQ(out.rows,
              (std::vector<std::vector<std::string>>{
                  {"async_insert"}, {"max_threads"}, {"log_comment"}}));
}

TEST(NativeTransportBlocks, AResultColumnOfAnotherTypeThrowsProtocolErrorNamingIt) {
    auto cluster = std::make_shared<::clickhouse::ColumnString>();
    cluster->Append("pins");
    auto count = std::make_shared<::clickhouse::ColumnUInt64>();
    count->Append(3);
    ::clickhouse::Block block;
    block.AppendColumn("name", cluster);
    block.AppendColumn("replicas", count);
    native::ResultSet out;
    try {
        native::append_result_block(block, out);
        FAIL() << "a UInt64 result column was read";
    } catch (const ::clickhouse::ProtocolError& e) {
        EXPECT_STREQ(e.what(),
                     "clickhouse native sink: metadata column `replicas` is UInt64; only String, "
                     "Nullable(String) and LowCardinality(String) are read");
    }

    ::clickhouse::Block nullable_number;
    nullable_number.AppendColumn(
        "n", std::make_shared<::clickhouse::ColumnNullableT<::clickhouse::ColumnInt64>>());
    native::ResultSet other;
    EXPECT_THROW(native::append_result_block(nullable_number, other), ::clickhouse::ProtocolError);
}

TEST(NativeTransportBlocks, AResultBlockWithOtherColumnsThrowsProtocolError) {
    ::clickhouse::Block first;
    first.AppendColumn("name", std::make_shared<::clickhouse::ColumnString>());
    ::clickhouse::Block renamed;
    renamed.AppendColumn("value", std::make_shared<::clickhouse::ColumnString>());
    ::clickhouse::Block wider;
    wider.AppendColumn("name", std::make_shared<::clickhouse::ColumnString>());
    wider.AppendColumn("value", std::make_shared<::clickhouse::ColumnString>());

    native::ResultSet out;
    native::append_result_block(first, out);
    EXPECT_THROW(native::append_result_block(renamed, out), ::clickhouse::ProtocolError);
    EXPECT_THROW(native::append_result_block(wider, out), ::clickhouse::ProtocolError);
}

// --- the real transport ---

TEST(NativeTransport, ConnectAfterAnInterruptThrowsConnectionAbortedAndOpensNoSocket) {
    CoListener listener;
    const auto transport = make_clickhouse_transport(co_options(listener.port()));
    const Endpoint ep{"127.0.0.1", listener.port()};

    transport->interrupt();
    EXPECT_EQ(co_system_errno([&] { transport->connect(ep); }), ECONNABORTED);
    EXPECT_EQ(co_system_errno([&] { transport->connect(ep); }), ECONNABORTED);
    EXPECT_FALSE(transport->connected());
    EXPECT_FALSE(listener.has_pending(100ms));
    EXPECT_EQ(transport->counters().connects, 0U);
}

TEST(NativeTransport, ARefusedConnectLeavesTheTransportUnconnected) {
    std::uint16_t port = 0;
    {
        const CoListener closed;
        port = closed.port();
    }
    const auto transport = make_clickhouse_transport(co_options(port));
    EXPECT_EQ(co_system_errno([&] { transport->connect(Endpoint{"127.0.0.1", port}); }),
              ECONNREFUSED);
    EXPECT_FALSE(transport->connected());
    EXPECT_EQ(transport->counters().connects, 0U);
    EXPECT_EQ(co_system_errno([&] { (void)transport->begin_insert(kCoInsert); }), ENOTCONN);
}

TEST(NativeTransport, AHandshakeReportsTheServerAndCountsEveryByteBothWays) {
    CoClientHello hello;
    std::optional<std::string> after_handshake;
    std::size_t server_received = 0;
    std::size_t server_sent = 0;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        hello = co_handshake(peer);
        after_handshake = peer.read_until_eof();
        server_received = peer.received();
        server_sent = peer.sent();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));

    transport->connect(server.endpoint());
    ASSERT_TRUE(transport->connected());
    const native::ServerIdentity& id = transport->server();
    EXPECT_EQ(id.display_name, "replica-7");
    EXPECT_EQ(id.major, 26U);
    EXPECT_EQ(id.minor, 8U);
    EXPECT_EQ(id.patch, 15U);
    EXPECT_EQ(id.revision, kCoRevision);
    EXPECT_EQ(id.endpoint, server.endpoint());

    // Already connected: nothing is built and nothing is sent.
    transport->connect(server.endpoint());
    const native::TransportCounters counters = transport->counters();
    EXPECT_EQ(counters.connects, 1U);

    transport->abandon();
    EXPECT_FALSE(transport->connected());
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(hello.client_name, "clickhouse-cpp");
    EXPECT_EQ(hello.database, "analytics");
    EXPECT_EQ(hello.user, "writer");
    EXPECT_EQ(hello.password, "s3cret");
    EXPECT_EQ(after_handshake, std::optional<std::string>(""));
    EXPECT_EQ(counters.bytes_written, server_received);
    EXPECT_EQ(counters.bytes_read, server_sent);
}

TEST(NativeTransport, CountersAddUpAcrossEveryClientTheTransportBuilt) {
    std::size_t first_received = 0;
    std::size_t second_received = 0;
    std::size_t second_sent = 0;
    CoServer server([&](CoServer& s) {
        {
            // Takes the Hello and hangs up, so the first client fails.
            CoPeer peer = s.accept();
            (void)co_read_client_hello(peer);
            first_received = peer.received();
        }
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_eof();
        second_received = peer.received();
        second_sent = peer.sent();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));

    // The client reports the hang-up as "closed", with whatever errno was left.
    EXPECT_THROW(transport->connect(server.endpoint()), std::system_error);
    EXPECT_FALSE(transport->connected());
    transport->connect(server.endpoint());
    ASSERT_TRUE(transport->connected());
    const native::TransportCounters counters = transport->counters();
    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");

    EXPECT_GT(first_received, 0U);
    EXPECT_EQ(counters.bytes_written, first_received + second_received);
    EXPECT_EQ(counters.bytes_read, second_sent);
    EXPECT_EQ(counters.connects, 1U);
}

TEST(NativeTransport, BeginInsertReturnsTheHeaderTheServerSent) {
    std::string query;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        query = peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}, {"name", "LowCardinality(String)", ""}}, 0));
        (void)peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());

    const auto header = transport->begin_insert(kCoInsert);
    ASSERT_EQ(header.size(), 2U);
    EXPECT_EQ(header[0].name, "id");
    EXPECT_EQ(header[0].type, "Int64");
    EXPECT_EQ(header[1].name, "name");
    EXPECT_EQ(header[1].type, "LowCardinality(String)");

    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_NE(query.find(kCoInsert), std::string::npos);
}

// Why abandon() poisons first: a client dropped mid-INSERT ends the INSERT
// from its destructor, and the server would commit what it had been sent.
TEST(NativeTransport, AClientDroppedMidInsertWithoutAPoisonSendsTheEndOfDataMarker) {
    std::string marker;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        // Hangs up once the marker is in, which ends the destructor's wait
        // for EndOfStream.
        marker = peer.read_until_quiet();
    });
    {
        ::clickhouse::Client client(
            make_client_options(co_options(server.port()), server.endpoint()));
        (void)client.BeginInsert(kCoInsert);
    }
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(marker, kCoEndOfData);
}

TEST(NativeTransport, AbandonMidInsertSendsNothingAfterTheQuery) {
    std::optional<std::string> after_header;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        after_header = peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());
    ASSERT_EQ(transport->begin_insert(kCoInsert).size(), 1U);

    transport->abandon();
    EXPECT_FALSE(transport->connected());
    server.join();
    ASSERT_EQ(server.error(), "");
    // The stream ended with no end-of-data marker, so the server discards
    // the INSERT.
    EXPECT_EQ(after_header, std::optional<std::string>(""));
}

TEST(NativeTransport, DestroyingTheTransportMidInsertSendsNothingAfterTheQuery) {
    std::optional<std::string> after_header;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        after_header = peer.read_until_eof();
    });
    {
        const auto transport = make_clickhouse_transport(co_options(server.port()));
        transport->connect(server.endpoint());
        ASSERT_EQ(transport->begin_insert(kCoInsert).size(), 1U);
    }
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(after_header, std::optional<std::string>(""));
}

TEST(NativeTransport, EndInsertSendsTheMarkerAndReturnsOnEndOfStream) {
    std::string marker;
    std::optional<std::string> after_end;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        marker = peer.read_until_quiet();
        peer.send(co_end_of_stream());
        after_end = peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());
    (void)transport->begin_insert(kCoInsert);

    transport->end_insert();
    EXPECT_TRUE(transport->connected());
    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(marker, kCoEndOfData);
    EXPECT_EQ(after_end, std::optional<std::string>(""));
}

TEST(NativeTransport, AnEndInsertPastTheDeadlineTimesOutAndSendsNothing) {
    std::optional<std::string> after_header;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        after_header = peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());
    (void)transport->begin_insert(kCoInsert);

    transport->set_deadline(CoClock::now() - 1ms);
    EXPECT_EQ(co_system_errno([&] { transport->end_insert(); }), ETIMEDOUT);
    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(after_header, std::optional<std::string>(""));
}

TEST(NativeTransport, ADeadlineSetBeforeConnectAppliesToTheNextClient) {
    std::optional<std::string> first;
    std::size_t second_received = 0;
    CoServer server([&](CoServer& s) {
        {
            CoPeer peer = s.accept();
            first = peer.read_until_eof();
        }
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_eof();
        second_received = peer.received();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));

    transport->set_deadline(CoClock::now() - 1ms);
    EXPECT_EQ(co_system_errno([&] { transport->connect(server.endpoint()); }), ETIMEDOUT);
    EXPECT_FALSE(transport->connected());
    transport->set_deadline(std::nullopt);
    transport->connect(server.endpoint());
    EXPECT_TRUE(transport->connected());
    const native::TransportCounters counters = transport->counters();
    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");
    // The first client's Hello never left.
    EXPECT_EQ(first, std::optional<std::string>(""));
    EXPECT_EQ(counters.bytes_written, second_received);
}

TEST(NativeTransport, AnInterruptDuringTheHandshakeFailsTheConnectWithin100Ms) {
    std::atomic<bool> hello_in{false};
    std::optional<std::string> after_hello;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_read_client_hello(peer);
        hello_in = true;
        // Never answers: the client stays blocked reading the server's Hello.
        after_hello = peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));

    std::exception_ptr failure;
    CoClock::time_point failed_at;
    std::thread connector([&] {
        try {
            transport->connect(server.endpoint());
        } catch (...) {
            failure = std::current_exception();
        }
        failed_at = CoClock::now();
    });
    const auto give_up = CoClock::now() + 5s;
    while (!hello_in && CoClock::now() < give_up) {
        std::this_thread::sleep_for(5ms);
    }
    std::this_thread::sleep_for(20ms);
    const auto interrupted_at = CoClock::now();
    transport->interrupt();
    connector.join();

    ASSERT_TRUE(hello_in);
    ASSERT_TRUE(failure);
    EXPECT_EQ(co_errno_of(failure), ECONNABORTED);
    EXPECT_LT(failed_at - interrupted_at, 100ms);
    EXPECT_FALSE(transport->connected());
    EXPECT_EQ(co_system_errno([&] { transport->connect(server.endpoint()); }), ECONNABORTED);
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_EQ(after_hello, std::optional<std::string>(""));
}

TEST(NativeTransport, AnInterruptWakesASelectBlockedOnTheServerWithin100Ms) {
    std::atomic<bool> query_in{false};
    std::string query;
    std::optional<std::string> after_query;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        query = peer.read_until_quiet();
        query_in = true;
        after_query = peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());

    CoClock::time_point interrupted_at;
    std::thread interrupter([&] {
        const auto give_up = CoClock::now() + 5s;
        while (!query_in && CoClock::now() < give_up) {
            std::this_thread::sleep_for(5ms);
        }
        std::this_thread::sleep_for(20ms);
        interrupted_at = CoClock::now();
        transport->interrupt();
    });
    const int err = co_system_errno(
        [&] { (void)transport->select(native::MetaQuery::Table, "SELECT CAST(1 AS String)"); });
    const auto woke_at = CoClock::now();
    interrupter.join();

    EXPECT_EQ(err, ECONNABORTED);
    EXPECT_LT(woke_at - interrupted_at, 100ms);
    transport->abandon();
    // Sticky: the transport never connects again.
    EXPECT_EQ(co_system_errno([&] { transport->connect(server.endpoint()); }), ECONNABORTED);
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_NE(query.find("SELECT CAST(1 AS String)"), std::string::npos);
    EXPECT_EQ(after_query, std::optional<std::string>(""));
}

// A cancel while the server has stopped reading mid-INSERT: send_block is
// blocked in send with the socket buffers full, and interrupt() must return
// it at once, not after the 5 s send timeout.
TEST(NativeTransport, AnInterruptWakesASendBlockBlockedOnAStalledServerWithin100Ms) {
    std::atomic<bool> header_out{false};
    std::atomic<bool> done{false};
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        (void)peer.read_until_quiet();
        peer.send(co_data_packet({{"id", "Int64", ""}}, 0));
        header_out = true;
        // Reads nothing more until the test is done with the connection.
        const auto give_up = CoClock::now() + 10s;
        while (!done && CoClock::now() < give_up) {
            std::this_thread::sleep_for(5ms);
        }
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());
    const auto header = transport->begin_insert(kCoInsert);
    const std::uint64_t before_blocks = transport->counters().bytes_written;

    // 64 KiB of values a block, so that the count moves with every block the
    // socket takes and stops when it takes no more.
    ::clickhouse::Block block;
    block.AppendColumn(
        "id", std::make_shared<::clickhouse::ColumnInt64>(std::vector<std::int64_t>(8192, 7)));
    std::atomic<bool> writing{false};
    std::atomic<bool> returned{false};
    std::exception_ptr failure;
    CoClock::time_point woke_at;
    std::thread writer([&] {
        writing = true;
        try {
            // 256 MiB at most, far more than the buffers of both ends hold.
            for (int i = 0; i < 4096; ++i) {
                transport->send_block(block);
            }
        } catch (...) {
            failure = std::current_exception();
        }
        woke_at = CoClock::now();
        returned = true;
    });
    const bool blocked = co_writer_blocks(*transport, writing, before_blocks, 200ms);
    const bool still_writing = !returned;
    const CoClock::time_point interrupted_at = CoClock::now();
    transport->interrupt();
    writer.join();
    done = true;
    transport->abandon();
    server.join();

    ASSERT_EQ(server.error(), "");
    ASSERT_TRUE(header_out);
    EXPECT_EQ(header.size(), 1U);
    EXPECT_TRUE(blocked) << "send_block never blocked in send";
    EXPECT_TRUE(still_writing) << "send_block finished before the interrupt";
    ASSERT_TRUE(failure);
    EXPECT_EQ(co_errno_of(failure), ECONNABORTED);
    EXPECT_LT(woke_at - interrupted_at, 100ms)
        << std::chrono::duration_cast<std::chrono::milliseconds>(woke_at - interrupted_at).count()
        << " ms";
}

TEST(NativeTransport, SelectReadsTheTextRowsOfEveryBlock) {
    std::string query;
    CoServer server([&](CoServer& s) {
        CoPeer peer = s.accept();
        (void)co_handshake(peer);
        query = peer.read_until_quiet();
        peer.send(co_data_packet({{"name", "String", ""}, {"value", "Nullable(String)", ""}}, 0));
        std::string names;
        co_put_string(names, "async_insert");
        co_put_string(names, "log_comment");
        std::string values("\x00\x01", 2);  // null map: the second value is NULL
        co_put_string(values, "0");
        co_put_string(values, "");
        peer.send(
            co_data_packet({{"name", "String", names}, {"value", "Nullable(String)", values}}, 2));
        peer.send(co_end_of_stream());
        (void)peer.read_until_eof();
    });
    const auto transport = make_clickhouse_transport(co_options(server.port()));
    transport->connect(server.endpoint());

    const std::string sql =
        "SELECT CAST(name AS String), CAST(value AS String) FROM system.settings";
    const native::ResultSet result = transport->select(native::MetaQuery::ServerSettings, sql);
    EXPECT_EQ(result.columns, (std::vector<std::string>{"name", "value"}));
    EXPECT_EQ(result.rows,
              (std::vector<std::vector<std::string>>{{"async_insert", "0"}, {"log_comment", ""}}));
    transport->abandon();
    server.join();
    ASSERT_EQ(server.error(), "");
    EXPECT_NE(query.find(sql), std::string::npos);
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)

TEST(NativeTransport, AnUnloadableCaFileRefusesOptionInvalidBeforeAnySocket) {
    CoListener listener;
    const std::string path = co_temp_file("bad-ca.pem", "this is not a certificate\n");
    SinkOptions o = co_options(listener.port());
    o.tls.enabled = true;
    o.tls.ca_file = path;
    const auto transport = make_clickhouse_transport(o);

    const auto refusal =
        co_refusal([&] { transport->connect(Endpoint{"127.0.0.1", listener.port()}); });
    ASSERT_TRUE(refusal.has_value());
    EXPECT_EQ(refusal->code(), native::code::kOptionInvalid);
    const std::string expected_start =
        "[clickhouse.option_invalid] clickhouse_native_sink: option "
        "'tls_ca_file' ('" +
        path + "') could not be loaded by OpenSSL: ";
    EXPECT_EQ(std::string(refusal->what()).substr(0, expected_start.size()), expected_start);
    EXPECT_GT(std::string(refusal->what()).size(), expected_start.size());
    EXPECT_FALSE(transport->connected());
    EXPECT_FALSE(listener.has_pending(100ms));
    std::filesystem::remove(path);
}

// OpenSSL accepts any directory when the factory is built, so a typo or an
// unmounted volume would otherwise reach the handshake and fail there as a
// verify error naming no key, or be ignored with tls_verify off.
TEST(NativeTransport, AMissingCaDirectoryRefusesOptionInvalidBeforeAnySocket) {
    for (const bool verify : {true, false}) {
        native::TlsOptions tls;
        tls.ca_dir = "/nonexistent/clink-native-co-ca.d";
        tls.verify = verify;
        EXPECT_EQ(co_ca_refusal(tls),
                  "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_dir' "
                  "('/nonexistent/clink-native-co-ca.d') does not exist")
            << "tls_verify=" << verify;
    }
}

TEST(NativeTransport, ACaDirectoryThatIsAFileRefusesOptionInvalidBeforeAnySocket) {
    const std::string path = co_temp_file("ca-dir-is-a-file", "x\n");
    native::TlsOptions tls;
    tls.ca_dir = path;
    EXPECT_EQ(co_ca_refusal(tls),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_dir' ('" + path +
                  "') is not a directory");
    std::filesystem::remove(path);
}

TEST(NativeTransport, AMissingCaFileRefusesOptionInvalidInItsOwnWords) {
    native::TlsOptions tls;
    tls.ca_file = "/nonexistent/clink-native-co-ca.pem";
    EXPECT_EQ(co_ca_refusal(tls),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_file' "
              "('/nonexistent/clink-native-co-ca.pem') does not exist");
}

TEST(NativeTransport, ACaFileThatIsADirectoryRefusesOptionInvalidBeforeAnySocket) {
    const std::string path = co_temp_dir("ca-file-is-a-dir");
    native::TlsOptions tls;
    tls.ca_file = path;
    EXPECT_EQ(co_ca_refusal(tls),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_file' ('" + path +
                  "') is a directory; a directory of CA certificates goes in 'tls_ca_dir'");
    std::filesystem::remove(path);
}

TEST(NativeTransport, AnUnsearchableCaDirectoryOrUnreadableCaFileRefusesOptionInvalid) {
    if (::geteuid() == 0) {
        GTEST_SKIP() << "root reads and searches whatever the mode says";
    }
    namespace fs = std::filesystem;
    const std::string denied = std::system_category().message(EACCES);
    // Readable but not searchable: OpenSSL could not open a certificate in it.
    const std::string dir = co_temp_dir("ca-unsearchable.d");
    fs::permissions(dir, fs::perms::owner_read | fs::perms::owner_write);
    const std::string file = co_temp_file("ca-unreadable.pem", "x\n");
    fs::permissions(file, fs::perms::owner_write);

    native::TlsOptions with_dir;
    with_dir.ca_dir = dir;
    EXPECT_EQ(co_ca_refusal(with_dir),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_dir' ('" + dir +
                  "') cannot be searched: " + denied);
    native::TlsOptions with_file;
    with_file.ca_file = file;
    EXPECT_EQ(co_ca_refusal(with_file),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_file' ('" + file +
                  "') cannot be read: " + denied);

    fs::permissions(dir, fs::perms::owner_all);
    fs::permissions(file, fs::perms::owner_read | fs::perms::owner_write);
    fs::remove(dir);
    fs::remove(file);
}

// With both keys set, a directory that is not there is named on its own, and
// once the paths are sound only the file can fail inside OpenSSL, so that is
// the one named.
TEST(NativeTransport, WithBothCaKeysSetTheRefusalNamesTheOneAtFault) {
    const std::string bad_file = co_temp_file("both-bad-ca.pem", "this is not a certificate\n");
    const std::string dir = co_temp_dir("both-ca.d");

    native::TlsOptions missing_dir;
    missing_dir.ca_dir = "/nonexistent/clink-native-co-ca.d";
    missing_dir.ca_file = bad_file;
    EXPECT_EQ(co_ca_refusal(missing_dir),
              "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_dir' "
              "('/nonexistent/clink-native-co-ca.d') does not exist");

    native::TlsOptions malformed_file;
    malformed_file.ca_dir = dir;
    malformed_file.ca_file = bad_file;
    const std::string refusal = co_ca_refusal(malformed_file);
    const std::string expected_start =
        "[clickhouse.option_invalid] clickhouse_native_sink: option 'tls_ca_file' ('" + bad_file +
        "') could not be loaded by OpenSSL: ";
    EXPECT_EQ(refusal.substr(0, expected_start.size()), expected_start) << refusal;
    EXPECT_GT(refusal.size(), expected_start.size());
    EXPECT_EQ(refusal.find("tls_ca_dir"), std::string::npos) << refusal;

    std::filesystem::remove(bad_file);
    std::filesystem::remove(dir);
}

// The check refuses only what cannot work: a directory that is there, even
// one holding nothing yet, is left to the handshake, which reaches the server.
TEST(NativeTransport, AnExistingCaDirectoryIsLeftToTheHandshake) {
    const std::string dir = co_temp_dir("empty-ca.d");
    CoServer server([](CoServer& s) {
        // Hangs up at once, so the handshake fails quickly.
        (void)s.accept();
    });
    SinkOptions o = co_options(server.port());
    o.tls.enabled = true;
    o.tls.ca_dir = dir;
    o.receive_timeout = 1000ms;
    const auto transport = make_clickhouse_transport(o);

    bool refused = false;
    bool failed = false;
    try {
        transport->connect(server.endpoint());
    } catch (const NativeSinkError& e) {
        refused = true;
        ADD_FAILURE() << e.what();
    } catch (const std::exception&) {
        failed = true;
    }
    server.join();
    EXPECT_FALSE(refused);
    EXPECT_TRUE(failed) << "a handshake against a peer that hung up succeeded";
    EXPECT_EQ(server.error(), "") << "the connect never reached the server";
    std::filesystem::remove(dir);
}

#else

TEST(NativeTransport, SecureRefusesTlsUnavailableOnABuildWithoutIt) {
    SinkOptions o = co_options();
    o.tls.enabled = true;
    const auto refusal = co_refusal([&] { (void)make_clickhouse_transport(o); });
    ASSERT_TRUE(refusal.has_value());
    EXPECT_EQ(refusal->code(), native::code::kTlsUnavailable);
}

#endif
