#pragma once

// A TCP proxy for fault tests: a client connects to port() instead of the
// server, and the test decides what the network does in between.
//
// It forwards bytes unchanged until told otherwise, and then can:
//   * drop every open connection with a reset (drop_all);
//   * refuse: reset every open connection and every new one at once, the
//     local model of a server that is down or unreachable (set_refusing);
//   * blackhole: keep the connections open but forward nothing either way,
//     so the client waits on its own timeouts (set_blackhole);
//   * cut an INSERT mid-upload: once the server's reply to an INSERT
//     statement has reached the client, reset the connection at the first
//     bytes the client sends after it, forwarding none of them, so the
//     server never sees a row of that INSERT (drop_at_upload);
//   * hold an INSERT's reply: from the client's first upload bytes on,
//     forward the client's bytes but withhold the server's, so the client
//     sits waiting for the end of its INSERT while the server has already
//     received it (hold_at_upload). drop_held() then resets those
//     connections without delivering anything that was withheld.
//
// The INSERT detection reads the ClickHouse native protocol only as far as
// it needs to: a query packet carries its statement text uncompressed, so
// "INSERT INTO" in the client's bytes marks a statement, and the client's
// next bytes after any reply from the server are its data blocks. Data
// blocks themselves are compressed, so they do not match by accident.
//
// Threads: one acceptor, and one pump per connection. Only a pump closes its
// own sockets; every other call sets a flag the pump acts on within one poll
// interval, and the calls that promise a reset wait for it.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <list>
#include <memory>
#include <mutex>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace clink::itest {

class TcpFaultProxy {
public:
    explicit TcpFaultProxy(std::uint16_t upstream_port, std::string upstream_host = "127.0.0.1")
        : upstream_host_(std::move(upstream_host)), upstream_port_(upstream_port) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            throw std::runtime_error("TcpFaultProxy: socket failed");
        }
        const int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(listen_fd_, 64) != 0) {
            ::close(listen_fd_);
            throw std::runtime_error("TcpFaultProxy: bind or listen failed");
        }
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        acceptor_ = std::thread([this] { accept_loop(); });
    }

    ~TcpFaultProxy() {
        stop_.store(true);
        if (acceptor_.joinable()) {
            acceptor_.join();
        }
        ::close(listen_fd_);
        std::list<std::shared_ptr<Conn>> conns;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            conns.swap(conns_);
        }
        for (auto& c : conns) {
            c->kill.store(true);
            if (c->pump.joinable()) {
                c->pump.join();
            }
        }
    }

    TcpFaultProxy(const TcpFaultProxy&) = delete;
    TcpFaultProxy& operator=(const TcpFaultProxy&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

    // Reset every open connection, and return once each is closed.
    void drop_all() {
        for_each_conn([](Conn& c) { c.kill.store(true); });
        await_closed([](const Conn&) { return true; });
    }

    // While on, every open connection is reset and every new one is reset
    // as soon as it is accepted.
    void set_refusing(bool on) {
        refusing_.store(on);
        if (on) {
            drop_all();
        }
    }

    // While on, nothing is forwarded in either direction. Connections stay
    // open and resume when it ends, unless their client gave up meanwhile.
    void set_blackhole(bool on) { blackhole_.store(on); }

    // Cut the next `n` INSERT uploads before a byte of them is forwarded.
    void drop_at_upload(std::size_t n) { drop_uploads_.fetch_add(n); }

    // Withhold the server's reply to the next `n` INSERT uploads.
    void hold_at_upload(std::size_t n) { hold_uploads_.fetch_add(n); }

    // Forget the cuts and holds not yet used. A connection already held stays
    // held until drop_held().
    void clear_upload_faults() {
        drop_uploads_.store(0);
        hold_uploads_.store(0);
    }

    // Reset every connection whose server bytes are being withheld, without
    // delivering them, and return once each is closed.
    void drop_held() {
        for_each_conn([](Conn& c) {
            if (c.held.load()) {
                c.kill.store(true);
            }
        });
        await_closed([](const Conn& c) { return c.held.load(); });
    }

    // True when at least one connection is held and none of the held ones
    // has had a byte from its client for `quiet`: each client has sent its
    // whole INSERT, end-of-data marker included, and waits for the reply.
    [[nodiscard]] bool held_clients_quiet(std::chrono::milliseconds quiet) const {
        const auto now = Clock::now().time_since_epoch().count();
        const auto quiet_ticks = std::chrono::duration_cast<Clock::duration>(quiet).count();
        bool any = false;
        const std::lock_guard<std::mutex> lock(mu_);
        for (const auto& c : conns_) {
            if (!c->held.load() || c->closed.load()) {
                continue;
            }
            any = true;
            if (now - c->last_client_bytes.load() < quiet_ticks) {
                return false;
            }
        }
        return any;
    }

    [[nodiscard]] std::size_t accepted() const noexcept { return accepted_.load(); }
    [[nodiscard]] std::size_t refused() const noexcept { return refused_.load(); }
    [[nodiscard]] std::size_t inserts_seen() const noexcept { return inserts_.load(); }
    [[nodiscard]] std::size_t uploads_dropped() const noexcept { return dropped_uploads_.load(); }
    [[nodiscard]] std::size_t uploads_held() const noexcept { return held_uploads_.load(); }

private:
    using Clock = std::chrono::steady_clock;
    static constexpr std::string_view kInsert = "INSERT INTO";
    static constexpr int kPollMs = 20;

    struct Conn {
        int client{-1};
        int upstream{-1};
        std::thread pump;
        std::atomic<bool> kill{false};
        std::atomic<bool> held{false};
        std::atomic<bool> closed{false};
        std::atomic<Clock::rep> last_client_bytes{0};
        // The protocol position, touched by the pump only.
        bool statement_seen{false};
        bool reply_seen{false};
        std::string tail;
    };

    static void no_sigpipe(int fd) {
#ifdef SO_NOSIGPIPE
        const int one = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
        (void)fd;
#endif
    }

    static ssize_t send_some(int fd, const char* data, std::size_t len) {
#ifdef MSG_NOSIGNAL
        return ::send(fd, data, len, MSG_NOSIGNAL);
#else
        return ::send(fd, data, len, 0);
#endif
    }

    static bool send_all(int fd, const char* data, std::size_t len) {
        while (len > 0) {
            const ssize_t n = send_some(fd, data, len);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                return false;
            }
            data += n;
            len -= static_cast<std::size_t>(n);
        }
        return true;
    }

    // Close with a reset rather than a FIN, so the peer fails at once.
    static void reset_close(int fd) {
        if (fd < 0) {
            return;
        }
        linger lg{};
        lg.l_onoff = 1;
        lg.l_linger = 0;
        ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        ::close(fd);
    }

    [[nodiscard]] int connect_upstream() const {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }
        no_sigpipe(fd);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(upstream_port_);
        if (::inet_pton(AF_INET, upstream_host_.c_str(), &addr.sin_addr) != 1) {
            ::close(fd);
            return -1;
        }
        // Bounded, so a server that is down cannot stall the acceptor.
        const int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (rc != 0 && errno == EINPROGRESS) {
            pollfd p{fd, POLLOUT, 0};
            if (::poll(&p, 1, 2000) == 1) {
                int err = 0;
                socklen_t len = sizeof(err);
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                rc = err == 0 ? 0 : -1;
            } else {
                rc = -1;
            }
        }
        if (rc != 0) {
            ::close(fd);
            return -1;
        }
        ::fcntl(fd, F_SETFL, flags);
        const int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        return fd;
    }

    void accept_loop() {
        while (!stop_.load()) {
            pollfd p{listen_fd_, POLLIN, 0};
            if (::poll(&p, 1, kPollMs) != 1) {
                reap();
                continue;
            }
            const int client = ::accept(listen_fd_, nullptr, nullptr);
            if (client < 0) {
                continue;
            }
            no_sigpipe(client);
            const int one = 1;
            ::setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            if (refusing_.load()) {
                refused_.fetch_add(1);
                reset_close(client);
                continue;
            }
            const int upstream = connect_upstream();
            if (upstream < 0) {
                // The server is down: the client sees what it would see
                // without the proxy, a connection that fails at once.
                refused_.fetch_add(1);
                reset_close(client);
                continue;
            }
            accepted_.fetch_add(1);
            auto conn = std::make_shared<Conn>();
            conn->client = client;
            conn->upstream = upstream;
            conn->last_client_bytes.store(Clock::now().time_since_epoch().count());
            {
                const std::lock_guard<std::mutex> lock(mu_);
                conns_.push_back(conn);
            }
            conn->pump = std::thread([this, conn] { pump(*conn); });
        }
    }

    // Join and forget the pumps that have finished.
    void reap() {
        std::list<std::shared_ptr<Conn>> done;
        {
            const std::lock_guard<std::mutex> lock(mu_);
            for (auto it = conns_.begin(); it != conns_.end();) {
                if ((*it)->closed.load()) {
                    done.push_back(*it);
                    it = conns_.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto& c : done) {
            if (c->pump.joinable()) {
                c->pump.join();
            }
        }
    }

    // Whether these client bytes complete an "INSERT INTO" statement.
    static bool carries_insert(Conn& c, const char* data, std::size_t n) {
        std::string window = c.tail;
        window.append(data, n);
        const bool found = window.find(kInsert) != std::string::npos;
        const std::size_t keep = std::min(window.size(), kInsert.size() - 1);
        c.tail = window.substr(window.size() - keep);
        return found;
    }

    static bool take_one(std::atomic<std::size_t>& budget) {
        std::size_t n = budget.load();
        while (n > 0) {
            if (budget.compare_exchange_weak(n, n - 1)) {
                return true;
            }
        }
        return false;
    }

    void pump(Conn& c) {
        char buf[64 * 1024];
        bool reset = false;
        while (!stop_.load()) {
            if (c.kill.load()) {
                reset = true;
                break;
            }
            // A side that is not read is not looked at either: poll reports a
            // hang-up whatever it was asked, and reading a held server would
            // deliver what is being withheld.
            const bool frozen = blackhole_.load();
            const bool read_client = !frozen;
            const bool read_server = !frozen && !c.held.load();
            pollfd fds[2] = {{c.client, static_cast<short>(read_client ? POLLIN : 0), 0},
                             {c.upstream, static_cast<short>(read_server ? POLLIN : 0), 0}};
            if (::poll(fds, 2, kPollMs) <= 0) {
                continue;
            }
            if (read_client && (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
                const ssize_t n = ::recv(c.client, buf, sizeof(buf), 0);
                if (n <= 0) {
                    break;
                }
                c.last_client_bytes.store(Clock::now().time_since_epoch().count());
                const auto len = static_cast<std::size_t>(n);
                if (carries_insert(c, buf, len)) {
                    c.statement_seen = true;
                    c.reply_seen = false;
                    inserts_.fetch_add(1);
                } else if (c.statement_seen && c.reply_seen) {
                    // The first bytes after the server's reply to the
                    // statement: the INSERT's data is under way.
                    c.statement_seen = false;
                    if (take_one(drop_uploads_)) {
                        dropped_uploads_.fetch_add(1);
                        reset = true;
                        break;
                    }
                    if (take_one(hold_uploads_)) {
                        held_uploads_.fetch_add(1);
                        c.held.store(true);
                    }
                }
                if (!send_all(c.upstream, buf, len)) {
                    reset = true;
                    break;
                }
            }
            if (read_server && !c.held.load() &&
                (fds[1].revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
                const ssize_t n = ::recv(c.upstream, buf, sizeof(buf), 0);
                if (n <= 0) {
                    // The server went away. Reset the client rather than
                    // close it cleanly, as a dead server's kernel would.
                    reset = true;
                    break;
                }
                if (c.statement_seen) {
                    c.reply_seen = true;
                }
                if (!send_all(c.client, buf, static_cast<std::size_t>(n))) {
                    reset = true;
                    break;
                }
            }
        }
        if (reset) {
            reset_close(c.client);
            reset_close(c.upstream);
        } else {
            ::close(c.client);
            ::close(c.upstream);
        }
        c.closed.store(true);
    }

    template <typename F>
    void for_each_conn(F&& f) {
        const std::lock_guard<std::mutex> lock(mu_);
        for (auto& c : conns_) {
            f(*c);
        }
    }

    // Wait until every connection that `which` names has closed.
    template <typename Which>
    void await_closed(Which&& which) {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        while (Clock::now() < deadline) {
            bool open = false;
            {
                const std::lock_guard<std::mutex> lock(mu_);
                for (const auto& c : conns_) {
                    if (c->kill.load() && which(*c) && !c->closed.load()) {
                        open = true;
                    }
                }
            }
            if (!open) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(kPollMs));
        }
    }

    std::string upstream_host_;
    std::uint16_t upstream_port_;
    int listen_fd_{-1};
    std::uint16_t port_{0};
    std::thread acceptor_;
    mutable std::mutex mu_;
    std::list<std::shared_ptr<Conn>> conns_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> refusing_{false};
    std::atomic<bool> blackhole_{false};
    std::atomic<std::size_t> drop_uploads_{0};
    std::atomic<std::size_t> hold_uploads_{0};
    std::atomic<std::size_t> accepted_{0};
    std::atomic<std::size_t> refused_{0};
    std::atomic<std::size_t> inserts_{0};
    std::atomic<std::size_t> dropped_uploads_{0};
    std::atomic<std::size_t> held_uploads_{0};
};

}  // namespace clink::itest
