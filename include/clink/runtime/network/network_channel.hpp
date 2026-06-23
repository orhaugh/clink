#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "clink/core/arrow_batcher.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/metrics/network_metrics.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/network/local_data_plane.hpp"
#include "clink/runtime/network/network_socket.hpp"
#include "clink/runtime/network/wire.hpp"

namespace clink::network {

// Send half of a TCP-backed stream channel. Connects out to a listener at
// (host, port) and serializes StreamElement<T>s onto the socket using the
// supplied Codec<T>.
//
// Single-connection: one Sink ↔ one Source. Across an N×M shuffle in the
// distributed runtime, each upstream subtask owns N×M Sinks (one per
// downstream subtask). Source has the symmetric story.
//
// Push is synchronous and blocks until the OS send buffer accepts the
// frame. TCP gives natural backpressure: if the receiver is slow, send()
// will eventually block until the receiver drains its buffer.
template <typename T>
class NetworkChannelSink {
public:
    // Codec-only ctor: builds the default (binary-fallback)
    // ArrowBatcher from the codec. The codec itself is used once
    // during construction and not retained - the wire path is purely
    // Arrow IPC. Symmetric with NetworkChannelSource's codec-only ctor.
    NetworkChannelSink(std::string host, std::uint16_t port, Codec<T> codec)
        : host_(std::move(host)), port_(port), batcher_(make_default_arrow_batcher<T>(codec)) {}

    // Full ctor: use when T has a specialised ArrowBatcher (int64,
    // string, custom user schemas). The codec parameter is accepted
    // for call-site API symmetry with NetworkBridgeSink but not
    // retained - once the batcher is given, the codec is no longer
    // needed on the wire path.
    NetworkChannelSink(std::string host,
                       std::uint16_t port,
                       Codec<T> /*codec*/,
                       ArrowBatcher<T> batcher)
        : host_(std::move(host)), port_(port), batcher_(std::move(batcher)) {}

    ~NetworkChannelSink() {
        stop_reader_();
        if (fd_ >= 0) {
            NetworkSocket::close(fd_);
        }
        // Mirror socket EOF semantics for the local fast path: when
        // the sender drops without an explicit close_send, the
        // receiver should still observe EOS. Each (host, port) has at
        // most one sender per the wire model, so closing here is safe.
        if (local_channel_) {
            local_channel_->close();
            local_channel_.reset();
        }
    }

    NetworkChannelSink(const NetworkChannelSink&) = delete;
    NetworkChannelSink& operator=(const NetworkChannelSink&) = delete;
    NetworkChannelSink(NetworkChannelSink&&) = delete;
    NetworkChannelSink& operator=(NetworkChannelSink&&) = delete;

    // Connect to the configured (host, port). For same-process peers
    // (LocalDataPlane registered (host, port)) this short-circuits to
    // direct typed push - no socket, no codec, no Arrow IPC. The
    // socket path is the cross-TM fallback.
    //
    // Throws on socket failure. Blocks only when we go through the
    // socket path - the local fast path returns once the registry
    // lookup completes.
    void connect() {
        auto local = LocalDataPlane::instance().lookup_endpoint<T>(host_, port_);
        if (local) {
            local_channel_ = std::move(local);
            return;
        }
        if (fd_ >= 0) {
            // Re-entering connect() with an open fd implies the previous
            // session was torn down and we're re-establishing. Mirror
            // that in the reconnect counter for dashboards.
            clink::metrics::net::reconnect("sink");
        }
        fd_ = NetworkSocket::connect_to(host_, port_);
        if (fd_ < 0) {
            throw std::runtime_error("NetworkChannelSink::connect failed for " + host_ + ":" +
                                     std::to_string(port_));
        }
        // Reader thread for receiver -> sender credit grants. Holds the
        // sink's fd; closing the fd in stop_reader_ unblocks the
        // recv_all() and lets the thread join.
        reader_ = std::thread([this] { credit_reader_loop_(); });
    }

    // Encode and send a StreamElement frame. Returns false if the socket
    // was closed unexpectedly. For Data frames, blocks on credit if the
    // budget is below batch size - that's the credit-based backpressure.
    //
    // Local fast path: when connect() resolved the peer through
    // LocalDataPlane, push() forwards the StreamElement directly into
    // the receiver's typed BoundedChannel - skipping codec encode,
    // Arrow IPC, socket loopback, and decode on the other side. The
    // BoundedChannel's blocking push provides natural backpressure in
    // place of the credit grants the socket path uses.
    bool push(const StreamElement<T>& el) {
        if (local_channel_) {
            return local_channel_->push(el);
        }
        if (el.is_data()) {
            const auto needed = static_cast<std::uint32_t>(el.as_data().size());
            if (!acquire_credit_(needed)) {
                return false;
            }
        }
        std::vector<std::byte> payload;
        if (el.is_data()) {
            // All data frames ride Arrow IPC. The batcher produces a
            // RecordBatch whose schema is either columnar (built-in
            // types) or binary-fallback (unknown types). Either way
            // the wire bytes are a self-describing Arrow IPC stream.
            payload.push_back(static_cast<std::byte>(Kind::ArrowBatch));
            const auto& batch = el.as_data();
            auto record_batch = batcher_.build(batch);
            if (!record_batch) {
                return false;
            }
            auto ipc_bytes = arrow_batch_to_ipc(*record_batch);
            payload.insert(payload.end(), ipc_bytes.begin(), ipc_bytes.end());
        } else if (el.is_watermark()) {
            const auto& wm = el.as_watermark();
            payload.push_back(
                static_cast<std::byte>(wm.is_idle() ? Kind::WatermarkIdle : Kind::Watermark));
            put_i64_be(payload, wm.timestamp().millis());
        } else if (el.is_drain()) {
            // Phase 29b: drain marker frame.
            payload.push_back(static_cast<std::byte>(Kind::Drain));
            put_u32_be(payload, el.as_drain().subtask_idx);
            put_u32_be(payload, el.as_drain().target_parallelism);
        } else {
            payload.push_back(static_cast<std::byte>(
                el.as_barrier().is_terminal() ? Kind::Terminal : Kind::Barrier));
            put_u64_be(payload, el.as_barrier().id().value());
            // Phase 26a: append the alignment mode as one byte. Old
            // peers that don't know about this byte get a 9-byte
            // payload; the length-prefixed framing ensures they read
            // the right amount and the mode byte is silently dropped
            // when their decoder ignores the trailing byte. New peers
            // read both 8-byte (legacy) and 9-byte (mode-bearing)
            // payloads; absent mode byte defaults to Aligned.
            payload.push_back(static_cast<std::byte>(el.as_barrier().mode()));
        }
        return send_frame(payload);
    }

    // Send a Close frame and shut down the send side of the socket. The
    // peer's pop() will return nullopt once it consumes the queued frames
    // and reads the Close marker. On the local fast path, closing the
    // BoundedChannel achieves the same: the receiver's pop returns
    // nullopt once the queue drains.
    void close_send() {
        if (local_channel_) {
            local_channel_->close();
            return;
        }
        if (fd_ < 0) {
            return;
        }
        std::vector<std::byte> payload;
        payload.push_back(static_cast<std::byte>(Kind::Close));
        send_frame(payload);
        NetworkSocket::shutdown_write(fd_);
    }

    // Per-operator bytes attribution. The wrapping NetworkBridgeSink sets the
    // HOST registry + the op id of the operator this bridge's bytes belong to
    // (the chain's primary op) before connect()/the first send, so send_frame
    // can emit clink_op_bytes_sent_total{op_id} alongside the per-process
    // counter. Set on the runner thread before any send; read only in
    // send_frame (also runner thread), so no synchronisation is needed.
    void set_op_bytes_target(MetricsRegistry* reg, std::uint64_t op_id) noexcept {
        op_reg_ = reg;
        op_id_for_bytes_ = op_id;
    }

    int fd() const noexcept { return fd_; }
    // Current send credit. Exposed for metrics / tests; the value can
    // race with concurrent acquire_credit_ / credit_reader_loop_, so
    // treat it as advisory only.
    std::uint32_t credit_remaining() const noexcept {
        return remaining_credit_.load(std::memory_order_relaxed);
    }
    // Cumulative nanoseconds push() spent blocked waiting for credit.
    std::uint64_t blocked_ns_total() const noexcept {
        return blocked_ns_total_.load(std::memory_order_relaxed);
    }
    // Count of push() calls that hit the credit-cv slow path (i.e.,
    // were forced to wait at all). Distinct from blocked_ns_total in
    // that this counts events rather than aggregate time; an operator
    // dashboard can show event rate alongside total wait to tell brief
    // bursts apart from a sustained stall.
    std::uint64_t saturation_events() const noexcept {
        return saturation_events_.load(std::memory_order_relaxed);
    }
    // Count of CreditUpdate frames received from the receiver. Useful
    // for verifying the credit reverse channel is healthy and grants
    // are arriving at the cadence the receiver intends.
    std::uint64_t grants_received() const noexcept {
        return grants_received_.load(std::memory_order_relaxed);
    }

private:
    bool send_frame(const std::vector<std::byte>& payload) {
        std::lock_guard lock(send_mu_);
        std::vector<std::byte> header;
        put_u32_be(header, static_cast<std::uint32_t>(payload.size()));
        if (!NetworkSocket::send_all(fd_, header.data(), header.size())) {
            clink::metrics::net::send_error();
            return false;
        }
        if (!NetworkSocket::send_all(fd_, payload.data(), payload.size())) {
            clink::metrics::net::send_error();
            return false;
        }
        const auto frame_bytes = header.size() + payload.size();
        clink::metrics::net::bytes_sent_inc(frame_bytes);
        clink::metrics::op::bytes_sent_inc(op_reg_, op_id_for_bytes_, frame_bytes);
        return true;
    }

    // Wait until at least `n` credits are available, then deduct. Returns
    // false if the channel was torn down while waiting (reader thread
    // signalled closed_).
    bool acquire_credit_(std::uint32_t n) {
        auto before = remaining_credit_.load(std::memory_order_acquire);
        while (before >= n) {
            if (remaining_credit_.compare_exchange_weak(
                    before, before - n, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
        // Slow path: budget too small. Wait on cv for the reader thread
        // to top us up. Account blocked time and event count for the
        // backpressure metrics.
        saturation_events_.fetch_add(1, std::memory_order_relaxed);
        clink::metrics::net::credit_exhaustion();
        const auto wait_start = std::chrono::steady_clock::now();
        std::unique_lock lock(credit_mu_);
        credit_cv_.wait(lock, [&] {
            const auto cur = remaining_credit_.load(std::memory_order_acquire);
            return cur >= n || closed_.load(std::memory_order_acquire);
        });
        const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - wait_start)
                                .count();
        blocked_ns_total_.fetch_add(static_cast<std::uint64_t>(waited), std::memory_order_relaxed);
        if (closed_.load(std::memory_order_acquire)) {
            return false;
        }
        // Re-attempt the deduction; we held the lock so racing acquires
        // are blocked, but a separate thread could still have raced us
        // through compare_exchange. Loop is bounded by the same-cv
        // wait above.
        auto cur = remaining_credit_.load(std::memory_order_acquire);
        while (cur >= n) {
            if (remaining_credit_.compare_exchange_weak(
                    cur, cur - n, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    // Reader thread body. Loops on recv_all of frame header + body.
    // CreditUpdate frames bump remaining_credit_ + notify cv. Any other
    // kind (or recv_all failure) terminates the loop - peer closed.
    void credit_reader_loop_() {
        while (!closed_.load(std::memory_order_acquire)) {
            std::array<std::byte, 4> header_buf{};
            if (!NetworkSocket::recv_all(fd_, header_buf.data(), header_buf.size())) {
                break;
            }
            const std::uint32_t frame_len = read_u32_be(header_buf.data());
            if (frame_len == 0)
                break;
            std::vector<std::byte> body(frame_len);
            if (!NetworkSocket::recv_all(fd_, body.data(), body.size())) {
                break;
            }
            const auto kind = static_cast<Kind>(body[0]);
            if (kind == Kind::CreditUpdate && frame_len >= 5) {
                const std::uint32_t delta = read_u32_be(body.data() + 1);
                remaining_credit_.fetch_add(delta, std::memory_order_acq_rel);
                grants_received_.fetch_add(1, std::memory_order_relaxed);
                {
                    std::lock_guard lock(credit_mu_);
                    credit_cv_.notify_all();
                }
            }
            // Other kinds on the reverse channel are unexpected; drop.
        }
        closed_.store(true, std::memory_order_release);
        std::lock_guard lock(credit_mu_);
        credit_cv_.notify_all();
    }

    void stop_reader_() {
        closed_.store(true, std::memory_order_release);
        {
            std::lock_guard lock(credit_mu_);
            credit_cv_.notify_all();
        }
        if (reader_.joinable()) {
            // shutdown_read wakes any blocking recv_all in the reader.
            if (fd_ >= 0) {
                NetworkSocket::shutdown_read(fd_);
            }
            reader_.join();
        }
    }

    std::string host_;
    std::uint16_t port_;
    ArrowBatcher<T> batcher_;
    int fd_{-1};
    std::mutex send_mu_;
    // Credit budget. Starts at 0; receiver bootstraps with an initial
    // grant after accept. push() decrements (or blocks); the reader
    // thread tops up on CreditUpdate.
    std::atomic<std::uint32_t> remaining_credit_{0};
    std::atomic<std::uint64_t> blocked_ns_total_{0};
    std::atomic<std::uint64_t> grants_received_{0};
    std::atomic<std::uint64_t> saturation_events_{0};
    std::atomic<bool> closed_{false};
    std::mutex credit_mu_;
    std::condition_variable credit_cv_;
    std::thread reader_;
    // Local fast path: when connect() finds the peer registered in
    // LocalDataPlane, push() forwards StreamElements through this
    // typed channel instead of opening a socket. Mutually exclusive
    // with the fd_/reader_ socket path.
    std::shared_ptr<LocalEndpointChannel<T>> local_channel_;
    // Per-operator bytes attribution (set by the bridge before any send).
    MetricsRegistry* op_reg_{nullptr};
    std::uint64_t op_id_for_bytes_{0};
};

// Receive half. Listens on a port, accepts a single connection, decodes
// frames into StreamElement<T>s and pushes them into a typed
// BoundedChannel. pop() drains that channel.
//
// The same channel is exposed via LocalDataPlane so a same-process
// NetworkChannelSink can push StreamElements directly into it without
// going through the socket+codec path. This is the "local fast path"
// for colocated subtasks; the socket recv path remains the cross-TM
// fallback. Both pathways feed the same queue, so pop() is uniform.
template <typename T>
class NetworkChannelSource {
public:
    static constexpr std::size_t kLocalChannelCapacity = 256;

    // Codec-only ctor: builds the default binary-fallback ArrowBatcher
    // from the codec. The codec itself is used once during construction
    // and not retained - the wire path is purely Arrow IPC.
    NetworkChannelSource(std::uint16_t port,
                         Codec<T> codec,
                         std::string bind_host = default_data_bind_host())
        : requested_port_(port),
          bound_port_(port),
          bind_host_(std::move(bind_host)),
          batcher_(make_default_arrow_batcher<T>(codec)),
          local_channel_(std::make_shared<LocalEndpointChannel<T>>(kLocalChannelCapacity)) {}

    NetworkChannelSource(std::uint16_t port,
                         Codec<T> /*codec*/,
                         ArrowBatcher<T> batcher,
                         std::string bind_host = default_data_bind_host())
        : requested_port_(port),
          bound_port_(port),
          bind_host_(std::move(bind_host)),
          batcher_(std::move(batcher)),
          local_channel_(std::make_shared<LocalEndpointChannel<T>>(kLocalChannelCapacity)) {}

    ~NetworkChannelSource() {
        // Wake any blocked socket thread and join it before tearing
        // down the fds. The recv-thread parses frames into
        // local_channel_; closing the channel here also wakes any
        // thread blocked in pop(). To unblock the recv-thread out of
        // accept_one or recv_all we shutdown + close the underlying fds.
        local_channel_->close();
        if (recv_thread_.joinable()) {
            if (peer_fd_ >= 0) {
                NetworkSocket::shutdown_read(peer_fd_);
            }
            if (listener_fd_ >= 0) {
                int fd = listener_fd_;
                listener_fd_ = -1;
                // shutdown_read wakes a blocked accept() on Linux; close alone
                // does NOT (close only wakes accept on macOS/BSD). BOTH are
                // needed. Without the shutdown, recv_loop_'s accept_one never
                // returns on Linux when no TCP peer ever connected (the common
                // colocated case, where the sink used the LocalDataPlane
                // fast path), so this join() - and the whole subtask teardown,
                // and thus the job's SubtaskFinished / JobCompleted - hangs.
                NetworkSocket::shutdown_read(fd);
                NetworkSocket::close(fd);
            }
            recv_thread_.join();
        }
        if (peer_fd_ >= 0) {
            NetworkSocket::close(peer_fd_);
        }
        if (listener_fd_ >= 0) {
            NetworkSocket::close(listener_fd_);
        }
        LocalDataPlane::instance().unregister_endpoint(bind_host_, bound_port_);
    }

    NetworkChannelSource(const NetworkChannelSource&) = delete;
    NetworkChannelSource& operator=(const NetworkChannelSource&) = delete;
    NetworkChannelSource(NetworkChannelSource&&) = delete;
    NetworkChannelSource& operator=(NetworkChannelSource&&) = delete;

    // Bind + listen. Returns the actual port that was bound (lets callers
    // pass port=0 and discover the OS-assigned port). Also publishes
    // the source's local_channel_ to LocalDataPlane so a same-process
    // sink can short-circuit through it.
    std::uint16_t listen() {
        std::uint16_t p = requested_port_;
        listener_fd_ = NetworkSocket::listen_on(p, bind_host_);
        if (listener_fd_ < 0) {
            throw std::runtime_error("NetworkChannelSource::listen failed");
        }
        bound_port_ = p;
        LocalDataPlane::instance().register_endpoint<T>(bind_host_, bound_port_, local_channel_);
        return p;
    }

    // Per-operator bytes attribution. The wrapping NetworkBridgeSource sets the
    // HOST registry + the op id of the operator this bridge's received bytes
    // belong to (the chain's primary op) BEFORE accept() spawns the recv
    // thread, so recv_loop_ can emit clink_op_bytes_received_total{op_id}. Set
    // before the thread starts (happens-before), so no synchronisation needed.
    void set_op_bytes_target(MetricsRegistry* reg, std::uint64_t op_id) noexcept {
        op_reg_ = reg;
        op_id_for_bytes_ = op_id;
    }

    // Spawn the recv-thread that does accept + frame parse + push into
    // local_channel_. Returns immediately - the recv-thread runs
    // asynchronously so that a local-only flow (sink uses
    // LocalDataPlane bypass and never opens a socket) doesn't block
    // here forever. If a socket peer never connects, the recv-thread
    // sits in accept_one until shutdown_recv() / destructor wakes it.
    void accept() {
        recv_thread_ = std::thread([this] { recv_loop_(); });
    }

    // Pop a single StreamElement<T>. Returns nullopt when the source
    // is closed (channel closed + drained) - which happens when the
    // recv-thread sees EOF/Close OR when a local sink calls
    // close_send() on its LocalDataPlane channel handle. The first
    // nullopt sets `closed_` so closed() reports terminal state.
    //
    // Credit grant: when a socket peer is connected, popping a data
    // batch issues a CreditUpdate equal to the batch size. This paces
    // the sender to the consumer (vs. crediting at parse time, which
    // would only pace the sender to recv_all - missing the actual
    // backpressure signal). The local-only path has no socket peer
    // (peer_fd_ < 0) so this is a no-op there - the BoundedChannel's
    // blocking push provides backpressure for that path.
    std::optional<StreamElement<T>> pop() {
        auto e = local_channel_->pop();
        if (!e.has_value()) {
            closed_ = true;
            return e;
        }
        if (peer_fd_ >= 0 && e->is_data()) {
            const auto n = static_cast<std::uint32_t>(e->as_data().size());
            send_credit_(n);
        }
        return e;
    }

    // Terminal-state query: true only AFTER pop() has returned nullopt
    // (channel closed AND drained). Going by BoundedChannel::closed()
    // alone would shortcut produce()-style callers that bail on closed
    // before they finish draining queued records.
    bool closed() const noexcept { return closed_; }
    std::uint16_t bound_port() const noexcept { return bound_port_; }

    // Wake any thread blocked in pop() and the socket-side recv
    // thread. Subsequent pop() calls return nullopt.
    void shutdown_recv() {
        local_channel_->close();
        if (peer_fd_ >= 0) {
            NetworkSocket::shutdown_read(peer_fd_);
        }
        if (listener_fd_ >= 0) {
            // Wake the recv-thread's blocked accept_one. shutdown_read wakes
            // accept() on Linux; close wakes it on macOS/BSD (where shutdown on
            // a listening socket is a no-op). BOTH are needed for portability -
            // close alone leaves accept() blocked on Linux when no TCP peer
            // ever connected (the colocated LocalDataPlane fast path).
            int fd = listener_fd_;
            listener_fd_ = -1;
            NetworkSocket::shutdown_read(fd);
            NetworkSocket::close(fd);
        }
    }

private:
    // Single-threaded socket recv loop. Lives for the lifetime of the
    // source: parses one frame at a time, pushes each StreamElement
    // into local_channel_, and exits on EOF / Close / parse failure.
    // Closing local_channel_ on exit signals EOS to pop() ONLY if no
    // local sink is still producing - the caller (shutdown_recv or
    // destructor) takes care of that ordering.
    void recv_loop_() {
        // Make sure the local channel is closed (and any blocked
        // pop() unblocks) on every exit path from this function -
        // including parse errors, schema mismatches, socket EOF, and
        // the destructor-driven listener-fd close that wakes accept.
        struct CloseOnExit {
            std::shared_ptr<LocalEndpointChannel<T>> ch;
            ~CloseOnExit() { ch->close(); }
        };
        CloseOnExit guard{local_channel_};
        peer_fd_ = NetworkSocket::accept_one(listener_fd_);
        if (peer_fd_ < 0) {
            // accept woken by shutdown / destructor. CloseOnExit
            // closes the channel; any local sink pushes still in the
            // channel will drain before pop() returns nullopt.
            return;
        }
        NetworkSocket::close(listener_fd_);
        listener_fd_ = -1;
        send_credit_(kInitialNetworkCredit);

        while (true) {
            std::array<std::byte, 4> header_buf{};
            if (!NetworkSocket::recv_all(peer_fd_, header_buf.data(), header_buf.size())) {
                clink::metrics::net::recv_error();
                break;
            }
            const std::uint32_t frame_len = read_u32_be(header_buf.data());
            if (frame_len == 0) {
                break;
            }
            std::vector<std::byte> body(frame_len);
            if (!NetworkSocket::recv_all(peer_fd_, body.data(), body.size())) {
                clink::metrics::net::recv_error();
                break;
            }
            const auto frame_bytes = header_buf.size() + body.size();
            clink::metrics::net::bytes_received_inc(frame_bytes);
            clink::metrics::op::bytes_received_inc(op_reg_, op_id_for_bytes_, frame_bytes);
            const auto kind = static_cast<Kind>(body[0]);
            std::size_t pos = 1;
            switch (kind) {
                case Kind::Data:
                    // Legacy per-record path: see history comment
                    // below. Hard protocol error.
                    return;
                case Kind::ArrowBatch: {
                    auto record_batch = arrow_batch_from_ipc(body.data() + pos, body.size() - pos);
                    if (!record_batch) {
                        return;
                    }
                    // Credit is issued by pop() on consumer dequeue,
                    // not here. Crediting at parse time would only
                    // pace the sender to socket throughput, missing
                    // backpressure when the consumer is slow.
                    if (batcher_.schema) {
                        auto expected = batcher_.schema();
                        if (!record_batch->schema()->Equals(*expected, /*check_metadata=*/false)) {
                            return;
                        }
                    }
                    auto parsed = batcher_.parse(*record_batch);
                    if (!parsed.has_value()) {
                        return;
                    }
                    if (!local_channel_->push(StreamElement<T>::data(std::move(*parsed)))) {
                        return;
                    }
                    break;
                }
                case Kind::Watermark: {
                    const std::int64_t t = read_i64_be(body.data() + pos);
                    if (!local_channel_->push(
                            StreamElement<T>::watermark(Watermark{EventTime{t}}))) {
                        return;
                    }
                    break;
                }
                case Kind::WatermarkIdle: {
                    const std::int64_t t = read_i64_be(body.data() + pos);
                    if (!local_channel_->push(
                            StreamElement<T>::watermark(Watermark::idle(EventTime{t})))) {
                        return;
                    }
                    break;
                }
                case Kind::Barrier: {
                    const std::uint64_t id = read_u64_be(body.data() + pos);
                    // Phase 26a: mode is the byte AFTER the id when
                    // the frame is long enough; pre-26a frames omit
                    // it and we default to Aligned.
                    auto mode = CheckpointBarrier::Mode::Aligned;
                    if (body.size() >= pos + 8 + 1) {
                        mode = static_cast<CheckpointBarrier::Mode>(
                            static_cast<std::uint8_t>(body[pos + 8]));
                    }
                    if (!local_channel_->push(StreamElement<T>::barrier(
                            CheckpointBarrier{CheckpointId{id}, /*terminal=*/false, mode}))) {
                        return;
                    }
                    break;
                }
                case Kind::Terminal: {
                    const std::uint64_t id = read_u64_be(body.data() + pos);
                    auto mode = CheckpointBarrier::Mode::Aligned;
                    if (body.size() >= pos + 8 + 1) {
                        mode = static_cast<CheckpointBarrier::Mode>(
                            static_cast<std::uint8_t>(body[pos + 8]));
                    }
                    if (!local_channel_->push(StreamElement<T>::barrier(
                            CheckpointBarrier{CheckpointId{id}, /*terminal=*/true, mode}))) {
                        return;
                    }
                    break;
                }
                case Kind::CreditUpdate:
                    // Receivers don't expect to RECEIVE credit grants.
                    // Protocol noise; loop.
                    break;
                case Kind::Drain: {
                    // Phase 29b: drain marker. [u32 subtask_idx][u32 target_parallelism].
                    DrainMarker d;
                    d.subtask_idx = read_u32_be(body.data() + pos);
                    d.target_parallelism = read_u32_be(body.data() + pos + 4);
                    if (!local_channel_->push(StreamElement<T>::drain(d))) {
                        return;
                    }
                    break;
                }
                case Kind::Close:
                    return;
            }
        }
    }

    // Send a CreditUpdate(delta) back to the sender. Best-effort: a
    // failed write just means the sender will run dry on credit sooner
    // than expected. send_mu_ serialises against concurrent grant
    // attempts (the receiver is single-threaded today, but the lock
    // makes it future-safe).
    void send_credit_(std::uint32_t delta) {
        if (peer_fd_ < 0 || delta == 0)
            return;
        std::lock_guard lock(send_mu_);
        std::vector<std::byte> payload;
        payload.push_back(static_cast<std::byte>(Kind::CreditUpdate));
        put_u32_be(payload, delta);
        std::vector<std::byte> header;
        put_u32_be(header, static_cast<std::uint32_t>(payload.size()));
        if (!NetworkSocket::send_all(peer_fd_, header.data(), header.size()))
            return;
        NetworkSocket::send_all(peer_fd_, payload.data(), payload.size());
    }

    std::uint16_t requested_port_;
    // Atomic because they are touched from more than one thread: the recv
    // thread sets peer_fd_ in recv_loop_ (after accept) and tears down
    // listener_fd_, while the consumer thread reads peer_fd_ in pop()/
    // send_credit_ and a teardown thread reads both in shutdown_recv()/the
    // destructor. Plain ints here were a TSan data race (recv_loop_ write of
    // peer_fd_ vs pop() read). Default seq_cst is fine - these are
    // checkpoint/lifecycle scalars, not a hot path.
    std::atomic<std::uint16_t> bound_port_;
    std::string bind_host_;
    ArrowBatcher<T> batcher_;
    std::atomic<int> listener_fd_{-1};
    std::atomic<int> peer_fd_{-1};
    std::atomic<bool> closed_{false};
    std::mutex send_mu_;
    // Unified queue for both socket-recv path and same-process
    // LocalDataPlane pushes. The recv-thread (when a socket peer
    // connects) parses frames and pushes into this channel; local
    // sinks push StreamElements in directly. pop() drains from one
    // place regardless of how the records got in.
    std::shared_ptr<LocalEndpointChannel<T>> local_channel_;
    std::thread recv_thread_;
    // Per-operator bytes attribution (set by the bridge before accept()).
    MetricsRegistry* op_reg_{nullptr};
    std::uint64_t op_id_for_bytes_{0};
};

}  // namespace clink::network
