#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "clink/core/codec.hpp"
#include "clink/metrics/network_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/network/network_channel.hpp"

namespace clink::network {

// Sink that adapts a NetworkChannelSink<T> into the operator interface.
// Use as the terminal sink in a DAG running on one process; pair it with a
// NetworkBridgeSource<T> on another process listening at the matching port.
//
// Lifecycle:
//   1. Construct on this side with the peer's (host, port) - both sides
//      must agree on the port out-of-band.
//   2. The peer's NetworkBridgeSource must have already called
//      prepare_listen() (binding the port) before this sink's open() runs;
//      otherwise connect() races and may fail. The simplest pattern in
//      tests is: build the source first, capture its bound port, then
//      build the sink with that port.
//   3. open() connects, on_data/watermark/barrier serialize and send, and
//      close() sends a Close frame and shuts down the send side.
template <typename T>
class NetworkBridgeSink final : public Sink<T> {
public:
    NetworkBridgeSink(std::string host,
                      std::uint16_t port,
                      Codec<T> codec,
                      std::string name = "network_bridge_sink")
        : channel_(std::move(host), port, std::move(codec)), name_(std::move(name)) {}

    NetworkBridgeSink(std::string host,
                      std::uint16_t port,
                      Codec<T> codec,
                      ArrowBatcher<T> batcher,
                      std::string name = "network_bridge_sink")
        : channel_(std::move(host), port, std::move(codec), std::move(batcher)),
          name_(std::move(name)) {}

    void open() override {
        // Point the channel's per-operator byte counter at the operator this
        // bridge's output bytes belong to (the chain's primary op). Set before
        // connect()/any send, on the runner thread.
        if (auto* rt = this->runtime()) {
            channel_.set_op_bytes_target(rt->metrics(), rt->attributed_op_id());
        }
        clink::metrics::net::connect_attempt("sink");
        channel_.connect();
    }

    void on_data(const Batch<T>& batch) override {
        // Const-ref fallback: copies the batch to satisfy the channel
        // push contract. Move-aware callers should reach the
        // Batch<T>&& overload below; this path stays for any sink
        // wrapper that still routes via the const-ref API.
        const auto sz = batch.size();
        channel_.push(StreamElement<T>::data(Batch<T>{batch}));
        clink::metrics::net::records_sent_inc(sz);
    }

    void on_data(Batch<T>&& batch) override {
        // Move path: the dag runner owns the StreamElement that holds
        // this batch and doesn't reuse it after on_data returns, so
        // we can ship it through the channel by move - no per-record
        // refcount work, no realloc, no init_with_size cost.
        const auto sz = batch.size();
        channel_.push(StreamElement<T>::data(std::move(batch)));
        clink::metrics::net::records_sent_inc(sz);
    }

    void on_watermark(Watermark wm) override { channel_.push(StreamElement<T>::watermark(wm)); }

    void on_barrier(CheckpointBarrier b) override { channel_.push(StreamElement<T>::barrier(b)); }

    void close() override {
        clink::metrics::net::close_send();
        channel_.close_send();
    }

    std::string name() const override { return name_; }

private:
    NetworkChannelSink<T> channel_;
    std::string name_;
};

// Source that adapts a NetworkChannelSource<T> into the operator interface.
//
// Lifecycle is two-phase to keep the listening port available before the
// peer's sink tries to connect:
//   1. Construct with port=0 (or a fixed port).
//   2. Call prepare_listen() to bind and discover the OS-assigned port -
//      do this before the executor starts, so the peer can connect to a
//      known address.
//   3. Pass that port to the peer's NetworkBridgeSink.
//   4. The executor's runner calls open() (accepts) and then drives
//      produce() until the peer half-closes.
//
// Cancellation: cancel() shuts down the read side of the socket, waking
// any thread blocked in produce(). The next pop() returns nullopt and
// the runner exits.
template <typename T>
class NetworkBridgeSource final : public Source<T> {
public:
    NetworkBridgeSource(std::uint16_t port,
                        Codec<T> codec,
                        std::string name = "network_bridge_source",
                        std::string bind_host = default_data_bind_host())
        : channel_(port, std::move(codec), std::move(bind_host)), name_(std::move(name)) {}

    NetworkBridgeSource(std::uint16_t port,
                        Codec<T> codec,
                        ArrowBatcher<T> batcher,
                        std::string name = "network_bridge_source",
                        std::string bind_host = default_data_bind_host())
        : channel_(port, std::move(codec), std::move(batcher), std::move(bind_host)),
          name_(std::move(name)) {}

    // Bind and listen. Returns the bound port. Idempotent enough - calling
    // twice will throw the second time.
    std::uint16_t prepare_listen() { return channel_.listen(); }

    void open() override {
        // Point the channel's per-operator byte counter at the operator this
        // bridge's received bytes belong to (the chain's primary op). Set
        // before accept() spawns the recv thread, so the assignment
        // happens-before the thread reads it.
        if (auto* rt = this->runtime()) {
            channel_.set_op_bytes_target(rt->metrics(), rt->attributed_op_id());
        }
        channel_.accept();
    }

    bool produce(Emitter<T>& out) override {
        if (this->cancelled() || channel_.closed()) {
            return false;
        }
        auto e = channel_.pop();
        if (!e.has_value()) {
            return false;
        }
        if (e->is_data()) {
            const auto sz = e->as_data().size();
            out.emit_data(std::move(e->as_data()));
            clink::metrics::net::records_received_inc(sz);
        } else if (e->is_watermark()) {
            out.emit_watermark(e->as_watermark());
        } else if (e->is_drain()) {
            // A rescale drain marker arrives over the wire when an upstream
            // subtask on the sending worker winds down. Forward it so the marker
            // keeps flowing through this worker's chain to the eventual sink (which
            // no-ops it). The old else-branch called as_barrier() on the Drain
            // variant and threw bad_variant_access, killing the recv consumer.
            out.emit_drain(e->as_drain());
        } else {
            out.emit_barrier(e->as_barrier());
        }
        return true;
    }

    void cancel() override {
        Source<T>::cancel();
        channel_.shutdown_recv();
    }

    // A network bridge is a relay, not the actual stream source. Any
    // terminal barrier the original source emitted is already in the
    // network stream and gets forwarded via produce(); the dag runner
    // must NOT add its own terminal on top, or downstream 2PC sinks
    // will commit twice and overwrite their first commit with the
    // empty pending file from the second.
    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

    std::string name() const override { return name_; }

private:
    NetworkChannelSource<T> channel_;
    std::string name_;
};

}  // namespace clink::network
