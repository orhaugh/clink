#pragma once

// WebSocket source: connects to a ws:// or wss:// endpoint, optionally sends
// a subscription message, and emits each received text message as one
// std::string record. Feeds that speak JSON text (the norm for market-data
// and event APIs) land on the string channel and bridge to Row - with the
// columnar JSON decode - exactly as a Kafka JSON table does.
//
// DELIVERY, stated plainly: a WebSocket feed is an ephemeral push stream -
// no offsets, no acknowledgement, nothing to rewind to. Delivery is
// therefore AT-MOST-ONCE ACROSS RESTARTS: messages that arrive while the
// job is down (or between a crash and the last checkpoint) are gone, and
// snapshot_offset/restore_offset stay the no-op base defaults because there
// is no cursor to persist. Within one connection, delivery is in-order and
// complete. The two honest patterns for stronger guarantees: bridge the
// feed to a durable log (this source feeding a Kafka sink, processing from
// the topic), or pair the job with the flight recorder (--capture-dir),
// which makes an unreplayable feed locally replayable after capture.
//
// RECONNECT: a dropped connection is re-dialled with exponential backoff
// (capped by reconnect_backoff_max) and the subscription message is sent
// again. Gaps across the drop are inherent to the transport. A server-
// initiated close counts as a drop when `reconnect` is on (the default);
// with reconnect off it ends the stream instead (max watermark, source
// exhausted), which is also how a bounded `max_messages` run ends.
//
// BACKPRESSURE: the client reads only inside produce(), so a stalled
// downstream stops the reads and TCP flow control pushes back on the
// server. Venues commonly disconnect slow consumers; the reconnect path
// absorbs that, at the price of the gap.
//
// PARALLELISM: one connection is one stream, so only subtask 0 connects;
// the other subtasks are dormant (the MQTT source's pattern). Run at
// parallelism 1; fan out AFTER the source with a keyed shuffle.

#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/websocket/ws_client.hpp"

namespace clink::websocket {

struct WebSocketSourceOptions {
    std::string url;                       // required: ws://host[:port]/path or wss://...
    std::string subscribe;                 // optional text frame sent after every (re)connect
    std::chrono::milliseconds block{500};  // produce() read window
    std::chrono::milliseconds ping_interval{30'000};  // 0 disables client pings
    std::chrono::milliseconds open_timeout{10'000};
    std::uint64_t max_messages{0};  // 0 = unbounded; >0 = stop after N (tests, demos)
    bool reconnect{true};
    std::chrono::milliseconds reconnect_backoff_max{30'000};
    bool tls_verify{true};
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
    std::string name{"websocket_source"};
};

class WebSocketSource : public Source<std::string> {
public:
    explicit WebSocketSource(WebSocketSourceOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {
        if (opts_.url.empty()) {
            throw std::runtime_error(opts_.name + ": 'url' is required");
        }
        if (!parse_ws_url(opts_.url).has_value()) {
            throw std::runtime_error(opts_.name + ": malformed WebSocket URL '" + opts_.url +
                                     "' (expected ws://host[:port]/path or wss://...)");
        }
        if (opts_.block.count() <= 0) {
            opts_.block = std::chrono::milliseconds{500};
        }
    }

    void open() override {
        if (dormant_) {
            return;
        }
        WsClientOptions co;
        co.url = opts_.url;
        co.open_timeout = opts_.open_timeout;
        co.tls_verify = opts_.tls_verify;
        co.name = opts_.name;
        client_ = std::make_unique<WsClient>(std::move(co));
        // The first connect failing is a deploy-time problem the operator
        // surfaces loudly; produce() owns every later reconnect.
        connect_();
    }

    bool produce(Emitter<std::string>& out) override {
        if (dormant_) {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
            return !this->cancelled();
        }
        if (this->cancelled() || client_ == nullptr) {
            return false;
        }

        if (!client_->connected()) {
            if (!opts_.reconnect) {
                return finish_(out);
            }
            if (!backoff_then_reconnect_()) {
                return !this->cancelled();  // still down; try again next produce()
            }
        }

        maybe_ping_();

        WsClient::PollResult polled;
        try {
            polled = client_->poll(opts_.block);
        } catch (const std::exception&) {
            // Socket or protocol failure: the connection is dead. With
            // reconnect on this is a drop like any other; without it,
            // surface the error so the job fails visibly.
            clink::metrics::connector::error_inc("websocket", "source");
            if (!opts_.reconnect) {
                throw;
            }
            return !this->cancelled();
        }

        if (!polled.texts.empty()) {
            Batch<std::string> batch;
            std::uint64_t bytes = 0;
            for (auto& text : polled.texts) {
                bytes += text.size();
                batch.emplace(std::move(text));
                ++emitted_;
                if (opts_.max_messages > 0 && emitted_ >= opts_.max_messages) {
                    break;
                }
            }
            clink::metrics::connector::records_in_inc("websocket", batch.size());
            clink::metrics::connector::bytes_in_inc("websocket", bytes);
            out.emit_data(std::move(batch));
        }
        if (polled.binaries_skipped > 0) {
            // Binary frames carry no JSON text row; count them rather than
            // guess at a decoding.
            clink::metrics::connector::error_inc("websocket", "binary_skipped");
        }

        if (opts_.max_messages > 0 && emitted_ >= opts_.max_messages) {
            return finish_(out);
        }
        if (polled.closed && !opts_.reconnect) {
            return finish_(out);
        }
        return !this->cancelled();
    }

    void close() override {
        if (client_ != nullptr) {
            client_->close();
            client_.reset();
        }
    }

    // A WebSocket feed with a message cap is a bounded demo/test stream;
    // without one it never ends.
    [[nodiscard]] bool is_bounded() const noexcept override { return opts_.max_messages > 0; }

    std::string name() const override { return opts_.name; }

    [[nodiscard]] bool dormant() const noexcept { return dormant_; }

private:
    void connect_() {
        client_->open();
        if (!opts_.subscribe.empty()) {
            client_->send_text(opts_.subscribe);
        }
        backoff_ = std::chrono::milliseconds{0};
        last_ping_ = std::chrono::steady_clock::now();
    }

    // Sleep out the current backoff in cancel-aware slices, then try one
    // reconnect. Returns true when connected again.
    bool backoff_then_reconnect_() {
        auto left = backoff_;
        while (left.count() > 0 && !this->cancelled()) {
            const auto slice = std::min(left, std::chrono::milliseconds{200});
            std::this_thread::sleep_for(slice);
            left -= slice;
        }
        if (this->cancelled()) {
            return false;
        }
        backoff_ = backoff_.count() == 0 ? std::chrono::milliseconds{500}
                                         : std::min(backoff_ * 2, opts_.reconnect_backoff_max);
        try {
            connect_();
            return true;
        } catch (const std::exception&) {
            clink::metrics::connector::error_inc("websocket", "source");
            return false;
        }
    }

    void maybe_ping_() {
        if (opts_.ping_interval.count() <= 0 || !client_->connected()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now - last_ping_ < opts_.ping_interval) {
            return;
        }
        last_ping_ = now;
        try {
            client_->send_ping();
        } catch (const std::exception&) {
            // The next poll()/reconnect owns a dead socket; a failed ping
            // is only its earliest symptom.
        }
    }

    // End of stream: max watermark so downstream event time closes, then
    // report the source exhausted.
    bool finish_(Emitter<std::string>& out) {
        out.emit_watermark(Watermark::max());
        return false;
    }

    WebSocketSourceOptions opts_;
    bool dormant_{false};
    std::unique_ptr<WsClient> client_;
    std::uint64_t emitted_{0};
    std::chrono::milliseconds backoff_{0};
    std::chrono::steady_clock::time_point last_ping_{};
};

}  // namespace clink::websocket
