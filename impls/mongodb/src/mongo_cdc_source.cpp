// MongoDB change-streams CDC source implementation. Watches a collection / database
// / deployment via mongocxx change streams and emits each row-level change as a flat
// JSON object string (clink::mongodb::change_event_to_json), mirroring the
// Postgres / MySQL CDC source contract. The checkpoint cursor is the change-stream
// resume token. See mongo_cdc_source.hpp for the full delivery + requirement notes.

#include "clink/mongodb/mongo_cdc_source.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <bsoncxx/json.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "clink/config/json.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/mongodb/mongo_event.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::mongodb {

namespace {

constexpr const char* kLabel = "mongodb";
constexpr const char* kTokenKey = "__mongo_cdc_token__";
constexpr int kMaxEventsPerProduce = 2048;  // bound work per produce() call
constexpr std::size_t kMaxBatch = 1024;     // emit batch size cap

class MongoCdcSource final : public Source<std::string> {
public:
    explicit MongoCdcSource(MongoCdcOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {}

    void open() override {
        if (dormant_) {
            return;
        }
        client_.emplace(make_client(opts_.uri));
        mongocxx::options::change_stream copts;
        copts.max_await_time(opts_.max_await);
        if (opts_.full_document_lookup) {
            copts.full_document("updateLookup");
        }
        if (!restored_token_.empty()) {
            // Resume AFTER the checkpointed token. Throws on the first read if the
            // oplog has rolled past it (no silent gap); a fresh start is then needed.
            copts.resume_after(bsoncxx::from_json(restored_token_));
        }
        if (!opts_.collection.empty()) {
            if (opts_.database.empty()) {
                throw std::runtime_error(opts_.name + ": 'collection' requires 'database'");
            }
            stream_ = std::make_unique<mongocxx::change_stream>(
                (*client_)[opts_.database][opts_.collection].watch(copts));
        } else if (!opts_.database.empty()) {
            stream_ =
                std::make_unique<mongocxx::change_stream>((*client_)[opts_.database].watch(copts));
        } else {
            stream_ = std::make_unique<mongocxx::change_stream>(client_->watch(copts));
        }
    }

    bool produce(Emitter<std::string>& out) override {
        if (dormant_) {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
            return !this->cancelled();
        }
        if (this->cancelled() || !stream_) {
            return false;
        }

        Batch<std::string> batch;
        std::uint64_t bytes = 0;
        int n = 0;
        try {
            // One pass over the currently-available batch. begin() blocks up to
            // max_await_time, so an idle stream returns promptly (cancel latency);
            // re-entering produce() resumes the same tailable cursor.
            //
            // CRITICAL: mongocxx's begin() does NOT advance the cursor - it re-yields
            // the current position. So when we stop at the batch cap we must advance
            // the iterator PAST the consumed event before breaking; otherwise the
            // next produce()'s begin() re-yields it and the boundary event is emitted
            // twice (steady-state, not just on replay). An explicit iterator (rather
            // than a range-for we break out of) makes that advance possible.
            const auto end = stream_->end();
            for (auto it = stream_->begin(); it != end; ++it) {
                if (this->cancelled()) {
                    break;  // shutdown/restart resumes from the checkpoint token, not this cursor
                }
                std::string raw = bsoncxx::to_json(*it, bsoncxx::ExtendedJsonMode::k_relaxed);
                clink::config::JsonValue parsed = clink::config::parse(raw);
                if (auto js = change_event_to_json(parsed)) {
                    bytes += js->size();
                    batch.emplace(std::move(*js));
                }
                if (++n >= kMaxEventsPerProduce || batch.size() >= kMaxBatch) {
                    ++it;  // advance past the consumed event so it is not re-emitted
                    break;
                }
            }
        } catch (const std::exception& e) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(opts_.name + ": change stream error: " + e.what());
        }

        // The post-batch resume token advances even across an empty getMore, so the
        // checkpoint cursor keeps moving on an idle stream.
        if (auto tok = stream_->get_resume_token()) {
            resume_token_ = bsoncxx::to_json(*tok, bsoncxx::ExtendedJsonMode::k_relaxed);
        }
        if (!batch.empty()) {
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, bytes);
            out.emit_data(std::move(batch));
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override {
        stream_.reset();
        client_.reset();
    }

    [[nodiscard]] bool is_bounded() const noexcept override { return false; }

    std::string name() const override { return opts_.name; }

    void snapshot_offset(StateBackend& backend, OperatorId op_id, CheckpointId /*ckpt*/) override {
        if (dormant_ || resume_token_.empty()) {
            return;
        }
        backend.put_operator_state(
            op_id,
            std::string(kTokenKey),
            StateBackend::ValueView{resume_token_.data(), resume_token_.size()});
    }

    bool restore_offset(StateBackend& backend, OperatorId op_id) override {
        bool found = false;
        backend.scan_operator_state(op_id,
                                    [&](StateBackend::KeyView key, StateBackend::ValueView value) {
                                        if (std::string_view(key) != std::string_view(kTokenKey)) {
                                            return;
                                        }
                                        restored_token_.assign(value.data(), value.size());
                                        found = true;
                                    });
        return found;
    }

private:
    MongoCdcOptions opts_;
    bool dormant_{false};
    std::optional<mongocxx::client> client_;
    std::unique_ptr<mongocxx::change_stream> stream_;
    std::string resume_token_;    // latest token (the checkpoint cursor)
    std::string restored_token_;  // token loaded by restore_offset (resume point)
};

}  // namespace

std::shared_ptr<Source<std::string>> make_mongo_cdc_source(const MongoCdcOptions& opts) {
    return std::make_shared<MongoCdcSource>(opts);
}

}  // namespace clink::mongodb
