// MongoDB change-streams source implementation. Watches a collection / database /
// deployment via mongocxx change streams and emits each row-level change as a flat
// JSON object string (clink::mongodb::change_event_to_json). The checkpoint
// cursor is the change-stream resume token; change_pass.hpp holds the rule for
// which token a checkpoint may record. See mongo_cdc_source.hpp for the full
// delivery and requirement notes.

#include "clink/mongodb/mongo_cdc_source.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <bsoncxx/json.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/change_stream.hpp>

#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/mongodb/mongo_client.hpp"
#include "clink/mongodb/mongo_event.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/durable_file_write.hpp"
#include "clink/state/state_backend.hpp"

#include "change_pass.hpp"
#include "fault_points.hpp"
#include "sql_row.hpp"

namespace clink::mongodb {

namespace {

constexpr const char* kLabel = "mongodb";
constexpr const char* kTokenKey = "__mongo_cdc_token__";
// Events per produce() pass: bounds the work, and so the barrier and cancel
// latency, of one call.
constexpr std::size_t kMaxEventsPerPass = 1024;

const char* full_document_param(FullDocument mode) {
    switch (mode) {
        case FullDocument::UpdateLookup:
            return "updateLookup";
        case FullDocument::WhenAvailable:
            return "whenAvailable";
        case FullDocument::Required:
            return "required";
        case FullDocument::Off:
            break;
    }
    return nullptr;
}

// The change stream as change_pass.hpp reads it: one event per next(), never
// reading ahead of what it returned. mongocxx's begin() fetches the first event
// and ++ the next one, so the iterator is kept across passes and advanced only
// when the next event is asked for. Held by pointer: the iterator points at the
// stream.
class StreamCursor {
public:
    // `mode` is the Extended JSON each event is read as: canonical for the typed
    // SQL row, which tells every BSON type apart, relaxed for the JSON envelope.
    StreamCursor(mongocxx::change_stream stream, bsoncxx::ExtendedJsonMode mode)
        : stream_(std::move(stream)), mode_(mode) {}
    StreamCursor(const StreamCursor&) = delete;
    StreamCursor& operator=(const StreamCursor&) = delete;

    std::optional<std::string> next() {
        if (!it_) {
            it_.emplace(stream_.begin());
        } else {
            ++*it_;
        }
        if (*it_ == stream_.end()) {
            return std::nullopt;
        }
        return bsoncxx::to_json(**it_, mode_);
    }

    std::optional<std::string> resume_token() {
        if (auto token = stream_.get_resume_token()) {
            return bsoncxx::to_json(*token, bsoncxx::ExtendedJsonMode::k_relaxed);
        }
        return std::nullopt;
    }

private:
    mongocxx::change_stream stream_;
    bsoncxx::ExtendedJsonMode mode_;
    std::optional<mongocxx::change_stream::iterator> it_;
};

class MongoCdcSource final : public Source<std::string> {
public:
    explicit MongoCdcSource(MongoCdcOptions opts)
        : opts_(std::move(opts)), dormant_(opts_.subtask_idx != 0) {
        if (opts_.declared_types.empty()) {
            mapper_ = [mode = opts_.full_document](const clink::config::JsonValue& event) {
                return change_event_to_json(event, mode);
            };
        } else {
            std::vector<sql::Column> columns;
            try {
                columns = sql::parse_declared_types(opts_.declared_types);
            } catch (const sql::ConversionError& e) {
                throw std::runtime_error(opts_.name + ": " + e.what());
            }
            mapper_ = [mode = opts_.full_document,
                       columns = std::move(columns)](const clink::config::JsonValue& event) {
                return sql::change_event_to_sql_row(event, mode, columns);
            };
            json_mode_ = bsoncxx::ExtendedJsonMode::k_canonical;
        }
        if (!opts_.collection.empty() && opts_.database.empty()) {
            throw std::runtime_error(opts_.name + ": 'collection' requires 'database'");
        }
        if (opts_.max_await.count() <= 0) {
            throw std::runtime_error(opts_.name + ": 'max_await_ms' must be positive");
        }
    }

    void open() override {
        if (dormant_) {
            return;
        }
        const auto marker = start_marker_path_();
        bool from_run_start = false;
        if (restored_token_.empty()) {
            // No checkpoint holds a token. A restart before the run's first checkpoint
            // completed resumes where the run's stream started, so the changes since
            // are read again rather than skipped.
            if (marker) {
                if (auto token = read_start_marker_(*marker)) {
                    restored_token_ = std::move(*token);
                    from_run_start = true;
                }
            }
            // A restored run without a token would start at the current time and
            // skip what happened since its checkpoint.
            if (const auto* rt = this->runtime();
                !from_run_start && rt != nullptr && rt->restore_from_checkpoint_id() != 0) {
                throw std::runtime_error(
                    opts_.name + ": restored from checkpoint " +
                    std::to_string(rt->restore_from_checkpoint_id()) +
                    ", whose state holds no resume token for this source; it does not start at "
                    "the current time, which would skip the changes since that checkpoint. "
                    "Start the job without restoring the source's state to begin from now.");
            }
        }
        connection_.emplace(opts_.uri);
        auto& client = connection_->client();
        mongocxx::options::change_stream copts;
        copts.max_await_time(opts_.max_await);
        if (const char* fd = full_document_param(opts_.full_document)) {
            copts.full_document(fd);
        }
        if (!restored_token_.empty()) {
            // Resume after the checkpointed token. A token the server cannot resume
            // from fails here or on the first read, never silently.
            copts.resume_after(bsoncxx::from_json(restored_token_));
        }
        try {
            if (!opts_.collection.empty()) {
                cursor_ = std::make_unique<StreamCursor>(
                    client[opts_.database][opts_.collection].watch(copts), json_mode_);
            } else if (!opts_.database.empty()) {
                cursor_ =
                    std::make_unique<StreamCursor>(client[opts_.database].watch(copts), json_mode_);
            } else {
                cursor_ = std::make_unique<StreamCursor>(client.watch(copts), json_mode_);
            }
            // Where the stream starts: every checkpoint from here on holds a token,
            // so a restore never has to start at the current time.
            if (auto token = cursor_->resume_token()) {
                resume_token_ = std::move(*token);
            } else if (!restored_token_.empty()) {
                resume_token_ = restored_token_;
            }
        } catch (const mongocxx::exception& e) {
            fail_stream_(e);
        }
        // The run's start, durable before anything is read: a restart before the
        // first checkpoint completes resumes from here.
        if (marker && restored_token_.empty()) {
            write_start_marker_(*marker);
        }
        clink::log::info(
            "mongodb.source",
            opts_.name + ": change stream open on " + scope_() +
                (restored_token_.empty() ? ", from the current time"
                 : from_run_start ? ", resuming where this run's stream started (no checkpoint "
                                    "has completed yet)"
                                  : ", resuming after the checkpoint's resume token"));
    }

    bool produce(Emitter<std::string>& out) override {
        if (dormant_) {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{200});
            return !this->cancelled();
        }
        if (this->cancelled() || !cursor_) {
            return false;
        }

        detail::Pass pass;
        try {
            // A pass reads for at most max_await, plus the one wait for an event
            // that it may start before then.
            const auto deadline = std::chrono::steady_clock::now() + opts_.max_await;
            pass = detail::read_pass(*cursor_, mapper_, kMaxEventsPerPass, [this, deadline] {
                return this->cancelled() || std::chrono::steady_clock::now() >= deadline;
            });
        } catch (const mongocxx::exception& e) {
            fail_stream_(e);
        } catch (const std::exception& e) {
            clink::metrics::connector::error_inc(kLabel, "source");
            throw std::runtime_error(opts_.name + ": " + e.what());
        }

        if (!pass.rows.empty()) {
            Batch<std::string> batch;
            for (auto& row : pass.rows) {
                batch.emplace(std::move(row));
            }
            clink::metrics::connector::records_in_inc(kLabel, batch.size());
            clink::metrics::connector::bytes_in_inc(kLabel, pass.bytes);
            out.emit_data(std::move(batch));
            (void)CLINK_FAULT_POINT(points::kSourceAfterEmit);
        }
        // Taken after the pass, so it covers exactly what the pass handed over.
        if (pass.token) {
            resume_token_ = std::move(*pass.token);
        }
        return !this->cancelled();
    }

    void cancel() override { Source<std::string>::cancel(); }

    void close() override {
        cursor_.reset();
        connection_.reset();
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
    // Where this run records the token its stream started from: the job's durable
    // directory, keyed by the operator and the run's checkpoint-id floor, so a
    // restart of the run finds it and a new submission, whose floor is new, does
    // not. Absent outside a checkpointed cluster job.
    std::optional<std::filesystem::path> start_marker_path_() const {
        const auto* rt = this->runtime();
        if (rt == nullptr || rt->commit_receipt_dir().empty() ||
            !rt->run_checkpoint_floor().has_value()) {
            return std::nullopt;
        }
        return std::filesystem::path{rt->commit_receipt_dir()} /
               ("mongo-cdc-start-op" + std::to_string(rt->operator_id().value()) + "-run" +
                std::to_string(*rt->run_checkpoint_floor()));
    }

    std::optional<std::string> read_start_marker_(const std::filesystem::path& path) const {
        std::ifstream in(path);
        if (!in) {
            return std::nullopt;
        }
        std::stringstream all;
        all << in.rdbuf();
        auto token = all.str();
        if (token.empty()) {
            return std::nullopt;
        }
        return token;
    }

    // Without the record, a restart before the first checkpoint would start at the
    // current time and skip the changes since; the source fails instead.
    void write_start_marker_(const std::filesystem::path& path) const {
        if (resume_token_.empty()) {
            throw std::runtime_error(opts_.name +
                                     ": the change stream gave no resume token to record where "
                                     "this run starts");
        }
        try {
            std::filesystem::create_directories(path.parent_path());
            clink::state::detail::write_string_fsync_rename(path, resume_token_);
        } catch (const std::exception& e) {
            throw std::runtime_error(opts_.name +
                                     ": cannot record where this run's change "
                                     "stream starts (" +
                                     std::string(e.what()) +
                                     "), without which a restart before the first checkpoint "
                                     "would skip the changes since");
        }
    }

    std::string scope_() const {
        if (!opts_.collection.empty()) {
            return opts_.database + "." + opts_.collection;
        }
        return opts_.database.empty() ? std::string{"the whole deployment"}
                                      : "database " + opts_.database;
    }

    // A driver error from the stream, named for what it means to recovery.
    [[noreturn]] void fail_stream_(const mongocxx::exception& e) {
        clink::metrics::connector::error_inc(kLabel, "source");
        if (const auto why = resume_failure(e.code().value()); why && !restored_token_.empty()) {
            throw std::runtime_error(opts_.name + ": cannot resume the change stream: " + *why +
                                     ". The source does not fall back to the current time (" +
                                     e.what() + ")");
        }
        throw std::runtime_error(opts_.name + ": change stream error: " + e.what());
    }

    MongoCdcOptions opts_;
    bool dormant_{false};
    detail::EventMapper mapper_;
    bsoncxx::ExtendedJsonMode json_mode_{bsoncxx::ExtendedJsonMode::k_relaxed};
    std::optional<Connection> connection_;
    std::unique_ptr<StreamCursor> cursor_;
    std::string resume_token_;    // what the next checkpoint records
    std::string restored_token_;  // loaded by restore_offset: where open() resumes
};

}  // namespace

std::shared_ptr<Source<std::string>> make_mongo_cdc_source(const MongoCdcOptions& opts) {
    return std::make_shared<MongoCdcSource>(opts);
}

}  // namespace clink::mongodb
