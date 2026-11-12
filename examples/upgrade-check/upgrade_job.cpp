// The upgrade-check example's job: the one job the upgrade validation
// workflow supports (docs/internals/upgrade-validation.md).
//
// A controlled source reads numbered segment files from a local directory, in
// name order, each line one of:
//
//   E,<event time ms>,<account>,<amount>   an event
//   W,<event time ms>                      a watermark, emitted as it is
//   END                                    the end of the input
//
// A segment is read once it exists, so a run that has only the first segments
// reads them and then waits. The source's position (segment, line, and the
// last watermark it emitted) is its operator state, written under the key
// "uc.position" as 26 bytes: version 1, segment and line (u64 little-endian),
// a flag and the watermark (i64 little-endian). A segment read to its end
// moves the position to (next segment, line 0) at once, and END leaves it on
// END's line. A restored source emits its last watermark again before
// anything else, so the operators after it see the same event time a run that
// never stopped would have.
//
// Keyed by account, at parallelism 2:
//
//   uc-windows  the engine's keyed event-time tumbling window (10 s), whose
//               accumulator is the text "<count>,<sum>" in keyed state
//               (slot "windows", state version 1). Each window emits
//               "window,<account>,<start>,<end>,<count>,<sum>" when the
//               watermark reaches its end.
//   uc-idle     keyed state "last_seen" (the greatest event time seen) and
//               "idle_count" (int64, little-endian), and an event-time timer
//               at last_seen + 30 s, keyed by the account in the keyed
//               state's own encoding. When the timer fires and last_seen has
//               not moved, it emits "idle,<account>,<last_seen>,<timer>,<n>".
//
// Two sinks write each line to a file in a local directory. Nothing here
// talks to anything but the local files it is given.
//
// CLINK_UC_VARIANT selects a controlled defect, compiled only into the
// example's variant modules (examples/CMakeLists.txt), never into the job
// itself:
//
//   0  none
//   1  compatible: the same semantics written differently
//   2  wrong-aggregate: a negative amount is left out of the window's sum
//   3  window-boundary: an event exactly on a window boundary is given the
//      time 1 ms earlier, so it falls in the window before
//   4  lost-timers: uc-idle does not restore its timers
//   5  schema: uc-windows declares state version 2 with no migration
//   6  uid-changed: uc-windows is "uc-windows-v2"
//   7  uid-missing: uc-windows has no uid
//   8  crash-restore: uc-idle's process is killed while it restores its timers
//   9  crash-continue: uc-idle's process is killed after 40 events once restored
//      (SIGKILL rather than abort(): a crash all the same to the gate, and it
//      raises no crash report on the host)
//  10  memory-windows: the accumulator type is not registered on the job's
//      registry, so the fluent window keeps its windows in memory only and a
//      restore starts them empty (a baseline and candidate built alike agree;
//      only the reference tells them apart)
//  11  string-timer-keys: uc-idle keys its timers by the account as text, as
//      the KeyedProcessFunction example does, so a restore keeps each timer
//      by the key group of the text rather than of the account, and loses
//      those that land in another subtask's range (again only the reference
//      tells a baseline and candidate built alike apart)
//
// Environment, read by the process that submits the job:
//   CLINK_UC_FEED  the directory of segments (required)
//   CLINK_UC_OUT   the directory the sinks write to (required)

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/state_backend.hpp"

#ifndef CLINK_UC_VARIANT
#define CLINK_UC_VARIANT 0
#endif

namespace upgrade_check {

constexpr std::int64_t kWindowMs = 10000;
constexpr std::int64_t kIdleMs = 30000;
constexpr std::uint32_t kKeyedParallelism = 2;
constexpr int kVariant = CLINK_UC_VARIANT;

// "<account>,<amount>": an event's value, its time carried by the record.
struct Event {
    std::int64_t account{0};
    std::int64_t amount{0};
};

std::optional<Event> parse_event(const std::string& s) {
    const auto c = s.find(',');
    if (c == std::string::npos) {
        return std::nullopt;
    }
    try {
        return Event{std::stoll(s.substr(0, c)), std::stoll(s.substr(c + 1))};
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// ---- the controlled source ------------------------------------------------------

class SegmentSource final : public clink::Source<std::string> {
public:
    explicit SegmentSource(std::string feed) : feed_(std::move(feed)) {}

    bool produce(clink::Emitter<std::string>& out) override {
        if (reemit_ && last_wm_.has_value()) {
            reemit_ = false;
            out.emit_watermark(clink::Watermark{clink::EventTime{*last_wm_}});
            return true;
        }
        reemit_ = false;
        if (!load_segment_()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            return !cancelled();
        }
        if (line_ >= lines_.size()) {
            next_segment_();  // an empty segment, or a restore at a segment's end
            return true;
        }
        const std::string& first = lines_[line_];
        if (first == "END") {
            return false;
        }
        if (first.starts_with("W,")) {
            const auto ts = std::stoll(first.substr(2));
            ++line_;
            last_wm_ = ts;
            out.emit_watermark(clink::Watermark{clink::EventTime{ts}});
            advance_if_read_();
            return true;
        }
        clink::Batch<std::string> batch;
        while (line_ < lines_.size() && batch.size() < 64 && lines_[line_].starts_with("E,")) {
            // E,<ts>,<account>,<amount>
            const std::string& l = lines_[line_];
            const auto c1 = l.find(',', 2);
            std::int64_t ts = std::stoll(l.substr(2, c1 - 2));
            if (kVariant == 3 && ts % kWindowMs == 0) {
                ts -= 1;
            }
            batch.emplace(l.substr(c1 + 1), clink::EventTime{ts});
            ++line_;
        }
        if (batch.size() == 0) {
            throw std::runtime_error("upgrade-check source: unreadable line '" + first + "' in " +
                                     segment_path_().string());
        }
        out.emit_data(std::move(batch));
        advance_if_read_();
        return true;
    }

    // segment (u64), line (u64), has watermark (u8), watermark (i64), version first.
    void snapshot_offset(clink::StateBackend& backend,
                         clink::OperatorId op_id,
                         clink::CheckpointId /*ckpt*/) override {
        std::string v(26, '\0');
        v[0] = static_cast<char>(1);
        put_u64_(v, 1, segment_);
        put_u64_(v, 9, line_);
        v[17] = static_cast<char>(last_wm_.has_value() ? 1 : 0);
        put_u64_(v, 18, static_cast<std::uint64_t>(last_wm_.value_or(0)));
        backend.put_operator_state(op_id,
                                   clink::StateBackend::KeyView{kKey_.data(), kKey_.size()},
                                   clink::StateBackend::ValueView{v.data(), v.size()});
    }

    bool restore_offset(clink::StateBackend& backend, clink::OperatorId op_id) override {
        auto v = backend.get_operator_state(
            op_id, clink::StateBackend::KeyView{kKey_.data(), kKey_.size()});
        if (!v.has_value()) {
            return false;
        }
        if (v->size() != 26 || std::to_integer<int>((*v)[0]) != 1) {
            throw std::runtime_error("upgrade-check source: a position this job does not read (" +
                                     std::to_string(v->size()) + " bytes)");
        }
        const auto u64 = [&v](std::size_t at) {
            std::uint64_t x = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                x |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>((*v)[at + i]))
                     << (i * 8);
            }
            return x;
        };
        segment_ = u64(1);
        line_ = u64(9);
        if (std::to_integer<int>((*v)[17]) != 0) {
            last_wm_ = static_cast<std::int64_t>(u64(18));
        }
        lines_.clear();
        loaded_ = false;
        reemit_ = true;
        return true;
    }

    std::string name() const override { return "upgrade_check.source"; }

private:
    static constexpr std::string_view kKey_ = "uc.position";

    // A segment read to its end moves the position to the next one in the same
    // call, before a barrier can pass, so a checkpoint taken once a segment is
    // read records (next segment, line 0) whatever the timing.
    void advance_if_read_() {
        if (line_ >= lines_.size()) {
            next_segment_();
        }
    }
    void next_segment_() {
        ++segment_;
        line_ = 0;
        lines_.clear();
        loaded_ = false;
    }

    static void put_u64_(std::string& v, std::size_t at, std::uint64_t x) {
        for (std::size_t i = 0; i < 8; ++i) {
            v[at + i] = static_cast<char>((x >> (i * 8)) & 0xFF);
        }
    }

    std::filesystem::path segment_path_() const {
        std::string n = std::to_string(segment_);
        n.insert(0, n.size() < 6 ? 6 - n.size() : 0, '0');
        return std::filesystem::path{feed_} / ("segment-" + n + ".txt");
    }

    // Reads the current segment whole, once it exists. A segment is written
    // under another name and renamed, so one that exists is complete.
    bool load_segment_() {
        if (loaded_) {
            return true;
        }
        std::ifstream in(segment_path_());
        if (!in) {
            return false;
        }
        lines_.clear();
        for (std::string l; std::getline(in, l);) {
            if (!l.empty()) {
                lines_.push_back(std::move(l));
            }
        }
        if (line_ > lines_.size()) {
            throw std::runtime_error("upgrade-check source: the checkpoint's position (line " +
                                     std::to_string(line_) + ") is past the end of " +
                                     segment_path_().string());
        }
        loaded_ = true;
        return true;
    }

    std::string feed_;
    std::uint64_t segment_{1};
    std::uint64_t line_{0};
    std::optional<std::int64_t> last_wm_;
    std::vector<std::string> lines_;
    bool loaded_{false};
    bool reemit_{false};
};

// ---- keyed state and timers -------------------------------------------------------

// A timer's key is the account in the keyed state's own encoding (the int64
// codec's 8 bytes), so a restore that keeps each subtask's timers by key group
// keeps them where the account's state is.
std::string timer_key(std::int64_t account) {
    if constexpr (kVariant == 11) {
        return std::to_string(account);
    }
    const auto bytes = clink::int64_codec().encode(account);
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

std::int64_t account_of(const std::string& key) {
    if constexpr (kVariant == 11) {
        return std::stoll(key);
    }
    return clink::int64_codec()
        .decode(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(key.data()), key.size()})
        .value_or(0);
}

class IdleDetector final : public clink::Operator<std::string, std::string> {
public:
    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error("upgrade_check.idle needs a state backend");
        }
        last_seen_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "last_seen", clink::int64_codec(), clink::int64_codec()));
        idle_count_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "idle_count", clink::int64_codec(), clink::int64_codec()));
    }

    void process(const clink::StreamElement<std::string>& el,
                 clink::Emitter<std::string>& out) override {
        if (el.is_watermark()) {
            this->on_watermark(el.as_watermark(), out);
            return;
        }
        if (!el.is_data()) {
            this->on_barrier(el.as_barrier(), out);
            return;
        }
        for (const auto& rec : el.as_data()) {
            const auto e = parse_event(rec.value());
            if (!e || !rec.event_time().has_value()) {
                continue;
            }
            if (kVariant == 9 && restored_ && ++since_restore_ >= 40) {
                std::raise(SIGKILL);
            }
            const std::int64_t ts = rec.event_time()->millis();
            const auto seen = last_seen_->get(e->account);
            if (!seen.has_value() || ts > *seen) {
                last_seen_->put(e->account, ts);
                this->runtime()->timer_service()->register_event_time_timer(ts + kIdleMs,
                                                                            timer_key(e->account));
            }
        }
    }

    void on_event_time_timer(std::int64_t timestamp_ms,
                             const std::string& key,
                             clink::Emitter<std::string>& out) override {
        const std::int64_t account = account_of(key);
        const auto seen = last_seen_->get(account);
        if (!seen.has_value() || *seen + kIdleMs != timestamp_ms) {
            return;
        }
        const std::int64_t n = idle_count_->get(account).value_or(0) + 1;
        idle_count_->put(account, n);
        clink::Batch<std::string> b;
        b.emplace("idle," + std::to_string(account) + "," + std::to_string(*seen) + "," +
                      std::to_string(timestamp_ms) + "," + std::to_string(n),
                  clink::EventTime{timestamp_ms});
        out.emit_data(std::move(b));
    }

    void restore_timers(clink::StateBackend& backend,
                        clink::OperatorId op_id,
                        const std::string& slot) override {
        restored_ = true;
        if (kVariant == 8) {
            std::raise(SIGKILL);
        }
        if (kVariant == 4) {
            return;  // the timers the checkpoint holds are never restored
        }
        Operator::restore_timers(backend, op_id, slot);
    }

    std::string name() const override { return "upgrade_check.idle"; }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> last_seen_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> idle_count_;
    bool restored_{false};
    int since_restore_{0};
};

// ---- the local sink ----------------------------------------------------------------

class LineFileSink final : public clink::Sink<std::string> {
public:
    explicit LineFileSink(std::string path) : path_(std::move(path)) {}

    void open() override {
        out_.open(path_, std::ios::app);
        if (!out_) {
            throw std::runtime_error("upgrade-check sink: cannot open " + path_);
        }
    }
    void close() override {
        if (out_.is_open()) {
            out_.flush();
            out_.close();
        }
    }
    void on_data(const clink::Batch<std::string>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << '\n';
        }
    }
    void on_watermark(clink::Watermark /*wm*/) override {}
    void on_barrier(clink::CheckpointBarrier /*barrier*/) override { out_.flush(); }

private:
    std::string path_;
    std::ofstream out_;
};

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return p;
    }
    return fallback;
}

// "<count>,<sum>" folded with one event.
std::string fold(const std::string& acc, const std::string& value) {
    const auto e = parse_event(value);
    if (!e) {
        return acc;
    }
    const auto c = acc.find(',');
    std::int64_t count = std::stoll(acc.substr(0, c));
    std::int64_t sum = std::stoll(acc.substr(c + 1));
    if constexpr (kVariant == 1) {
        // The same fold, written another way.
        count += 1;
        sum = sum + e->amount;
    } else {
        ++count;
        if (kVariant != 2 || e->amount >= 0) {
            sum += e->amount;
        }
    }
    return std::to_string(count) + "," + std::to_string(sum);
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    auto& reg = pipeline.registry();
    // The window's accumulator is text. Registering the type on the job's own
    // registry is what gives the fluent window its codec, and so keyed state
    // that a checkpoint holds; without it the windows live in memory only.
    if constexpr (kVariant != 10) {
        reg.register_type<std::string>("string", clink::string_codec());
    }
    reg.register_source<std::string>("upgrade_check.source",
                                     [](const clink::plugin::BuildContext& c) {
                                         return std::make_shared<SegmentSource>(c.param_or("feed"));
                                     });
    reg.register_operator<std::string, std::string>(
        "upgrade_check.idle",
        [](const clink::plugin::BuildContext&) { return std::make_shared<IdleDetector>(); });
    reg.register_sink<std::string>("upgrade_check.lines", [](const clink::plugin::BuildContext& c) {
        return std::make_shared<LineFileSink>(c.param_or("path"));
    });
    reg.register_key_extractor<std::string>("upgrade_check.by_account",
                                            [](const std::string& s) -> std::int64_t {
                                                const auto e = parse_event(s);
                                                return e ? e->account : 0;
                                            });

    const std::string feed = env_or("CLINK_UC_FEED", "");
    const std::string out = env_or("CLINK_UC_OUT", "");

    pipeline.set_parallelism(1);
    clink::api::SourceDescriptor src;
    src.op_type = "upgrade_check.source";
    src.channel_type = "string";
    src.params["feed"] = feed;
    auto source = pipeline.source<std::string>(src, "source");
    source.uid("uc-source");

    pipeline.set_parallelism(kKeyedParallelism);
    auto keyed = source.key_by("upgrade_check.by_account");
    auto windows =
        keyed.tumbling_window(std::chrono::milliseconds(kWindowMs))
            .parallelism(kKeyedParallelism)
            .aggregate<std::string, std::string>(
                []() { return std::string{"0,0"}; },
                [](const std::string& acc, const std::string& v) { return fold(acc, v); },
                [](std::int64_t account, const clink::TimeWindow& w, const std::string& acc) {
                    return "window," + std::to_string(account) + "," + std::to_string(w.start) +
                           "," + std::to_string(w.end) + "," + acc;
                },
                "windows");
    if constexpr (kVariant == 6) {
        windows.uid("uc-windows-v2");
    } else if constexpr (kVariant != 7) {
        windows.uid("uc-windows");
    }
    pipeline.expect_state_version("uc-windows", "uc.window", kVariant == 5 ? 2 : 1, "windows");

    auto idle = keyed.process<std::string>("upgrade_check.idle", {}, "idle");
    idle.uid("uc-idle");
    pipeline.expect_state_version("uc-idle", "uc.last_seen", 1, "last_seen");
    pipeline.expect_state_version("uc-idle", "uc.idle_count", 1, "idle_count");

    pipeline.set_parallelism(1);
    clink::api::SinkDescriptor ws;
    ws.op_type = "upgrade_check.lines";
    ws.channel_type = "string";
    ws.params["path"] = out.empty() ? std::string{} : out + "/windows.txt";
    windows.sink(ws, "windows-sink");
    clink::api::SinkDescriptor is;
    is.op_type = "upgrade_check.lines";
    is.channel_type = "string";
    is.params["path"] = out.empty() ? std::string{} : out + "/idle.txt";
    idle.sink(is, "idle-sink");
}

}  // namespace upgrade_check

CLINK_REGISTER_JOB("upgrade-check",
                   "1.0",
                   "keyed event-time tumbling windows and idle timers over a controlled local "
                   "source, for the upgrade validation workflow",
                   upgrade_check::define_job);
