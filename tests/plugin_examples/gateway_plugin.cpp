// Gateway-parity plugin: a realistic multi-stage shape
// (FragmentReassembly → EnrichmentJoin → sinks) packaged for the
// integration test that proves clink can run such a topology
// end-to-end without substitutions.
//
// Pieces exercised:
//   * keyBy (gateway.by_key) at parallelism > 1, on both a single-input
//     keyed operator and the two inputs of a CoOperator
//   * KeyedState<string, V> with vector<Fragment> as the "ListState"
//     analogue for the reassembler buffer and the join pending buffer
//   * Side outputs over the cluster wire (liveness side stream
//     emitting std::string records to a separate sink)
//   * Plugin-defined custom types (Fragment, Enrichment, Enriched)
//     with codecs registered via PluginRegistry::register_type
//
// All deliberately self-contained: the test feeds the source script
// via op params so the test, not the plugin, owns the input data.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <clink/operators/operator_base.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/plugin/plugin.hpp>
#include <clink/runtime/runtime_context.hpp>

namespace gateway {

struct Fragment {
    std::string group_id;
    std::int32_t index{0};
    std::int32_t count{1};
    std::int64_t value{0};
};

struct Enrichment {
    std::string key;
    std::string profile;
};

struct Enriched {
    std::string key;
    std::string profile;
    std::int64_t value{0};
};

// Length-prefixed codec helpers (mirrors greeting_codec in hello_plugin).
void put_u32(std::vector<std::byte>& out, std::uint32_t v) {
    for (int i = 3; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xFFU));
    }
}
void put_u64(std::vector<std::byte>& out, std::uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xFFU));
    }
}
void put_str(std::vector<std::byte>& out, const std::string& s) {
    put_u32(out, static_cast<std::uint32_t>(s.size()));
    for (char c : s) {
        out.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    }
}

bool read_u32(std::span<const std::byte> b, std::size_t& pos, std::uint32_t& v) {
    if (pos + 4 > b.size())
        return false;
    v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<unsigned char>(b[pos++]);
    }
    return true;
}
bool read_u64(std::span<const std::byte> b, std::size_t& pos, std::uint64_t& v) {
    if (pos + 8 > b.size())
        return false;
    v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<unsigned char>(b[pos++]);
    }
    return true;
}
bool read_str(std::span<const std::byte> b, std::size_t& pos, std::string& s) {
    std::uint32_t len = 0;
    if (!read_u32(b, pos, len))
        return false;
    if (pos + len > b.size())
        return false;
    s.assign(reinterpret_cast<const char*>(b.data() + pos), len);
    pos += len;
    return true;
}

clink::Codec<Fragment> fragment_codec() {
    return clink::Codec<Fragment>{
        [](const Fragment& f) {
            std::vector<std::byte> out;
            put_str(out, f.group_id);
            put_u32(out, static_cast<std::uint32_t>(f.index));
            put_u32(out, static_cast<std::uint32_t>(f.count));
            put_u64(out, static_cast<std::uint64_t>(f.value));
            return out;
        },
        [](clink::Codec<Fragment>::BytesView b) -> std::optional<Fragment> {
            std::size_t pos = 0;
            Fragment f;
            std::uint32_t idx = 0;
            std::uint32_t cnt = 0;
            std::uint64_t val = 0;
            if (!read_str(b, pos, f.group_id) || !read_u32(b, pos, idx) || !read_u32(b, pos, cnt) ||
                !read_u64(b, pos, val)) {
                return std::nullopt;
            }
            f.index = static_cast<std::int32_t>(idx);
            f.count = static_cast<std::int32_t>(cnt);
            f.value = static_cast<std::int64_t>(val);
            return f;
        }};
}

clink::Codec<Enrichment> enrichment_codec() {
    return clink::Codec<Enrichment>{
        [](const Enrichment& e) {
            std::vector<std::byte> out;
            put_str(out, e.key);
            put_str(out, e.profile);
            return out;
        },
        [](clink::Codec<Enrichment>::BytesView b) -> std::optional<Enrichment> {
            std::size_t pos = 0;
            Enrichment e;
            if (!read_str(b, pos, e.key) || !read_str(b, pos, e.profile)) {
                return std::nullopt;
            }
            return e;
        }};
}

clink::Codec<Enriched> enriched_codec() {
    return clink::Codec<Enriched>{
        [](const Enriched& e) {
            std::vector<std::byte> out;
            put_str(out, e.key);
            put_str(out, e.profile);
            put_u64(out, static_cast<std::uint64_t>(e.value));
            return out;
        },
        [](clink::Codec<Enriched>::BytesView b) -> std::optional<Enriched> {
            std::size_t pos = 0;
            Enriched e;
            std::uint64_t v = 0;
            if (!read_str(b, pos, e.key) || !read_str(b, pos, e.profile) || !read_u64(b, pos, v)) {
                return std::nullopt;
            }
            e.value = static_cast<std::int64_t>(v);
            return e;
        }};
}

// Split "a|b|c" into {"a","b","c"}.
std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == delim) {
            out.push_back(std::move(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) {
        out.push_back(std::move(cur));
    }
    return out;
}

// Parse "group_id:index:count:value".
std::optional<Fragment> parse_fragment(const std::string& tok) {
    const auto parts = split(tok, ':');
    if (parts.size() != 4) {
        return std::nullopt;
    }
    Fragment f;
    f.group_id = parts[0];
    try {
        f.index = std::stoi(parts[1]);
        f.count = std::stoi(parts[2]);
        f.value = std::stoll(parts[3]);
    } catch (...) {
        return std::nullopt;
    }
    return f;
}

std::optional<Enrichment> parse_enrichment(const std::string& tok) {
    const auto parts = split(tok, ':');
    if (parts.size() != 2) {
        return std::nullopt;
    }
    return Enrichment{parts[0], parts[1]};
}

// Source: emits Fragments parsed from a pipe-separated script.
class FragmentSource final : public clink::Source<Fragment> {
public:
    FragmentSource(std::vector<Fragment> records, std::int64_t delay_ms)
        : records_(std::move(records)), delay_ms_(delay_ms) {}

    bool produce(clink::Emitter<Fragment>& out) override {
        if (next_ >= records_.size()) {
            out.emit_watermark(clink::Watermark::max());
            return false;
        }
        clink::Batch<Fragment> b;
        b.emplace(records_[next_]);
        out.emit_data(std::move(b));
        ++next_;
        if (delay_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds{delay_ms_});
        }
        return true;
    }
    std::string name() const override { return "gateway.FragmentSource"; }

private:
    std::vector<Fragment> records_;
    std::int64_t delay_ms_{0};
    std::size_t next_{0};
};

class EnrichmentSource final : public clink::Source<Enrichment> {
public:
    EnrichmentSource(std::vector<Enrichment> records, std::int64_t delay_ms)
        : records_(std::move(records)), delay_ms_(delay_ms) {}

    bool produce(clink::Emitter<Enrichment>& out) override {
        if (next_ >= records_.size()) {
            out.emit_watermark(clink::Watermark::max());
            return false;
        }
        clink::Batch<Enrichment> b;
        b.emplace(records_[next_]);
        out.emit_data(std::move(b));
        ++next_;
        if (delay_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds{delay_ms_});
        }
        return true;
    }
    std::string name() const override { return "gateway.EnrichmentSource"; }

private:
    std::vector<Enrichment> records_;
    std::int64_t delay_ms_{0};
    std::size_t next_{0};
};

// Reassembler: buffers fragments by group_id in keyed state; emits a
// single Fragment with the summed `value` once `count` fragments are
// collected. Non-fragmented records (count == 1) pass through with no
// state interaction. Realistic fragment-reassembly shape, minus the
// JSON navigation and the inactivity timer (neither is needed by the
// scenario the integration test exercises).
class ReassemblerOperator final : public clink::Operator<Fragment, Fragment> {
public:
    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error(
                "ReassemblerOperator: no RuntimeContext / state backend at open()");
        }
        buffer_.emplace(this->runtime()->template keyed_state<std::string, std::vector<Fragment>>(
            "fragments", clink::string_codec(), clink::vector_codec(fragment_codec())));
    }

    void process(const clink::StreamElement<Fragment>& el, clink::Emitter<Fragment>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<Fragment> out_batch;
        for (const auto& rec : el.as_data()) {
            const auto& f = rec.value();
            if (f.count <= 1) {
                out_batch.emplace(f);
                continue;
            }
            auto buf = buffer_->get(f.group_id).value_or(std::vector<Fragment>{});
            buf.push_back(f);
            if (static_cast<std::int32_t>(buf.size()) == f.count) {
                std::sort(buf.begin(), buf.end(), [](const Fragment& a, const Fragment& b) {
                    return a.index < b.index;
                });
                Fragment merged;
                merged.group_id = f.group_id;
                merged.index = 0;
                merged.count = 1;
                merged.value = 0;
                for (const auto& part : buf) {
                    merged.value += part.value;
                }
                out_batch.emplace(std::move(merged));
                buffer_->erase(f.group_id);
            } else {
                buffer_->put(f.group_id, buf);
            }
        }
        if (!out_batch.empty()) {
            out.emit_data(std::move(out_batch));
        }
    }

    std::string name() const override { return "gateway.Reassembler"; }

private:
    std::optional<clink::KeyedState<std::string, std::vector<Fragment>>> buffer_;
};

// EnrichmentJoin: KeyedCoProcessFunction analogue.
// processElement1 (Fragment): if enrichment_state has the matching key,
// emit Enriched and a liveness side record. Otherwise buffer the
// fragment in pending_state for later replay.
// processElement2 (Enrichment): update the enrichment state, drain any
// pending fragments by emitting Enriched + liveness for each, then
// clear pending.
//
// Emits "liveness:<key>" to the side output named "gateway.liveness"
// every time an Enriched record is produced. Downstream wiring sends
// the side records to a separate sink so the test can confirm both
// channels flow across the wire correctly.
class EnrichmentJoinCoOperator final : public clink::CoOperator<Fragment, Enrichment, Enriched> {
public:
    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error(
                "EnrichmentJoinCoOperator: no RuntimeContext / state backend at open()");
        }
        enrichment_state_.emplace(this->runtime()->template keyed_state<std::string, Enrichment>(
            "enrichment", clink::string_codec(), enrichment_codec()));
        pending_.emplace(this->runtime()->template keyed_state<std::string, std::vector<Fragment>>(
            "pending", clink::string_codec(), clink::vector_codec(fragment_codec())));
    }

    void process_element1(const clink::StreamElement<Fragment>& el,
                          clink::Emitter<Enriched>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<Enriched> main_batch;
        clink::Batch<std::string> live_batch;
        for (const auto& rec : el.as_data()) {
            const auto& f = rec.value();
            auto enr = enrichment_state_->get(f.group_id);
            if (enr.has_value()) {
                main_batch.emplace(Enriched{f.group_id, enr->profile, f.value});
                live_batch.emplace("liveness:" + f.group_id);
            } else {
                auto buf = pending_->get(f.group_id).value_or(std::vector<Fragment>{});
                buf.push_back(f);
                pending_->put(f.group_id, buf);
            }
        }
        if (!main_batch.empty()) {
            out.emit_data(std::move(main_batch));
        }
        if (!live_batch.empty()) {
            auto side = this->runtime()->template side_output<std::string>(
                clink::OutputTag<std::string>("gateway.liveness"));
            side.emit_data(std::move(live_batch));
        }
    }

    void process_element2(const clink::StreamElement<Enrichment>& el,
                          clink::Emitter<Enriched>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<Enriched> main_batch;
        clink::Batch<std::string> live_batch;
        for (const auto& rec : el.as_data()) {
            const auto& e = rec.value();
            enrichment_state_->put(e.key, e);
            auto pending = pending_->get(e.key).value_or(std::vector<Fragment>{});
            for (const auto& f : pending) {
                main_batch.emplace(Enriched{e.key, e.profile, f.value});
                live_batch.emplace("liveness:" + e.key);
            }
            if (!pending.empty()) {
                pending_->erase(e.key);
            }
        }
        if (!main_batch.empty()) {
            out.emit_data(std::move(main_batch));
        }
        if (!live_batch.empty()) {
            auto side = this->runtime()->template side_output<std::string>(
                clink::OutputTag<std::string>("gateway.liveness"));
            side.emit_data(std::move(live_batch));
        }
    }

    std::string name() const override { return "gateway.EnrichmentJoin"; }

private:
    std::optional<clink::KeyedState<std::string, Enrichment>> enrichment_state_;
    std::optional<clink::KeyedState<std::string, std::vector<Fragment>>> pending_;
};

class EnrichedFileSink final : public clink::Sink<Enriched> {
public:
    explicit EnrichedFileSink(std::string path) : path_(std::move(path)) {}

    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("EnrichedFileSink: failed to open " + path_);
        }
    }
    void on_data(const clink::Batch<Enriched>& b) override {
        for (const auto& r : b) {
            out_ << r.value().key << ":" << r.value().profile << ":" << r.value().value << "\n";
        }
    }
    void close() override { out_.close(); }
    std::string name() const override { return "gateway.EnrichedFileSink"; }

private:
    std::string path_;
    std::ofstream out_;
};

void register_plugin(clink::plugin::PluginRegistry& reg) {
    reg.register_type<Fragment>("gateway.Fragment", fragment_codec());
    reg.register_type<Enrichment>("gateway.Enrichment", enrichment_codec());
    reg.register_type<Enriched>("gateway.Enriched", enriched_codec());

    reg.register_source<Fragment>(
        "gateway.FragmentSource",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Source<Fragment>> {
            const auto script = ctx.param_or("script", "");
            const auto delay_ms = ctx.param_int64_or("delay_ms", 0);
            std::vector<Fragment> records;
            for (const auto& tok : split(script, '|')) {
                if (auto f = parse_fragment(tok); f.has_value()) {
                    records.push_back(*f);
                }
            }
            return std::make_shared<FragmentSource>(std::move(records), delay_ms);
        });

    reg.register_source<Enrichment>(
        "gateway.EnrichmentSource",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Source<Enrichment>> {
            const auto script = ctx.param_or("script", "");
            const auto delay_ms = ctx.param_int64_or("delay_ms", 0);
            std::vector<Enrichment> records;
            for (const auto& tok : split(script, '|')) {
                if (auto e = parse_enrichment(tok); e.has_value()) {
                    records.push_back(*e);
                }
            }
            return std::make_shared<EnrichmentSource>(std::move(records), delay_ms);
        });

    reg.register_operator<Fragment, Fragment>(
        "gateway.Reassembler",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::Operator<Fragment, Fragment>> {
            return std::make_shared<ReassemblerOperator>();
        });

    reg.register_co_operator<Fragment, Enrichment, Enriched>(
        "gateway.EnrichmentJoin",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::CoOperator<Fragment, Enrichment, Enriched>> {
            return std::make_shared<EnrichmentJoinCoOperator>();
        });

    reg.register_sink<Enriched>(
        "gateway.EnrichedFileSink",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Sink<Enriched>> {
            auto path = ctx.param_or("path", "");
            if (path.empty()) {
                throw std::runtime_error("gateway.EnrichedFileSink: 'path' required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            return std::make_shared<EnrichedFileSink>(std::move(path));
        });

    // Same key extractor name across two channel types: routing both
    // upstreams of the EnrichmentJoin co-operator to the same subtask
    // for matching keys requires that hash(group_id) == hash(key) for
    // the same string. KeyExtractorRegistry keys on (channel, name),
    // so we register both.
    auto hash_str = [](const std::string& s) -> std::int64_t {
        return static_cast<std::int64_t>(std::hash<std::string>{}(s));
    };
    reg.register_key_extractor<Fragment>(
        "gateway.by_key", [hash_str](const Fragment& f) { return hash_str(f.group_id); });
    reg.register_key_extractor<Enrichment>(
        "gateway.by_key", [hash_str](const Enrichment& e) { return hash_str(e.key); });
}

}  // namespace gateway

CLINK_DECLARE_PLUGIN("gateway-plugin", "1.0.0", "clink gateway parity test plugin");
CLINK_REGISTER_PLUGIN(gateway::register_plugin);
