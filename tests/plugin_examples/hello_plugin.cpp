// Hello-world clink plugin. Defines a user record type (Greeting),
// a Source<Greeting>, and a Sink<Greeting> that captures records into
// an exported file. Used by tests/test_plugin_loader.cpp to verify the
// dlopen -> register -> dispatch path end-to-end.

#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include <clink/operators/operator_base.hpp>
#include <clink/operators/source_operator.hpp>
#include <clink/plugin/plugin.hpp>
#include <clink/runtime/runtime_context.hpp>

namespace hello {

// User record type. Crosses the network bridge between TMs as
// "hello.Greeting"; serialised by greeting_codec() below.
struct Greeting {
    std::int64_t id{0};
    std::string message;
};

clink::Codec<Greeting> greeting_codec() {
    return clink::Codec<Greeting>{
        .encode = [](const Greeting& g) -> std::vector<std::byte> {
            std::vector<std::byte> out;
            const auto u = static_cast<std::uint64_t>(g.id);
            for (int i = 0; i < 8; ++i) {
                out.push_back(static_cast<std::byte>((u >> (i * 8)) & 0xFF));
            }
            const auto len = static_cast<std::uint32_t>(g.message.size());
            for (int i = 0; i < 4; ++i) {
                out.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
            }
            for (char c : g.message) {
                out.push_back(static_cast<std::byte>(c));
            }
            return out;
        },
        .decode = [](std::span<const std::byte> b) -> std::optional<Greeting> {
            if (b.size() < 12) {
                return std::nullopt;
            }
            std::uint64_t u = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                u |= static_cast<std::uint64_t>(static_cast<unsigned char>(b[i])) << (i * 8);
            }
            std::uint32_t len = 0;
            for (std::size_t i = 0; i < 4; ++i) {
                len |= static_cast<std::uint32_t>(static_cast<unsigned char>(b[8 + i])) << (i * 8);
            }
            if (b.size() != 12 + len) {
                return std::nullopt;
            }
            Greeting g;
            g.id = static_cast<std::int64_t>(u);
            g.message.assign(reinterpret_cast<const char*>(b.data() + 12), len);
            return g;
        }};
}

// Source: emits a fixed sequence configurable via params. When
// `delay_ms` is non-zero, emits one record per produce() call with a
// per-record sleep so distributed-checkpointing barriers triggered by
// the JM have time to interleave between records.
class GreetingSource final : public clink::Source<Greeting> {
public:
    GreetingSource(std::int64_t count, std::int64_t start, std::int64_t delay_ms)
        : count_(count), start_(start), delay_ms_(delay_ms) {}

    bool produce(clink::Emitter<Greeting>& out) override {
        if (delay_ms_ <= 0) {
            if (emitted_) {
                return false;
            }
            clink::Batch<Greeting> b;
            for (std::int64_t i = 0; i < count_; ++i) {
                b.emplace(Greeting{start_ + i, "hello-" + std::to_string(start_ + i)});
            }
            out.emit_data(std::move(b));
            out.emit_watermark(clink::Watermark::max());
            emitted_ = true;
            return false;
        }
        if (next_idx_ >= count_) {
            out.emit_watermark(clink::Watermark::max());
            return false;
        }
        clink::Batch<Greeting> b;
        b.emplace(Greeting{start_ + next_idx_, "hello-" + std::to_string(start_ + next_idx_)});
        out.emit_data(std::move(b));
        ++next_idx_;
        std::this_thread::sleep_for(std::chrono::milliseconds{delay_ms_});
        return true;
    }
    std::string name() const override { return "hello.GreetingSource"; }

private:
    std::int64_t count_;
    std::int64_t start_;
    std::int64_t delay_ms_{0};
    std::int64_t next_idx_{0};
    bool emitted_{false};
};

// Stateful operator: counts how many Greetings have been seen per
// parity-of-id-bucket. The bucket key (0 = even id, 1 = odd id) is
// extracted per-record, and the count is held in a KeyedState slot.
// Emits one Greeting per input, with `message` rewritten to
// "<bucket>:<count>" so the downstream sink sees the running counts.
//
// This is the smoke test for keyed state surviving across
// process() invocations within a subtask, sourced from the
// runtime()->keyed_state<K, V>() handle the cluster provisions per
// subtask via JobConfig::state_backend.
class ParityCounterOperator final : public clink::Operator<Greeting, Greeting> {
public:
    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error(
                "ParityCounterOperator: no RuntimeContext / state backend at open()");
        }
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "parity_counts", clink::int64_codec(), clink::int64_codec()));
    }

    void process(const clink::StreamElement<Greeting>& el, clink::Emitter<Greeting>& out) override {
        if (el.is_data()) {
            clink::Batch<Greeting> out_batch;
            for (const auto& rec : el.as_data()) {
                const std::int64_t bucket = rec.value().id % 2 == 0 ? 0 : 1;
                const auto current = state_->get(bucket).value_or(0);
                const auto next = current + 1;
                state_->put(bucket, next);

                Greeting g = rec.value();
                g.message = std::to_string(bucket) + ":" + std::to_string(next);
                out_batch.emplace(std::move(g));
            }
            out.emit_data(std::move(out_batch));
        } else if (el.is_watermark()) {
            this->on_watermark(el.as_watermark(), out);
        } else {
            this->on_barrier(el.as_barrier(), out);
        }
    }

    std::string name() const override { return "hello.ParityCounter"; }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> state_;
};

// Partitioner: forwards even-id Greetings on the main output and
// emits odd-id ones to a named side output as plain std::string
// ("odd:<id>"). The downstream consumers wire the main and the side
// to separate sinks. Demonstrates side outputs crossing the wire in
// the cluster.
class GreetingPartitionerOperator final : public clink::Operator<Greeting, Greeting> {
public:
    void process(const clink::StreamElement<Greeting>& el, clink::Emitter<Greeting>& out) override {
        if (!el.is_data()) {
            if (el.is_watermark()) {
                out.emit_watermark(el.as_watermark());
            } else {
                out.emit_barrier(el.as_barrier());
            }
            return;
        }
        clink::Batch<Greeting> main_batch;
        clink::Batch<std::string> side_batch;
        for (const auto& r : el.as_data()) {
            if (r.value().id % 2 == 0) {
                main_batch.emplace(r.value());
            } else {
                side_batch.emplace("odd:" + std::to_string(r.value().id));
            }
        }
        if (!main_batch.empty()) {
            out.emit_data(std::move(main_batch));
        }
        if (!side_batch.empty()) {
            auto side = this->runtime()->template side_output<std::string>(
                clink::OutputTag<std::string>("hello.odd_text"));
            side.emit_data(std::move(side_batch));
        }
    }
    std::string name() const override { return "hello.GreetingPartitioner"; }
};

// Two-input co-operator: Greeting (left) + int64 (right) -> Greeting (out).
// Demonstrates the clink::CoOperator surface that backs // CoProcessFunction. Left and right
// elements update different keyed state slots, but the operator instance is shared - so a single
// subtask sees both streams and can coordinate via state.
//
// process_element1 increments a per-parity counter and emits the greeting
// with message rewritten to "G:<bucket>:<count>".
// process_element2 accumulates the int64 values into a single-slot total
// and emits a synthetic Greeting{v, "I:<sum>"}.
//
// State per stream is independent so per-record outputs are deterministic
// regardless of left/right interleaving at the runner.
class BucketTallyCoOperator final : public clink::CoOperator<Greeting, std::int64_t, Greeting> {
public:
    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error(
                "BucketTallyCoOperator: no RuntimeContext / state backend at open()");
        }
        counts_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", clink::int64_codec(), clink::int64_codec()));
        sum_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "sum", clink::int64_codec(), clink::int64_codec()));
    }

    void process_element1(const clink::StreamElement<Greeting>& el,
                          clink::Emitter<Greeting>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<Greeting> b;
        for (const auto& r : el.as_data()) {
            const std::int64_t bucket = r.value().id % 2 == 0 ? 0 : 1;
            const auto c = counts_->get(bucket).value_or(0) + 1;
            counts_->put(bucket, c);
            Greeting g = r.value();
            g.message = "G:" + std::to_string(bucket) + ":" + std::to_string(c);
            b.emplace(std::move(g));
        }
        out.emit_data(std::move(b));
    }

    void process_element2(const clink::StreamElement<std::int64_t>& el,
                          clink::Emitter<Greeting>& out) override {
        if (!el.is_data()) {
            return;
        }
        clink::Batch<Greeting> b;
        for (const auto& r : el.as_data()) {
            const std::int64_t v = r.value();
            const auto s = sum_->get(0).value_or(0) + v;
            sum_->put(0, s);
            b.emplace(Greeting{v, "I:" + std::to_string(s)});
        }
        out.emit_data(std::move(b));
    }

    std::string name() const override { return "hello.BucketTally"; }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> counts_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> sum_;
};

// Sink: writes each greeting as "<id>:<message>" to a file path
// (auto-suffixed by subtask_idx when parallelism > 1, matching the
// behaviour of file_int64_sink).
class GreetingFileSink final : public clink::Sink<Greeting> {
public:
    explicit GreetingFileSink(std::string path) : path_(std::move(path)) {}

    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("GreetingFileSink: failed to open " + path_);
        }
    }
    void on_data(const clink::Batch<Greeting>& b) override {
        for (const auto& r : b) {
            out_ << r.value().id << ":" << r.value().message << "\n";
        }
    }
    void flush() override { out_.flush(); }
    void close() override { out_.close(); }
    std::string name() const override { return "hello.GreetingFileSink"; }

private:
    std::string path_;
    std::ofstream out_;
};

void register_plugin(clink::plugin::PluginRegistry& reg) {
    reg.register_type<Greeting>("hello.Greeting", greeting_codec());

    reg.register_source<Greeting>(
        "hello.GreetingSource",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Source<Greeting>> {
            const auto count = ctx.param_int64_or("count", 0);
            const auto start = ctx.param_int64_or("start", 1);
            const auto delay_ms = ctx.param_int64_or("delay_ms", 0);
            return std::make_shared<GreetingSource>(count, start, delay_ms);
        });

    reg.register_operator<Greeting, Greeting>(
        "hello.ParityCounter",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::Operator<Greeting, Greeting>> {
            return std::make_shared<ParityCounterOperator>();
        });

    // Key extractor: partition by parity of the greeting id. Returns 0
    // for even ids, 1 for odd. With parallelism=2 on the downstream
    // keyed operator, all even-id records land on one subtask and all
    // odd-id records on the other -> per-key state stays consistent.
    reg.register_key_extractor<Greeting>(
        "hello.by_parity", [](const Greeting& g) -> std::int64_t { return g.id % 2; });

    reg.register_co_operator<Greeting, std::int64_t, Greeting>(
        "hello.BucketTally",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::CoOperator<Greeting, std::int64_t, Greeting>> {
            return std::make_shared<BucketTallyCoOperator>();
        });

    reg.register_operator<Greeting, Greeting>(
        "hello.GreetingPartitioner",
        [](const clink::plugin::BuildContext&)
            -> std::shared_ptr<clink::Operator<Greeting, Greeting>> {
            return std::make_shared<GreetingPartitionerOperator>();
        });

    reg.register_sink<Greeting>(
        "hello.GreetingFileSink",
        [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<clink::Sink<Greeting>> {
            auto path = ctx.param_or("path");
            if (path.empty()) {
                throw std::runtime_error("hello.GreetingFileSink: 'path' required");
            }
            if (ctx.parallelism > 1) {
                path += "." + std::to_string(ctx.subtask_idx);
            }
            return std::make_shared<GreetingFileSink>(std::move(path));
        });
}

}  // namespace hello

CLINK_DECLARE_PLUGIN("hello-plugin", "1.0.0", "clink loader-test plugin");
CLINK_REGISTER_PLUGIN(hello::register_plugin);
