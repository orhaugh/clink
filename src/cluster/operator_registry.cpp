#include "clink/cluster/operator_registry.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "clink/core/record.hpp"
#include "clink/operators/filter_operator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"

namespace clink::cluster {

void OperatorRegistry::register_source(std::string type, SourceFactory f) {
    std::lock_guard lock(mu_);
    sources_[SourceKey{std::move(type), f.out}] = std::move(f);
}

void OperatorRegistry::register_operator(std::string type, OperatorFactory f) {
    std::lock_guard lock(mu_);
    operators_[OpKey{std::move(type), f.in, f.out}] = std::move(f);
}

void OperatorRegistry::register_sink(std::string type, SinkFactory f) {
    std::lock_guard lock(mu_);
    sinks_[SinkKey{std::move(type), f.in}] = std::move(f);
}

const SourceFactory* OperatorRegistry::find_source(const std::string& type, ChannelType out) const {
    {
        std::lock_guard lock(mu_);
        auto it = sources_.find(SourceKey{type, out});
        if (it != sources_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_source(type, out) : nullptr;
}

const OperatorFactory* OperatorRegistry::find_operator(const std::string& type,
                                                       ChannelType in,
                                                       ChannelType out) const {
    {
        std::lock_guard lock(mu_);
        auto it = operators_.find(OpKey{type, in, out});
        if (it != operators_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_operator(type, in, out) : nullptr;
}

const SinkFactory* OperatorRegistry::find_sink(const std::string& type, ChannelType in) const {
    {
        std::lock_guard lock(mu_);
        auto it = sinks_.find(SinkKey{type, in});
        if (it != sinks_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_sink(type, in) : nullptr;
}

namespace {

// File-line sink: writes string records one per line to a path supplied
// via params["path"]. Truncates on open. Useful for the producer→consumer
// integration test.
class FileLineSink final : public Sink<std::string> {
public:
    explicit FileLineSink(std::string path) : path_(std::move(path)) {}

    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileLineSink: failed to open " + path_);
        }
    }

    void on_data(const Batch<std::string>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }

    void flush() override { out_.flush(); }

    void close() override { out_.close(); }

    std::string name() const override { return "file_line_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

// File-line sink for int64: writes one integer per line.
class FileInt64Sink final : public Sink<std::int64_t> {
public:
    explicit FileInt64Sink(std::string path) : path_(std::move(path)) {}

    void open() override {
        out_.open(path_, std::ios::trunc);
        if (!out_.is_open()) {
            throw std::runtime_error("FileInt64Sink: failed to open " + path_);
        }
    }

    void on_data(const Batch<std::int64_t>& batch) override {
        for (const auto& r : batch) {
            out_ << r.value() << "\n";
        }
    }

    void flush() override { out_.flush(); }
    void close() override { out_.close(); }

    std::string name() const override { return "file_int64_sink"; }

private:
    std::string path_;
    std::ofstream out_;
};

void register_built_ins(OperatorRegistry& reg) {
    // int64_range_source: emits records start, start+step, ..., start+(count-1)*step.
    // params: count (required, int), start (default 1), step (default 1).
    // Parallelism-aware: with parallelism=N and subtask_idx=i, this
    // subtask emits indices {i, i+N, i+2N, ...} (round-robin over the
    // total range), so every record appears exactly once across the N
    // parallel subtasks.
    reg.register_source(
        "int64_range_source",
        SourceFactory{
            .out = std::string{clink::cluster::kChannelInt64},
            .build = [](const OperatorBuildContext& ctx) -> std::shared_ptr<void> {
                const auto count = param_int64_or(ctx, "count", 0);
                const auto start = param_int64_or(ctx, "start", 1);
                const auto step = param_int64_or(ctx, "step", 1);
                const auto par =
                    static_cast<std::int64_t>(ctx.parallelism == 0 ? 1 : ctx.parallelism);
                const auto idx = static_cast<std::int64_t>(ctx.subtask_idx);
                std::vector<Record<std::int64_t>> records;
                for (std::int64_t k = idx; k < count; k += par) {
                    records.emplace_back(Record<std::int64_t>{start + k * step});
                }
                return std::static_pointer_cast<void>(std::make_shared<VectorSource<std::int64_t>>(
                    std::move(records), "int64_range_source"));
            },
        });

    // string_lines_source: emits each entry of params[lines] as a record.
    // params[lines] is a comma-separated list (no escaping in v1).
    reg.register_source(
        "string_lines_source",
        SourceFactory{
            .out = std::string{clink::cluster::kChannelString},
            .build = [](const OperatorBuildContext& ctx) -> std::shared_ptr<void> {
                const auto raw = param_or(ctx, "lines", "");
                std::vector<Record<std::string>> records;
                std::size_t start = 0;
                while (start <= raw.size()) {
                    const auto pos = raw.find(',', start);
                    const auto end = (pos == std::string::npos) ? raw.size() : pos;
                    records.emplace_back(Record<std::string>{raw.substr(start, end - start)});
                    if (pos == std::string::npos) {
                        break;
                    }
                    start = pos + 1;
                }
                return std::static_pointer_cast<void>(std::make_shared<VectorSource<std::string>>(
                    std::move(records), "string_lines_source"));
            },
        });

    // file_int64_sink: writes int64 records, one per line, to
    // params["path"]. When parallelism>1, each parallel subtask writes
    // to "<path>.<subtask_idx>" so concurrent writers don't truncate
    // each other.
    reg.register_sink(
        "file_int64_sink",
        SinkFactory{
            .in = std::string{clink::cluster::kChannelInt64},
            .build = [](const OperatorBuildContext& ctx) -> std::shared_ptr<void> {
                auto path = param_or(ctx, "path");
                if (path.empty()) {
                    throw std::runtime_error("file_int64_sink: 'path' param is required");
                }
                if (ctx.parallelism > 1) {
                    path += "." + std::to_string(ctx.subtask_idx);
                }
                return std::static_pointer_cast<void>(std::make_shared<FileInt64Sink>(path));
            },
        });

    // file_line_sink: writes string records, one per line. Per-subtask
    // suffix when parallelism>1, mirroring file_int64_sink.
    reg.register_sink(
        "file_line_sink",
        SinkFactory{
            .in = std::string{clink::cluster::kChannelString},
            .build = [](const OperatorBuildContext& ctx) -> std::shared_ptr<void> {
                auto path = param_or(ctx, "path");
                if (path.empty()) {
                    throw std::runtime_error("file_line_sink: 'path' param is required");
                }
                if (ctx.parallelism > 1) {
                    path += "." + std::to_string(ctx.subtask_idx);
                }
                return std::static_pointer_cast<void>(std::make_shared<FileLineSink>(path));
            },
        });

    // collecting_int64_sink and collecting_string_sink: in-process test
    // helpers that stash records in a CollectingSink. Useful for unit
    // tests that assert on what arrived without touching the filesystem.
    reg.register_sink("collecting_int64_sink",
                      SinkFactory{
                          .in = std::string{clink::cluster::kChannelInt64},
                          .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                              return std::static_pointer_cast<void>(
                                  std::make_shared<CollectingSink<std::int64_t>>());
                          },
                      });
    reg.register_sink("collecting_string_sink",
                      SinkFactory{
                          .in = std::string{clink::cluster::kChannelString},
                          .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                              return std::static_pointer_cast<void>(
                                  std::make_shared<CollectingSink<std::string>>());
                          },
                      });

    // identity_<T>: passes records through unchanged. Useful for graphs
    // that want a sink with no transform, or as a routing waypoint when
    // the user wants to force two ops onto different workers.
    reg.register_operator(
        "identity_int64",
        OperatorFactory{
            .in = std::string{clink::cluster::kChannelInt64},
            .out = std::string{clink::cluster::kChannelInt64},
            .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                return std::static_pointer_cast<void>(
                    std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                        [](const std::int64_t& v) { return v; }, "identity_int64"));
            },
        });
    reg.register_operator(
        "identity_string",
        OperatorFactory{
            .in = std::string{clink::cluster::kChannelString},
            .out = std::string{clink::cluster::kChannelString},
            .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                return std::static_pointer_cast<void>(
                    std::make_shared<MapOperator<std::string, std::string>>(
                        [](const std::string& v) { return v; }, "identity_string"));
            },
        });

    // multiply_int64: params[factor]. Demonstrates a parameterised
    // transform without bringing in user code.
    reg.register_operator(
        "multiply_int64",
        OperatorFactory{
            .in = std::string{clink::cluster::kChannelInt64},
            .out = std::string{clink::cluster::kChannelInt64},
            .build = [](const OperatorBuildContext& ctx) -> std::shared_ptr<void> {
                const auto factor = param_int64_or(ctx, "factor", 1);
                return std::static_pointer_cast<void>(
                    std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
                        [factor](const std::int64_t& v) { return v * factor; }, "multiply_int64"));
            },
        });

    // int64_to_string / string_to_int64: type-changing maps. Useful for
    // testing cross-channel-type chains (Int64 -> String -> Int64).
    reg.register_operator("int64_to_string",
                          OperatorFactory{
                              .in = std::string{clink::cluster::kChannelInt64},
                              .out = std::string{clink::cluster::kChannelString},
                              .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                                  return std::static_pointer_cast<void>(
                                      std::make_shared<MapOperator<std::int64_t, std::string>>(
                                          [](const std::int64_t& v) { return std::to_string(v); },
                                          "int64_to_string"));
                              },
                          });
    reg.register_operator("string_to_int64",
                          OperatorFactory{
                              .in = std::string{clink::cluster::kChannelString},
                              .out = std::string{clink::cluster::kChannelInt64},
                              .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                                  return std::static_pointer_cast<void>(
                                      std::make_shared<MapOperator<std::string, std::int64_t>>(
                                          [](const std::string& s) -> std::int64_t {
                                              try {
                                                  return std::stoll(s);
                                              } catch (...) {
                                                  return 0;
                                              }
                                          },
                                          "string_to_int64"));
                              },
                          });

    // even_filter_int64: keeps records where v % 2 == 0. Demonstrates a
    // filter operator over the wire.
    reg.register_operator(
        "even_filter_int64",
        OperatorFactory{
            .in = std::string{clink::cluster::kChannelInt64},
            .out = std::string{clink::cluster::kChannelInt64},
            .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                return std::static_pointer_cast<void>(
                    std::make_shared<FilterOperator<std::int64_t>>(
                        [](const std::int64_t& v) { return (v % 2) == 0; }, "even_filter_int64"));
            },
        });
}

}  // namespace

OperatorRegistry& OperatorRegistry::default_instance() {
    static OperatorRegistry registry;
    static std::once_flag init_flag;
    std::call_once(init_flag, [] { register_built_ins(registry); });
    return registry;
}

// ---------- SelectorRegistry ----------

void SelectorRegistry::register_int64(std::string name, Int64Selector fn) {
    std::lock_guard lock(mu_);
    int64_[std::move(name)] = std::move(fn);
}

void SelectorRegistry::register_string(std::string name, StringSelector fn) {
    std::lock_guard lock(mu_);
    string_[std::move(name)] = std::move(fn);
}

const SelectorRegistry::Int64Selector* SelectorRegistry::find_int64(const std::string& name) const {
    {
        std::lock_guard lock(mu_);
        auto it = int64_.find(name);
        if (it != int64_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_int64(name) : nullptr;
}

const SelectorRegistry::StringSelector* SelectorRegistry::find_string(
    const std::string& name) const {
    {
        std::lock_guard lock(mu_);
        auto it = string_.find(name);
        if (it != string_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_string(name) : nullptr;
}

namespace {

void register_built_in_selectors(SelectorRegistry& r) {
    // int64_even_odd: routes even values to branch 0, odd to branch 1.
    r.register_int64("int64_even_odd", [](const std::int64_t& v) { return (v % 2 == 0) ? 0 : 1; });
    // int64_mod_n: routes by v mod N. N is fixed per registration; we
    // register a small set of common values.
    r.register_int64("int64_mod_2",
                     [](const std::int64_t& v) { return static_cast<int>(((v % 2) + 2) % 2); });
    r.register_int64("int64_mod_3",
                     [](const std::int64_t& v) { return static_cast<int>(((v % 3) + 3) % 3); });
    // string_first_char_parity: routes by parity of the first byte's
    // numeric value. Useful for testing splits on string streams.
    r.register_string("string_first_char_parity",
                      [](const std::string& s) { return (s.empty() || (s[0] % 2 == 0)) ? 0 : 1; });
}

}  // namespace

SelectorRegistry& SelectorRegistry::default_instance() {
    static SelectorRegistry registry;
    static std::once_flag init_flag;
    std::call_once(init_flag, [] { register_built_in_selectors(registry); });
    return registry;
}

KeyExtractorRegistry& KeyExtractorRegistry::default_instance() {
    static KeyExtractorRegistry registry;
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
        // Built-in identity extractors for the common cases.
        // hash("int64") = the value itself.
        registry.register_extractor<std::int64_t>(
            std::string{kChannelInt64}, "identity", [](const std::int64_t& v) { return v; });
        // hash("string") = std::hash<std::string>{}.
        registry.register_extractor<std::string>(
            std::string{kChannelString}, "identity", [](const std::string& v) -> std::int64_t {
                return static_cast<std::int64_t>(std::hash<std::string>{}(v));
            });
    });
    return registry;
}

}  // namespace clink::cluster
