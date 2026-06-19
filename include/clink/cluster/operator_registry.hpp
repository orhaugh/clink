#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace clink::cluster {

// ChannelType identifies an element type that flows across a network
// bridge between clink subtasks. v2 makes this a plain string so
// plugins can register their own types under arbitrary names like
// "myplugin.MyEvent". Two channel names are built into the binary:
// kChannelInt64 and kChannelString. The string identity is what
// appears in submitted JobGraphSpec JSON and is wire-stable: don't
// rename a registered type without a wire-protocol bump.
using ChannelType = std::string;

inline constexpr std::string_view kChannelInt64 = "int64";
inline constexpr std::string_view kChannelString = "string";

// Identity helpers kept for callsite source-compat after the enum →
// string migration. channel_type_name(s) returns s; channel_type_from_name
// validates against the closed built-in set (used by JSON validation
// where we want to reject unknown types early; for plugin-registered
// types the registry's find_* methods are the real gate).
inline std::string channel_type_name(const ChannelType& t) {
    return t;
}

inline std::optional<ChannelType> channel_type_from_name(std::string_view name) {
    if (name.empty()) {
        return std::nullopt;
    }
    return ChannelType{name};
}

// Operator factories receive this. params come straight from the
// OperatorSpec in the submitted JobGraphSpec; subtask_idx and parallelism
// come from the JobPlanner so a single registered factory can spin up
// per-subtask state without each instance having to coordinate.
struct OperatorBuildContext {
    std::map<std::string, std::string> params;
    std::uint32_t subtask_idx{0};
    std::uint32_t parallelism{1};
};

// Factories return shared_ptr<void> bound to the concrete typed object
// (e.g., shared_ptr<Source<std::int64_t>>). Callers use the in/out
// ChannelType to static_pointer_cast back to the right typed shared_ptr.
// Not pretty but contained: only the generic subtask role on the TM
// performs the cast, and it has the channel types in hand.
using BoxedFactory = std::function<std::shared_ptr<void>(const OperatorBuildContext&)>;

struct SourceFactory {
    ChannelType out;
    BoxedFactory build;
};

struct OperatorFactory {
    ChannelType in;
    ChannelType out;
    BoxedFactory build;
};

struct SinkFactory {
    ChannelType in;
    BoxedFactory build;
};

// OperatorRegistry maps op-type strings (the "type" field in an
// OperatorSpec) to factories. Lookups are keyed by (type, in, out) for
// operators, (type, out) for sources, and (type, in) for sinks - so a
// single op-type name can have multiple registered specialisations across
// channel types and the generic role still dispatches deterministically.
class OperatorRegistry {
public:
    OperatorRegistry() = default;
    explicit OperatorRegistry(const OperatorRegistry* parent) : parent_(parent) {}

    void register_source(std::string type, SourceFactory f);
    void register_operator(std::string type, OperatorFactory f);
    void register_sink(std::string type, SinkFactory f);

    // Returns nullptr if no matching factory is registered. Falls
    // through to `parent` on miss (per-job bundles layer over the
    // default-instance for built-ins this way; matches RunnerRegistry
    // semantics).
    const SourceFactory* find_source(const std::string& type, ChannelType out) const;
    const OperatorFactory* find_operator(const std::string& type,
                                         ChannelType in,
                                         ChannelType out) const;
    const SinkFactory* find_sink(const std::string& type, ChannelType in) const;

    // Singleton with built-in registrations (vector source, collecting
    // sink, etc.). Process-wide; safe to access from multiple threads
    // for reads after the static initialisers finish.
    static OperatorRegistry& default_instance();

private:
    struct OpKey {
        std::string type;
        ChannelType in;
        ChannelType out;
        bool operator==(const OpKey&) const = default;
    };
    struct OpKeyHash {
        std::size_t operator()(const OpKey& k) const noexcept {
            std::size_t h = std::hash<std::string>{}(k.type);
            h ^= std::hash<std::string>{}(k.in) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<std::string>{}(k.out) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
    struct SourceKey {
        std::string type;
        ChannelType out;
        bool operator==(const SourceKey&) const = default;
    };
    struct SourceKeyHash {
        std::size_t operator()(const SourceKey& k) const noexcept {
            std::size_t h = std::hash<std::string>{}(k.type);
            h ^= std::hash<std::string>{}(k.out) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };
    struct SinkKey {
        std::string type;
        ChannelType in;
        bool operator==(const SinkKey&) const = default;
    };
    struct SinkKeyHash {
        std::size_t operator()(const SinkKey& k) const noexcept {
            std::size_t h = std::hash<std::string>{}(k.type);
            h ^= std::hash<std::string>{}(k.in) + 0x9e3779b9 + (h << 6) + (h >> 2);
            return h;
        }
    };

    mutable std::mutex mu_;
    std::unordered_map<SourceKey, SourceFactory, SourceKeyHash> sources_;
    std::unordered_map<OpKey, OperatorFactory, OpKeyHash> operators_;
    std::unordered_map<SinkKey, SinkFactory, SinkKeyHash> sinks_;
    const OperatorRegistry* parent_{nullptr};
};

// SelectorRegistry: named record-routing functions used by split
// output groups. The selector returns the branch index for a record;
// records emitted via Dag::add_split go to that branch.
//
// Typed by channel-type to match the operator pipeline. The registry
// stores one map per type; lookups go through the right one.
class SelectorRegistry {
public:
    using Int64Selector = std::function<int(const std::int64_t&)>;
    using StringSelector = std::function<int(const std::string&)>;

    SelectorRegistry() = default;
    explicit SelectorRegistry(const SelectorRegistry* parent) : parent_(parent) {}

    void register_int64(std::string name, Int64Selector fn);
    void register_string(std::string name, StringSelector fn);

    // Returns nullptr if name isn't registered for that channel type.
    // Falls through to `parent` on miss.
    const Int64Selector* find_int64(const std::string& name) const;
    const StringSelector* find_string(const std::string& name) const;

    static SelectorRegistry& default_instance();

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Int64Selector> int64_;
    std::unordered_map<std::string, StringSelector> string_;
    const SelectorRegistry* parent_{nullptr};
};

// KeyExtractorRegistry holds named (T -> int64_t) functions used by
// RoutingMode::Hash to partition records across downstream subtasks.
// The returned int64_t is treated as the hash slot; the routing layer
// reduces it modulo the number of peers. Same name can be registered
// for multiple channel types - a CoOperator keyed by "user_id" might
// have one upstream emitting `User` and another emitting `Event`, and
// both register an extractor named "user_id" for their respective
// element type.
//
// Type-erased internally so plugin types (registered via
// PluginRegistry::register_key_extractor<T>) live in the same registry
// as built-in extractors. Lookup is templated; the caller static_casts
// back to the right typed shared_ptr.
class KeyExtractorRegistry {
public:
    KeyExtractorRegistry() = default;
    explicit KeyExtractorRegistry(const KeyExtractorRegistry* parent) : parent_(parent) {}

    template <typename T>
    void register_extractor(std::string channel_name,
                            std::string name,
                            std::function<std::int64_t(const T&)> fn) {
        auto typed = std::make_shared<std::function<std::int64_t(const T&)>>(std::move(fn));
        std::lock_guard lock(mu_);
        extractors_[std::make_pair(std::move(channel_name), std::move(name))] =
            std::static_pointer_cast<void>(typed);
    }

    template <typename T>
    std::function<std::int64_t(const T&)> find(const std::string& channel_name,
                                               const std::string& name) const {
        {
            std::lock_guard lock(mu_);
            auto it = extractors_.find(std::make_pair(channel_name, name));
            if (it != extractors_.end()) {
                auto typed =
                    std::static_pointer_cast<std::function<std::int64_t(const T&)>>(it->second);
                return *typed;
            }
        }
        return parent_ != nullptr ? parent_->template find<T>(channel_name, name)
                                  : std::function<std::int64_t(const T&)>{};
    }

    bool has(const std::string& channel_name, const std::string& name) const {
        {
            std::lock_guard lock(mu_);
            if (extractors_.find(std::make_pair(channel_name, name)) != extractors_.end()) {
                return true;
            }
        }
        return parent_ != nullptr && parent_->has(channel_name, name);
    }

    static KeyExtractorRegistry& default_instance();

private:
    mutable std::mutex mu_;
    std::map<std::pair<std::string, std::string>, std::shared_ptr<void>> extractors_;
    const KeyExtractorRegistry* parent_{nullptr};
};

// Convenience accessor for params with a default and basic int parsing.
inline std::string param_or(const OperatorBuildContext& ctx,
                            const std::string& key,
                            std::string fallback = {}) {
    auto it = ctx.params.find(key);
    return it == ctx.params.end() ? std::move(fallback) : it->second;
}

inline std::int64_t param_int64_or(const OperatorBuildContext& ctx,
                                   const std::string& key,
                                   std::int64_t fallback = 0) {
    auto it = ctx.params.find(key);
    if (it == ctx.params.end()) {
        return fallback;
    }
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

}  // namespace clink::cluster
