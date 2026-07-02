#pragma once

// SQL-native AI: the model-inference provider SPI + a registry of provider factories.
//
// A CREATE MODEL declares a model as a catalog object (name + INPUT/OUTPUT schema +
// provider WITH-options). ML_PREDICT then applies it per row. The ACTUAL inference is
// done by a ModelProvider - a small object built at operator-open time from the
// model's WITH-options. Providers are supplied by name: a built-in HTTP provider
// (impls/http_connector), a future ONNX provider (impls/onnx), or a user-registered
// C++ closure. The ml_predict_row operator looks the provider up by its `provider`
// WITH-option and calls predict() once per input row.
//
// v1 is synchronous: predict() maps a features Row to an outputs Row. Inference is a
// blocking call on the operator thread (so throughput is one inference at a time); a
// concurrent / async provider is a documented follow-on and would extend this SPI
// with an async entry point rather than change it.
//
// Resolve the registry through global() ONLY - RTLD_LOCAL + a static clink_core gives
// each plugin .so a private singleton, so a default-constructed local registry would
// not see host registrations (the same rule as PtfRegistry / AsyncFunctionRegistry).

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/sql/row.hpp"

namespace clink::sql {

// Inference over one feature row -> one prediction row. The features Row is keyed by
// the DESCRIPTOR feature-column names; the returned Row must carry the model's OUTPUT
// columns (the operator merges them into the input row). The provider knows its OUTPUT
// column names via the "output_columns" option the operator injects at construction.
class ModelProvider {
public:
    virtual ~ModelProvider() = default;
    virtual Row predict(const Row& features) = 0;
    [[nodiscard]] virtual std::string name() const { return "model_provider"; }

    // Async inference. A provider whose inference is a slow I/O call (an HTTP endpoint)
    // overrides is_async() to return true; the ml_predict_row factory then drives it on
    // the AsyncLookupOperator, running predict() on a thread pool with many inferences in
    // flight at once, instead of the one-at-a-time sync flatmap. CPU-bound providers
    // (local ONNX) stay synchronous. The default is a synchronous provider.
    //
    // CONTRACT: when is_async() is true, predict() MUST be safe to call concurrently from
    // multiple threads (the pool fans it out). A provider with per-call mutable state (an
    // HTTP client holding one socket) must therefore build that state per call. The
    // async operator owns the pool and the nudge-safe polling coroutine, so the provider
    // exposes only the plain synchronous predict().
    [[nodiscard]] virtual bool is_async() const { return false; }
};

// A ModelProvider backed by a std::function - the "closure SPI". Lets a user (or a
// test) register a deterministic in-process model without a bespoke class.
class ClosureModelProvider final : public ModelProvider {
public:
    ClosureModelProvider(std::string name, std::function<Row(const Row&)> fn)
        : name_(std::move(name)), fn_(std::move(fn)) {}
    Row predict(const Row& features) override { return fn_(features); }
    [[nodiscard]] std::string name() const override { return name_; }

private:
    std::string name_;
    std::function<Row(const Row&)> fn_;
};

inline std::shared_ptr<ModelProvider> make_closure_provider(std::string name,
                                                            std::function<Row(const Row&)> fn) {
    return std::make_shared<ClosureModelProvider>(std::move(name), std::move(fn));
}

// An async ModelProvider backed by a std::function: is_async() is true, so the async
// operator drives predict() on its thread pool with many closures in flight at once. The
// closure must be safe to call concurrently (a pure function is). Used to exercise the
// async ML_PREDICT path in tests, and as the minimal template for a real async provider.
class AsyncClosureModelProvider final : public ModelProvider {
public:
    AsyncClosureModelProvider(std::string name, std::function<Row(const Row&)> fn)
        : name_(std::move(name)), fn_(std::move(fn)) {}

    Row predict(const Row& features) override { return fn_(features); }
    [[nodiscard]] bool is_async() const override { return true; }
    [[nodiscard]] std::string name() const override { return name_; }

private:
    std::string name_;
    std::function<Row(const Row&)> fn_;
};

inline std::shared_ptr<ModelProvider> make_async_closure_provider(
    std::string name, std::function<Row(const Row&)> fn) {
    return std::make_shared<AsyncClosureModelProvider>(std::move(name), std::move(fn));
}

// Registry of provider factories keyed by the `provider` WITH-option value (e.g.
// "http", "onnx", or a user name). A factory builds a fresh ModelProvider from the
// model's options (provider / endpoint / task / ... plus the operator-injected
// feature_columns / output_columns), so per-model config is not shared mutable state.
class ModelProviderRegistry {
public:
    using Factory =
        std::function<std::shared_ptr<ModelProvider>(const std::map<std::string, std::string>&)>;

    void register_provider(std::string name, Factory factory) {
        if (name.empty()) {
            throw std::runtime_error("ModelProviderRegistry: empty provider name");
        }
        if (!factory) {
            throw std::runtime_error("ModelProviderRegistry: null factory for '" + name + "'");
        }
        std::lock_guard<std::mutex> lk(mu_);
        providers_[std::move(name)] = std::move(factory);
    }

    [[nodiscard]] bool contains(const std::string& name) const {
        std::lock_guard<std::mutex> lk(mu_);
        return providers_.find(name) != providers_.end();
    }

    // Build a provider for `name` from `opts`. Throws if the provider is unregistered
    // (e.g. its impl is not linked) - a clear operator-build error.
    [[nodiscard]] std::shared_ptr<ModelProvider> create(
        const std::string& name, const std::map<std::string, std::string>& opts) const {
        Factory factory;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = providers_.find(name);
            if (it == providers_.end()) {
                throw std::runtime_error(
                    "ML_PREDICT: model provider '" + name +
                    "' is not registered (is its implementation linked / installed?)");
            }
            factory = it->second;
        }
        return factory(opts);
    }

    [[nodiscard]] std::vector<std::string> names() const {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<std::string> out;
        out.reserve(providers_.size());
        for (const auto& [n, _] : providers_) {
            out.push_back(n);
        }
        return out;
    }

    static ModelProviderRegistry& global() {
        static ModelProviderRegistry instance;
        return instance;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Factory> providers_;
};

}  // namespace clink::sql
