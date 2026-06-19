#include "clink/cluster/runner_registry.hpp"

#include <utility>

namespace clink::cluster {

void RunnerRegistry::register_source(std::string op_type,
                                     std::string out_channel,
                                     SubtaskRunner runner) {
    std::lock_guard lock(mu_);
    sources_[SourceKey{std::move(op_type), std::move(out_channel)}] = std::move(runner);
}

void RunnerRegistry::register_operator(std::string op_type,
                                       std::string in_channel,
                                       std::string out_channel,
                                       SubtaskRunner runner) {
    std::lock_guard lock(mu_);
    operators_[OpKey{std::move(op_type), std::move(in_channel), std::move(out_channel)}] =
        std::move(runner);
}

void RunnerRegistry::register_sink(std::string op_type,
                                   std::string in_channel,
                                   SubtaskRunner runner) {
    std::lock_guard lock(mu_);
    sinks_[SinkKey{std::move(op_type), std::move(in_channel)}] = std::move(runner);
}

void RunnerRegistry::register_join(std::string op_type,
                                   std::string in1_channel,
                                   std::string in2_channel,
                                   std::string out_channel,
                                   SubtaskRunner runner) {
    std::lock_guard lock(mu_);
    joins_[JoinKey{std::move(op_type),
                   std::move(in1_channel),
                   std::move(in2_channel),
                   std::move(out_channel)}] = std::move(runner);
}

const SubtaskRunner* RunnerRegistry::find_source(const std::string& op_type,
                                                 const std::string& out_channel) const {
    {
        std::lock_guard lock(mu_);
        auto it = sources_.find(SourceKey{op_type, out_channel});
        if (it != sources_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_source(op_type, out_channel) : nullptr;
}

const SubtaskRunner* RunnerRegistry::find_operator(const std::string& op_type,
                                                   const std::string& in_channel,
                                                   const std::string& out_channel) const {
    {
        std::lock_guard lock(mu_);
        auto it = operators_.find(OpKey{op_type, in_channel, out_channel});
        if (it != operators_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_operator(op_type, in_channel, out_channel) : nullptr;
}

const SubtaskRunner* RunnerRegistry::find_sink(const std::string& op_type,
                                               const std::string& in_channel) const {
    {
        std::lock_guard lock(mu_);
        auto it = sinks_.find(SinkKey{op_type, in_channel});
        if (it != sinks_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_sink(op_type, in_channel) : nullptr;
}

const SubtaskRunner* RunnerRegistry::find_join(const std::string& op_type,
                                               const std::string& in1_channel,
                                               const std::string& in2_channel,
                                               const std::string& out_channel) const {
    {
        std::lock_guard lock(mu_);
        auto it = joins_.find(JoinKey{op_type, in1_channel, in2_channel, out_channel});
        if (it != joins_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find_join(op_type, in1_channel, in2_channel, out_channel)
                              : nullptr;
}

void RunnerRegistry::register_co_operator(std::string op_type,
                                          std::string in1_channel,
                                          std::string in2_channel,
                                          std::string out_channel,
                                          SubtaskRunner runner) {
    // Canonicalize (in1, in2) ordering: register under sorted form so
    // lookups are order-independent. The user's runner closure already
    // partitions in_bridges by channel_type, so swapping (in1, in2) in
    // the key doesn't change which template parameter each bridge ends
    // up bound to - that's still determined by the closure's captured
    // (in1_channel, in2_channel). The only thing the key controls is
    // whether find_co_operator can locate the registration.
    if (in2_channel < in1_channel) {
        std::swap(in1_channel, in2_channel);
    }
    std::lock_guard lock(mu_);
    co_operators_[JoinKey{std::move(op_type),
                          std::move(in1_channel),
                          std::move(in2_channel),
                          std::move(out_channel)}] = std::move(runner);
}

const SubtaskRunner* RunnerRegistry::find_co_operator(const std::string& op_type,
                                                      const std::string& in1_channel,
                                                      const std::string& in2_channel,
                                                      const std::string& out_channel) const {
    // Canonicalize (in1, in2) to match the sort applied at registration.
    const std::string& a = in1_channel < in2_channel ? in1_channel : in2_channel;
    const std::string& b = in1_channel < in2_channel ? in2_channel : in1_channel;
    {
        std::lock_guard lock(mu_);
        auto it = co_operators_.find(JoinKey{op_type, a, b, out_channel});
        if (it != co_operators_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr
               ? parent_->find_co_operator(op_type, in1_channel, in2_channel, out_channel)
               : nullptr;
}

bool RunnerRegistry::has_join_for_type(const std::string& op_type) const {
    {
        std::lock_guard lock(mu_);
        for (const auto& [k, _v] : joins_) {
            if (k.type == op_type) {
                return true;
            }
        }
    }
    return parent_ != nullptr && parent_->has_join_for_type(op_type);
}

bool RunnerRegistry::has_co_operator_for_type(const std::string& op_type) const {
    {
        std::lock_guard lock(mu_);
        for (const auto& [k, _v] : co_operators_) {
            if (k.type == op_type) {
                return true;
            }
        }
    }
    return parent_ != nullptr && parent_->has_co_operator_for_type(op_type);
}

RunnerRegistry& RunnerRegistry::default_instance() {
    static RunnerRegistry r;
    return r;
}

}  // namespace clink::cluster
