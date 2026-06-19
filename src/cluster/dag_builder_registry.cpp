#include "clink/cluster/dag_builder_registry.hpp"

namespace clink::cluster {

void DagBuilderRegistry::register_builder(std::string op_type, DagBuilder fn) {
    std::lock_guard lock(mu_);
    by_op_type_[std::move(op_type)] = std::move(fn);
}

const DagBuilder* DagBuilderRegistry::find(const std::string& op_type) const {
    {
        std::lock_guard lock(mu_);
        auto it = by_op_type_.find(op_type);
        if (it != by_op_type_.end()) {
            return &it->second;
        }
    }
    if (parent_ != nullptr) {
        return parent_->find(op_type);
    }
    return nullptr;
}

DagBuilderRegistry& DagBuilderRegistry::default_instance() {
    static DagBuilderRegistry instance;
    return instance;
}

}  // namespace clink::cluster
