#include "clink/cluster/type_registry.hpp"

namespace clink::cluster {

const TypeOps* TypeRegistry::find(const std::string& channel_name) const {
    {
        std::lock_guard lock(mu_);
        auto it = by_channel_.find(channel_name);
        if (it != by_channel_.end()) {
            return &it->second;
        }
    }
    return parent_ != nullptr ? parent_->find(channel_name) : nullptr;
}

std::string TypeRegistry::channel_for_typeid(const std::string& typeid_name) const {
    {
        std::lock_guard lock(mu_);
        auto it = typeid_to_channel_.find(typeid_name);
        if (it != typeid_to_channel_.end()) {
            return it->second;
        }
    }
    return parent_ != nullptr ? parent_->channel_for_typeid(typeid_name) : std::string{};
}

TypeRegistry& TypeRegistry::default_instance() {
    static TypeRegistry r;
    return r;
}

}  // namespace clink::cluster
