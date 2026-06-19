#pragma once

#include <cstdint>
#include <string>
#include <type_traits>

#include "clink/cluster/type_registry.hpp"

namespace clink::api {

// Compile-time mapping from a C++ type to its registered channel-type
// name string. Built-in types have specializations baked in here so the
// user doesn't have to repeat them at every fluent call site.
// Plugin-defined types resolve at run time via TypeRegistry, which the
// plugin populates via PluginRegistry::register_type<T>(name, codec).
//
// The fallback queries the TypeRegistry singleton; if the type isn't
// registered we throw a precise error rather than silently using
// typeid().name() (which is mangled and useless across the wire).
template <typename T>
struct ChannelName {
    static std::string get() {
        const auto name =
            clink::cluster::TypeRegistry::default_instance().channel_for_typeid(typeid(T).name());
        if (name.empty()) {
            throw std::runtime_error(
                std::string{"ChannelName<T>: type "} + typeid(T).name() +
                " is not registered. Built-in: instantiate"
                " clink::cluster::ensure_built_ins_registered() before submit."
                " Plugin: load the plugin .so so its register_type<T>(\"name\", codec)"
                " call has run.");
        }
        return name;
    }
};

template <>
struct ChannelName<std::int64_t> {
    static std::string get() { return "int64"; }
};

template <>
struct ChannelName<std::string> {
    static std::string get() { return "string"; }
};

}  // namespace clink::api
