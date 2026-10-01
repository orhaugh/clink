#include "native/insert_transport.hpp"

#include <mutex>
#include <utility>

namespace clink::clickhouse::native {

namespace {

std::mutex& factory_mutex() {
    static std::mutex mu;
    return mu;
}

TransportFactory& factory_override() {
    static TransportFactory factory;
    return factory;
}

}  // namespace

void set_transport_factory_for_testing(TransportFactory factory) {
    const std::lock_guard<std::mutex> lock(factory_mutex());
    factory_override() = std::move(factory);
}

TransportFactory current_transport_factory() {
    const std::lock_guard<std::mutex> lock(factory_mutex());
    if (factory_override()) {
        return factory_override();
    }
    return [](const SinkOptions& options) { return make_clickhouse_transport(options); };
}

}  // namespace clink::clickhouse::native
