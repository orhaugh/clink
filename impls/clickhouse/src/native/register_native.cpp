#include "native/register_native.hpp"

#include <memory>
#include <string>

#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/row.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

void register_native(clink::plugin::PluginRegistry& registry) {
    registry.register_sink<sql::Row>(
        "clickhouse_native_sink",
        [](const clink::plugin::BuildContext& /*ctx*/) -> std::shared_ptr<Sink<sql::Row>> {
            throw NativeSinkError(code::kNativeUnavailable,
                                  "clickhouse_native_sink is not available in this build");
        });
}

}  // namespace clink::clickhouse::native
