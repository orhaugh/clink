#include "native/target_table.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

TargetInfo probe_target(InsertTransport& /*transport*/, const SinkOptions& /*opts*/) {
    not_implemented("probe_target");
}

std::optional<std::string> engine_full_setting(std::string_view /*engine_full*/,
                                               std::string_view /*name*/) {
    not_implemented("engine_full_setting");
}

std::optional<DistributedTarget> parse_distributed(std::string_view /*engine_full*/) {
    not_implemented("parse_distributed");
}

EngineFamily engine_family(std::string_view /*engine*/) {
    not_implemented("engine_family");
}

bool is_tested_line(std::uint64_t major, std::uint64_t minor) noexcept {
    return major == 26 && (minor == 3 || minor == 8);
}

}  // namespace clink::clickhouse::native
