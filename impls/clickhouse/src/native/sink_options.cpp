#include "native/sink_options.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

SinkOptions parse_sink_options(const std::map<std::string, std::string>& /*params*/,
                               std::uint32_t /*subtask_idx*/,
                               std::uint32_t /*parallelism*/) {
    not_implemented("parse_sink_options");
}

const std::vector<std::string>& own_option_keys() {
    not_implemented("own_option_keys");
}

const std::vector<std::string>& pass_through_keys() {
    not_implemented("pass_through_keys");
}

std::string describe(const SinkOptions& /*options*/) {
    not_implemented("describe");
}

}  // namespace clink::clickhouse::native
