#include "native/errors.hpp"
#include "native/insert_transport.hpp"

namespace clink::clickhouse::native {

std::unique_ptr<InsertTransport> make_clickhouse_transport(const SinkOptions& /*options*/) {
    not_implemented("make_clickhouse_transport");
}

}  // namespace clink::clickhouse::native
