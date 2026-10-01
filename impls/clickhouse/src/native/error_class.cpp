#include "native/error_class.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

Failure to_failure(std::exception_ptr /*e*/, Phase /*phase*/, bool /*after_send*/) {
    not_implemented("to_failure");
}

FailureClass classify(const Failure& /*f*/, bool /*quorum_inserts*/) {
    not_implemented("classify");
}

const char* to_string(FailureClass /*c*/) {
    not_implemented("to_string(FailureClass)");
}

}  // namespace clink::clickhouse::native
