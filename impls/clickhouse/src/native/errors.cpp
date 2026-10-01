#include "native/errors.hpp"

#include <utility>

namespace clink::clickhouse::native {

NativeSinkError::NativeSinkError(std::string code, const std::string& message)
    : std::runtime_error("[" + code + "] " + message), code_(std::move(code)) {}

ConversionError::ConversionError(std::string column, std::int64_t row, const std::string& reason)
    : NativeSinkError(code::kConversionFailed,
                      "column `" + column + "`, row " + std::to_string(row) + ": " + reason),
      column_(std::move(column)),
      row_(row) {}

void not_implemented(const char* what) {
    throw std::logic_error(std::string("clickhouse native sink: ") + what + " is not implemented");
}

}  // namespace clink::clickhouse::native
