#include "native/types.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

std::vector<SqlColumn> parse_sql_column_types(const std::string& /*spec*/) {
    not_implemented("parse_sql_column_types");
}

std::shared_ptr<arrow::DataType> arrow_type_for(const SqlType& /*type*/) {
    not_implemented("arrow_type_for");
}

ChType parse_ch_type(const std::string& /*spelling*/) {
    not_implemented("parse_ch_type");
}

std::string client_header_spelling(const std::string& /*spelling*/) {
    not_implemented("client_header_spelling");
}

}  // namespace clink::clickhouse::native
