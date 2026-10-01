#include "native/column_plan.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

std::string ColumnPlan::report() const {
    not_implemented("ColumnPlan::report");
}

PlanResult compile_column_plan(const std::vector<SqlColumn>& /*input*/,
                               const std::vector<TargetColumn>& /*target*/) {
    not_implemented("compile_column_plan");
}

ColumnPlan compile_or_refuse(const std::vector<SqlColumn>& /*input*/,
                             const std::vector<TargetColumn>& /*target*/,
                             const std::string& /*qualified_table*/) {
    not_implemented("compile_or_refuse");
}

std::vector<std::string> header_drift(const ColumnPlan& /*plan*/,
                                      const std::vector<HeaderColumn>& /*header*/) {
    not_implemented("header_drift");
}

}  // namespace clink::clickhouse::native
