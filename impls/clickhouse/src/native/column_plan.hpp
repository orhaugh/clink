#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "native/types.hpp"

namespace clink::clickhouse::native {

// Where the sink's column list came from: the planner's declared SQL types, or
// the schema of a typed struct's batcher. It changes what the open report says
// about the input and how a refusal words its remedy.
enum class InputKind : std::uint8_t { SqlTable, TypedStruct };

enum class DefaultKind : std::uint8_t { None, Default, Materialized, Alias, Ephemeral };

struct TargetColumn {
    std::string name;
    std::string type;  // system.columns.type
    DefaultKind default_kind{DefaultKind::None};
    std::uint32_t position{0};
};

enum class Conversion : std::uint8_t {
    Copy,
    WidenInt,
    NarrowInt,
    SignedToUnsigned,
    RealToDouble,
    BoolUnpack,
    StringZeroCopy,
    StringToFixed,
    StringToEnum,
    StringToUuid,
    StringToIpv4,
    StringToIpv6,
    DecimalRescale,
    TimestampToDateTime64,
    TimestampToDateTime,
    DateToDate32,
    DateToDate,
    List,
    Map,
    Struct
};

struct ColumnBinding {
    // A child is named <parent>.element, <parent>.key, <parent>.value or
    // <parent>.<field>, so a conversion error can say where it failed.
    std::string name;
    int input_index{0};  // column index in the Arrow chunk; a child's position in its parent
    SqlType source;
    ChType target;
    Conversion conversion{Conversion::Copy};
    std::vector<ColumnBinding> children;  // List element; Map key, value; Struct elements
    // 10^(S - s) for decimals, at most 10^18 (the plan refuses a wider
    // rescale); 10^(P - d) for timestamps when P >= d.
    std::int64_t multiplier{1};
    std::int64_t divisor{1};  // 10^(d - P) for timestamps when P < d; exact or ConversionError
    bool zero_copy{false};
    std::string expected_header_type;  // client_header_spelling(target.spelling)
};

struct ColumnPlan {
    std::vector<ColumnBinding> columns;        // INSERT column-list order = input order
    std::vector<TargetColumn> omitted;         // left to the server
    bool retains_chunks{false};                // any zero_copy binding, at any depth
    std::string column_list_sql;               // (`a`, `b`)
    [[nodiscard]] std::string report() const;  // the subtask-0 full report
};

struct PlanProblem {
    std::string column;
    std::string message;
};

// Pairs the declared input columns with the target's columns and the type
// rules. Collects every problem rather than stopping at the first. `kind`
// picks the remedy a problem names: a change to the SELECT or the clink table
// for SqlTable, to the struct or its CLINK_FIELDS declaration for TypedStruct.
// The rules are the same for both.
struct PlanResult {
    std::optional<ColumnPlan> plan;
    std::vector<PlanProblem> problems;
};
[[nodiscard]] PlanResult compile_column_plan(const std::vector<SqlColumn>& input,
                                             const std::vector<TargetColumn>& target,
                                             InputKind kind = InputKind::SqlTable);

// compile_column_plan, or throw NativeSinkError(column_plan) whose message
// lists every problem, one per line, with the remediation for each.
[[nodiscard]] ColumnPlan compile_or_refuse(const std::vector<SqlColumn>& input,
                                           const std::vector<TargetColumn>& target,
                                           const std::string& qualified_table,
                                           InputKind kind = InputKind::SqlTable);

struct HeaderColumn {
    std::string name;
    std::string type;  // the client's spelling (Type()->GetName())
};

// Empty when the header BeginInsert returned matches the plan; otherwise one
// line per difference ("column `b`: plan Int64, server Nullable(Int64)").
[[nodiscard]] std::vector<std::string> header_drift(const ColumnPlan&,
                                                    const std::vector<HeaderColumn>&);

}  // namespace clink::clickhouse::native
