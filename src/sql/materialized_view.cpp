#include "clink/sql/materialized_view.hpp"

#include <algorithm>
#include <array>
#include <string>
#include <string_view>

#include "clink/sql/binder.hpp"
#include "clink/sql/parser.hpp"  // TranslationError

namespace clink::sql {

namespace {

// Connectors that appear in BOTH the source and sink factory tables (so one
// table can be written by the maintenance job and read by referencing
// queries). postgres is source-only, s3 sink-only, lookup scan-only.
constexpr std::array<std::string_view, 6> kBothCapableConnectors = {
    "file", "filesystem", "kafka", "clickhouse", "parquet", "s3_parquet"};

bool is_both_capable(std::string_view connector) {
    return std::find(kBothCapableConnectors.begin(), kBothCapableConnectors.end(), connector) !=
           kBothCapableConnectors.end();
}

// v1 honours only continuous (streaming) freshness. These spellings all mean
// "keep the view live with a continuously running maintenance job"; any other
// value names a relaxed-refresh budget the scheduled-batch arm does not yet
// implement.
bool is_continuous_freshness(std::string_view f) {
    return f.empty() || f == "0" || f == "0s" || f == "0ms" || f == "0m" || f == "continuous";
}

// Walk the plan spine (root, then first input, ...) looking for a non-windowed
// keyed aggregation. Such a plan emits a changelog (the latest aggregate per
// key), so its backing store must be upsert keyed on the group columns to read
// back the current value rather than every intermediate. A windowed aggregate
// (LogicalWindowAggregate) is append-only and is deliberately not matched.
const LogicalAggregate* find_spine_aggregate(const LogicalPlan* p) {
    while (p != nullptr) {
        if (const auto* agg = dynamic_cast<const LogicalAggregate*>(p)) {
            return agg;
        }
        auto ins = p->inputs();
        if (ins.empty()) {
            break;
        }
        p = ins[0];
    }
    return nullptr;
}

std::string join_csv(const std::vector<std::string>& parts) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out += ',';
        }
        out += parts[i];
    }
    return out;
}

}  // namespace

MaterializedViewPlan plan_materialized_view(ast::CreateMaterializedViewStmt stmt,
                                            Catalog& catalog,
                                            std::string_view definition_sql) {
    // Gather the WITH options up front; connector + freshness gate v1.
    std::string connector;
    std::string freshness;
    bool user_set_mode = false;
    bool user_set_pk = false;
    for (const auto& opt : stmt.options) {
        if (opt.key == "connector") {
            connector = opt.value;
        } else if (opt.key == "freshness") {
            freshness = opt.value;
        } else if (opt.key == "mode") {
            user_set_mode = true;
        } else if (opt.key == "primary_key") {
            user_set_pk = true;
        }
    }

    if (connector.empty()) {
        throw TranslationError("materialized view '" + stmt.view_name +
                                   "': a connector is required (e.g. connector='file')",
                               stmt.loc.pos);
    }
    if (!is_both_capable(connector)) {
        throw TranslationError(
            "materialized view '" + stmt.view_name + "': connector='" + connector +
                "' cannot back a view (it is not both readable and writable); use one of "
                "file, kafka, clickhouse, parquet, s3_parquet",
            stmt.loc.pos);
    }
    if (!is_continuous_freshness(freshness)) {
        throw TranslationError(
            "materialized view '" + stmt.view_name + "': freshness='" + freshness +
                "' requires scheduled refresh, which is not yet supported; use freshness='0' "
                "(continuous) in v1",
            stmt.loc.pos);
    }

    // Bind the defining query once to derive the output schema and detect a
    // keyed aggregation. A fresh binder keeps any WITH-clause state isolated
    // from the maintenance bind below.
    Binder schema_binder(catalog);
    auto schema_plan = schema_binder.bind_select(stmt.query);
    auto out_schema = schema_plan->schema();
    if (!out_schema || out_schema->num_fields() == 0) {
        throw TranslationError(
            "materialized view '" + stmt.view_name + "': defining query has no output columns",
            stmt.loc.pos);
    }
    const LogicalAggregate* spine_agg = find_spine_aggregate(schema_plan.get());

    // Build the backing TableDef: columns from the query schema, properties
    // from the WITH options plus the MV discriminator metadata.
    TableDef backing;
    backing.name = stmt.view_name;
    backing.columns.reserve(static_cast<std::size_t>(out_schema->num_fields()));
    for (const auto& field : out_schema->fields()) {
        backing.columns.push_back(ColumnSpec{field->name(), field->type()});
    }
    for (const auto& opt : stmt.options) {
        backing.properties[opt.key] = opt.value;  // connector/format/mode/primary_key/freshness/...
    }
    backing.properties["view_kind"] = "materialized";
    if (!definition_sql.empty()) {
        backing.properties["definition_sql"] = std::string(definition_sql);
    }
    if (backing.properties.find("freshness") == backing.properties.end()) {
        backing.properties["freshness"] = "0";  // default continuous
    }

    // Auto-derive an upsert backing for a keyed aggregation when the user did
    // not state mode/primary_key. A keyed GROUP BY emits the latest aggregate
    // per key, so the backing keeps the current row per key rather than every
    // intermediate. Other changelog-emitting shapes are not auto-derived; if
    // the user left an append backing for one, bind_insert's changelog/append
    // compatibility check rejects it with a clear message.
    if (!user_set_mode && spine_agg != nullptr && !spine_agg->group_keys().empty()) {
        backing.properties["mode"] = "upsert";
        if (!user_set_pk) {
            backing.properties["primary_key"] = join_csv(spine_agg->group_keys());
        }
    } else if (!user_set_mode && spine_agg != nullptr && spine_agg->group_keys().empty()) {
        // A global (ungrouped) aggregate emits a changelog with no natural key,
        // so an append backing would silently accumulate every intermediate
        // value rather than the current one. Reject rather than materialise a
        // misleading view; the user can add a GROUP BY or declare an explicit
        // mode='upsert' + primary_key for a constant-key view.
        throw TranslationError(
            "materialized view '" + stmt.view_name +
                "': a global (ungrouped) aggregate has no key to materialise; add a GROUP BY, "
                "or declare mode='upsert' with a primary_key",
            stmt.loc.pos);
    }

    // Lift the primary_key property into the typed field. register_table(TableDef)
    // does not run the CREATE TABLE lift path, and bind_insert checks the typed
    // primary_key vector (not the property) for upsert sinks.
    if (auto pk = backing.properties.find("primary_key"); pk != backing.properties.end()) {
        backing.primary_key.clear();
        std::size_t start = 0;
        while (start <= pk->second.size()) {
            const auto comma = pk->second.find(',', start);
            const auto end = comma == std::string::npos ? pk->second.size() : comma;
            std::string col = pk->second.substr(start, end - start);
            const auto l = col.find_first_not_of(" \t");
            const auto r = col.find_last_not_of(" \t");
            if (l != std::string::npos) {
                backing.primary_key.push_back(col.substr(l, r - l + 1));
            }
            if (comma == std::string::npos) {
                break;
            }
            start = comma + 1;
        }
    }

    // Register the backing table before binding the maintenance INSERT (which
    // resolves its target from the catalog). Throws on a name collision.
    catalog.register_table(backing);

    // Synthesise INSERT INTO <view> <defining SELECT> and bind it to the
    // maintenance LogicalSink. This reuses every sink-compatibility and
    // changelog/append check bind_insert already enforces.
    ast::InsertStmt insert;
    insert.loc = stmt.loc;
    insert.target = ast::TableRef{stmt.view_name, std::nullopt, std::nullopt, stmt.loc};
    insert.select = std::move(stmt.query);

    Binder maintenance_binder(catalog);
    auto maintenance = maintenance_binder.bind_insert(insert);

    return MaterializedViewPlan{std::move(backing), std::move(maintenance)};
}

}  // namespace clink::sql
