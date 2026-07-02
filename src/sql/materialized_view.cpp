#include "clink/sql/materialized_view.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

#include "clink/sql/binder.hpp"
#include "clink/sql/logical_plan.hpp"  // LogicalScan
#include "clink/sql/parser.hpp"        // TranslationError

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

// The refresh budget parsed from the FRESHNESS WITH-option. A zero / "continuous"
// budget keeps a live streaming maintenance job; a positive interval selects the
// full (scheduled / on-demand) arm that recomputes and atomically overwrites.
struct FreshnessSpec {
    bool continuous = true;
    std::int64_t interval_ms = 0;
};

FreshnessSpec parse_freshness(std::string_view f) {
    if (f.empty() || f == "0" || f == "0s" || f == "0ms" || f == "0m" || f == "continuous") {
        return {true, 0};
    }
    std::size_t i = 0;
    while (i < f.size() && std::isdigit(static_cast<unsigned char>(f[i])) != 0) {
        ++i;
    }
    if (i == 0) {
        throw TranslationError("freshness '" + std::string(f) +
                                   "': expected <number><unit> (ms/s/m/h/d) or '0'/'continuous'",
                               0);
    }
    const std::int64_t num = std::stoll(std::string(f.substr(0, i)));
    const std::string_view unit = f.substr(i);
    std::int64_t mult = 0;
    if (unit == "ms") {
        mult = 1;
    } else if (unit == "s") {
        mult = 1000;
    } else if (unit == "m") {
        mult = 60LL * 1000;
    } else if (unit == "h") {
        mult = 3600LL * 1000;
    } else if (unit == "d") {
        mult = 24LL * 3600 * 1000;
    } else {
        throw TranslationError("freshness '" + std::string(f) + "': unknown unit '" +
                                   std::string(unit) + "' (expected ms/s/m/h/d)",
                               0);
    }
    if (num <= 0) {
        return {true, 0};
    }
    return {false, num * mult};
}

// Source connectors that can be read as a bounded scan (so a full refresh can run to
// completion). An unbounded stream (kafka, CDC, poll) cannot back a full-refresh view
// - the recompute job would never finish - so it is rejected at plan time.
constexpr std::array<std::string_view, 5> kBoundedSourceConnectors = {
    "file", "filesystem", "parquet", "s3_parquet", "clickhouse"};

bool is_bounded_source(std::string_view connector) {
    return std::find(kBoundedSourceConnectors.begin(), kBoundedSourceConnectors.end(), connector) !=
           kBoundedSourceConnectors.end();
}

// Walk the bound defining plan; reject if any source scan reads an unbounded stream.
void reject_unbounded_sources(const LogicalPlan* p, const std::string& view, int pos) {
    if (p == nullptr) {
        return;
    }
    if (const auto* scan = dynamic_cast<const LogicalScan*>(p)) {
        const auto& t = scan->table();
        const auto it = t.properties.find("connector");
        const std::string connector = it != t.properties.end() ? it->second : "";
        if (!is_bounded_source(connector)) {
            throw TranslationError(
                "materialized view '" + view +
                    "': full (scheduled) refresh requires bounded-readable sources; source '" +
                    t.name + "' (connector='" + connector +
                    "') is not a bounded source. Use freshness='0' (continuous), or back it with a "
                    "bounded snapshot (file / parquet / clickhouse)",
                pos);
        }
    }
    for (const auto* in : p->inputs()) {
        reject_unbounded_sources(in, view, pos);
    }
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
    // FRESHNESS selects the arm: continuous (a live streaming maintenance job) or
    // full (recompute + atomic overwrite, driven by REFRESH / a scheduler).
    const FreshnessSpec fresh = parse_freshness(freshness);
    const RefreshArm arm = fresh.continuous ? RefreshArm::Continuous : RefreshArm::Full;

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
    // A full refresh runs a bounded recompute to completion, so every source must be
    // bounded-readable; reject an unbounded stream up front.
    if (arm == RefreshArm::Full) {
        reject_unbounded_sources(schema_plan.get(), stmt.view_name, stmt.loc.pos);
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
    if (arm == RefreshArm::Full) {
        // The full arm recomputes the whole result and atomically overwrites the
        // backing on each refresh; the write_mode drives the sink's staged swap.
        backing.properties["refresh_arm"] = "full";
        backing.properties["freshness_ms"] = std::to_string(fresh.interval_ms);
        backing.properties["write_mode"] = "overwrite";
    }

    // partition_by (bucketing): every partition column must exist in the view's
    // output. A full-refresh partitioned backing writes one file per partition value
    // and atomically publishes the whole partitioned set on each refresh.
    if (const auto pb = backing.properties.find("partition_by");
        pb != backing.properties.end() && !pb->second.empty()) {
        std::size_t start = 0;
        while (start <= pb->second.size()) {
            const auto comma = pb->second.find(',', start);
            const auto end = comma == std::string::npos ? pb->second.size() : comma;
            std::string col = pb->second.substr(start, end - start);
            const auto a = col.find_first_not_of(" \t");
            const auto b = col.find_last_not_of(" \t");
            if (a != std::string::npos) {
                const std::string name = col.substr(a, b - a + 1);
                if (out_schema->GetFieldByName(name) == nullptr) {
                    throw TranslationError("materialized view '" + stmt.view_name +
                                               "': partition_by column '" + name +
                                               "' is not in the view's output columns",
                                           stmt.loc.pos);
                }
            }
            if (comma == std::string::npos) {
                break;
            }
            start = comma + 1;
        }
    }

    // Auto-derive an upsert backing for a keyed aggregation when the user did
    // not state mode/primary_key. A keyed GROUP BY emits the latest aggregate
    // per key, so the backing keeps the current row per key rather than every
    // intermediate. This applies to BOTH arms:
    //  - continuous: the upsert sink nets the live changelog by primary key;
    //  - full-refresh: the bounded recompute's changelog is netted by primary key
    //    and the final relation is written atomically on flush. file_json_upsert_sink
    //    already writes its whole netted state to <path>.tmp and renames over <path>,
    //    so an upsert backing IS a full atomic overwrite - no separate sink is needed
    //    (the write_mode=overwrite set for the full arm is a no-op for that sink).
    // A global (ungrouped) aggregate has no natural key on either arm, so it is
    // rejected (add a GROUP BY, or declare an explicit mode='upsert' + primary_key for
    // a constant-key view). Other changelog-emitting shapes are not auto-derived; if
    // the user left an append backing for one, bind_insert's changelog/append guard
    // rejects it with a clear message.
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

    // A full-refresh keyed aggregate nets by primary key via the upsert sink, which
    // writes a single atomically-overwritten file. partition_overwrite_sink instead
    // writes every row for a partition with no primary-key netting, so a partitioned
    // aggregating full-refresh would materialise intermediate changelog rows rather
    // than the final relation. Reject the combination; per-partition upsert netting is
    // a follow-on.
    if (arm == RefreshArm::Full && spine_agg != nullptr && !spine_agg->group_keys().empty()) {
        const auto pb = backing.properties.find("partition_by");
        if (pb != backing.properties.end() && !pb->second.empty()) {
            throw TranslationError(
                "materialized view '" + stmt.view_name +
                    "': partition_by with an aggregating full-refresh query is not supported "
                    "(per-partition upsert netting is a follow-on); drop partition_by or the "
                    "aggregation",
                stmt.loc.pos);
        }
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

    return MaterializedViewPlan{std::move(backing), arm, std::move(maintenance)};
}

std::unique_ptr<LogicalPlan> plan_materialized_view_refresh(const std::string& view_name,
                                                            Catalog& catalog) {
    const TableDef* backing = catalog.get_table(view_name);
    if (backing == nullptr) {
        throw TranslationError("REFRESH: materialized view '" + view_name + "' does not exist", 0);
    }
    if (!backing->is_materialized_view()) {
        throw TranslationError("REFRESH: '" + view_name + "' is not a materialized view", 0);
    }
    const auto arm_it = backing->properties.find("refresh_arm");
    if (arm_it == backing->properties.end() || arm_it->second != "full") {
        throw TranslationError(
            "REFRESH: '" + view_name +
                "' is a continuous materialized view (kept live by its maintenance job); REFRESH "
                "applies to full-refresh views declared with freshness > 0",
            0);
    }
    const std::string def_sql = backing->definition_sql();
    if (def_sql.empty()) {
        throw TranslationError("REFRESH: '" + view_name + "' has no stored definition to recompute",
                               0);
    }
    // Re-parse the stored definition and locate the CREATE MATERIALIZED VIEW for this
    // view; its defining SELECT is recomputed as INSERT INTO <backing> (which already
    // carries write_mode=overwrite, so the sink atomically publishes on completion).
    auto script = parse(def_sql);
    for (auto& s : script.statements) {
        auto* mv = std::get_if<ast::CreateMaterializedViewStmt>(&s);
        if (mv != nullptr && mv->view_name == view_name) {
            ast::InsertStmt insert;
            insert.loc = mv->loc;
            insert.target = ast::TableRef{view_name, std::nullopt, std::nullopt, mv->loc};
            insert.select = std::move(mv->query);
            Binder binder(catalog);
            return binder.bind_insert(insert);
        }
    }
    throw TranslationError("REFRESH: could not locate the defining query for '" + view_name + "'",
                           0);
}

}  // namespace clink::sql
