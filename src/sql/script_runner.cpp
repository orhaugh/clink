#include "clink/sql/script_runner.hpp"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <utility>
#include <variant>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/base64.hpp"
#include "clink/http/http_client.hpp"
#include "clink/operators/agg_function_registry.hpp"
#include "clink/operators/scalar_function_registry.hpp"
#include "clink/operators/udf_language_registry.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/analyze.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/cardinality.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/materialized_view.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"
#include "clink/sql/type.hpp"
#include "clink/sql/view.hpp"

namespace clink::sql {

namespace {

// Percent-encode a query-param value (RFC 3986 unreserved chars pass through).
// A state-backend URI can carry its own "://" and even a "?a=1&b=2" query, so
// it must be encoded to survive the JM's query parsing; the server decodes it.
std::string url_encode(const std::string& s) {
    static const char* kHex = "0123456789ABCDEF";
    std::string out;
    out.reserve(s.size() * 3);
    for (char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (std::isalnum(c) != 0 || c == '-' || c == '_' || c == '.' || c == '~') {
            out += ch;
        } else {
            out += '%';
            out += kHex[c >> 4];
            out += kHex[c & 0x0FU];
        }
    }
    return out;
}

}  // namespace

SubmitFn make_http_submit(std::string jm_host,
                          std::uint16_t jm_port,
                          std::string state_backend_uri,
                          std::ostream& out,
                          std::ostream& err) {
    return [jm_host = std::move(jm_host),
            jm_port,
            state_backend_uri = std::move(state_backend_uri),
            &out,
            &err](const cluster::JobGraphSpec& spec, const std::string& name) -> int {
        clink::http::HttpClient client(jm_host, jm_port);
        std::string path = "/api/v1/jobs/spec";
        char sep = '?';
        auto add_param = [&](const std::string& key, const std::string& value) {
            if (value.empty()) {
                return;
            }
            path += sep;
            path += key + "=" + url_encode(value);
            sep = '&';
        };
        add_param("name", name);
        add_param("state_backend", state_backend_uri);
        auto resp = client.post(path, spec.to_json());
        if (resp.status == 0) {
            err << "error: HTTP transport failure: " << resp.error << "\n";
            return 1;
        }
        out << resp.body << "\n";
        if (resp.status != 200) {
            err << "error: JM returned status " << resp.status << "\n";
            return 1;
        }
        return 0;
    };
}

SubmitFn make_print_submit(std::ostream& out) {
    return [&out](const cluster::JobGraphSpec& spec, const std::string& /*name*/) -> int {
        out << spec.to_json() << "\n";
        return 0;
    };
}

int run_script(const std::string& sql,
               Catalog& catalog,
               const ScriptRunOptions& opts,
               const ScriptIO& io,
               const SubmitFn& submit) {
    auto& out = *io.out;
    auto& err = *io.err;
    Binder binder(catalog);
    PhysicalPlanner planner;

    try {
        auto script = parse(sql);
        bool produced_spec = false;
        int stdout_table_seq = 0;
        // Lazily register the SQL operator/source factories into the host
        // registry (default_instance) so ANALYZE's in-process scan can build the
        // table's Row source. Once per process; the registrations outlive the
        // temporary PluginRegistry (which binds to OperatorRegistry::default).
        auto ensure_local_ops_installed = [installed = false]() mutable {
            if (installed) {
                return;
            }
            installed = true;
            clink::cluster::ensure_built_ins_registered();
            clink::plugin::PluginRegistry reg;
            clink::sql::install(reg);
        };
        // Fan the compiled plan out to `opts.parallelism` subtasks per op. The
        // planner emits everything at parallelism 1; setting it uniformly here
        // lets the runtime hash-partition keyed ops by key and split Kafka
        // partitions across source subtasks. Assumes a parallelizable plan (no
        // forced-singleton op); the common SQL shapes qualify.
        auto apply_parallelism = [&](cluster::JobGraphSpec& spec) {
            if (opts.parallelism <= 1) {
                return;
            }
            for (auto& op : spec.ops) {
                op.parallelism = opts.parallelism;
            }
        };
        // SQL-declared UDFs live in the catalog (CREATE FUNCTION registers
        // them there, persisted when the catalog has a dir). A registration
        // is process-local, so a job submitted to a remote cluster carries
        // the declarations it references (module payload included) and
        // every TaskManager registers them at deploy. Referenced = the
        // function name appears in an op param (lowered expressions embed
        // the call as {"op":"<name>",...}); a substring match
        // over-approximates, which only ships an unused declaration, never
        // misses a used one.
        auto attach_udfs = [&](cluster::JobGraphSpec& spec) {
            for (const auto& fname : catalog.list_functions()) {
                const auto* f = catalog.get_function(fname);
                // Shippable = a packaged module (wasm), or a language whose
                // definitions ARE the implementation (sql expression bodies).
                if (f == nullptr || (f->module_b64.empty() && f->language != "sql")) {
                    continue;
                }
                bool referenced = false;
                for (const auto& op : spec.ops) {
                    for (const auto& [key, value] : op.params) {
                        if (value.find(f->name) != std::string::npos) {
                            referenced = true;
                            break;
                        }
                    }
                    if (referenced) {
                        break;
                    }
                }
                if (referenced) {
                    spec.udfs.push_back(cluster::UdfSpec{f->name,
                                                         f->language,
                                                         f->arg_names,
                                                         f->arg_types,
                                                         f->return_type,
                                                         f->definitions,
                                                         f->module_b64,
                                                         f->kind});
                }
            }
        };
        // Persisted CREATE FUNCTION declarations: re-register any this
        // process does not know yet (a fresh process, a JM after failover)
        // so binding and evaluation resolve them without a re-CREATE. A
        // failure warns rather than fails the script: a function the script
        // never calls must not block it (e.g. its language impl is not
        // linked here); using it then errors as an unknown function.
        for (const auto& fname : catalog.list_functions()) {
            const auto* f = catalog.get_function(fname);
            if (f == nullptr) {
                continue;
            }
            const bool is_aggregate = f->kind == "aggregate";
            const bool already = is_aggregate ? AggFunctionRegistry::global().contains(f->name)
                                              : ScalarFunctionRegistry::global().contains(f->name);
            if (already) {
                continue;
            }
            try {
                UdfLanguageRegistry::FunctionDecl decl;
                decl.name = f->name;
                decl.arg_names = f->arg_names;
                decl.is_aggregate = is_aggregate;
                decl.arg_types.reserve(f->arg_types.size());
                for (const auto& t : f->arg_types) {
                    decl.arg_types.push_back(udf_type_from_wire_name(t));
                }
                if (!f->return_type.empty()) {
                    decl.return_type = udf_type_from_wire_name(f->return_type);
                }
                decl.definitions = f->definitions;
                if (!f->module_b64.empty()) {
                    const auto bytes = clink::base64_decode(f->module_b64);
                    if (!bytes) {
                        throw std::runtime_error("invalid persisted module payload");
                    }
                    decl.module_bytes.assign(bytes->begin(), bytes->end());
                }
                UdfLanguageRegistry::global().load(f->language, decl);
            } catch (const std::exception& e) {
                err << "warning: persisted function '" << fname
                    << "' was not re-registered: " << e.what() << "\n";
            }
        }
        // Bind + optimize + compile an INSERT, then submit (or explain).
        auto handle_insert = [&](const ast::InsertStmt& ins) -> int {
            auto plan = binder.bind_insert(ins);
            plan = optimize(std::move(plan));
            if (opts.explain) {
                out << explain_with_estimates(*plan);
                return 0;
            }
            const auto& sink = static_cast<const LogicalSink&>(*plan);
            auto spec = planner.compile(sink);
            apply_parallelism(spec);
            attach_udfs(spec);
            return submit(spec, opts.job_name);
        };
        for (auto& stmt : script.statements) {
            if (std::holds_alternative<std::unique_ptr<ast::ExplainStmt>>(stmt)) {
                const auto& exp = *std::get<std::unique_ptr<ast::ExplainStmt>>(stmt);
                if (std::holds_alternative<ast::InsertStmt>(exp.query)) {
                    // Optimize before rendering so EXPLAIN shows the plan that
                    // would actually run (join reorders, pushdowns applied).
                    auto plan = optimize(binder.bind_insert(std::get<ast::InsertStmt>(exp.query)));
                    out << explain_with_estimates(*plan);
                } else if (std::holds_alternative<ast::SelectStmt>(exp.query)) {
                    auto plan = optimize(binder.bind_select(std::get<ast::SelectStmt>(exp.query)));
                    out << explain_with_estimates(*plan);
                } else {
                    err << "error: EXPLAIN only supports SELECT / INSERT INTO\n";
                    return 1;
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<ast::CreateTableStmt>(stmt)) {
                catalog.register_table(std::get<ast::CreateTableStmt>(stmt));
                continue;
            }
            if (std::holds_alternative<ast::CreateModelStmt>(stmt)) {
                // SQL-native AI: CREATE MODEL is pure catalog registration (no job).
                // ML_PREDICT reads the model's OUTPUT columns + provider properties
                // from the catalog when it binds.
                catalog.register_model(std::get<ast::CreateModelStmt>(stmt));
                continue;
            }
            if (std::holds_alternative<ast::CreateViewStmt>(stmt)) {
                // A logical view is pure catalog registration (no storage, no
                // job): bind the defining query for its columns and store it; a
                // reference to the view is expanded inline at bind time.
                register_view(catalog, std::move(std::get<ast::CreateViewStmt>(stmt)));
                continue;
            }
            if (std::holds_alternative<ast::CreateFunctionStmt>(stmt)) {
                // Scalar UDF declaration: resolve the LANGUAGE's installed
                // loader (e.g. 'wasm' from clink::wasm) and hand it the
                // declared signature + AS definitions; the loader validates
                // and registers the runnable closure into
                // ScalarFunctionRegistry, where the binder and evaluator
                // already find it. No job is submitted.
                const auto& cf = std::get<ast::CreateFunctionStmt>(stmt);
                const char* noun = cf.is_aggregate ? "aggregate" : "function";
                try {
                    UdfLanguageRegistry::FunctionDecl decl;
                    decl.name = cf.function_name;
                    decl.arg_names = cf.arg_names;
                    decl.is_aggregate = cf.is_aggregate;
                    decl.return_type = sql_type_to_arrow(cf.return_type);
                    decl.arg_types.reserve(cf.arg_types.size());
                    for (const auto& t : cf.arg_types) {
                        decl.arg_types.push_back(sql_type_to_arrow(t));
                    }
                    decl.definitions = cf.definitions;
                    const bool exists =
                        cf.is_aggregate
                            ? AggFunctionRegistry::global().contains(cf.function_name)
                            : ScalarFunctionRegistry::global().contains(cf.function_name);
                    if (!cf.or_replace && exists) {
                        throw std::runtime_error(std::string{noun} + " '" + cf.function_name +
                                                 "' already exists (use CREATE OR REPLACE " +
                                                 (cf.is_aggregate ? "AGGREGATE" : "FUNCTION") +
                                                 " to replace it)");
                    }
                    UdfLanguageRegistry::global().load(cf.language, decl);
                    // Record the declaration in the catalog (persisted when
                    // the catalog has a dir, so it survives a restart like a
                    // table or model). The packaged payload makes the reload
                    // and any cluster submit independent of the AS path;
                    // packaging failures (unreadable module, over the ship
                    // cap) fail the statement here, not a later deploy. A
                    // language without a packager stays declaration-only.
                    FunctionDef fdef;
                    fdef.name = cf.function_name;
                    fdef.language = cf.language;
                    fdef.arg_names = cf.arg_names;
                    fdef.arg_types.reserve(decl.arg_types.size());
                    for (const auto& t : decl.arg_types) {
                        fdef.arg_types.push_back(t->ToString());
                    }
                    fdef.return_type =
                        decl.return_type != nullptr ? decl.return_type->ToString() : "";
                    fdef.definitions = cf.definitions;
                    fdef.kind = cf.is_aggregate ? "aggregate" : "";
                    if (auto payload = UdfLanguageRegistry::global().package(cf.language, decl);
                        !payload.empty()) {
                        fdef.module_b64 = clink::base64_encode(std::string_view{
                            reinterpret_cast<const char*>(payload.data()), payload.size()});
                    }
                    // The duplicate gate above ran against the runtime
                    // registry (which the catalog pass pre-seeded), so the
                    // catalog write is always a replace.
                    catalog.register_function(std::move(fdef), /*or_replace=*/true);
                } catch (const std::exception& e) {
                    err << "error: CREATE " << (cf.is_aggregate ? "AGGREGATE " : "FUNCTION ")
                        << cf.function_name << ": " << e.what() << "\n";
                    return 1;
                }
                out << "created " << noun << " " << cf.function_name << " (language " << cf.language
                    << ")\n";
                continue;
            }
            if (std::holds_alternative<ast::DropFunctionStmt>(stmt)) {
                const auto& df = std::get<ast::DropFunctionStmt>(stmt);
                for (const auto& fname : df.function_names) {
                    const bool known = ScalarFunctionRegistry::global().contains(fname) ||
                                       AggFunctionRegistry::global().contains(fname) ||
                                       catalog.get_function(fname) != nullptr;
                    if (!known) {
                        if (df.if_exists) {
                            out << "function " << fname << " does not exist, skipping\n";
                            continue;
                        }
                        err << "error: DROP FUNCTION: function '" << fname << "' does not exist\n";
                        return 1;
                    }
                    // Drops the declaration (and its persisted file) plus
                    // THIS process's registration - scalar or aggregate
                    // (DROP FUNCTION and DROP AGGREGATE share semantics).
                    // TaskManagers that registered it from an earlier deploy
                    // keep theirs for the process lifetime, like
                    // C++-registered UDFs.
                    ScalarFunctionRegistry::global().remove(fname);
                    AggFunctionRegistry::global().remove(fname);
                    catalog.drop_function(fname);
                    out << "dropped function " << fname << "\n";
                }
                continue;
            }
            if (std::holds_alternative<ast::AlterTableStmt>(stmt)) {
                // ALTER TABLE mutates the catalog declaration (column add/drop);
                // a streaming table has no stored data to rewrite.
                catalog.alter_table(std::get<ast::AlterTableStmt>(stmt));
                continue;
            }
            if (std::holds_alternative<ast::RenameStmt>(stmt)) {
                // ALTER TABLE RENAME TO / RENAME COLUMN: a catalog rename, rejected
                // (and rolled back) if it would break a dependent logical view.
                rename_object(catalog, std::get<ast::RenameStmt>(stmt));
                continue;
            }
            if (std::holds_alternative<ast::CreateMaterializedViewStmt>(stmt)) {
                // MATTBL: register the backing table and submit the continuous
                // maintenance job (INSERT INTO <view> <SELECT>). The original
                // statement text is stored on the backing table for future
                // restart re-binding; passing the whole script is a v1
                // approximation (single-statement submits are the common case).
                auto& mv = std::get<ast::CreateMaterializedViewStmt>(stmt);
                auto mvplan = plan_materialized_view(std::move(mv), catalog, sql);
                const std::string view_name = mvplan.backing.name;
                const bool full_refresh = mvplan.arm == RefreshArm::Full;
                auto plan = optimize(std::move(mvplan.maintenance));
                if (opts.explain) {
                    out << explain_with_estimates(*plan);
                } else {
                    const auto& sink = static_cast<const LogicalSink&>(*plan);
                    auto spec = planner.compile(sink);
                    apply_parallelism(spec);
                    attach_udfs(spec);
                    // Continuous: a live maintenance job (mv_<name>). Full-refresh: a
                    // one-shot bounded initial population (refresh_<name>) - it runs to
                    // completion and the overwrite sink atomically publishes; a later
                    // REFRESH MATERIALIZED VIEW re-runs the same recompute.
                    const std::string default_name =
                        (full_refresh ? "refresh_" : "mv_") + view_name;
                    const std::string name = opts.job_name.empty() ? default_name : opts.job_name;
                    if (int rc = submit(spec, name); rc != 0) {
                        return rc;
                    }
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<ast::RefreshMatViewStmt>(stmt)) {
                // REFRESH MATERIALIZED VIEW <name>: recompute the full-refresh backing
                // as a bounded INSERT that atomically overwrites it. Runs the same
                // recompute the initial population + a scheduler tick would.
                const auto& rf = std::get<ast::RefreshMatViewStmt>(stmt);
                auto plan = optimize(plan_materialized_view_refresh(rf.view_name, catalog));
                if (opts.explain) {
                    out << explain_with_estimates(*plan);
                } else {
                    const auto& sink = static_cast<const LogicalSink&>(*plan);
                    auto spec = planner.compile(sink);
                    apply_parallelism(spec);
                    attach_udfs(spec);
                    const std::string name =
                        opts.job_name.empty() ? ("refresh_" + rf.view_name) : opts.job_name;
                    if (int rc = submit(spec, name); rc != 0) {
                        return rc;
                    }
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<ast::DropTableStmt>(stmt)) {
                const auto& drop = std::get<ast::DropTableStmt>(stmt);
                const char* kind_noun =
                    drop.object_kind == ast::DropKind::MaterializedView
                        ? "materialized view"
                        : (drop.object_kind == ast::DropKind::View ? "view" : "table");
                bool failed = false;
                for (const auto& tbl : drop.table_names) {
                    switch (catalog.drop_object(tbl, drop.object_kind)) {
                        case Catalog::DropResult::Dropped:
                            break;
                        case Catalog::DropResult::NotFound:
                            if (!drop.if_exists) {
                                err << "error: " << kind_noun << " does not exist: " << tbl << "\n";
                                failed = true;
                            }
                            break;
                        case Catalog::DropResult::KindMismatch:
                            err << "error: \"" << tbl << "\" is not a " << kind_noun << "\n";
                            failed = true;
                            break;
                    }
                }
                if (failed) {
                    return 1;
                }
                continue;
            }
            if (std::holds_alternative<ast::ShowTablesStmt>(stmt)) {
                auto names = catalog.list_tables();
                for (const auto& name : names) {
                    out << name << "\n";
                }
                continue;
            }
            if (std::holds_alternative<ast::AnalyzeStmt>(stmt)) {
                // ANALYZE runs a local in-process bounded scan, so the SQL source
                // factories must be registered here (a spec-compiling front door
                // otherwise only builds specs for the JM). Install once, lazily.
                ensure_local_ops_installed();
                const auto& an = std::get<ast::AnalyzeStmt>(stmt);
                try {
                    analyze_table(catalog, an.table, an.columns);
                } catch (const std::exception& e) {
                    err << "error: ANALYZE " << an.table << ": " << e.what() << "\n";
                    return 1;
                }
                out << "analyzed " << an.table << "\n";
                continue;
            }
            if (std::holds_alternative<ast::InsertStmt>(stmt)) {
                if (int rc = handle_insert(std::get<ast::InsertStmt>(stmt)); rc != 0) {
                    return rc;
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<ast::SelectStmt>(stmt)) {
                auto& sel = std::get<ast::SelectStmt>(stmt);
                if (opts.explain) {
                    auto plan = optimize(binder.bind_select(sel));
                    out << explain_with_estimates(*plan);
                } else if (opts.bare_select_to_collect != nullptr) {
                    // Bind the SELECT for its output schema and synthesise a
                    // connector='collect' sink table carrying that schema -
                    // changelog-aware, so a retracting plan streams with a
                    // leading row_kind column instead of being rejected.
                    auto plan = binder.bind_select(sel);
                    const auto schema = plan->schema();
                    TableDef def;
                    static std::atomic<std::uint64_t> collect_table_seq{0};
                    def.name = "__collect_" + std::to_string(collect_table_seq++);
                    def.columns.reserve(static_cast<std::size_t>(schema->num_fields()));
                    for (const auto& field : schema->fields()) {
                        def.columns.push_back(ColumnSpec{field->name(), field->type()});
                    }
                    def.properties["connector"] = "collect";
                    if (is_changelog_plan(*plan)) {
                        def.properties["changelog"] = "true";
                    }
                    catalog.register_table(def);
                    ast::InsertStmt ins;
                    ins.target.name = def.name;
                    ins.select = std::move(sel);
                    if (int rc = handle_insert(ins); rc != 0) {
                        return rc;
                    }
                    opts.bare_select_to_collect->push_back(def.name);
                } else if (opts.bare_select_to_print) {
                    // Bind the SELECT for its output schema, synthesise a
                    // connector='print' sink table carrying that schema
                    // verbatim (ColumnSpec holds Arrow types directly), and
                    // compile the statement as INSERT INTO it.
                    auto plan = binder.bind_select(sel);
                    const auto schema = plan->schema();
                    TableDef def;
                    def.name = "__stdout_" + std::to_string(stdout_table_seq++);
                    def.columns.reserve(static_cast<std::size_t>(schema->num_fields()));
                    for (const auto& field : schema->fields()) {
                        def.columns.push_back(ColumnSpec{field->name(), field->type()});
                    }
                    def.properties["connector"] = "print";
                    catalog.register_table(def);
                    ast::InsertStmt ins;
                    ins.target.name = def.name;
                    ins.select = std::move(sel);
                    if (int rc = handle_insert(ins); rc != 0) {
                        return rc;
                    }
                } else {
                    err << "error: bare SELECT must be wrapped in INSERT INTO ... "
                        << "SELECT (no print-to-stdout sink yet); "
                        << "use --explain to inspect the LogicalPlan\n";
                    return 1;
                }
                produced_spec = true;
                continue;
            }
        }
        if (!produced_spec) {
            err << "warning: no INSERT / SELECT statement found; " << script.statements.size()
                << " statement(s) processed\n";
        }
        return 0;
    } catch (const ParseError& e) {
        err << "parse error at position " << e.cursor_position() << ": " << e.what() << "\n";
        return 1;
    } catch (const TranslationError& e) {
        err << "compile error at position " << e.cursor_position() << ": " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        err << "error: " << e.what() << "\n";
        return 1;
    }
}

}  // namespace clink::sql
