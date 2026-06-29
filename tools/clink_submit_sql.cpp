// clink_submit_sql: SQL-to-JobGraphSpec compiler.
//
// Reads a SQL file (or inline -e expression), processes statements:
//   * CREATE TABLE: registers in an in-memory catalog
//   * INSERT INTO ... SELECT: binds + compiles to a JobGraphSpec
//                              and prints the JSON to stdout
//
// Submission to a running JM is NOT yet wired (a future HTTP submit
// path will take the JSON spec directly). For now, pipe the JSON to a
// separate submit tool or use it for inspection.
//
// Usage:
//   clink_submit_sql --file path/to/job.sql
//   clink_submit_sql -e "CREATE TABLE ...; INSERT INTO ..."
//   clink_submit_sql --explain --file job.sql   (prints LogicalPlan tree)

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/http/http_client.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/sql/analyze.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/install.hpp"
#include "clink/sql/materialized_view.hpp"
#include "clink/sql/optimizer.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/physical_plan.hpp"

namespace {

struct Args {
    std::string file;
    std::string inline_sql;
    std::string catalog_dir;
    std::string jm_host;
    std::uint16_t jm_port = 0;
    std::string job_name;
    bool explain = false;
    std::uint32_t parallelism = 1;  // uniform op parallelism (>1 fans the plan out)
};

void print_usage() {
    std::cerr
        << "Usage: clink_submit_sql --file <path> | -e <sql>\n"
        << "                        [--catalog-dir <dir>]\n"
        << "                        [--jm-host <host> --jm-port <port> [--name <job>]]\n"
        << "                        [--explain]\n"
        << "\n"
        << "  --file <path>       Read SQL from a file.\n"
        << "  -e <sql>            Read SQL from the command line.\n"
        << "  --catalog-dir <dir> Persistent catalog dir. Loaded at startup; tables\n"
        << "                      registered via CREATE TABLE auto-save as JSON-per-table.\n"
        << "  --jm-host <host>    JM HTTP host. When set with --jm-port, INSERT statements\n"
        << "  --jm-port <port>    POST the compiled JobGraphSpec to the JM instead of\n"
        << "                      printing JSON. The JM runs the job and replies with a\n"
        << "                      job id.\n"
        << "  --name <job>        Job name (default 'sql_job'). Only used with --jm-host.\n"
        << "  --parallelism <n>   Uniform op parallelism (default 1). >1 fans every op out to\n"
        << "  -p <n>              n subtasks; keyed ops hash-partition by key, sources split\n"
        << "                      partitions across subtasks (Kafka needs >= n partitions).\n"
        << "                      Assumes a parallelizable plan (the common SQL shapes).\n"
        << "  --explain           Print the LogicalPlan tree instead of the JobGraphSpec JSON.\n";
}

Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--file" || arg == "-f") {
            if (++i >= argc) {
                std::cerr << "error: --file requires a path\n";
                std::exit(2);
            }
            a.file = argv[i];
        } else if (arg == "-e") {
            if (++i >= argc) {
                std::cerr << "error: -e requires a SQL string\n";
                std::exit(2);
            }
            a.inline_sql = argv[i];
        } else if (arg == "--catalog-dir") {
            if (++i >= argc) {
                std::cerr << "error: --catalog-dir requires a path\n";
                std::exit(2);
            }
            a.catalog_dir = argv[i];
        } else if (arg == "--jm-host") {
            if (++i >= argc) {
                std::cerr << "error: --jm-host requires a host\n";
                std::exit(2);
            }
            a.jm_host = argv[i];
        } else if (arg == "--jm-port") {
            if (++i >= argc) {
                std::cerr << "error: --jm-port requires a port\n";
                std::exit(2);
            }
            a.jm_port = static_cast<std::uint16_t>(std::stoi(argv[i]));
        } else if (arg == "--name") {
            if (++i >= argc) {
                std::cerr << "error: --name requires a string\n";
                std::exit(2);
            }
            a.job_name = argv[i];
        } else if (arg == "--parallelism" || arg == "-p") {
            if (++i >= argc) {
                std::cerr << "error: --parallelism requires a number\n";
                std::exit(2);
            }
            a.parallelism = static_cast<std::uint32_t>(std::stoul(argv[i]));
            if (a.parallelism < 1) {
                std::cerr << "error: --parallelism must be >= 1\n";
                std::exit(2);
            }
        } else if (arg == "--explain") {
            a.explain = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        } else {
            std::cerr << "error: unknown argument: " << arg << "\n";
            print_usage();
            std::exit(2);
        }
    }
    if (a.file.empty() == a.inline_sql.empty()) {
        std::cerr << "error: exactly one of --file or -e is required\n";
        print_usage();
        std::exit(2);
    }
    return a;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        std::cerr << "error: cannot open " << path << "\n";
        std::exit(1);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    std::string sql = args.file.empty() ? args.inline_sql : read_file(args.file);

    clink::sql::Catalog catalog;
    if (!args.catalog_dir.empty()) {
        try {
            catalog.load_from_dir(args.catalog_dir);
            catalog.set_persistence_dir(args.catalog_dir);
        } catch (const std::exception& e) {
            std::cerr << "error: failed to load catalog from " << args.catalog_dir << ": "
                      << e.what() << "\n";
            return 1;
        }
    }
    clink::sql::Binder binder(catalog);
    clink::sql::PhysicalPlanner planner;

    try {
        auto script = clink::sql::parse(sql);
        bool produced_spec = false;
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
        // Submit a compiled JobGraphSpec to the JM (or print it when no JM host
        // is configured). Shared by the INSERT and CREATE MATERIALIZED VIEW
        // paths. Returns 0 on success, non-zero on a transport / JM error.
        // Fan the compiled plan out to `args.parallelism` subtasks per op. The
        // planner emits everything at parallelism 1; setting it uniformly here
        // lets the runtime hash-partition keyed ops by key and split Kafka
        // partitions across source subtasks. Assumes a parallelizable plan (no
        // forced-singleton op); the common Nexmark/SQL shapes qualify.
        auto apply_parallelism = [&](clink::cluster::JobGraphSpec& spec) {
            if (args.parallelism <= 1) {
                return;
            }
            for (auto& op : spec.ops) {
                op.parallelism = args.parallelism;
            }
        };
        auto submit_or_print = [&](const std::string& json, const std::string& name) -> int {
            if (!args.jm_host.empty() && args.jm_port != 0) {
                clink::http::HttpClient client(args.jm_host, args.jm_port);
                std::string path = "/api/v1/jobs/spec";
                if (!name.empty()) {
                    path += "?name=" + name;
                }
                auto resp = client.post(path, json);
                if (resp.status == 0) {
                    std::cerr << "error: HTTP transport failure: " << resp.error << "\n";
                    return 1;
                }
                std::cout << resp.body << "\n";
                if (resp.status != 200) {
                    std::cerr << "error: JM returned status " << resp.status << "\n";
                    return 1;
                }
            } else {
                std::cout << json << "\n";
            }
            return 0;
        };
        for (auto& stmt : script.statements) {
            if (std::holds_alternative<std::unique_ptr<clink::sql::ast::ExplainStmt>>(stmt)) {
                const auto& exp = *std::get<std::unique_ptr<clink::sql::ast::ExplainStmt>>(stmt);
                if (std::holds_alternative<clink::sql::ast::InsertStmt>(exp.query)) {
                    auto plan =
                        binder.bind_insert(std::get<clink::sql::ast::InsertStmt>(exp.query));
                    std::cout << plan->explain();
                } else if (std::holds_alternative<clink::sql::ast::SelectStmt>(exp.query)) {
                    auto plan =
                        binder.bind_select(std::get<clink::sql::ast::SelectStmt>(exp.query));
                    std::cout << plan->explain();
                } else {
                    std::cerr << "error: EXPLAIN only supports SELECT / INSERT INTO\n";
                    return 1;
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::CreateTableStmt>(stmt)) {
                catalog.register_table(std::get<clink::sql::ast::CreateTableStmt>(stmt));
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::CreateMaterializedViewStmt>(stmt)) {
                // MATTBL: register the backing table and submit the continuous
                // maintenance job (INSERT INTO <view> <SELECT>). The original
                // statement text is stored on the backing table for future
                // restart re-binding; passing the whole script is a v1
                // approximation (single-statement submits are the common case).
                auto& mv = std::get<clink::sql::ast::CreateMaterializedViewStmt>(stmt);
                auto mvplan = clink::sql::plan_materialized_view(std::move(mv), catalog, sql);
                const std::string view_name = mvplan.backing.name;
                auto plan = clink::sql::optimize(std::move(mvplan.maintenance));
                if (args.explain) {
                    std::cout << plan->explain();
                } else {
                    const auto& sink = static_cast<const clink::sql::LogicalSink&>(*plan);
                    auto spec = planner.compile(sink);
                    apply_parallelism(spec);
                    const std::string name =
                        args.job_name.empty() ? ("mv_" + view_name) : args.job_name;
                    if (int rc = submit_or_print(spec.to_json(), name); rc != 0) {
                        return rc;
                    }
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::DropTableStmt>(stmt)) {
                const auto& drop = std::get<clink::sql::ast::DropTableStmt>(stmt);
                const char* kind_noun =
                    drop.object_kind == clink::sql::ast::DropKind::MaterializedView
                        ? "materialized view"
                        : (drop.object_kind == clink::sql::ast::DropKind::View ? "view" : "table");
                bool failed = false;
                for (const auto& tbl : drop.table_names) {
                    switch (catalog.drop_object(tbl, drop.object_kind)) {
                        case clink::sql::Catalog::DropResult::Dropped:
                            break;
                        case clink::sql::Catalog::DropResult::NotFound:
                            if (!drop.if_exists) {
                                std::cerr << "error: " << kind_noun << " does not exist: " << tbl
                                          << "\n";
                                failed = true;
                            }
                            break;
                        case clink::sql::Catalog::DropResult::KindMismatch:
                            std::cerr << "error: \"" << tbl << "\" is not a " << kind_noun << "\n";
                            failed = true;
                            break;
                    }
                }
                if (failed) {
                    return 1;
                }
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::ShowTablesStmt>(stmt)) {
                auto names = catalog.list_tables();
                for (const auto& name : names) {
                    std::cout << name << "\n";
                }
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::AnalyzeStmt>(stmt)) {
                // ANALYZE runs a local in-process bounded scan, so the SQL source
                // factories must be registered here (the submit tool otherwise
                // only builds specs for the JM). Install once, lazily.
                ensure_local_ops_installed();
                const auto& an = std::get<clink::sql::ast::AnalyzeStmt>(stmt);
                try {
                    clink::sql::analyze_table(catalog, an.table, an.columns);
                } catch (const std::exception& e) {
                    std::cerr << "error: ANALYZE " << an.table << ": " << e.what() << "\n";
                    return 1;
                }
                std::cout << "analyzed " << an.table << "\n";
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::InsertStmt>(stmt)) {
                auto plan = binder.bind_insert(std::get<clink::sql::ast::InsertStmt>(stmt));
                plan = clink::sql::optimize(std::move(plan));
                if (args.explain) {
                    std::cout << plan->explain();
                } else {
                    const auto& sink = static_cast<const clink::sql::LogicalSink&>(*plan);
                    auto spec = planner.compile(sink);
                    apply_parallelism(spec);
                    auto json = spec.to_json();
                    if (!args.jm_host.empty() && args.jm_port != 0) {
                        clink::http::HttpClient client(args.jm_host, args.jm_port);
                        std::string path = "/api/v1/jobs/spec";
                        if (!args.job_name.empty()) {
                            path += "?name=" + args.job_name;
                        }
                        auto resp = client.post(path, json);
                        if (resp.status == 0) {
                            std::cerr << "error: HTTP transport failure: " << resp.error << "\n";
                            return 1;
                        }
                        std::cout << resp.body << "\n";
                        if (resp.status != 200) {
                            std::cerr << "error: JM returned status " << resp.status << "\n";
                            return 1;
                        }
                    } else {
                        std::cout << json << "\n";
                    }
                }
                produced_spec = true;
                continue;
            }
            if (std::holds_alternative<clink::sql::ast::SelectStmt>(stmt)) {
                auto plan = binder.bind_select(std::get<clink::sql::ast::SelectStmt>(stmt));
                if (args.explain) {
                    std::cout << plan->explain();
                } else {
                    std::cerr << "error: bare SELECT must be wrapped in INSERT INTO ... "
                              << "SELECT (no print-to-stdout sink yet); "
                              << "use --explain to inspect the LogicalPlan\n";
                    return 1;
                }
                produced_spec = true;
                continue;
            }
        }
        if (!produced_spec) {
            std::cerr << "warning: no INSERT / SELECT statement found; " << script.statements.size()
                      << " statement(s) processed\n";
        }
        return 0;
    } catch (const clink::sql::ParseError& e) {
        std::cerr << "parse error at position " << e.cursor_position() << ": " << e.what() << "\n";
        return 1;
    } catch (const clink::sql::TranslationError& e) {
        std::cerr << "compile error at position " << e.cursor_position() << ": " << e.what()
                  << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
