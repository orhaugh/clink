// clink state-query - run SQL over a checkpoint/savepoint's keyed state,
// in-process, using the engine's own SQL frontend and embedded runtime.
//
// The snapshot's decoded entries are rendered to a temp Parquet file (the
// state-query projection: op_id, key_group, slot, user_key, key_int,
// value, value_int - see state_processor::write_state_query_parquet),
// exposed as a SQL table named `state`, and the user's SELECT runs
// against it. Results are netted through the changelog sink (so
// retracting plans - GROUP BY, DISTINCT - print their FINAL rows) and
// written to stdout as NDJSON, one row per line.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <variant>
#include <vector>

#include "clink/embed/embedded_engine.hpp"
#include "clink/sql/ast.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/sql/type.hpp"
#include "clink/state_processor/parquet_export.hpp"
#include "clink/state_processor/savepoint.hpp"
#include "clink/state_processor/state_diff.hpp"

#include "state_tool_io.hpp"

namespace {

namespace fs = std::filesystem;

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

void query_usage() {
    std::cout
        << "usage: clink state-query (--from=<path> | --dir=<root> --id=N) --sql=\"SELECT ...\"\n"
        << "\n"
        << "Run SQL over a checkpoint/savepoint's keyed state, in-process. The\n"
        << "state is exposed as a table named `state`:\n"
        << "\n"
        << "  op_id BIGINT, key_group BIGINT, slot TEXT,\n"
        << "  user_key TEXT (printable bytes verbatim, else 0x-hex),\n"
        << "  key_int BIGINT (the key's int64 reading when 8 bytes, else NULL),\n"
        << "  value TEXT, value_int BIGINT (rendered the same way)\n"
        << "\n"
        << "Results print to stdout as NDJSON, one row per line; retracting\n"
        << "plans (GROUP BY, DISTINCT) print their final netted rows.\n"
        << "\n"
        << "  --from=<path>   a .snap/.arrows snapshot file, or a RocksDB checkpoint\n"
        << "                  directory (RocksDB-linked builds)\n"
        << "  --dir/--id      merge a multi-subtask checkpoint instead of --from\n"
        << "  --job/--jm      query a RUNNING job's live state via the JM route\n"
        << "  --sql=<query>   the SELECT to run against `state`\n";
}

}  // namespace

int clink_cmd_state_query(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        query_usage();
        return 0;
    }
    const auto from = get_arg(argc, argv, "from");
    const auto dir = get_arg(argc, argv, "dir");
    const auto id_str = get_arg(argc, argv, "id");
    const auto job = get_arg(argc, argv, "job");
    const auto jm = get_arg(argc, argv, "jm");
    const auto sql = get_arg(argc, argv, "sql");
    const bool dir_form = !dir.empty() || !id_str.empty();
    const bool dir_form_complete = !dir.empty() && !id_str.empty();
    const int input_forms = (!from.empty() ? 1 : 0) + (dir_form ? 1 : 0) + (!job.empty() ? 1 : 0);
    if (sql.empty() || input_forms != 1 || (dir_form && !dir_form_complete)) {
        query_usage();
        return 2;
    }
    const auto tag = std::to_string(getpid());
    const auto state_pq = fs::temp_directory_path() / ("clink_state_query_" + tag + ".parquet");
    const auto out_path = fs::temp_directory_path() / ("clink_state_query_out_" + tag + ".ndjson");
    try {
        // 1. Snapshot -> decoded entries -> the query-projection Parquet.
        auto resolved = clink_tools::resolve_state_input(from, dir, id_str, job, jm);
        clink::Snapshot snap;
        snap.bytes = std::move(resolved.bytes);
        auto sp = clink::state_processor::Savepoint::load_from_snapshot(std::move(snap));
        const auto entries = clink::state_processor::collect_entries(sp);
        fs::remove(state_pq);
        clink::state_processor::write_state_query_parquet(entries, state_pq);

        const std::string state_ddl =
            "CREATE TABLE state (op_id BIGINT, key_group BIGINT, slot TEXT, user_key TEXT, "
            "key_int BIGINT, value TEXT, value_int BIGINT) WITH (connector='parquet', "
            "format='json', path='" +
            state_pq.string() + "')";

        // 2. Derive the SELECT's output schema by binding it against a
        // catalog holding `state`, so the result table can be declared.
        auto script = clink::sql::parse(sql);
        if (script.statements.size() != 1 ||
            !std::holds_alternative<clink::sql::ast::SelectStmt>(script.statements[0])) {
            std::cerr << "state-query: --sql must be a single SELECT statement\n";
            return 2;
        }
        clink::sql::Catalog catalog;
        {
            auto ddl_script = clink::sql::parse(state_ddl);
            catalog.register_table(
                std::get<clink::sql::ast::CreateTableStmt>(ddl_script.statements[0]));
        }
        clink::sql::Binder binder{catalog};
        auto plan = binder.bind_select(std::get<clink::sql::ast::SelectStmt>(script.statements[0]));
        const auto schema = plan->schema();

        std::string result_ddl = "CREATE TABLE __clink_query_result (";
        for (int i = 0; i < schema->num_fields(); ++i) {
            if (i > 0) {
                result_ddl += ", ";
            }
            result_ddl += "\"" + schema->field(i)->name() + "\" " +
                          clink::sql::arrow_to_sql_type_string(*schema->field(i)->type());
        }
        fs::remove(out_path);
        result_ddl += ") WITH (connector='file', format='json', path='" + out_path.string() + "')";

        // 3. Run: bounded parquet scan -> user query -> netting sink.
        std::ostringstream err;
        clink::embed::EngineOptions opts;
        opts.out = &err;  // keep stdout pure NDJSON (engine info logs -> capture)
        opts.err = &err;
        clink::embed::EmbeddedEngine engine{std::move(opts)};
        const int rc = engine.execute_script(state_ddl + ";" + result_ddl +
                                             ";INSERT INTO __clink_query_result " + sql);
        const bool ok = engine.await_all();
        if (rc != 0 || !ok) {
            std::cerr << "state-query: query failed\n" << err.str();
            for (const auto job : engine.job_ids()) {
                for (const auto& e : engine.job_errors(job)) {
                    std::cerr << "state-query: " << e << "\n";
                }
            }
            fs::remove(state_pq);
            return 2;
        }

        // 4. Net the changelog client-side, preserving multiplicity: an
        // insert/update_after adds one occurrence of the bare row, a
        // delete/update_before removes one; each surviving row prints
        // count times (exact multiset semantics - unlike the changelog
        // sink's relation view, duplicate result rows are kept). Rows in
        // sorted serialised order for determinism.
        std::size_t rows = 0;
        {
            struct Net {
                std::string line;
                std::int64_t count{0};
            };
            std::map<std::string, Net> net;
            std::ifstream in(out_path);
            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) {
                    continue;
                }
                auto obj = clink::config::parse_object(line);
                if (!obj) {
                    continue;
                }
                bool deletion = false;
                if (auto it = obj->find(clink::sql::kRowKindField); it != obj->end()) {
                    deletion = it->second.is_string() &&
                               clink::sql::is_delete_like(it->second.as_string());
                    obj->erase(clink::sql::kRowKindField);
                }
                std::string bare = clink::config::JsonValue{std::move(*obj)}.serialize(0);
                auto& slot = net[bare];
                slot.count += deletion ? -1 : 1;
                slot.line = std::move(bare);
            }
            for (const auto& [_, slot] : net) {
                for (std::int64_t i = 0; i < slot.count; ++i) {
                    std::cout << slot.line << "\n";
                    ++rows;
                }
            }
        }
        std::cerr << "state-query: " << rows << " rows from " << resolved.label << "\n";
        if (const char* keep = std::getenv("CLINK_STATE_QUERY_KEEP");
            keep == nullptr || keep[0] != '1') {
            fs::remove(state_pq);
            fs::remove(out_path);
        } else {
            std::cerr << "state-query: kept " << state_pq.string() << " and " << out_path.string()
                      << "\n";
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "state-query: " << e.what() << "\n";
        std::error_code ec;
        fs::remove(state_pq, ec);
        fs::remove(out_path, ec);
        return 2;
    }
}
