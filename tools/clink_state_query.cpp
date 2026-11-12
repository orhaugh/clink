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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <variant>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include "clink/embed/embedded_engine.hpp"
#include "clink/sql/ast.hpp"
#include "clink/sql/binder.hpp"
#include "clink/sql/catalog.hpp"
#include "clink/sql/logical_plan.hpp"
#include "clink/sql/parser.hpp"
#include "clink/sql/row_kind.hpp"
#include "clink/sql/type.hpp"
#include "clink/state_processor/checkpoint_dataset.hpp"
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

// A checkpoint dataset's entries as the `state` table: identity, scope and
// participant as they are, and keys and values only as hex and lengths. No
// column reads a value as a type: nothing in this build knows a job's codecs.
constexpr std::int64_t kProjectionRowGroup = 65536;

void write_dataset_projection(const clink::state_processor::Dataset& d, const fs::path& path) {
    namespace sp = clink::state_processor;
    const auto schema = arrow::schema({arrow::field("op_id", arrow::utf8()),
                                       arrow::field("slot", arrow::utf8()),
                                       arrow::field("scope", arrow::utf8()),
                                       arrow::field("key_group", arrow::int64()),
                                       arrow::field("participant", arrow::int64()),
                                       arrow::field("user_key_hex", arrow::utf8()),
                                       arrow::field("value_hex", arrow::utf8()),
                                       arrow::field("user_key_len", arrow::int64()),
                                       arrow::field("value_len", arrow::int64()),
                                       arrow::field("operator", arrow::utf8()),
                                       arrow::field("subtask", arrow::int64())});
    const auto ok = [](const arrow::Status& st) {
        if (!st.ok()) {
            throw std::runtime_error("cannot write the query projection: " + st.ToString());
        }
    };
    auto sink = arrow::io::FileOutputStream::Open(path.string());
    ok(sink.status());
    auto opened = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), *sink);
    ok(opened.status());
    auto writer = std::move(*opened);
    arrow::StringBuilder op, slot, scope, uk, val;
    arrow::Int64Builder kg, part, uk_len, val_len, subtask;
    arrow::StringBuilder op_name;
    std::int64_t pending = 0;
    const auto flush = [&] {
        if (pending == 0) {
            return;
        }
        std::vector<std::shared_ptr<arrow::Array>> cols(11);
        ok(op.Finish(&cols[0]));
        ok(slot.Finish(&cols[1]));
        ok(scope.Finish(&cols[2]));
        ok(kg.Finish(&cols[3]));
        ok(part.Finish(&cols[4]));
        ok(uk.Finish(&cols[5]));
        ok(val.Finish(&cols[6]));
        ok(uk_len.Finish(&cols[7]));
        ok(val_len.Finish(&cols[8]));
        ok(op_name.Finish(&cols[9]));
        ok(subtask.Finish(&cols[10]));
        ok(writer->WriteTable(*arrow::Table::Make(schema, cols, pending), pending));
        pending = 0;
    };
    sp::for_each_entry(d, [&](const sp::DatasetEntry& e) {
        ok(op.Append(std::to_string(e.op)));
        ok(slot.Append(e.slot));
        ok(scope.Append(sp::scope_name(e.scope)));
        ok(e.key_group >= 0 ? kg.Append(e.key_group) : kg.AppendNull());
        ok(part.Append(e.participant));
        ok(uk.Append(sp::hex_bytes(e.user_key)));
        ok(val.Append(sp::hex_bytes(e.value)));
        ok(uk_len.Append(static_cast<std::int64_t>(e.user_key.size())));
        ok(val_len.Append(static_cast<std::int64_t>(e.value.size())));
        const auto name = d.op_names.find(e.op);
        ok(name != d.op_names.end() ? op_name.Append(name->second) : op_name.AppendNull());
        const auto sub = d.layout.find(e.participant);
        ok(sub != d.layout.end() ? subtask.Append(static_cast<std::int64_t>(sub->second.second))
                                 : subtask.AppendNull());
        if (++pending == kProjectionRowGroup) {
            flush();
        }
        return true;
    });
    flush();
    ok(writer->Close());
}

// An aggregate re-emits each group's running result as its input arrives
// (append-mode snapshots, docs/internals/sql-frontend.md), so over a bounded
// table the result is each group's last row. That is decidable only for one
// aggregate at the top of the query whose group columns are all in its output:
// a HAVING, a LIMIT or an outer query over it, or an aggregate inside a
// subquery, would act on snapshots that a later one supersedes, and no row
// says so. Those are refused rather than answered wrongly.
struct FinalRows {
    bool by_key{false};
    std::vector<std::string> key;  // output columns naming a group
    std::string refusal;
};

FinalRows final_rows_of(const clink::sql::LogicalPlan& root) {
    std::function<std::size_t(const clink::sql::LogicalPlan&)> aggregates =
        [&](const clink::sql::LogicalPlan& n) -> std::size_t {
        std::size_t c = (n.kind() == "Aggregate" || n.kind() == "WindowAggregate") ? 1 : 0;
        for (const auto* in : n.inputs()) {
            c += in != nullptr ? aggregates(*in) : 0;
        }
        return c;
    };
    FinalRows out;
    const auto total = aggregates(root);
    if (total == 0) {
        return out;
    }
    std::vector<const clink::sql::LogicalProject*> projects;
    const clink::sql::LogicalPlan* n = &root;
    while (n->kind() == "Project") {
        projects.push_back(static_cast<const clink::sql::LogicalProject*>(n));
        n = &projects.back()->input();
    }
    const char* why =
        "state-query answers an aggregate only as one GROUP BY at the top of the query, with "
        "every group column selected and no HAVING, LIMIT or outer query around it: its "
        "streaming aggregate emits each group's running result, and only then can each "
        "group's final row be kept";
    if (n->kind() != "Aggregate" || total != 1) {
        out.refusal = why;
        return out;
    }
    const auto* agg = static_cast<const clink::sql::LogicalAggregate*>(n);
    out.key = agg->key_output_names().empty() ? agg->group_keys() : agg->key_output_names();
    for (auto it = projects.rbegin(); it != projects.rend(); ++it) {
        for (auto& k : out.key) {
            std::string renamed;
            for (const auto& o : (*it)->outputs()) {
                const auto e = clink::config::parse(o.expr_json);
                if (e.is_object() && e.as_object().size() == 1 && e.contains("col") &&
                    e.at("col").is_string() && e.at("col").as_string() == k) {
                    renamed = o.name;
                    break;
                }
            }
            if (renamed.empty()) {
                out.refusal = why;
                return out;
            }
            k = renamed;
        }
    }
    for (const auto& k : out.key) {
        if (root.schema() == nullptr || root.schema()->GetFieldIndex(k) < 0) {
            out.refusal = why;
            return out;
        }
    }
    out.by_key = true;
    return out;
}

void query_usage() {
    std::cout
        << "usage: clink state-query --dataset=<dataset> --sql=\"SELECT ...\" [--allow-partial]\n"
        << "           [--digest=<sha256>]\n"
        << "       clink state-query (--from=<path> | --dir=<root> --id=N) --sql=\"SELECT ...\"\n"
        << "\n"
        << "--dataset queries a checkpoint dataset (clink state-export --checkpoint-dir)\n"
        << "after verifying that its entry table and bundle manifest hash to what its\n"
        << "dataset.json records, and with --digest that dataset.json has that SHA-256.\n"
        << "A partial dataset is refused (exit 3) unless\n"
        << "--allow-partial; either way its completeness is stated on stderr. Its\n"
        << "`state` table holds exact identities and no typed readings:\n"
        << "\n"
        << "  op_id TEXT (the operator id, decimal), slot TEXT,\n"
        << "  scope TEXT (keyed | operator | raw), key_group BIGINT (keyed only),\n"
        << "  participant BIGINT, user_key_hex TEXT, value_hex TEXT,\n"
        << "  user_key_len BIGINT, value_len BIGINT,\n"
        << "  operator TEXT (the graph operator it is named as, when the dataset names it),\n"
        << "  subtask BIGINT (the participant's subtask in its operator, when recorded)\n"
        << "\n"
        << "The --from and --dir forms read snapshot files without the checkpoint's\n"
        << "marker or participants (not checkpoint-consistent), and render keys and\n"
        << "values by guess:\n"
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
        << "  --job/--coordinator      query a RUNNING job's live state via the coordinator route\n"
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
    const auto coordinator = get_arg(argc, argv, "coordinator");
    const auto mat_store_path = get_arg(argc, argv, "materialisation-store");
    const auto sql = get_arg(argc, argv, "sql");
    const auto dataset_dir = get_arg(argc, argv, "dataset");
    const bool dir_form = !dir.empty() || !id_str.empty();
    const bool dir_form_complete = !dir.empty() && !id_str.empty();
    const int input_forms = (!from.empty() ? 1 : 0) + (dir_form ? 1 : 0) + (!job.empty() ? 1 : 0) +
                            (!dataset_dir.empty() ? 1 : 0);
    if (sql.empty() || input_forms != 1 || (dir_form && !dir_form_complete)) {
        query_usage();
        return 2;
    }
    const auto tag = std::to_string(getpid());
    const auto state_pq = fs::temp_directory_path() / ("clink_state_query_" + tag + ".parquet");
    const auto out_path = fs::temp_directory_path() / ("clink_state_query_out_" + tag + ".ndjson");
    try {
        std::string label;
        std::string state_ddl;
        if (!dataset_dir.empty()) {
            // 1. A verified dataset -> its hex projection.
            std::optional<clink::state_processor::Dataset> d;
            try {
                d = clink::state_processor::open_dataset(dataset_dir,
                                                         get_arg(argc, argv, "digest"));
            } catch (const clink::state_processor::DatasetInvalid& e) {
                std::cerr << "state-query: " << e.what() << "\n";
                return 2;
            }
            label = d->root.string() + " (job " + std::to_string(d->job) + ", checkpoint " +
                    std::to_string(d->checkpoint) + ", " + (d->complete ? "complete" : "PARTIAL") +
                    ")";
            if (!d->complete && !has_flag(argc, argv, "allow-partial")) {
                std::cerr << "state-query: " << label
                          << " is partial (see incomplete and unsupported in its dataset.json); "
                             "--allow-partial queries it anyway, and the result is not the "
                             "job's whole state\n";
                return 3;
            }
            std::cerr << "state-query: dataset " << label << "\n";
            fs::remove(state_pq);
            write_dataset_projection(*d, state_pq);
            state_ddl =
                "CREATE TABLE state (op_id TEXT, slot TEXT, scope TEXT, key_group BIGINT, "
                "participant BIGINT, user_key_hex TEXT, value_hex TEXT, user_key_len BIGINT, "
                "value_len BIGINT, operator TEXT, subtask BIGINT) WITH (connector='parquet', "
                "format='json', path='" +
                state_pq.string() + "')";
        } else {
            // 1. Snapshot -> decoded entries -> the query-projection Parquet.
            auto resolved = clink_tools::resolve_state_input(
                from,
                dir,
                id_str,
                job,
                coordinator,
                clink_tools::materialisation_store_for(mat_store_path));
            label = resolved.label;
            clink::Snapshot snap;
            snap.bytes = std::move(resolved.bytes);
            auto sp = clink::state_processor::Savepoint::load_from_snapshot(std::move(snap));
            const auto entries = clink::state_processor::collect_entries(sp);
            fs::remove(state_pq);
            clink::state_processor::write_state_query_parquet(entries, state_pq);
            state_ddl =
                "CREATE TABLE state (op_id BIGINT, key_group BIGINT, slot TEXT, user_key TEXT, "
                "key_int BIGINT, value TEXT, value_int BIGINT) WITH (connector='parquet', "
                "format='json', path='" +
                state_pq.string() + "')";
        }

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
        const auto final_rows = final_rows_of(*plan);
        if (!final_rows.refusal.empty()) {
            std::cerr << "state-query: " << final_rows.refusal << "\n";
            return 2;
        }

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
        // sink's relation view, duplicate result rows are kept). An
        // aggregate's result is instead each group's last row (final_rows_of).
        // Rows in sorted serialised order for determinism.
        std::size_t rows = 0;
        {
            struct Net {
                std::string line;
                std::int64_t count{0};
            };
            std::map<std::string, Net> net;
            std::map<std::string, std::string> last;  // group -> its last row
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
                std::string bare = clink::config::JsonValue{*obj}.serialize(0);
                if (final_rows.by_key) {
                    std::string group;
                    for (const auto& k : final_rows.key) {
                        const auto f = obj->find(k);
                        group += (f == obj->end() ? std::string{"null"} : f->second.serialize(0)) +
                                 "\x1f";
                    }
                    if (deletion) {
                        last.erase(group);
                    } else {
                        last[group] = std::move(bare);
                    }
                    continue;
                }
                auto& slot = net[bare];
                slot.count += deletion ? -1 : 1;
                slot.line = std::move(bare);
            }
            if (final_rows.by_key) {
                std::vector<std::string> sorted;
                sorted.reserve(last.size());
                for (auto& [_, row] : last) {
                    sorted.push_back(std::move(row));
                }
                std::sort(sorted.begin(), sorted.end());
                for (const auto& row : sorted) {
                    std::cout << row << "\n";
                    ++rows;
                }
            }
            for (const auto& [_, slot] : net) {
                for (std::int64_t i = 0; i < slot.count; ++i) {
                    std::cout << slot.line << "\n";
                    ++rows;
                }
            }
        }
        std::cerr << "state-query: " << rows << " rows from " << label << "\n";
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
