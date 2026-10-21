// A local baseline for the Neo4j lookup: lookups per second and latency
// percentiles against one server, for a point read and for the two-hop device
// read of examples/kafka-to-neo4j, at several request concurrencies, and the
// lookup join operator end to end. A baseline for comparing changes on one
// machine, not a capacity figure: the server shares the machine, and the
// numbers move with it.
//
//   CLINK_NEO4J_TEST_ENDPOINT=http://localhost:17474 \
//   CLINK_NEO4J_TEST_PASSWORD=clink-live-test-pw \
//   build/benchmarks/clink_neo4j_lookup_bench [lookups per run, default 5000]
//
// It seeds 1000 synthetic accounts under a label of its own, each using two
// devices of its own and one of 100 devices shared by ten accounts, warms the
// server with 2000 unmeasured lookups of each statement, removes the data when
// it ends, and prints one line per run.
//
// Pool runs: `concurrency` client threads each submit a lookup and wait for
// it, against a pool with as many request threads, so nothing queues and the
// latency is one lookup's, queueing excluded. Operator run: probe rows fed to
// neo4j_lookup_join_row as fast as it takes them, max_in_flight 64; the
// latency there includes waiting for a request thread.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "clink/config/json.hpp"
#include "clink/core/base64.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/sql/row.hpp"

#include "lookup.hpp"
#include "options.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using clink::config::JsonValue;
using clink::sql::Row;

constexpr int kAccounts = 1000;

std::string env(const char* name) {
    const char* v = std::getenv(name);
    return v == nullptr ? std::string{} : std::string{v};
}

class Admin {
public:
    Admin(const std::string& endpoint, const std::string& password) {
        clink::http_connector::HttpRequest::Options o;
        o.base_url = endpoint;
        o.headers["Authorization"] = "Basic " + clink::base64_encode("neo4j:" + password);
        o.rw_timeout_ms = 120000;
        http_ = std::make_unique<clink::http_connector::HttpRequest>(std::move(o));
    }
    void run(const std::string& statement) {
        std::string body = "{\"statement\":";
        JsonValue{statement}.serialize_into(body);
        body += "}";
        const auto r = http_->post("/db/neo4j/query/v2", body, "application/json");
        if (r.status == 0 || r.body.find("\"errors\"") != std::string::npos) {
            throw std::runtime_error("seed: " + (r.status == 0 ? r.error : r.body));
        }
    }

private:
    std::unique_ptr<clink::http_connector::HttpRequest> http_;
};

struct Result {
    std::size_t lookups{0};
    double seconds{0};
    std::vector<double> latencies_ms;
};

double percentile(std::vector<double> v, double p) {
    if (v.empty()) {
        return 0;
    }
    std::sort(v.begin(), v.end());
    const auto i = static_cast<std::size_t>(p * static_cast<double>(v.size() - 1));
    return v[i];
}

void print(const char* what, std::uint64_t concurrency, const Result& r) {
    std::printf(
        "%-28s concurrency=%-3llu lookups=%-6zu %9.0f lookups/s  p50=%6.2f ms  p95=%6.2f ms  "
        "p99=%6.2f ms  max=%7.2f ms\n",
        what,
        static_cast<unsigned long long>(concurrency),
        r.lookups,
        static_cast<double>(r.lookups) / r.seconds,
        percentile(r.latencies_ms, 0.50),
        percentile(r.latencies_ms, 0.95),
        percentile(r.latencies_ms, 0.99),
        percentile(r.latencies_ms, 1.0));
    std::fflush(stdout);
}

std::map<std::string, std::string> params(const std::string& endpoint,
                                          const std::string& password,
                                          const std::string& statement,
                                          const std::string& result_types,
                                          std::uint64_t requests) {
    std::string dim_columns = "account";
    std::string dim_types = "account:VARCHAR";
    std::size_t start = 0;
    while (start < result_types.size()) {
        const auto end = std::min(result_types.find(';', start), result_types.size());
        const auto entry = result_types.substr(start, end - start);
        dim_columns += "," + entry.substr(0, entry.find(':'));
        dim_types += ";" + entry;
        start = end + 1;
    }
    return {
        {"probe_alias", "s"},
        {"dim_alias", "d"},
        {"probe_columns", "seq,account"},
        {"dim_columns", dim_columns},
        {"join_type", "left_outer"},
        {"probe_key_column", "account"},
        {"probe_key_type", "VARCHAR"},
        {"dim_key_column", "account"},
        {"dim_column_types", dim_types},
        {"neo4j.endpoint", endpoint},
        {"neo4j.username", "neo4j"},
        {"neo4j.password", password},
        {"neo4j.statement", statement},
        {"neo4j.max_concurrent_requests", std::to_string(requests)},
        {"neo4j.max_in_flight", "64"},
    };
}

std::string key_of(std::size_t i) {
    return "acct-" + std::to_string(i % kAccounts);
}

// `concurrency` threads, each looking up its share of `n` keys one at a time.
Result run_pool(const clink::neo4j::LookupOptions& opts, std::size_t n) {
    clink::neo4j::LookupPool pool(opts, clink::CancelSignal{}, nullptr, 1);
    pool.open();
    const auto threads = static_cast<std::size_t>(opts.max_concurrent_requests);
    std::vector<std::vector<double>> per_thread(threads);
    std::atomic<std::size_t> next{0};
    std::atomic<bool> failed{false};
    const auto start = Clock::now();
    std::vector<std::thread> clients;
    for (std::size_t t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
            for (std::size_t i = next++; i < n; i = next++) {
                auto call = std::make_shared<clink::neo4j::LookupCall>();
                call->key = "{\"$type\":\"String\",\"_value\":\"" + key_of(i * 7919) + "\"}";
                const auto t0 = Clock::now();
                call->queued_at = t0;
                pool.submit(call);
                while (!call->done.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                per_thread[t].push_back(
                    std::chrono::duration<double, std::milli>(Clock::now() - t0).count());
                if (call->error || !call->result.matched) {
                    failed.store(true);
                }
            }
        });
    }
    for (auto& c : clients) {
        c.join();
    }
    Result r;
    r.seconds = std::chrono::duration<double>(Clock::now() - start).count();
    for (auto& v : per_thread) {
        r.latencies_ms.insert(r.latencies_ms.end(), v.begin(), v.end());
    }
    r.lookups = r.latencies_ms.size();
    pool.stop();
    if (failed.load()) {
        throw std::runtime_error("a lookup failed or did not match");
    }
    return r;
}

// Probe rows through the operator, fed one per batch as fast as it takes them;
// latency from the moment a row is handed to the operator to its emission.
Result run_operator(const clink::neo4j::LookupOptions& opts, std::size_t n) {
    clink::neo4j::Neo4jLookupJoin op(opts);
    op.open();
    std::vector<Clock::time_point> fed(n);
    Result r;
    r.latencies_ms.reserve(n);
    clink::Emitter<Row> out{[&](clink::StreamElement<Row> e) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                const auto seq = static_cast<std::size_t>(rec.value().values.at("s_seq").as_int());
                r.latencies_ms.push_back(
                    std::chrono::duration<double, std::milli>(Clock::now() - fed[seq]).count());
            }
        }
        return true;
    }};
    const auto start = Clock::now();
    for (std::size_t i = 0; i < n; ++i) {
        clink::Batch<Row> b;
        Row row;
        row.values["seq"] = JsonValue{static_cast<std::int64_t>(i)};
        row.values["account"] = JsonValue{key_of(i * 7919)};
        b.emplace(std::move(row));
        fed[i] = Clock::now();
        op.process(clink::StreamElement<Row>::data(std::move(b)), out);
    }
    op.flush(out);
    r.seconds = std::chrono::duration<double>(Clock::now() - start).count();
    r.lookups = r.latencies_ms.size();
    op.close();
    return r;
}

}  // namespace

int main(int argc, char** argv) {
    const auto endpoint = env("CLINK_NEO4J_TEST_ENDPOINT");
    const auto password = env("CLINK_NEO4J_TEST_PASSWORD");
    if (endpoint.empty() || password.empty()) {
        std::fprintf(stderr,
                     "set CLINK_NEO4J_TEST_ENDPOINT and CLINK_NEO4J_TEST_PASSWORD (a server "
                     "scripts/neo4j-live.sh's compose file starts)\n");
        return 2;
    }
    const std::size_t n = argc > 1 ? static_cast<std::size_t>(std::stoull(argv[1])) : 5000;
    const std::string label = "BenchAccount_" + std::to_string(::getpid());
    const std::string device = "BenchDevice_" + std::to_string(::getpid());
    Admin admin(endpoint, password);
    admin.run("CREATE CONSTRAINT c_" + label + " IF NOT EXISTS FOR (a:" + label +
              ") REQUIRE a.id IS UNIQUE");
    admin.run("UNWIND range(0, " + std::to_string(kAccounts - 1) + ") AS i CREATE (a:" + label +
              " {id: 'acct-' + toString(i), shared: i % 7, evidence: 'ev-' + toString(i)}) "
              "CREATE (a)-[:USES_DEVICE]->(:" +
              device +
              " {id: 'own-a-' + toString(i)}) "
              "CREATE (a)-[:USES_DEVICE]->(:" +
              device + " {id: 'own-b-' + toString(i)})");
    admin.run("UNWIND range(0, 99) AS d CREATE (:" + device + " {id: 'shared-' + toString(d)})");
    admin.run("MATCH (a:" + label + "), (d:" + device +
              ") WHERE d.id = 'shared-' + toString(toInteger(substring(a.id, 5)) % 100) "
              "CREATE (a)-[:USES_DEVICE]->(d)");
    int rc = 0;
    try {
        const std::string point =
            "MATCH (a:" + label + " {id: $key}) RETURN a.shared AS shared, a.evidence AS evidence";
        const std::string two_hop =
            "MATCH (a:" + label + " {id: $key}) WHERE EXISTS { (a)-[:USES_DEVICE]->(:" + device +
            ") } WITH a, COLLECT { MATCH (a)-[:USES_DEVICE]->(:" + device +
            ")<-[:USES_DEVICE]-(o:" + label +
            ") WHERE o <> a RETURN DISTINCT o.id AS id ORDER BY id LIMIT 1000 } AS others "
            "RETURN COUNT { (a)-[:USES_DEVICE]->(:" +
            device +
            ") } AS devices, size(others) AS shared_accounts, reduce(s = \"\", x IN "
            "others[0..10] | s + CASE WHEN s = \"\" THEN \"\" ELSE \",\" END + x) AS "
            "evidence_accounts";
        std::printf("neo4j lookup baseline: %s, %d accounts, %zu lookups per run\n",
                    endpoint.c_str(),
                    kAccounts,
                    n);
        // Unmeasured: the server plans each statement once and warms its page
        // cache, which the first measured run would otherwise carry.
        for (const auto& [statement, types] :
             {std::pair<std::string, std::string>{point, "shared:BIGINT;evidence:VARCHAR"},
              std::pair<std::string, std::string>{
                  two_hop, "devices:BIGINT;shared_accounts:BIGINT;evidence_accounts:VARCHAR"}}) {
            (void)run_pool(clink::neo4j::parse_lookup_options(
                               params(endpoint, password, statement, types, 4), 0, 1),
                           2000);
        }
        for (const std::uint64_t c : {1, 4, 8, 16}) {
            print(
                "pool, point read",
                c,
                run_pool(clink::neo4j::parse_lookup_options(
                             params(endpoint, password, point, "shared:BIGINT;evidence:VARCHAR", c),
                             0,
                             1),
                         n));
        }
        for (const std::uint64_t c : {1, 4, 8, 16}) {
            print("pool, two-hop device read",
                  c,
                  run_pool(clink::neo4j::parse_lookup_options(
                               params(endpoint,
                                      password,
                                      two_hop,
                                      "devices:BIGINT;shared_accounts:BIGINT;evidence_accounts:"
                                      "VARCHAR",
                                      c),
                               0,
                               1),
                           n));
        }
        for (const std::uint64_t c : {4, 16}) {
            print("operator, two-hop device read",
                  c,
                  run_operator(clink::neo4j::parse_lookup_options(
                                   params(endpoint,
                                          password,
                                          two_hop,
                                          "devices:BIGINT;shared_accounts:BIGINT;evidence_accounts:"
                                          "VARCHAR",
                                          c),
                                   0,
                                   1),
                               n));
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "benchmark failed: %s\n", e.what());
        rc = 1;
    }
    admin.run("MATCH (n) WHERE n:" + label + " OR n:" + device + " DETACH DELETE n");
    admin.run("DROP CONSTRAINT c_" + label + " IF EXISTS");
    return rc;
}
