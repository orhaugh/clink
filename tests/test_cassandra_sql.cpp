// The Cassandra sinks through the SQL frontend, run on an embedded engine
// against the server scripts/cassandra-live.sh starts: an append query into
// connector='cassandra', and retracting queries into mode='upsert' whose deletes
// and updates the table must end up holding exactly. Each case reads the table
// back with the driver, row by row. Skipped without
// CLINK_CASSANDRA_TEST_CONTACT_POINTS.

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cassandra/install.hpp"
#include "clink/embed/embedded_engine.hpp"
#include "clink/plugin/plugin.hpp"

#include "cassandra_live.hpp"

namespace {

using namespace std::chrono_literals;
namespace live = clink::cassandra::live;

void ensure_installed() {
    static const bool done = [] {
        {
            clink::embed::EngineOptions opts;
            std::ostringstream sink;
            opts.out = &sink;
            opts.err = &sink;
            const clink::embed::EmbeddedEngine first{std::move(opts)};
        }
        clink::plugin::PluginRegistry reg;
        clink::cassandra::install(reg);
        return true;
    }();
    (void)done;
}

class CassandraSqlLive : public ::testing::Test {
protected:
    void SetUp() override {
        if (!live::configured()) {
            GTEST_SKIP() << "set CLINK_CASSANDRA_TEST_CONTACT_POINTS (scripts/cassandra-live.sh "
                            "does)";
        }
        ensure_installed();
        static int counter = 0;
        suffix_ = std::to_string(::getpid()) + "_" + std::to_string(++counter);
        session_ = std::make_unique<live::Session>();
    }

    void TearDown() override {
        if (engine_) {
            engine_->cancel_all();
            (void)engine_->await_all();
            engine_.reset();
        }
        if (!input_.empty()) {
            std::filesystem::remove(input_);
        }
    }

    void write_input(const std::string& lines) {
        input_ =
            std::filesystem::temp_directory_path() / ("clink_cassandra_sql_" + suffix_ + ".json");
        std::ofstream(input_) << lines;
    }

    // The sink table's WITH clause, pointed at the live server.
    std::string with(const std::string& table, const std::string& extra = "") const {
        return "WITH (connector='cassandra', contact_points='" + live::contact_points() +
               "', port='" + std::to_string(live::port()) + "', keyspace='" + live::kKeyspace +
               "', table='" + table + "'" + extra + ")";
    }

    // Runs `sql` to the end of its bounded input; true when it ended cleanly.
    bool run(const std::string& sql) {
        clink::embed::EngineOptions opts;
        opts.out = &out_;
        opts.err = &err_;
        engine_ = std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts));
        if (engine_->execute_script(sql) != 0) {
            return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + 120s;
        return engine_->await_all([&] { return std::chrono::steady_clock::now() >= deadline; });
    }

    std::string errors() const {
        std::string all = err_.str();
        if (engine_) {
            for (const auto& e : engine_->errors()) {
                all += "\n" + e;
            }
        }
        return all;
    }

    // Each row of the table as "col=value;..." in column-name order.
    std::set<std::string> rows(const std::string& table) {
        std::set<std::string> out;
        for (const auto& r : session_->rows(std::string{live::kKeyspace} + "." + table)) {
            std::map<std::string, std::string> cols;
            for (const auto& [k, v] : r.as_object()) {
                cols[k] = v.is_string() ? v.as_string() : v.serialize(0);
            }
            std::string line;
            for (const auto& [k, v] : cols) {
                line += (line.empty() ? "" : ";") + k + "=" + v;
            }
            out.insert(line);
        }
        return out;
    }

    std::string suffix_;
    std::filesystem::path input_;
    std::ostringstream out_;
    std::ostringstream err_;
    std::unique_ptr<live::Session> session_;
    std::unique_ptr<clink::embed::EmbeddedEngine> engine_;
};

}  // namespace

TEST_F(CassandraSqlLive, AnAppendQueryWritesEveryRow) {
    const std::string t = "sql_append_" + suffix_;
    live::fresh_table(*session_, t, "id bigint PRIMARY KEY, name text, score double");
    write_input(R"({"id":1,"name":"ann","score":1.5})"
                "\n"
                R"({"id":2,"name":"bo","score":-2.25})"
                "\n"
                R"({"id":3,"name":"cy","score":0})"
                "\n");
    ASSERT_TRUE(
        run("CREATE TABLE src (id BIGINT, name VARCHAR, score DOUBLE) WITH "
            "(connector='file', format='json', path='" +
            input_.string() +
            "'); "
            "CREATE TABLE dst (id BIGINT, name VARCHAR, score DOUBLE) " +
            with(t) + "; INSERT INTO dst SELECT id, name, score FROM src;"))
        << errors();
    EXPECT_EQ(rows(t),
              (std::set<std::string>{
                  "id=1;name=ann;score=1.5", "id=2;name=bo;score=-2.25", "id=3;name=cy;score=0"}))
        << errors();
}

// A top row per group into mode='upsert': each group's top row replaces by key,
// and a row that leaves the top is deleted by key; a GROUP BY's counts replace by
// their group. The tables end as the queries' answers.
TEST_F(CassandraSqlLive, RetractingQueriesMaintainTheTableByItsKey) {
    const std::string top = "sql_top_" + suffix_;
    const std::string tot = "sql_tot_" + suffix_;
    live::fresh_table(*session_, top, "g text, k text, v bigint, PRIMARY KEY ((g), k)");
    live::fresh_table(*session_, tot, "g text PRIMARY KEY, n bigint, best bigint");
    write_input(R"({"g":"a","k":"a1","v":1})"
                "\n"
                R"({"g":"a","k":"a2","v":5})"
                "\n"
                R"({"g":"b","k":"b1","v":3})"
                "\n"
                R"({"g":"a","k":"a3","v":4})"
                "\n"
                R"({"g":"b","k":"b2","v":9})"
                "\n"
                R"({"g":"c","k":"c1","v":7})"
                "\n");
    ASSERT_TRUE(
        run("CREATE TABLE t (g VARCHAR, k VARCHAR, v BIGINT) WITH (connector='file', "
            "format='json', path='" +
            input_.string() +
            "'); "
            "CREATE TABLE top_rows (g VARCHAR, k VARCHAR, v BIGINT) " +
            with(top, ", mode='upsert', primary_key='g,k', max_buffered_records='1'") +
            "; CREATE TABLE totals (g VARCHAR PRIMARY KEY, n BIGINT, best BIGINT) " +
            with(tot, ", mode='upsert'") +
            "; INSERT INTO top_rows SELECT g, k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION "
            "BY g ORDER BY v DESC) AS rn FROM t) s WHERE rn = 1; "
            "INSERT INTO totals SELECT g, COUNT(*) AS n, MAX(v) AS best FROM t GROUP BY g;"))
        << errors();
    EXPECT_EQ(rows(top), (std::set<std::string>{"g=a;k=a2;v=5", "g=b;k=b2;v=9", "g=c;k=c1;v=7"}))
        << errors();
    EXPECT_EQ(rows(tot),
              (std::set<std::string>{"best=5;g=a;n=3", "best=9;g=b;n=2", "best=7;g=c;n=1"}))
        << errors();
}

// The planner keys the upsert sink by the SQL PRIMARY KEY, and the sink checks
// at open that it is the Cassandra table's whole key: a SQL key naming only the
// partition key stops the job before anything is written.
TEST_F(CassandraSqlLive, ASqlKeyThatIsNotTheTablesWholeKeyStopsTheJob) {
    const std::string t = "sql_partkey_" + suffix_;
    live::fresh_table(*session_, t, "g text, k text, v bigint, PRIMARY KEY ((g), k)");
    write_input(R"({"g":"a","k":"a1","v":1})"
                "\n");
    EXPECT_FALSE(
        run("CREATE TABLE src (g VARCHAR, k VARCHAR, v BIGINT) WITH (connector='file', "
            "format='json', path='" +
            input_.string() +
            "'); "
            "CREATE TABLE dst (g VARCHAR PRIMARY KEY, k VARCHAR, v BIGINT) " +
            with(t, ", mode='upsert'") + "; INSERT INTO dst SELECT g, k, v FROM src;"));
    EXPECT_NE(errors().find("must name the table's whole primary key"), std::string::npos)
        << errors();
    EXPECT_TRUE(rows(t).empty());
}
