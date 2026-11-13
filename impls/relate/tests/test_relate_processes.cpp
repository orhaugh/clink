// The sink and a store that another process holds: Relate gives a store one
// writing process, so a sink in a second process is refused at open, and a
// process killed while it holds a store leaves it to the next one. The other
// process is clink_relate_store_holder, spawned and killed by the case.

#include <array>
#include <csignal>
#include <cstdint>
#include <map>
#include <memory>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <sys/wait.h>

#include "clink/config/json.hpp"
#include "clink/sql/row.hpp"

#include "options.hpp"
#include "relate_sink.hpp"
#include "store_reader.hpp"

extern char** environ;

using clink::config::JsonObject;
using clink::config::JsonValue;
using clink::relate::parse_options;
using clink::relate::RelateError;
using clink::relate::RelateSink;
using clink::relate::testing::count;
using clink::relate::testing::TempDir;
using clink::sql::Row;
namespace code = clink::relate::code;

namespace {

// clink_relate_store_holder, holding `store` until it is killed.
class Holder {
public:
    explicit Holder(const std::string& store) {
        std::array<int, 2> in{};
        std::array<int, 2> out{};
        if (::pipe(in.data()) != 0 || ::pipe(out.data()) != 0) {
            throw std::runtime_error("pipe failed");
        }
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_adddup2(&actions, in[0], 0);
        posix_spawn_file_actions_adddup2(&actions, out[1], 1);
        posix_spawn_file_actions_addclose(&actions, in[1]);
        posix_spawn_file_actions_addclose(&actions, out[0]);
        std::string exe = CLINK_RELATE_STORE_HOLDER;
        std::string arg = store;
        std::vector<char*> argv = {exe.data(), arg.data(), nullptr};
        const int rc = ::posix_spawn(&pid_, exe.c_str(), &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        ::close(in[0]);
        ::close(out[1]);
        stdin_ = in[1];
        stdout_ = out[0];
        if (rc != 0) {
            pid_ = -1;
            throw std::runtime_error("could not start " + exe);
        }
        char c = 0;
        while (::read(stdout_, &c, 1) == 1 && c != '\n') {
            first_line_ += c;
        }
    }
    ~Holder() {
        kill();
        ::close(stdin_);
        ::close(stdout_);
    }
    Holder(const Holder&) = delete;
    Holder& operator=(const Holder&) = delete;

    [[nodiscard]] const std::string& first_line() const { return first_line_; }

    // SIGKILL: the holder gets no chance to close the store.
    void kill() {
        if (pid_ <= 0) {
            return;
        }
        ::kill(pid_, SIGKILL);
        int status = 0;
        ::waitpid(pid_, &status, 0);
        pid_ = -1;
    }

private:
    pid_t pid_{-1};
    int stdin_{-1};
    int stdout_{-1};
    std::string first_line_;
};

Row account(std::int64_t id) {
    JsonObject obj;
    obj["id"] = JsonValue{id};
    Row r;
    r.values = clink::sql::row_columns_from_json(std::move(obj));
    return r;
}

std::unique_ptr<RelateSink> sink_on(const std::string& store) {
    return std::make_unique<RelateSink>(parse_options(
        {{"path", store},
         {"statement", "UNWIND $rows AS row MERGE (:Account {id: row.id})"},
         {"schema",
          "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE"},
         {"sql_column_types", "id:BIGINT"},
         {"flush_interval_ms", "0"}},
        0,
        1));
}

}  // namespace

TEST(RelateProcesses, AStoreAnotherProcessHoldsIsRefusedAndIsFreeOnceThatProcessIsKilled) {
    const TempDir dir;
    for (const auto& name : {std::string("held.rl"), std::string("held-directory")}) {
        SCOPED_TRACE(name);
        const auto store = dir.path(name);
        Holder holder(store);
        ASSERT_EQ(holder.first_line(), "holding");
        {
            auto sink = sink_on(store);
            try {
                sink->open();
                ADD_FAILURE() << "the sink opened a store another process holds";
            } catch (const RelateError& e) {
                EXPECT_EQ(e.code(), code::kStoreUnavailable);
                EXPECT_NE(std::string(e.what()).find("another process may have it open"),
                          std::string::npos)
                    << e.what();
            }
            sink->close();
        }
        holder.kill();
        auto sink = sink_on(store);
        sink->open();
        clink::Batch<Row> batch;
        for (std::int64_t id = 0; id < 4; ++id) {
            batch.emplace(account(id));
        }
        sink->on_data(batch);
        sink->flush();
        EXPECT_EQ(count(store, "MATCH (a:Account) RETURN count(a)"), 4);
        sink->close();
    }
}
