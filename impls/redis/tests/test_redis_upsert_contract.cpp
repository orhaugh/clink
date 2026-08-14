// Live instantiation of the public upsert contract suite against a real
// Redis: idempotency-key collapse on the sink's key-per-row model (the
// stored value is the whole row JSON at key_prefix + key + 0x1f). Self-
// skips without CLINK_REDIS_TEST_URL, same convention as the other live
// tests in this directory.

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/redis/redis_client.hpp"
#include "clink/redis/redis_upsert_sink.hpp"
#include "clink/test/upsert_contract.hpp"

namespace {

using clink::redis::Connection;
using clink::redis::ConnectOptions;
using clink::redis::RedisUpsertSink;
using clink::redis::RedisUpsertSinkOptions;
using clink::redis::Reply;
using clink::test::UpsertContractFixture;
using clink::test::UpsertState;

bool redis_configured() {
    return std::getenv("CLINK_REDIS_TEST_URL") != nullptr;
}

ConnectOptions redis_conn() {
    std::string url = std::getenv("CLINK_REDIS_TEST_URL");
    if (url.rfind("redis://", 0) == 0) {
        url = url.substr(8);
    }
    ConnectOptions o;
    if (const auto c = url.find(':'); c != std::string::npos) {
        o.host = url.substr(0, c);
        o.port = static_cast<std::uint16_t>(std::stoi(url.substr(c + 1)));
    } else {
        o.host = url;
    }
    return o;
}

std::string row_json(int id, const std::string& val) {
    return "{\"id\":" + std::to_string(id) + ",\"val\":\"" + val + "\"}";
}

// Every (key, stored-row) pair under `prefix`. The sink's key layout is
// prefix + <pk values> + 0x1f separators; for the single-int-PK fixture
// that is prefix + id + 0x1f.
UpsertState rows_under(const std::string& prefix) {
    UpsertState out;
    Connection c{redis_conn()};
    Reply keys = c.command({"KEYS", prefix + "*"});
    if (keys.get() == nullptr) {
        return out;
    }
    for (std::size_t i = 0; i < keys->elements; ++i) {
        const std::string key(keys->element[i]->str, keys->element[i]->len);
        Reply v = c.command({"GET", key});
        if (v.is_nil() || v.get() == nullptr || v->str == nullptr) {
            continue;
        }
        std::string id = key.substr(prefix.size());
        while (!id.empty() && id.back() == '\x1f') {
            id.pop_back();
        }
        out.emplace_back(id, std::string(v->str, v->len));
    }
    return out;
}

void wipe_prefix(const std::string& prefix) {
    Connection c{redis_conn()};
    Reply keys = c.command({"KEYS", prefix + "*"});
    if (keys.get() == nullptr) {
        return;
    }
    for (std::size_t i = 0; i < keys->elements; ++i) {
        c.command({"DEL", std::string(keys->element[i]->str, keys->element[i]->len)});
    }
}

struct RedisUpsertContract {
    using Value = std::string;
    static constexpr std::string_view kCapabilityName = "redis_upsert";

    static bool available() { return redis_configured(); }

    static UpsertContractFixture<std::string> make(const std::filesystem::path& dir) {
        const std::string prefix = "contract:" + dir.filename().string() + ":";
        wipe_prefix(prefix);

        UpsertContractFixture<std::string> fx;
        fx.records = {row_json(1, "a"), row_json(2, "b"), row_json(3, "c")};
        fx.updated_records = {row_json(1, "a2"), row_json(2, "b2"), row_json(3, "c2")};
        // Redis stores the WHOLE row as the value, so the expected state
        // pairs the key with the row JSON rather than a bare column.
        fx.expected = {{"1", row_json(1, "a")}, {"2", row_json(2, "b")}, {"3", row_json(3, "c")}};
        fx.expected_updated = {
            {"1", row_json(1, "a2")}, {"2", row_json(2, "b2")}, {"3", row_json(3, "c2")}};
        fx.fresh = [prefix] {
            RedisUpsertSinkOptions o;
            o.conn = redis_conn();
            o.key_columns = {"id"};
            o.key_prefix = prefix;
            return std::make_shared<RedisUpsertSink>(o);
        };
        fx.state = [prefix] { return rows_under(prefix); };
        return fx;
    }
};

}  // namespace

namespace clink::test {

INSTANTIATE_TYPED_TEST_SUITE_P(RedisUpsert, UpsertContractSuite, RedisUpsertContract);

}  // namespace clink::test
