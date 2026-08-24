// QUAL-11's workload: a typed plugin job whose keyed-state VALUE TYPE
// evolves across the campaign boundary.
//
// One source file builds three .so variants at ONE engine revision (the
// plugin ABI fingerprint is the engine's git SHA, so building the pair
// from different commits would trip the gate on its own; a compile
// define keeps all three at the same fingerprint):
//
//   (default)            v1: AccountState{count,sum}, schema version 1.
//   -DQUAL11_SCHEMA_V2   v2: AccountState{count,sum,vmin,vmax}, version 2,
//                        with the v1->v2 migration REGISTERED. vmin/vmax
//                        seed as the empty-range sentinels (INT64_MAX /
//                        INT64_MIN), so the migration is a pure,
//                        oracle-predictable bytes->bytes function: count
//                        and sum carried, the new fields provably empty.
//   -DQUAL11_SCHEMA_V2 -DQUAL11_BROKEN
//                        v2 WITHOUT the migration - the campaign's
//                        negative control. The pre-deploy check must
//                        REFUSE this .so; a check that approves it
//                        approves anything.
//
// Pipeline: Kafka JSON events {"k":"...","amount":N,...} -> key by k ->
// keyed process folding AccountState (count += 1, sum += amount; v2 also
// folds vmin/vmax) -> per-event JSON emit -> Kafka sink. The sink stream
// is an update stream (one row per event, last-per-key = the key's final
// state), so the harness folds the output topic exactly like the SQL
// campaigns' verification does.
//
// The state slot is uid-pinned ("account-agg" -> operator_id_from_uid)
// because restore, the version stamp, and the pre-deploy check all key
// on it - an unpinned stateful operator cannot be restored at all.
// A SECOND, unchanged slot ("event_counter", int64, implicit version 1
// in both variants) proves an untouched slot restores identically
// beside a migrating one.
//
// Broker endpoints come from the QUAL11_BROKERS environment variable at
// build_fn time (the campaign exports it before submit; the same .so
// then runs on the workers, where the value must match - the campaign
// sets it in the container environment).

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "clink/api/kafka_builders.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/kafka/install.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/schema_version.hpp"

namespace {

// --- the evolving state type ------------------------------------------------
//
// Fixed-width little-endian layout, hand-encoded so the v1 and v2 byte
// shapes are exact and the migration is a pure function of bytes:
//   v1: [count i64][sum i64]                      (16 bytes)
//   v2: [count i64][sum i64][vmin i64][vmax i64]  (32 bytes)

struct AccountState {
    std::int64_t count{0};
    std::int64_t sum{0};
#ifdef QUAL11_SCHEMA_V2
    std::int64_t vmin{std::numeric_limits<std::int64_t>::max()};
    std::int64_t vmax{std::numeric_limits<std::int64_t>::min()};
#endif
};

void put_i64(std::vector<std::byte>& out, std::int64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::byte>((static_cast<std::uint64_t>(v) >> (8 * i)) & 0xff));
    }
}

std::int64_t get_i64(const std::byte* p) {
    std::uint64_t u = 0;
    for (int i = 0; i < 8; ++i) {
        u |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(p[i])) << (8 * i);
    }
    return static_cast<std::int64_t>(u);
}

clink::Codec<AccountState> account_state_codec() {
    clink::Codec<AccountState> c;
    c.encode = [](const AccountState& s) {
        std::vector<std::byte> out;
        put_i64(out, s.count);
        put_i64(out, s.sum);
#ifdef QUAL11_SCHEMA_V2
        put_i64(out, s.vmin);
        put_i64(out, s.vmax);
#endif
        return out;
    };
    c.decode = [](clink::Codec<AccountState>::BytesView in) -> std::optional<AccountState> {
#ifdef QUAL11_SCHEMA_V2
        constexpr std::size_t kWidth = 32;
#else
        constexpr std::size_t kWidth = 16;
#endif
        if (in.size() < kWidth) {
            return std::nullopt;  // wrong-shape bytes must fail loudly, not misread
        }
        AccountState s;
        s.count = get_i64(in.data());
        s.sum = get_i64(in.data() + 8);
#ifdef QUAL11_SCHEMA_V2
        s.vmin = get_i64(in.data() + 16);
        s.vmax = get_i64(in.data() + 24);
#endif
        return s;
    };
    return c;
}

// --- tiny JSON field readers (inputs are the generator's flat objects) ------

std::string json_string_field(const std::string& s, const std::string& key) {
    const auto k = "\"" + key + "\":\"";
    const auto p = s.find(k);
    if (p == std::string::npos) {
        return {};
    }
    const auto start = p + k.size();
    const auto end = s.find('"', start);
    return end == std::string::npos ? std::string{} : s.substr(start, end - start);
}

std::int64_t json_int_field(const std::string& s, const std::string& key) {
    const auto k = "\"" + key + "\":";
    const auto p = s.find(k);
    if (p == std::string::npos) {
        return 0;
    }
    return std::strtoll(s.c_str() + p + k.size(), nullptr, 10);
}

// FNV-1a over the account key: the routing hash, stable across variants.
std::int64_t route_hash(const std::string& k) {
    std::uint64_t h = 1469598103934665603ULL;
    for (const char ch : k) {
        h ^= static_cast<std::uint8_t>(ch);
        h *= 1099511628211ULL;
    }
    return static_cast<std::int64_t>(h);
}

// --- the keyed fold -----------------------------------------------------------

class AccountFold final
    : public clink::KeyedProcessFunction<std::int64_t, std::string, std::string> {
public:
    void open(clink::RuntimeContext& ctx) override {
        accounts_ = std::make_unique<clink::KeyedState<std::string, AccountState>>(
            ctx.keyed_state<std::string, AccountState>(
                "account_state", clink::string_codec(), account_state_codec()));
        events_ = std::make_unique<clink::KeyedState<std::string, std::int64_t>>(
            ctx.keyed_state<std::string, std::int64_t>(
                "event_counter", clink::string_codec(), clink::int64_codec()));
    }

    void process_element(const std::string& line,
                         clink::ProcessFunctionContext<std::string>& /*ctx*/,
                         clink::Collector<std::string>& out) override {
        const auto key = json_string_field(line, "k");
        if (key.empty()) {
            return;  // not a data record; the generator never emits these
        }
        const auto amount = json_int_field(line, "amount");
        auto st = accounts_->get(key).value_or(AccountState{});
        st.count += 1;
        st.sum += amount;
#ifdef QUAL11_SCHEMA_V2
        st.vmin = std::min(st.vmin, amount);
        st.vmax = std::max(st.vmax, amount);
#endif
        accounts_->put(key, st);
        events_->put(key, events_->get(key).value_or(0) + 1);

        std::string row = "{\"k\":\"" + key + "\",\"n\":" + std::to_string(st.count) +
                          ",\"sum\":" + std::to_string(st.sum);
#ifdef QUAL11_SCHEMA_V2
        // The empty-range sentinels never reach the sink: a key is only
        // emitted on an event, which folds vmin/vmax first.
        row += ",\"vmin\":" + std::to_string(st.vmin) + ",\"vmax\":" + std::to_string(st.vmax);
        row += ",\"v\":2}";
#else
        row += ",\"v\":1}";
#endif
        out.collect(row);
    }

    std::string name() const override { return "account_fold"; }

private:
    std::unique_ptr<clink::KeyedState<std::string, AccountState>> accounts_;
    std::unique_ptr<clink::KeyedState<std::string, std::int64_t>> events_;
};

// --- the job -----------------------------------------------------------------

void define_job(clink::api::Pipeline& pipeline) {
    // The .so is dlopened RTLD_LOCAL with clink statically linked, so the
    // kafka factories must be installed into THIS job's registry - the
    // host's registrations are a different instance.
    clink::kafka::install(pipeline.registry());
#ifdef QUAL11_SCHEMA_V2
    constexpr std::uint32_t kAccountVersion = 2;
#ifndef QUAL11_BROKEN
    // The v1->v2 migration: count and sum carried byte-for-byte, the new
    // range fields seeded EMPTY (min=+inf sentinel, max=-inf sentinel). A
    // pure function of the input bytes - the campaign's independent
    // migration-effect check re-applies exactly this to the v1 savepoint
    // and diffs against the restored state.
    clink::StateMigrationRegistry::global().register_migration(
        "account_state", 1, 2, [](std::span<const std::byte> in) {
            std::vector<std::byte> out(in.begin(), in.end());
            put_i64(out, std::numeric_limits<std::int64_t>::max());
            put_i64(out, std::numeric_limits<std::int64_t>::min());
            return out;
        });
#endif
#else
    constexpr std::uint32_t kAccountVersion = 1;
#endif

    const char* brokers_env = std::getenv("QUAL11_BROKERS");
    const std::string brokers = brokers_env != nullptr ? brokers_env : "localhost:9092";

    auto source = clink::api::KafkaTextSource::builder()
                      .brokers(brokers)
                      .topic("qual11_in")
                      .group_id("qual11-job")
                      .auto_offset_reset("earliest")
                      .build();
    auto sink =
        clink::api::KafkaTextSink::builder().brokers(brokers).topic("qual11_out").build();

    pipeline.source<std::string>(source, "qual11-source")
        .key_by([](const std::string& line) { return route_hash(json_string_field(line, "k")); })
        .process<std::string>(std::make_shared<AccountFold>(), "account-fold")
        .uid("account-agg")
        .sink(sink, "qual11-sink");

    pipeline.expect_state_version("account-agg", "account_state", kAccountVersion);
}

}  // namespace

#ifdef QUAL11_BROKEN
CLINK_REGISTER_JOB("qual11-account-v2-broken",
                   "2.0-broken",
                   "QUAL-11 negative control: v2 schema, migration NOT registered",
                   define_job);
#elif defined(QUAL11_SCHEMA_V2)
CLINK_REGISTER_JOB("qual11-account-v2",
                   "2.0",
                   "QUAL-11 evolved job: AccountState v2 (count,sum,vmin,vmax) + v1->v2 migration",
                   define_job);
#else
CLINK_REGISTER_JOB("qual11-account-v1",
                   "1.0",
                   "QUAL-11 baseline job: AccountState v1 (count,sum)",
                   define_job);
#endif
