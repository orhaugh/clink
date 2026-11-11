// The state-inspection example's job: per-account keyed state from a Kafka
// topic, on a cluster, at a parallelism above one.
//
// Each record is "<account>,<kind>,<amount>", the account a positive integer,
// the kind deposit, withdraw or close, the amount a whole number. The
// operator keeps two keyed-state slots per account, keyed by the account as
// an int64 (the built-in int64 codec: 8 bytes, little-endian):
//
//   count    how many deposits and withdrawals it has seen (int64)
//   balance  deposits less withdrawals (int64); in version 2 of the job, in
//            hundredths, with a migration from version 1
//
// A close removes both. The source's positions are its own operator state.
// Nothing here is needed to read the state back: the example decodes it from
// a checkpoint dataset with its own knowledge of these two codecs.
//
// Environment, read in every process that builds the job:
//   CLINK_SI_BROKERS      bootstrap servers (required)
//   CLINK_SI_TOPIC        the topic (required)
//   CLINK_SI_PARALLELISM  the source's and the operator's parallelism (2)
//   CLINK_SI_SCHEMA       1 or 2: the balance slot's version (1)

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/job/register_job.hpp"
#include "clink/kafka/install.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/schema_version.hpp"

namespace state_inspection {

struct Event {
    std::int64_t account{0};
    std::string kind;
    std::int64_t amount{0};
};

// A record that does not parse is not an event: the job skips it.
std::optional<Event> parse(const std::string& s) {
    const auto c1 = s.find(',');
    const auto c2 = c1 == std::string::npos ? c1 : s.find(',', c1 + 1);
    if (c2 == std::string::npos) {
        return std::nullopt;
    }
    try {
        Event e;
        e.account = std::stoll(s.substr(0, c1));
        e.kind = s.substr(c1 + 1, c2 - c1 - 1);
        e.amount = std::stoll(s.substr(c2 + 1));
        if (e.account <= 0 || (e.kind != "deposit" && e.kind != "withdraw" && e.kind != "close")) {
            return std::nullopt;
        }
        return e;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

class Accounts final : public clink::Operator<std::string, std::string> {
public:
    explicit Accounts(std::int64_t scale) : scale_(scale) {}

    void open() override {
        if (this->runtime() == nullptr || !this->runtime()->has_state_backend()) {
            throw std::runtime_error("state_inspection.accounts needs a state backend");
        }
        count_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "count", clink::int64_codec(), clink::int64_codec()));
        balance_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "balance", clink::int64_codec(), clink::int64_codec()));
    }

    void process(const clink::StreamElement<std::string>& el,
                 clink::Emitter<std::string>& out) override {
        if (!el.is_data()) {
            if (el.is_watermark()) {
                this->on_watermark(el.as_watermark(), out);
            } else {
                this->on_barrier(el.as_barrier(), out);
            }
            return;
        }
        for (const auto& rec : el.as_data()) {
            const auto e = parse(rec.value());
            if (!e) {
                continue;
            }
            if (e->kind == "close") {
                count_->erase(e->account);
                balance_->erase(e->account);
                continue;
            }
            const auto delta = (e->kind == "deposit" ? e->amount : -e->amount) * scale_;
            count_->put(e->account, count_->get(e->account).value_or(0) + 1);
            balance_->put(e->account, balance_->get(e->account).value_or(0) + delta);
        }
    }

    std::string name() const override { return "state_inspection.accounts"; }

private:
    std::int64_t scale_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> count_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> balance_;
};

class Discard final : public clink::Sink<std::string> {
public:
    void on_data(const clink::Batch<std::string>& /*batch*/) override {}
    void on_watermark(clink::Watermark /*wm*/) override {}
    void on_barrier(clink::CheckpointBarrier /*barrier*/) override {}
};

std::string env_or(const char* name, const std::string& fallback) {
    if (const char* p = std::getenv(name); p != nullptr && *p != '\0') {
        return p;
    }
    return fallback;
}

void define_job(clink::api::Pipeline& pipeline) {
    clink::cluster::ensure_built_ins_registered();
    clink::kafka::install(pipeline.registry());
    const auto parallelism =
        static_cast<std::uint32_t>(std::stoul(env_or("CLINK_SI_PARALLELISM", "2")));
    const auto schema = std::stoi(env_or("CLINK_SI_SCHEMA", "1"));
    if (schema != 1 && schema != 2) {
        throw std::runtime_error("CLINK_SI_SCHEMA must be 1 or 2");
    }

    auto& reg = pipeline.registry();
    reg.register_operator<std::string, std::string>(
        "state_inspection.accounts", [schema](const clink::plugin::BuildContext&) {
            return std::make_shared<Accounts>(schema == 2 ? 100 : 1);
        });
    reg.register_sink<std::string>(
        "state_inspection.discard",
        [](const clink::plugin::BuildContext&) { return std::make_shared<Discard>(); });
    reg.register_key_extractor<std::string>("state_inspection.by_account",
                                            [](const std::string& s) -> std::int64_t {
                                                const auto e = parse(s);
                                                return e ? e->account : 0;
                                            });
    if (schema == 2) {
        // Version 2 keeps the balance in hundredths: a restored version-1
        // balance is multiplied by 100.
        clink::StateMigrationRegistry::global().register_migration(
            "balance", 1, 2, [](std::span<const std::byte> in) {
                const auto v = clink::int64_codec().decode(in).value_or(0) * 100;
                return clink::int64_codec().encode(v);
            });
    }

    pipeline.set_parallelism(parallelism);
    clink::api::SourceDescriptor src;
    src.op_type = "kafka_source_string";
    src.channel_type = "string";
    src.params["brokers"] = env_or("CLINK_SI_BROKERS", "");
    src.params["topic"] = env_or("CLINK_SI_TOPIC", "");
    src.params["group_id"] = "state-inspection";
    src.params["auto_offset_reset"] = "earliest";
    auto source = pipeline.source<std::string>(src, "source");
    source.uid("si-source");

    auto accounts = source.key_by("state_inspection.by_account")
                        .process<std::string>("state_inspection.accounts", {}, "accounts");
    accounts.uid("si-accounts").rescalable(1, 4);
    pipeline.expect_state_version("si-accounts", "count", 1, "count");
    pipeline.expect_state_version(
        "si-accounts", "balance", static_cast<std::uint32_t>(schema), "balance");

    pipeline.set_parallelism(1);
    clink::api::SinkDescriptor sink;
    sink.op_type = "state_inspection.discard";
    sink.channel_type = "string";
    accounts.sink(sink, "sink");
}

}  // namespace state_inspection

CLINK_REGISTER_JOB("state-inspection-accounts",
                   "1.0",
                   "per-account count and balance from Kafka, for the state-inspection example",
                   state_inspection::define_job);
