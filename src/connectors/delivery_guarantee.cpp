#include "clink/connectors/delivery_guarantee.hpp"

#include <algorithm>
#include <sstream>

namespace clink::connectors {

std::string_view to_string(EndToEndGuarantee g) noexcept {
    switch (g) {
        case EndToEndGuarantee::NoRecoveryGuarantee:
            return "NO_RECOVERY_GUARANTEE";
        case EndToEndGuarantee::AtMostOnceSource:
            return "AT_MOST_ONCE_SOURCE";
        case EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce:
            return "STATE_EXACTLY_ONCE_OUTPUT_AT_LEAST_ONCE";
        case EndToEndGuarantee::EffectivelyOnceRequiresIdempotentKey:
            return "EFFECTIVELY_ONCE_REQUIRES_IDEMPOTENT_KEY";
        case EndToEndGuarantee::EndToEndExactlyOnce:
            return "END_TO_END_EXACTLY_ONCE";
    }
    return "?";
}

namespace {

bool has_option(const PipelineConnector& c, std::string_view opt) {
    return std::find(c.supplied_options.begin(), c.supplied_options.end(), opt) !=
           c.supplied_options.end();
}

}  // namespace

GuaranteeReport analyse_pipeline(const PipelineFacts& facts) {
    GuaranteeReport r;
    auto& reasons = r.reasons;

    // ---- 1. The floor: without checkpointing nothing above has meaning.
    if (!facts.checkpointing_enabled) {
        r.level = EndToEndGuarantee::NoRecoveryGuarantee;
        r.limiting_factor = "checkpointing disabled";
        reasons.emplace_back(
            "checkpointing is not enabled, so there is no point a restart can resume from");
        if (facts.requested.has_value() &&
            strength(*facts.requested) > strength(DeliveryGuarantee::AtMostOnce)) {
            r.rejections.emplace_back(
                std::string("requested ") + std::string(to_string(*facts.requested)) +
                " but checkpointing is disabled; enable periodic checkpointing "
                "(checkpoint_dir + interval) or drop the requested guarantee");
        }
        return r;
    }
    reasons.emplace_back("checkpointing is enabled");

    // ---- 2. State durability caps everything.
    if (!facts.durable_state_backend) {
        reasons.emplace_back(
            "the state backend does not survive a process restart, so operator state is "
            "rebuilt from nothing on recovery");
    } else {
        reasons.emplace_back("the state backend is durable across a process restart");
    }

    // ---- 3. Sources: can the input be re-read from a recorded position?
    std::size_t source_count = 0;
    bool all_sources_replayable = true;
    std::string weakest_source;
    for (const auto& c : facts.connectors) {
        if (!c.is_source) {
            continue;
        }
        ++source_count;
        if (c.declaration_missing) {
            all_sources_replayable = false;
            weakest_source = c.op_type;
            reasons.emplace_back("source '" + c.op_type +
                                 "' has no capability declaration, so its replayability cannot "
                                 "be established and is assumed absent");
            continue;
        }
        const auto* caps = CapabilityRegistry::instance().find(c.connector_name);
        if (caps == nullptr || !caps->replayable || !caps->checkpoint_integrated) {
            all_sources_replayable = false;
            weakest_source = c.op_type;
            reasons.emplace_back("source '" + c.op_type +
                                 "' cannot be replayed from a checkpointed position");
        } else {
            reasons.emplace_back("source '" + c.op_type + "' replays from " +
                                 std::string(to_string(caps->offset_model)));
        }
    }
    if (source_count == 0) {
        reasons.emplace_back("no declared sources in the plan");
    }

    if (!all_sources_replayable) {
        r.level = EndToEndGuarantee::AtMostOnceSource;
        r.limiting_factor = "source '" + weakest_source + "'";
        if (facts.requested.has_value() &&
            strength(*facts.requested) > strength(DeliveryGuarantee::AtMostOnce)) {
            r.rejections.emplace_back(
                std::string("requested ") + std::string(to_string(*facts.requested)) +
                " but source '" + weakest_source +
                "' cannot replay from a checkpointed position; records in flight at the "
                "moment of failure are lost, so no stronger guarantee is achievable");
        }
        return r;
    }

    // ---- 4. Sinks: the weakest sink caps the output guarantee.
    auto weakest = DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    std::string weakest_sink;
    bool any_sink = false;
    bool needs_idempotent_key = false;
    for (const auto& c : facts.connectors) {
        if (c.is_source) {
            continue;
        }
        any_sink = true;
        if (c.declaration_missing) {
            weakest = DeliveryGuarantee::AtLeastOnce;
            weakest_sink = c.op_type;
            reasons.emplace_back("sink '" + c.op_type +
                                 "' has no capability declaration; assuming at-least-once");
            continue;
        }
        const auto* caps = CapabilityRegistry::instance().find(c.connector_name);
        if (caps == nullptr) {
            weakest = DeliveryGuarantee::AtLeastOnce;
            weakest_sink = c.op_type;
            reasons.emplace_back("sink '" + c.op_type +
                                 "' is not in the capability registry; assuming at-least-once");
            continue;
        }
        auto sink_level = caps->delivery;

        // A sink that CLAIMS exactly-once only delivers it when the
        // options that make the mechanism work were actually supplied. A
        // transactional Kafka sink without a transactional.id is not a
        // transactional sink.
        for (const auto& required : caps->required_options_for_exactly_once) {
            if (!has_option(c, required)) {
                reasons.emplace_back("sink '" + c.op_type + "' declares " +
                                     std::string(to_string(caps->delivery)) + " but option '" +
                                     required +
                                     "' was not supplied, so it degrades to at-least-once");
                sink_level = DeliveryGuarantee::AtLeastOnce;
                break;
            }
        }
        if (sink_level == DeliveryGuarantee::EffectivelyOnceIdempotent) {
            needs_idempotent_key = true;
            if (!caps->idempotency_key_option.empty() &&
                !has_option(c, caps->idempotency_key_option)) {
                reasons.emplace_back("sink '" + c.op_type + "' is idempotent-upsert but '" +
                                     caps->idempotency_key_option +
                                     "' was not supplied, so duplicates would not collapse");
                sink_level = DeliveryGuarantee::AtLeastOnce;
            } else {
                r.warnings.emplace_back(
                    "sink '" + c.op_type +
                    "' collapses duplicates only if the configured key uniquely identifies a "
                    "row; clink cannot verify that");
            }
        }
        if (strength(sink_level) < strength(weakest)) {
            weakest = sink_level;
            weakest_sink = c.op_type;
        }
        reasons.emplace_back("sink '" + c.op_type + "' provides " +
                             std::string(to_string(sink_level)));
    }

    if (!any_sink) {
        // No sink means no external output to be exactly-once about. State
        // is still exactly-once if it is durable.
        r.level = facts.durable_state_backend ? EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce
                                              : EndToEndGuarantee::NoRecoveryGuarantee;
        r.limiting_factor = facts.durable_state_backend ? "" : "non-durable state backend";
        reasons.emplace_back("no sinks in the plan, so there is no external output to guarantee");
        return r;
    }

    // ---- 5. Combine.
    if (!facts.durable_state_backend) {
        r.level = EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce;
        r.limiting_factor = "non-durable state backend";
        r.warnings.emplace_back(
            "state is held in a backend that does not survive a restart, so the "
            "'state exactly once' half of this label applies only within a single process "
            "lifetime");
    } else {
        switch (weakest) {
            case DeliveryGuarantee::ExactlyOnceTwoPhaseCommit:
            case DeliveryGuarantee::ExactlyOnceAtomicPublish:
                r.level = EndToEndGuarantee::EndToEndExactlyOnce;
                break;
            case DeliveryGuarantee::EffectivelyOnceIdempotent:
                r.level = EndToEndGuarantee::EffectivelyOnceRequiresIdempotentKey;
                r.limiting_factor = "sink '" + weakest_sink + "'";
                break;
            case DeliveryGuarantee::AtLeastOnce:
                r.level = EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce;
                r.limiting_factor = "sink '" + weakest_sink + "'";
                break;
            case DeliveryGuarantee::AtMostOnce:
            case DeliveryGuarantee::NoDurableRestartGuarantee:
                r.level = EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce;
                r.limiting_factor = "sink '" + weakest_sink + "'";
                r.warnings.emplace_back("sink '" + weakest_sink +
                                        "' offers no restart guarantee of its own");
                break;
        }
    }
    (void)needs_idempotent_key;

    // ---- 6. Determinism. Orthogonal to delivery: a job can commit each
    // record exactly once and still emit DIFFERENT records on replay.
    if (!facts.determinism.deterministic()) {
        std::string what;
        for (std::size_t i = 0; i < facts.determinism.sources_of_nondeterminism.size(); ++i) {
            if (i > 0) {
                what += ", ";
            }
            what += facts.determinism.sources_of_nondeterminism[i];
        }
        r.warnings.emplace_back(
            "this job contains non-deterministic operations (" +
            (what.empty() ? std::string("unspecified") : what) +
            "), so a replay from a checkpoint can produce different output than the original "
            "run even where each record is committed once");
        if (r.level == EndToEndGuarantee::EndToEndExactlyOnce) {
            reasons.emplace_back(
                "delivery is exactly-once, but the OUTPUT is not reproducible across a replay");
        }
    }

    // ---- 7. Honour the request.
    if (facts.requested.has_value()) {
        const auto want = *facts.requested;
        const bool want_exactly_once = want == DeliveryGuarantee::ExactlyOnceTwoPhaseCommit ||
                                       want == DeliveryGuarantee::ExactlyOnceAtomicPublish;
        if (want_exactly_once && r.level != EndToEndGuarantee::EndToEndExactlyOnce) {
            r.rejections.emplace_back(std::string("requested ") + std::string(to_string(want)) +
                                      " but the strongest guarantee this pipeline can provide is " +
                                      std::string(to_string(r.level)) +
                                      (r.limiting_factor.empty()
                                           ? std::string{}
                                           : (", limited by " + r.limiting_factor)));
        }
        if (want == DeliveryGuarantee::EffectivelyOnceIdempotent &&
            r.level == EndToEndGuarantee::StateExactlyOnceOutputAtLeastOnce) {
            r.rejections.emplace_back(
                "requested effectively-once via idempotent upsert, but no sink in this "
                "pipeline performs keyed upserts");
        }
    }
    return r;
}

std::string GuaranteeReport::render_text() const {
    std::ostringstream os;
    os << "delivery guarantee: " << to_string(level) << "\n";
    if (!limiting_factor.empty()) {
        os << "  limited by: " << limiting_factor << "\n";
    }
    for (const auto& reason : reasons) {
        os << "  - " << reason << "\n";
    }
    for (const auto& w : warnings) {
        os << "  ! " << w << "\n";
    }
    for (const auto& rej : rejections) {
        os << "  REJECTED: " << rej << "\n";
    }
    return os.str();
}

std::string GuaranteeReport::render_json() const {
    const auto q = [](std::string_view s) {
        std::string out = "\"";
        for (const char c : s) {
            if (c == '"' || c == '\\') {
                out += '\\';
                out += c;
            } else if (c == '\n') {
                out += "\\n";
            } else {
                out += c;
            }
        }
        return out + "\"";
    };
    const auto arr = [&](const std::vector<std::string>& v) {
        std::string out = "[";
        for (std::size_t i = 0; i < v.size(); ++i) {
            if (i > 0) {
                out += ",";
            }
            out += q(v[i]);
        }
        return out + "]";
    };
    std::string out = "{";
    out += "\"level\":" + q(to_string(level));
    out += ",\"limiting_factor\":" + q(limiting_factor);
    out += ",\"reasons\":" + arr(reasons);
    out += ",\"warnings\":" + arr(warnings);
    out += ",\"rejections\":" + arr(rejections);
    out += ",\"acceptable\":" + std::string(acceptable() ? "true" : "false");
    out += "}";
    return out;
}

}  // namespace clink::connectors
