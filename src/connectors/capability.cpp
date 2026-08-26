#include "clink/connectors/capability.hpp"

#include <atomic>
#include <sstream>

#include "clink/core/allocator_info.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/plugin/abi_version.hpp"
#include "clink/state/checkpoint_integrity.hpp"

namespace clink::connectors {

std::string_view to_string(DeliveryGuarantee g) noexcept {
    switch (g) {
        case DeliveryGuarantee::AtMostOnce:
            return "at_most_once";
        case DeliveryGuarantee::AtLeastOnce:
            return "at_least_once";
        case DeliveryGuarantee::EffectivelyOnceIdempotent:
            return "effectively_once_idempotent";
        case DeliveryGuarantee::ExactlyOnceAtomicPublish:
            return "exactly_once_atomic_publish";
        case DeliveryGuarantee::ExactlyOnceTwoPhaseCommit:
            return "exactly_once_two_phase_commit";
        case DeliveryGuarantee::NoDurableRestartGuarantee:
            return "no_durable_restart_guarantee";
    }
    return "?";
}

std::optional<DeliveryGuarantee> delivery_from_string(std::string_view s) noexcept {
    if (s == "at_most_once")
        return DeliveryGuarantee::AtMostOnce;
    if (s == "at_least_once")
        return DeliveryGuarantee::AtLeastOnce;
    if (s == "effectively_once_idempotent")
        return DeliveryGuarantee::EffectivelyOnceIdempotent;
    if (s == "exactly_once_atomic_publish")
        return DeliveryGuarantee::ExactlyOnceAtomicPublish;
    if (s == "exactly_once_two_phase_commit")
        return DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    if (s == "no_durable_restart_guarantee")
        return DeliveryGuarantee::NoDurableRestartGuarantee;
    // The historic DDL spelling. Accepted as INPUT only (a user asking for
    // exactly-once), never produced as output, so the ambiguity it caused
    // does not survive into anything clink reports.
    if (s == "exactly_once")
        return DeliveryGuarantee::ExactlyOnceTwoPhaseCommit;
    return std::nullopt;
}

int strength(DeliveryGuarantee g) noexcept {
    switch (g) {
        case DeliveryGuarantee::NoDurableRestartGuarantee:
            return 0;
        case DeliveryGuarantee::AtMostOnce:
            return 1;
        case DeliveryGuarantee::AtLeastOnce:
            return 2;
        case DeliveryGuarantee::EffectivelyOnceIdempotent:
            return 3;
        case DeliveryGuarantee::ExactlyOnceAtomicPublish:
            return 4;
        case DeliveryGuarantee::ExactlyOnceTwoPhaseCommit:
            return 4;
    }
    return 0;
}

std::string_view to_string(OffsetModel m) noexcept {
    switch (m) {
        case OffsetModel::None:
            return "none";
        case OffsetModel::FileOffset:
            return "file_offset";
        case OffsetModel::LogOffset:
            return "log_offset";
        case OffsetModel::Lsn:
            return "lsn";
        case OffsetModel::Timestamp:
            return "timestamp";
        case OffsetModel::OpaqueToken:
            return "opaque_token";
    }
    return "?";
}

std::string_view to_string(Boundedness b) noexcept {
    switch (b) {
        case Boundedness::Bounded:
            return "bounded";
        case Boundedness::Unbounded:
            return "unbounded";
        case Boundedness::Either:
            return "either";
    }
    return "?";
}

std::vector<std::string> ConnectorCapabilities::self_check() const {
    std::vector<std::string> problems;
    if (name.empty()) {
        problems.emplace_back("name is empty");
    }
    if (!is_source && !is_sink) {
        problems.emplace_back(name + ": declared neither source nor sink");
    }
    const bool claims_exactly_once = delivery == DeliveryGuarantee::ExactlyOnceTwoPhaseCommit ||
                                     delivery == DeliveryGuarantee::ExactlyOnceAtomicPublish;

    if (delivery == DeliveryGuarantee::ExactlyOnceTwoPhaseCommit && !transactional) {
        problems.emplace_back(name +
                              ": claims two-phase-commit delivery but is not marked transactional");
    }
    if (claims_exactly_once && !checkpoint_integrated) {
        problems.emplace_back(
            name +
            ": claims exactly-once delivery but does not participate in checkpointing; "
            "the commit has nothing to be tied to");
    }
    if (delivery == DeliveryGuarantee::EffectivelyOnceIdempotent &&
        idempotency_key_option.empty()) {
        problems.emplace_back(
            name +
            ": claims idempotent effectively-once but names no option carrying the key, so "
            "nothing can validate that the user supplied one");
    }
    if (is_source && replayable && offset_model == OffsetModel::None) {
        problems.emplace_back(
            name + ": a replayable source must declare the offset model it replays from");
    }
    // A source has two no-loss recovery models: replay from a client-side
    // offset the checkpoint carries, or BROKER REDELIVERY - the ack is
    // deferred to checkpoint completion, so everything unacknowledged sits
    // on the server and comes back after a crash (AMQP/JetStream/Pulsar
    // acks, Pub/Sub ackDeadline, Redis PEL, MQTT persistent sessions).
    // The second model has no offset to replay, so `replayable` is
    // honestly false, yet at-least-once holds; it is recognisable here by
    // checkpoint participation. A source with NEITHER model cannot offer
    // better than at-most-once on restart.
    if (is_source && !replayable && !checkpoint_integrated &&
        strength(delivery) > strength(DeliveryGuarantee::AtMostOnce)) {
        problems.emplace_back(
            name +
            ": a source that neither replays from an offset nor defers acknowledgement to "
            "checkpoint completion cannot offer better than at-most-once on restart");
    }
    if (!required_options_for_exactly_once.empty() && !claims_exactly_once) {
        problems.emplace_back(name +
                              ": lists options required for exactly-once but does not claim it");
    }
    return problems;
}

CapabilityRegistry& CapabilityRegistry::instance() {
    static CapabilityRegistry reg;
    return reg;
}

void CapabilityRegistry::declare(ConnectorCapabilities caps) {
    std::lock_guard lock(mu_);
    by_name_[caps.name] = std::move(caps);
}

void CapabilityRegistry::undeclare(std::string_view name) {
    std::lock_guard lock(mu_);
    if (const auto it = by_name_.find(name); it != by_name_.end()) {
        by_name_.erase(it);
    }
}

const ConnectorCapabilities* CapabilityRegistry::find(std::string_view name) const {
    std::lock_guard lock(mu_);
    const auto it = by_name_.find(name);
    return it == by_name_.end() ? nullptr : &it->second;
}

std::vector<ConnectorCapabilities> CapabilityRegistry::all() const {
    std::lock_guard lock(mu_);
    std::vector<ConnectorCapabilities> out;
    out.reserve(by_name_.size());
    for (const auto& [_, caps] : by_name_) {
        out.push_back(caps);
    }
    return out;
}

std::size_t CapabilityRegistry::size() const {
    std::lock_guard lock(mu_);
    return by_name_.size();
}

namespace {
// Set by clink::sql::install(). A plain atomic rather than a weak symbol:
// weak-symbol tricks behave differently across the platforms clink builds
// on, and the flag is written once during startup.
std::atomic<bool> g_sql_present{false};
}  // namespace

void mark_sql_frontend_present() {
    g_sql_present.store(true, std::memory_order_relaxed);
}

bool sql_frontend_present() {
    return g_sql_present.load(std::memory_order_relaxed);
}

BuildFacts current_build_facts() {
    BuildFacts f;
#ifdef CLINK_VERSION_STRING
    f.clink_version = CLINK_VERSION_STRING;
#else
    f.clink_version = "unknown";
#endif
    f.git_sha = clink::plugin::kAbiHash;
    f.git_clean = clink::plugin::kAbiHashIsClean;
    // SQL lives in a higher layer (clink_sql) than this TU, so a compile
    // -time #ifdef here would always read false. The SQL frontend flips
    // this flag from its own install() instead, which is also the honest
    // question: is the SQL frontend actually present in this process.
    f.sql = sql_frontend_present();
#ifdef CLINK_HAS_HTTP
    f.http = true;
#endif
#ifdef CLINK_HAS_OPENSSL
    f.tls = true;
#endif
#ifdef CLINK_HAS_WASM
    f.wasm = true;
#endif
#ifdef CLINK_WITH_ONNX
    f.onnx = true;
#endif
    f.fault_injection = clink::fault::available();
    f.allocator = clink::allocator_name();
    f.allows_unverified_checkpoints = clink::state::unverified_checkpoints_allowed();
    f.checkpoint_meta_version = clink::state::kCheckpointMetaVersion;
    return f;
}

namespace {

std::string yn(bool b) {
    return b ? "yes" : "no";
}

std::string join(const std::vector<std::string>& v, const char* sep = ", ") {
    std::string out;
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += v[i];
    }
    return out.empty() ? "-" : out;
}

// Minimal JSON string escaping, matching the hand-rolled encoders the rest
// of the control plane uses.
std::string jq(std::string_view s) {
    std::string out = "\"";
    for (const char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
    return out;
}

std::string jarray(const std::vector<std::string>& v) {
    std::string out = "[";
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i > 0) {
            out += ",";
        }
        out += jq(v[i]);
    }
    return out + "]";
}

std::string jbool(bool b) {
    return b ? "true" : "false";
}

}  // namespace

std::string render_manifest_text(const BuildFacts& facts,
                                 const std::vector<ConnectorCapabilities>& caps) {
    std::ostringstream os;
    os << "clink capability manifest\n";
    os << "=========================\n";
    os << "This manifest describes THIS BINARY, not the clink project. A\n";
    os << "connector absent here was not compiled in.\n\n";
    os << "version                       " << facts.clink_version << "\n";
    os << "built from                    " << facts.git_sha
       << (facts.git_clean ? "" : " (tree had uncommitted changes)") << "\n";
    os << "sql frontend                  " << yn(facts.sql) << "\n";
    os << "http subsystem                " << yn(facts.http) << "\n";
    os << "tls                           " << yn(facts.tls) << "\n";
    os << "wasm udfs                     " << yn(facts.wasm) << "\n";
    os << "onnx inference                " << yn(facts.onnx) << "\n";
    os << "checkpoint metadata version   " << facts.checkpoint_meta_version << "\n";
    os << "allocator                     " << facts.allocator << "\n";
    os << "fault injection compiled in   " << yn(facts.fault_injection);
    if (facts.fault_injection) {
        os << "   (test/debug surface; CLINK_FAULT_INJECT is live)";
    }
    os << "\n";
    os << "unverified checkpoints        " << yn(facts.allows_unverified_checkpoints);
    if (facts.allows_unverified_checkpoints) {
        os << "   (CLINK_ALLOW_UNVERIFIED_CHECKPOINTS is set - recovery integrity is WEAKENED)";
    }
    os << "\n\n";

    os << "connectors (" << caps.size() << " declared)\n";
    os << "------------------------------\n";
    if (caps.empty()) {
        os << "(none declared in this binary)\n";
        return os.str();
    }
    for (const auto& c : caps) {
        std::vector<std::string> roles;
        if (c.is_source) {
            roles.emplace_back("source");
        }
        if (c.is_sink) {
            roles.emplace_back("sink");
        }
        os << "\n" << c.name << "  (" << join(roles, "+") << ", v" << c.version << ")\n";
        os << "  delivery            " << to_string(c.delivery) << "\n";
        if (c.is_source) {
            os << "  boundedness         " << to_string(c.boundedness) << "\n";
            os << "  replayable          " << yn(c.replayable)
               << " (offsets: " << to_string(c.offset_model) << ")\n";
            os << "  partition discovery " << yn(c.partition_discovery) << "\n";
        }
        os << "  checkpointed        " << yn(c.checkpoint_integrated) << "\n";
        os << "  transactional       " << yn(c.transactional) << "\n";
        if (!c.idempotency_key_option.empty()) {
            os << "  idempotency key     " << c.idempotency_key_option << " (required)\n";
        }
        os << "  formats             " << join(c.formats) << "\n";
        os << "  auth                " << join(c.auth_methods) << "   tls: " << yn(c.tls) << "\n";
        os << "  backpressure        " << yn(c.backpressure) << "   retries: " << yn(c.retries)
           << "\n";
        os << "  timeout options     " << join(c.timeout_options) << "\n";
        if (c.max_message_bytes != 0) {
            os << "  max message bytes   " << c.max_message_bytes << "\n";
        }
        os << "  schema evolution    " << yn(c.schema_evolution) << "\n";
        os << "  surfaces            sql: " << yn(c.available_in_sql)
           << "   c++: " << yn(c.available_in_cpp) << "\n";
        os << "  build deps          " << join(c.build_dependencies) << "\n";
        if (!c.required_options_for_exactly_once.empty()) {
            os << "  exactly-once needs  " << join(c.required_options_for_exactly_once) << "\n";
        }
        for (const auto& l : c.limitations) {
            os << "  limitation          " << l << "\n";
        }
    }
    return os.str();
}

std::string render_manifest_json(const BuildFacts& facts,
                                 const std::vector<ConnectorCapabilities>& caps) {
    std::string out = "{";
    out += "\"schema_version\":" + std::to_string(kCapabilityManifestSchemaVersion);
    out += ",\"clink_version\":" + jq(facts.clink_version);
    out += ",\"build\":{";
    out += "\"git_sha\":" + jq(facts.git_sha);
    out += ",\"git_clean\":" + jbool(facts.git_clean);
    out += ",\"sql\":" + jbool(facts.sql);
    out += ",\"http\":" + jbool(facts.http);
    out += ",\"tls\":" + jbool(facts.tls);
    out += ",\"wasm\":" + jbool(facts.wasm);
    out += ",\"onnx\":" + jbool(facts.onnx);
    out += ",\"fault_injection\":" + jbool(facts.fault_injection);
    out += ",\"allocator\":" + jq(facts.allocator);
    out += ",\"allows_unverified_checkpoints\":" + jbool(facts.allows_unverified_checkpoints);
    out += ",\"checkpoint_meta_version\":" + std::to_string(facts.checkpoint_meta_version);
    out += "}";
    out += ",\"connectors\":[";
    for (std::size_t i = 0; i < caps.size(); ++i) {
        const auto& c = caps[i];
        if (i > 0) {
            out += ",";
        }
        out += "{";
        out += "\"name\":" + jq(c.name);
        out += ",\"version\":" + jq(c.version);
        out += ",\"source\":" + jbool(c.is_source);
        out += ",\"sink\":" + jbool(c.is_sink);
        out += ",\"build_dependencies\":" + jarray(c.build_dependencies);
        out += ",\"runtime_dependencies\":" + jarray(c.runtime_dependencies);
        out += ",\"formats\":" + jarray(c.formats);
        out += ",\"boundedness\":" + jq(to_string(c.boundedness));
        out += ",\"replayable\":" + jbool(c.replayable);
        out += ",\"offset_model\":" + jq(to_string(c.offset_model));
        out += ",\"checkpoint_integrated\":" + jbool(c.checkpoint_integrated);
        out += ",\"delivery_guarantee\":" + jq(to_string(c.delivery));
        out += ",\"transactional\":" + jbool(c.transactional);
        out += ",\"idempotency_key_option\":" + jq(c.idempotency_key_option);
        out += ",\"schema_evolution\":" + jbool(c.schema_evolution);
        out += ",\"partition_discovery\":" + jbool(c.partition_discovery);
        out += ",\"auth_methods\":" + jarray(c.auth_methods);
        out += ",\"tls\":" + jbool(c.tls);
        out += ",\"backpressure\":" + jbool(c.backpressure);
        out += ",\"retries\":" + jbool(c.retries);
        out += ",\"timeout_options\":" + jarray(c.timeout_options);
        out += ",\"max_message_bytes\":" + std::to_string(c.max_message_bytes);
        out += ",\"available_in_sql\":" + jbool(c.available_in_sql);
        out += ",\"available_in_cpp\":" + jbool(c.available_in_cpp);
        out +=
            ",\"required_options_for_exactly_once\":" + jarray(c.required_options_for_exactly_once);
        out += ",\"limitations\":" + jarray(c.limitations);
        out += "}";
    }
    out += "]}";
    return out;
}

}  // namespace clink::connectors
