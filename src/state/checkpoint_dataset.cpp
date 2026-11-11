#include "clink/state_processor/checkpoint_dataset.hpp"

#ifndef CLINK_HAS_ARROW
#error "clink requires CLINK_BUILD_ARROW=ON. The state-snapshot format is Arrow-IPC-only."
#endif

#include <algorithm>
#include <array>
#include <bitset>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <limits>
#include <memory>
#include <queue>
#include <random>
#include <sstream>
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/util/utf8.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/core/sha256.hpp"
#include "clink/core/types.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/schema_version.hpp"
#include "clink/state/snapshot_arrow_writer.hpp"
#include "clink/state/snapshot_canonicalise.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::state_processor {

namespace {

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
using config::JsonArray;
using config::JsonObject;
using config::JsonValue;

constexpr std::uint8_t kKeyed = 0;
constexpr std::uint8_t kOperator = 1;
constexpr std::uint8_t kRaw = 2;
constexpr const char* kRawSlot = "<raw>";
constexpr std::uint64_t kMaxDatasetJsonBytes = 64ULL << 20;

JsonValue num(std::uint64_t v) {
    return JsonValue{static_cast<std::int64_t>(v)};
}

std::string utc_now() {
    const auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

std::string sha256_text(std::string_view s) {
    Sha256 h;
    h.update(s.data(), s.size());
    return Sha256::to_hex(h.finalize());
}

// A regular file that is not a link, read whole, within `max` bytes.
std::string read_plain_file(const fs::path& p, std::uint64_t max) {
    std::error_code ec;
    const auto st = fs::symlink_status(p, ec);
    if (ec || !fs::is_regular_file(st)) {
        throw DatasetInvalid(p.string() + " is not a regular file");
    }
    const auto size = fs::file_size(p, ec);
    if (ec || size > max) {
        throw DatasetInvalid(p.string() + " is larger than " + std::to_string(max) + " bytes");
    }
    std::ifstream in(p, std::ios::binary);
    std::string body(static_cast<std::size_t>(size), '\0');
    in.read(body.data(), static_cast<std::streamsize>(size));
    if (!in) {
        throw DatasetInvalid("cannot read " + p.string());
    }
    return body;
}

std::string kg_ranges(const std::bitset<kNumKeyGroups>& b) {
    std::string out;
    std::size_t i = 0;
    while (i < b.size()) {
        if (!b.test(i)) {
            ++i;
            continue;
        }
        std::size_t j = i;
        while (j + 1 < b.size() && b.test(j + 1)) {
            ++j;
        }
        if (!out.empty()) {
            out += ",";
        }
        out += std::to_string(i);
        if (j > i) {
            out += "-" + std::to_string(j);
        }
        i = j + 1;
    }
    return out;
}

// ---- one participant's snapshot ---------------------------------------------

struct Row {
    std::uint64_t op{0};
    std::string slot;
    std::uint8_t scope{kKeyed};
    std::int16_t kg{-1};
    std::string user_key;
    std::string value;
    std::string stored_key;
};

bool row_less(const Row& x, const Row& y) {
    return std::tie(x.op, x.slot, x.scope, x.kg, x.user_key) <
           std::tie(y.op, y.slot, y.scope, y.kg, y.user_key);
}

bool same_identity(const Row& x, const Row& y) {
    return x.op == y.op && x.slot == y.slot && x.scope == y.scope && x.kg == y.kg &&
           x.user_key == y.user_key;
}

// The stored key's layout (docs/internals/state-snapshot-format.md, "Key
// layout"). A key group byte below 128, then slot '|' user key, is keyed
// state. A leading 0xFF is operator state: slot '|' user key, or, for a key
// with no separator (source positions and watermark progress are stored so),
// slot <raw> and the bytes after the prefix as its user key. Anything else,
// and a slot name that is not UTF-8, is a raw row: slot <raw>, the whole key
// as its user key. The stored key itself is kept beside every row.
Row classify(std::uint64_t op, std::string_view k, std::string_view v) {
    static const bool utf8_ready = [] {
        ::arrow::util::InitializeUTF8();
        return true;
    }();
    (void)utf8_ready;
    Row r;
    r.op = op;
    r.value = std::string{v};
    r.stored_key = std::string{k};
    const auto pipe = k.size() >= 2 ? k.find('|', 1) : std::string_view::npos;
    const auto lead = k.empty() ? 0U : static_cast<unsigned>(static_cast<unsigned char>(k[0]));
    const bool shaped =
        pipe != std::string_view::npos && ::arrow::util::ValidateUTF8(k.substr(1, pipe - 1));
    if (shaped && (lead < kNumKeyGroups || lead == kOperatorStateKeyPrefix)) {
        r.scope = lead == kOperatorStateKeyPrefix ? kOperator : kKeyed;
        if (r.scope == kKeyed) {
            r.kg = static_cast<std::int16_t>(lead);
        }
        r.slot = std::string{k.substr(1, pipe - 1)};
        r.user_key = std::string{k.substr(pipe + 1)};
        return r;
    }
    r.slot = kRawSlot;
    if (lead == kOperatorStateKeyPrefix) {
        r.scope = kOperator;
        r.user_key = std::string{k.substr(1)};
    } else {
        r.scope = kRaw;
        r.user_key = std::string{k};
    }
    return r;
}

struct ParsedSnapshot {
    std::vector<Row> rows;
    std::uint32_t format_version{1};
    StateVersionMap versions;
    StateFingerprintMap fingerprints;
    bool replayed_changelog{false};
};

struct SnapshotUnsupported : std::runtime_error {
    using std::runtime_error::runtime_error;
};
struct RowBound : std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::shared_ptr<arrow::ipc::RecordBatchStreamReader> open_stream(const std::vector<std::byte>& b) {
    auto buf = std::make_shared<arrow::Buffer>(reinterpret_cast<const std::uint8_t*>(b.data()),
                                               static_cast<std::int64_t>(b.size()));
    auto r =
        arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buf));
    if (!r.ok()) {
        throw std::runtime_error("not an Arrow IPC stream: " + r.status().ToString());
    }
    return *r;
}

ParsedSnapshot parse_snapshot(const fs::path& path, std::uint64_t max_rows) {
    ParsedSnapshot out;
    std::vector<std::byte> bytes;
    {
        std::error_code ec;
        const auto size = fs::file_size(path, ec);
        if (ec) {
            throw std::runtime_error("cannot size " + path.string());
        }
        bytes.resize(static_cast<std::size_t>(size));
        std::ifstream in(path, std::ios::binary);
        in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
        if (!in) {
            throw std::runtime_error("short read of " + path.string());
        }
    }
    if (bytes.empty()) {
        return out;
    }
    auto reader = open_stream(bytes);
    if (reader->schema()->num_fields() == 4 && reader->schema()->field(0)->name() == "row_kind") {
        reader.reset();  // it reads `bytes`, which the replay consumes
        try {
            bytes = canonicalise_state_snapshot(std::move(bytes));
        } catch (const std::exception& e) {
            throw SnapshotUnsupported(
                std::string{"a changelog snapshot that cannot be replayed from the checkpoint "
                            "alone (its materialisation is held elsewhere): "} +
                e.what());
        }
        out.replayed_changelog = true;
        if (bytes.empty()) {
            return out;
        }
        reader = open_stream(bytes);
    }
    const auto schema = reader->schema();
    const auto md = schema->metadata();
    verify_snapshot_format_version(md, path.filename().string());
    if (md) {
        if (const auto i = md->FindKey(kSnapshotFormatVersionKey); i != -1) {
            out.format_version = static_cast<std::uint32_t>(std::stoul(md->value(i)));
        }
        if (const auto i = md->FindKey(kStateVersionsMetadataKey); i != -1) {
            out.versions = StateVersionMap::unpack(md->value(i));
        }
        if (const auto i = md->FindKey(kStateFingerprintsMetadataKey); i != -1) {
            out.fingerprints = StateFingerprintMap::unpack(md->value(i));
        }
    }
    if (schema->num_fields() != 3 || schema->field(0)->name() != "op_id" ||
        schema->field(1)->name() != "key_bytes" || schema->field(2)->name() != "value_bytes" ||
        !schema->field(0)->type()->Equals(arrow::uint64()) ||
        !schema->field(1)->type()->Equals(arrow::binary()) ||
        !schema->field(2)->type()->Equals(arrow::binary())) {
        throw std::runtime_error("not the canonical snapshot schema: " + schema->ToString());
    }
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        const auto st = reader->ReadNext(&batch);
        if (!st.ok()) {
            throw std::runtime_error("the stream does not decode: " + st.ToString());
        }
        if (!batch) {
            break;
        }
        const auto ops = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        const auto keys = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));
        const auto vals = std::static_pointer_cast<arrow::BinaryArray>(batch->column(2));
        if (ops->null_count() + keys->null_count() + vals->null_count() > 0) {
            throw std::runtime_error("a row has a null column, which the format never writes");
        }
        for (std::int64_t i = 0; i < batch->num_rows(); ++i) {
            if (out.rows.size() >= max_rows) {
                throw RowBound("past the entry bound");
            }
            out.rows.push_back(classify(ops->Value(i), keys->GetView(i), vals->GetView(i)));
        }
    }
    std::sort(out.rows.begin(), out.rows.end(), row_less);
    return out;
}

// ---- the entry table --------------------------------------------------------

std::shared_ptr<arrow::Schema> entries_schema() {
    return arrow::schema({arrow::field("op_id", arrow::uint64(), false),
                          arrow::field("slot", arrow::utf8(), false),
                          arrow::field("scope", arrow::utf8(), false),
                          arrow::field("key_group", arrow::int16(), true),
                          arrow::field("participant", arrow::uint32(), false),
                          arrow::field("user_key", arrow::binary(), false),
                          arrow::field("value", arrow::binary(), false),
                          arrow::field("stored_key", arrow::binary(), false)});
}

void check(const arrow::Status& st, const char* what) {
    if (!st.ok()) {
        throw std::runtime_error(std::string{what} + ": " + st.ToString());
    }
}

class EntriesWriter {
public:
    explicit EntriesWriter(const fs::path& path) : schema_(entries_schema()) {
        auto sink = arrow::io::FileOutputStream::Open(path.string());
        check(sink.status(), "cannot create entries.parquet");
        auto props = parquet::WriterProperties::Builder()
                         .compression(parquet::Compression::ZSTD)
                         ->max_row_group_length(std::numeric_limits<std::int64_t>::max())
                         ->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
        auto w = parquet::arrow::FileWriter::Open(
            *schema_, arrow::default_memory_pool(), *sink, props, arrow_props);
        check(w.status(), "cannot open the entries writer");
        writer_ = std::move(*w);
    }

    // One participant's rows, already in identity order, as one row group.
    void write(std::uint32_t participant, const std::vector<Row>& rows) {
        arrow::UInt64Builder op;
        arrow::StringBuilder slot;
        arrow::StringBuilder scope;
        arrow::Int16Builder kg;
        arrow::UInt32Builder part;
        arrow::BinaryBuilder uk;
        arrow::BinaryBuilder val;
        arrow::BinaryBuilder sk;
        for (const auto& r : rows) {
            check(op.Append(r.op), "append");
            check(slot.Append(r.slot), "append");
            check(scope.Append(scope_name(r.scope)), "append");
            check(r.scope == kKeyed ? kg.Append(r.kg) : kg.AppendNull(), "append");
            check(part.Append(participant), "append");
            check(uk.Append(r.user_key), "append");
            check(val.Append(r.value), "append");
            check(sk.Append(r.stored_key), "append");
        }
        std::vector<std::shared_ptr<arrow::Array>> cols(8);
        check(op.Finish(&cols[0]), "finish");
        check(slot.Finish(&cols[1]), "finish");
        check(scope.Finish(&cols[2]), "finish");
        check(kg.Finish(&cols[3]), "finish");
        check(part.Finish(&cols[4]), "finish");
        check(uk.Finish(&cols[5]), "finish");
        check(val.Finish(&cols[6]), "finish");
        check(sk.Finish(&cols[7]), "finish");
        const auto table =
            arrow::Table::Make(schema_, cols, static_cast<std::int64_t>(rows.size()));
        check(writer_->WriteTable(*table, static_cast<std::int64_t>(rows.size())),
              "cannot write a row group");
        ++row_groups_;
    }

    void close() { check(writer_->Close(), "cannot close entries.parquet"); }
    [[nodiscard]] std::int64_t row_groups() const { return row_groups_; }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    std::int64_t row_groups_{0};
};

// ---- export ------------------------------------------------------------------

struct SlotStamp {
    std::optional<std::uint64_t> fingerprint;
    std::map<std::string, std::uint32_t> versions;
};

std::string fp_hex(std::uint64_t fp) {
    char buf[17];
    std::snprintf(buf, sizeof buf, "%016llx", static_cast<unsigned long long>(fp));
    return buf;
}

std::string absent_reason(const incident::Bundle& b, const std::string& rel) {
    if (b.manifest.contains("absent") && b.manifest.at("absent").is_array()) {
        for (const auto& a : b.manifest.at("absent").as_array()) {
            if (a.is_object() && a.string_or("path", "") == rel) {
                return a.string_or("reason", "absent");
            }
        }
    }
    return "absent from the collection";
}

struct GraphOp {
    std::string id;
    std::string uid;
    std::string type;
    std::uint32_t parallelism{1};
    std::optional<std::uint64_t> op_id;  // when it has a uid
};

// The job graph the bundle holds: its SHA-256 (over the graph text as the
// bundle records it, credential-like parameters redacted) and its operators.
// An operator with a uid stores its state under operator_id_from_uid(uid);
// one without has an id the runtime derives from its place in its chain,
// which is not recomputed here, so it is listed without one.
std::pair<std::string, std::vector<GraphOp>> read_graph(const incident::Bundle& b,
                                                        std::vector<std::string>& notes) {
    if (b.find("job/manifest.json") == nullptr) {
        return {};
    }
    std::string graph_text;
    try {
        const auto m = config::parse(read_plain_file(b.root / "job/manifest.json", 16ULL << 20));
        if (m.is_object() && m.contains("graph_json") && m.at("graph_json").is_string()) {
            graph_text = m.at("graph_json").as_string();
        }
    } catch (const std::exception& e) {
        notes.push_back(std::string{"the job manifest does not read: "} + e.what());
        return {};
    }
    if (graph_text.empty()) {
        return {};
    }
    std::vector<GraphOp> ops;
    try {
        const auto spec = cluster::JobGraphSpec::from_json(graph_text);
        for (const auto& op : spec.ops) {
            GraphOp g{.id = op.id,
                      .uid = op.uid,
                      .type = op.type,
                      .parallelism = op.parallelism,
                      .op_id = op.uid.empty() ? std::nullopt
                                              : std::optional<std::uint64_t>{
                                                    operator_id_from_uid(op.uid).value()}};
            ops.push_back(std::move(g));
        }
    } catch (const std::exception& e) {
        notes.push_back(std::string{"the job graph does not parse, so operators are not named: "} +
                        e.what());
    }
    return {sha256_text(graph_text), std::move(ops)};
}

class Exporter {
public:
    explicit Exporter(const DatasetOptions& o) : o_(o), started_(Clock::now()) {}
    DatasetResult run();

private:
    void build(const fs::path& root);
    [[nodiscard]] bool past_deadline() const { return Clock::now() - started_ > o_.timeout; }

    const DatasetOptions& o_;
    Clock::time_point started_;
    std::vector<std::string> incomplete_;
    std::vector<std::string> unsupported_;
    std::vector<std::string> notes_;
    std::uint64_t entries_{0};
    std::uint64_t participants_{0};
};

void Exporter::build(const fs::path& root) {
    const auto b = incident::read_bundle(root);
    const auto job = b.job();
    const auto checkpoint = b.checkpoint();
    const auto& sel = b.manifest.at("selection");
    const auto generation = static_cast<std::uint32_t>(sel.int_or("generation", 0));
    const auto state_rel = sel.string_or("state_root", "");
    const bool overridden = sel.bool_or("overridden", false);
    const bool confirmed = b.find("checkpoint/_jobs/" + std::to_string(job) + "/CONFIRMED-" +
                                  std::to_string(checkpoint)) != nullptr;
    std::optional<bool> needs_confirmation;
    if (b.manifest.at("job").contains("commit_confirmation") &&
        b.manifest.at("job").at("commit_confirmation").is_bool()) {
        needs_confirmation = b.manifest.at("job").at("commit_confirmation").as_bool();
    }

    // The engine's own checks over what was collected.
    incident::VerifyOptions vo;
    vo.profile = "consistency";
    const auto checks = incident::verify_core(b, vo);
    std::set<std::string> required = {"bundle.manifest",
                                      "bundle.artifacts",
                                      "bundle.completeness",
                                      "checkpoint.marker",
                                      "checkpoint.participants",
                                      "state.decode"};
    if (!overridden) {
        required.insert("checkpoint.restore_rule");
    }
    JsonArray verification;
    std::vector<std::string> unsupported_checks;
    for (const auto& c : checks) {
        JsonObject j;
        j["id"] = JsonValue{c.id};
        j["verdict"] = JsonValue{std::string{incident::to_string(c.verdict)}};
        j["reason"] = JsonValue{c.reason};
        verification.push_back(JsonValue{std::move(j)});
        if (!required.contains(c.id) || c.verdict == incident::Verdict::Passed) {
            continue;
        }
        const auto line = c.id + " " + incident::to_string(c.verdict) + ": " + c.reason;
        if (c.verdict == incident::Verdict::Unsupported) {
            unsupported_checks.push_back(line);
        } else {
            incomplete_.push_back(line);
        }
    }

    // Where the state is, as the marker in the bundle names it.
    cluster::RestoreSource src;
    try {
        src = cluster::resolve_restore_source(
            (b.checkpoint_dir() / "_jobs" / std::to_string(job)).string(), checkpoint);
    } catch (const std::exception& e) {
        incomplete_.push_back(std::string{"the checkpoint's state cannot be located: "} + e.what());
    }
    if (src.legacy) {
        unsupported_.push_back(
            "the checkpoint predates job-namespaced state; its participants are not recorded");
    } else if (!b.contains("state")) {
        unsupported_.push_back("the checkpoint's state is not in the checkpoint directory (" +
                               absent_reason(b, src.state_root) +
                               "); only the file backend's canonical snapshots are exported");
    }
    if (!src.participants.has_value() && !src.legacy) {
        incomplete_.push_back(
            "the marker records no participant set, so which snapshots make up the job is "
            "unknown");
    }

    auto [graph_sha, graph_ops] = read_graph(b, notes_);
    std::map<std::uint64_t, const GraphOp*> by_op;
    for (const auto& g : graph_ops) {
        if (g.op_id && !by_op.emplace(*g.op_id, &g).second) {
            notes_.push_back("two graph operators share operator id " + std::to_string(*g.op_id));
        }
    }

    // Each participant: verify, decode, write its row group.
    EntriesWriter writer(root / kEntriesFile);
    JsonArray parts;
    std::map<std::uint64_t, std::array<std::int64_t, kNumKeyGroups>>
        owner;  // op -> kg -> participant
    std::map<std::pair<std::uint64_t, std::string>, std::map<std::uint32_t, SlotStamp>> stamps;
    std::map<SlotKey, std::uint64_t> slot_rows;
    std::map<std::uint64_t, std::set<std::uint32_t>> op_parts;
    std::set<std::pair<std::uint64_t, std::string>> conflicts;
    bool bound = false;
    bool missing_snapshot = false;
    const std::set<std::uint32_t> participants =
        src.participants.value_or(std::set<std::uint32_t>{});
    const auto name = "checkpoint-" + std::to_string(checkpoint) + ".snap";
    for (const auto p : participants) {
        ++participants_;
        JsonObject pj;
        pj["index"] = num(p);
        const auto rel = clink::state_dir_for(state_rel, generation, p) + "/" + name;
        const auto* art = b.find(rel);
        pj["snapshot"] = art ? JsonValue{rel} : JsonValue{};
        pj["row_group"] = JsonValue{static_cast<std::int64_t>(-1)};
        pj["rows"] = num(0);
        if (art == nullptr || !b.contains("state")) {
            const auto why = absent_reason(b, rel);
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{why};
            if (b.contains("state")) {
                missing_snapshot = true;
                incomplete_.push_back("participant " + std::to_string(p) + ": no snapshot (" + why +
                                      ")");
            }
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        }
        pj["bytes"] = num(art->bytes);
        pj["sha256"] = JsonValue{art->sha256};
        const auto sidecar = state::verify_checkpoint(b.root / rel);
        pj["sidecar"] = JsonValue{std::string{state::to_string(sidecar.status)}};
        if (bound || past_deadline()) {
            if (!bound) {
                incomplete_.push_back(
                    "the time bound (--timeout-s) was reached before participant " +
                    std::to_string(p) + "; it and every later participant are not exported");
            }
            bound = true;
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{std::string{"past a bound"}};
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        }
        if (!sidecar.ok()) {
            // A snapshot that does not match its sidecar is not read.
            incomplete_.push_back("participant " + std::to_string(p) + ": " +
                                  std::string{state::to_string(sidecar.status)} +
                                  " snapshot: " + sidecar.detail);
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{"the snapshot does not match its sidecar: " + sidecar.detail};
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        }
        ParsedSnapshot snap;
        try {
            snap = parse_snapshot(b.root / rel, o_.max_entries - entries_);
        } catch (const RowBound&) {
            incomplete_.push_back(
                "the entry bound (--max-entries=" + std::to_string(o_.max_entries) +
                ") was reached at participant " + std::to_string(p) +
                "; it and every later participant are not exported");
            bound = true;
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{std::string{"past a bound"}};
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        } catch (const SnapshotUnsupported& e) {
            unsupported_.push_back("participant " + std::to_string(p) + ": " + e.what());
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{std::string{e.what()}};
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        } catch (const std::exception& e) {
            incomplete_.push_back("participant " + std::to_string(p) +
                                  ": the snapshot does not decode: " + e.what());
            pj["exported"] = JsonValue{false};
            pj["reason"] = JsonValue{std::string{e.what()}};
            parts.push_back(JsonValue{std::move(pj)});
            continue;
        }
        pj["format_version"] = num(snap.format_version);
        if (snap.replayed_changelog) {
            pj["changelog_replayed"] = JsonValue{true};
        }
        JsonArray vs;
        for (const auto& e : snap.versions.entries()) {
            vs.push_back(JsonValue{std::to_string(e.op_id.value()) + "|" + e.state_type + "|" +
                                   std::to_string(e.version) +
                                   (e.slot.empty() ? std::string{} : "|" + e.slot)});
        }
        pj["state_versions"] = JsonValue{std::move(vs)};
        JsonArray fs_;
        for (const auto& e : snap.fingerprints.entries()) {
            fs_.push_back(JsonValue{std::to_string(e.op_id.value()) + "|" + e.slot + "|" +
                                    fp_hex(e.fingerprint)});
        }
        pj["state_fingerprints"] = JsonValue{std::move(fs_)};

        // Duplicates within the snapshot, and key-group ownership across them.
        std::map<std::uint64_t, std::array<std::uint64_t, 3>> per_op;
        std::map<std::uint64_t, std::bitset<kNumKeyGroups>> held;
        for (std::size_t i = 0; i < snap.rows.size(); ++i) {
            const auto& r = snap.rows[i];
            if (i > 0 && same_identity(snap.rows[i - 1], r)) {
                incomplete_.push_back("participant " + std::to_string(p) +
                                      " holds one entry twice under operator " +
                                      std::to_string(r.op) + " slot " + r.slot);
            }
            ++per_op[r.op][r.scope];
            ++slot_rows[SlotKey{r.op, r.slot, r.scope}];
            op_parts[r.op].insert(p);
            stamps[{r.op, r.slot}];  // every slot gets a schema record
            if (r.scope == kKeyed) {
                held[r.op].set(static_cast<std::size_t>(r.kg));
            }
        }
        for (const auto& [op, groups] : held) {
            auto [it, fresh] = owner.try_emplace(op);
            if (fresh) {
                it->second.fill(-1);
            }
            for (std::size_t g = 0; g < groups.size(); ++g) {
                if (!groups.test(g)) {
                    continue;
                }
                auto& o = it->second[g];
                if (o >= 0 && o != static_cast<std::int64_t>(p)) {
                    incomplete_.push_back("key group " + std::to_string(g) + " of operator " +
                                          std::to_string(op) + " is held by participants " +
                                          std::to_string(o) + " and " + std::to_string(p) +
                                          ": the snapshots are not one consistent assignment");
                }
                o = static_cast<std::int64_t>(p);
            }
        }
        JsonArray ops;
        for (const auto& [op, counts] : per_op) {
            JsonObject oj;
            oj["op_id"] = JsonValue{std::to_string(op)};
            oj["keyed"] = num(counts[kKeyed]);
            oj["operator"] = num(counts[kOperator]);
            oj["raw"] = num(counts[kRaw]);
            oj["key_groups"] = JsonValue{kg_ranges(held[op])};
            ops.push_back(JsonValue{std::move(oj)});
        }
        pj["operators"] = JsonValue{std::move(ops)};

        // Schema stamps, per slot this participant names.
        for (const auto& e : snap.fingerprints.entries()) {
            stamps[{e.op_id.value(), e.slot}][p].fingerprint = e.fingerprint;
        }
        for (const auto& e : snap.versions.entries()) {
            if (!e.slot.empty()) {
                stamps[{e.op_id.value(), e.slot}][p].versions[e.state_type] = e.version;
            }
        }
        for (auto& [key, by_part] : stamps) {
            for (const auto& e : snap.versions.entries()) {
                if (e.slot.empty() && e.op_id.value() == key.first) {
                    by_part[p].versions.try_emplace(e.state_type, e.version);
                }
            }
        }

        if (!snap.rows.empty()) {
            pj["row_group"] = JsonValue{writer.row_groups()};
            writer.write(p, snap.rows);
        }
        pj["rows"] = num(snap.rows.size());
        pj["exported"] = JsonValue{true};
        entries_ += snap.rows.size();
        parts.push_back(JsonValue{std::move(pj)});
    }
    writer.close();
    // A check that found nothing it could decode is unsupported only when
    // every snapshot was there to decode; otherwise it is the absence.
    for (auto& line : unsupported_checks) {
        (missing_snapshot ? incomplete_ : unsupported_).push_back(std::move(line));
    }
    if (participants.empty() && src.participants.has_value()) {
        incomplete_.push_back("the marker records an empty participant set");
    }

    // Each slot's schema; participants that stamp it differently are a
    // contradiction, not a schema.
    std::map<std::pair<std::uint64_t, std::string>, SlotStamp> schema;
    for (const auto& [key, by_part] : stamps) {
        SlotStamp merged;
        for (const auto& [p, s] : by_part) {
            if (s.fingerprint) {
                if (merged.fingerprint && *merged.fingerprint != *s.fingerprint) {
                    conflicts.insert(key);
                }
                merged.fingerprint = s.fingerprint;
            }
            for (const auto& [t, v] : s.versions) {
                auto [it, fresh] = merged.versions.try_emplace(t, v);
                if (!fresh && it->second != v) {
                    conflicts.insert(key);
                }
            }
        }
        schema[key] = std::move(merged);
    }
    for (const auto& [op, slot] : conflicts) {
        incomplete_.push_back("participants stamp operator " + std::to_string(op) + " slot " +
                              slot + " with different schemas");
    }

    const auto entries_path = root / kEntriesFile;
    const auto entries_sha = incident::sha256_file(entries_path);
    std::error_code ec;
    const auto entries_bytes = fs::file_size(entries_path, ec);
    const auto manifest_sha = incident::sha256_file(root / "manifest.json");

    // The contract.
    JsonObject d;
    d["format"] = JsonValue{std::string{kDatasetFormat}};
    d["version"] = JsonValue{kDatasetVersion};
    const auto facts = connectors::current_build_facts();
    JsonObject ex;
    ex["exported_at"] = JsonValue{utc_now()};
    ex["clink_version"] = JsonValue{facts.clink_version};
    ex["git_sha"] = JsonValue{facts.git_sha};
    d["exporter"] = JsonValue{std::move(ex)};

    JsonObject jj;
    jj["id"] = num(job);
    jj["graph_sha256"] = graph_sha.empty() ? JsonValue{} : JsonValue{graph_sha};
    jj["commit_confirmation"] = needs_confirmation ? JsonValue{*needs_confirmation} : JsonValue{};
    jj["rescaled"] = JsonValue{b.manifest.at("job").bool_or("rescaled", false)};
    d["job"] = JsonValue{std::move(jj)};

    JsonObject cj;
    cj["id"] = num(checkpoint);
    cj["generation"] = num(generation);
    cj["status"] = JsonValue{std::string{confirmed ? "confirmed" : "completed"}};
    cj["state_root"] = JsonValue{state_rel};
    d["checkpoint"] = JsonValue{std::move(cj)};

    JsonObject sj;
    sj["rule"] = JsonValue{std::string{overridden ? "named" : "restore-point"}};
    sj["engine_choice"] = sel.contains("engine_choice") ? sel.at("engine_choice") : JsonValue{};
    sj["restored"] = sel.contains("restored") ? sel.at("restored") : JsonValue{};
    d["selection"] = JsonValue{std::move(sj)};

    JsonObject ij;
    if (!needs_confirmation.has_value()) {
        ij["needs_resolution"] = JsonValue{};
        ij["statement"] = JsonValue{std::string{
            "whether the job's sinks need commit confirmation is not recorded (no job manifest "
            "or --commit-confirmation), so whether a restore from this checkpoint resolves "
            "transactions in doubt first is not known"}};
    } else if (*needs_confirmation && !confirmed) {
        ij["needs_resolution"] = JsonValue{true};
        ij["statement"] = JsonValue{std::string{
            "completed but not confirmed: the job's sinks need commit confirmation, so a "
            "restore from this checkpoint resolves its sinks' transactions in doubt before it "
            "runs"}};
    } else {
        ij["needs_resolution"] = JsonValue{false};
        ij["statement"] = JsonValue{std::string{
            *needs_confirmation ? "confirmed: its sinks' commits were acknowledged to the "
                                  "coordinator"
                                : "the job's sinks do not need commit confirmation"}};
    }
    ij["external_outcomes"] = JsonValue{std::string{
        "not verified: this dataset holds the job's state at the checkpoint, not what any "
        "external system received or committed"}};
    d["in_doubt"] = JsonValue{std::move(ij)};

    d["participants"] = JsonValue{std::move(parts)};

    // Operators: every one that holds state, named from the graph where it can be.
    std::set<std::uint64_t> state_ops;
    for (const auto& [k, n] : slot_rows) {
        state_ops.insert(std::get<0>(k));
    }
    for (const auto& [k, s] : schema) {
        state_ops.insert(k.first);
    }
    JsonArray ops;
    for (const auto op : state_ops) {
        JsonObject oj;
        oj["op_id"] = JsonValue{std::to_string(op)};
        if (const auto it = by_op.find(op); it != by_op.end()) {
            oj["graph_id"] = JsonValue{it->second->id};
            oj["uid"] = it->second->uid.empty() ? JsonValue{} : JsonValue{it->second->uid};
            oj["type"] = JsonValue{it->second->type};
            oj["parallelism"] = num(it->second->parallelism);
        } else {
            oj["graph_id"] = JsonValue{};
        }
        JsonArray ps;
        for (const auto p : op_parts[op]) {
            ps.push_back(num(p));
        }
        oj["participants"] = JsonValue{std::move(ps)};
        JsonArray slots;
        std::set<std::pair<std::string, std::uint8_t>> seen;
        for (const auto& [k, n] : slot_rows) {
            if (std::get<0>(k) == op) {
                seen.emplace(std::get<1>(k), std::get<2>(k));
            }
        }
        for (const auto& [k, s] : schema) {
            if (k.first == op) {
                bool any = false;
                for (const auto& [sl, sc] : seen) {
                    any = any || sl == k.second;
                }
                if (!any) {
                    seen.emplace(k.second, kKeyed);  // stamped, holds no entry
                }
            }
        }
        for (const auto& [sl, sc] : seen) {
            JsonObject slj;
            slj["slot"] = JsonValue{sl};
            slj["scope"] = JsonValue{std::string{scope_name(sc)}};
            const auto it = slot_rows.find(SlotKey{op, sl, sc});
            slj["rows"] = num(it == slot_rows.end() ? 0 : it->second);
            JsonObject sch;
            if (const auto s = schema.find({op, sl}); s != schema.end()) {
                sch["fingerprint"] =
                    s->second.fingerprint ? JsonValue{fp_hex(*s->second.fingerprint)} : JsonValue{};
                JsonObject vj;
                for (const auto& [t, v] : s->second.versions) {
                    vj[t] = num(v);
                }
                sch["versions"] = JsonValue{std::move(vj)};
            }
            slj["schema"] = JsonValue{std::move(sch)};
            slots.push_back(JsonValue{std::move(slj)});
        }
        oj["slots"] = JsonValue{std::move(slots)};
        ops.push_back(JsonValue{std::move(oj)});
    }
    d["operators"] = JsonValue{std::move(ops)};

    JsonObject gj;
    gj["present"] = JsonValue{!graph_sha.empty()};
    JsonArray gops;
    for (const auto& g : graph_ops) {
        JsonObject o;
        o["id"] = JsonValue{g.id};
        o["uid"] = g.uid.empty() ? JsonValue{} : JsonValue{g.uid};
        o["type"] = JsonValue{g.type};
        o["parallelism"] = num(g.parallelism);
        o["op_id"] = g.op_id ? JsonValue{std::to_string(*g.op_id)} : JsonValue{};
        o["holds_state"] = g.op_id ? JsonValue{state_ops.contains(*g.op_id)} : JsonValue{};
        gops.push_back(JsonValue{std::move(o)});
    }
    gj["operators"] = JsonValue{std::move(gops)};
    d["graph"] = JsonValue{std::move(gj)};

    JsonObject ej;
    ej["path"] = JsonValue{std::string{kEntriesFile}};
    ej["sha256"] = JsonValue{entries_sha};
    ej["bytes"] = num(entries_bytes);
    ej["rows"] = num(entries_);
    ej["row_groups"] = JsonValue{writer.row_groups()};
    ej["columns"] = JsonValue{std::string{
        "op_id uint64, slot utf8, scope utf8 (keyed|operator|raw), key_group int16 (keyed "
        "only), participant uint32, user_key binary, value binary, stored_key binary"}};
    ej["order"] = JsonValue{std::string{
        "one row group per participant holding entries, each sorted by op_id, slot, scope, "
        "key_group, user_key (bytes, unsigned)"}};
    ej["identity"] = JsonValue{std::string{
        "keyed: op_id, slot, key_group, user_key; operator and raw: op_id, slot, participant, "
        "user_key"}};
    d["entries"] = JsonValue{std::move(ej)};

    JsonObject bj;
    bj["manifest"] = JsonValue{std::string{"manifest.json"}};
    bj["manifest_sha256"] = JsonValue{manifest_sha};
    d["bundle"] = JsonValue{std::move(bj)};
    d["verification"] = JsonValue{std::move(verification)};

    const bool complete = incomplete_.empty() && unsupported_.empty();
    d["complete"] = JsonValue{complete};
    JsonArray inc;
    for (const auto& s : incomplete_) {
        inc.push_back(JsonValue{s});
    }
    d["incomplete"] = JsonValue{std::move(inc)};
    JsonArray uns;
    for (const auto& s : unsupported_) {
        uns.push_back(JsonValue{s});
    }
    d["unsupported"] = JsonValue{std::move(uns)};
    JsonArray nts;
    for (const auto& s : notes_) {
        nts.push_back(JsonValue{s});
    }
    d["notes"] = JsonValue{std::move(nts)};
    JsonObject lj;
    lj["max_bytes"] = num(o_.max_bytes);
    lj["max_files"] = num(o_.max_files);
    lj["max_entries"] = num(o_.max_entries);
    lj["timeout_s"] = num(static_cast<std::uint64_t>(o_.timeout.count()));
    d["limits"] = JsonValue{std::move(lj)};
    d["represented"] = JsonValue{JsonArray{
        JsonValue{std::string{"keyed state, every slot, as stored bytes"}},
        JsonValue{std::string{"operator state (source positions, broadcast and union slots), "
                              "each participant's own rows"}},
        JsonValue{std::string{"timers, as the state rows operators write for them"}},
        JsonValue{std::string{"SQL operators' internal slots, as stored bytes"}},
        JsonValue{std::string{"in-flight records an older unaligned checkpoint kept, as the rows "
                              "it stored"}},
        JsonValue{std::string{"rows whose key does not follow the layout, kept whole as raw"}}}};
    d["not_represented"] = JsonValue{JsonArray{
        JsonValue{std::string{"the job's live state after this checkpoint"}},
        JsonValue{std::string{"anything the checkpoint does not hold: pending asynchronous "
                              "operations, buffers between operators"}},
        JsonValue{std::string{"state held outside the checkpoint directory, and other backends' "
                              "formats (reported unsupported)"}},
        JsonValue{std::string{"what external systems received or committed"}},
        JsonValue{std::string{"typed readings of keys and values: no codec in this build "
                              "establishes a job's types"}}}};

    std::ofstream out(root / kDatasetFile, std::ios::binary | std::ios::trunc);
    out << JsonValue{std::move(d)}.serialize(2) << "\n";
    out.close();
    if (!out) {
        throw std::runtime_error("cannot write dataset.json");
    }
}

DatasetResult Exporter::run() {
    if (o_.out.empty() || o_.checkpoint_dir.empty() || o_.job == 0) {
        throw DatasetRefused("a checkpoint directory, a job and --out are required", "other");
    }
    std::error_code ec;
    fs::path out = fs::absolute(o_.out, ec).lexically_normal();
    if (!out.has_filename()) {
        out = out.parent_path();
    }
    if (fs::exists(fs::symlink_status(out, ec))) {
        throw DatasetRefused(out.string() + " already exists; a dataset is never written over",
                             "other");
    }
    const auto parent = out.parent_path();
    if (!fs::is_directory(fs::symlink_status(parent, ec))) {
        throw DatasetRefused(parent.string() + " is not a directory (a link is not followed)",
                             "other");
    }
    std::random_device rd;
    const auto tmp = parent / ("." + out.filename().string() + ".dataset-incomplete-" +
                               std::to_string(static_cast<long>(::getpid())) + "-" +
                               std::to_string(rd() % 1000000));
    fs::create_directory(tmp);
    const auto staged = tmp / "dataset";
    DatasetResult r;
    try {
        incident::CollectOptions c;
        c.checkpoint_dir = o_.checkpoint_dir;
        c.job = o_.job;
        c.ha_dir = o_.ha_dir;
        c.commit_confirmation = o_.commit_confirmation;
        c.checkpoint = o_.checkpoint;
        c.include_state = true;
        c.max_bytes = o_.max_bytes;
        c.max_files = o_.max_files;
        c.timeout = o_.timeout;
        c.out = staged.string();
        c.after_copy = o_.after_copy;
        incident::CollectResult collected;
        try {
            collected = incident::collect(c);
        } catch (const incident::CollectError& e) {
            throw DatasetRefused(e.what(), incident::collect_error_kind_name(e.kind));
        }
        r.checkpoint = collected.checkpoint;
        build(staged);
        const bool complete = incomplete_.empty() && unsupported_.empty();
        if (!complete && !o_.allow_partial) {
            std::string why;
            for (const auto& s : unsupported_) {
                why += "\n  unsupported: " + s;
            }
            for (const auto& s : incomplete_) {
                why += "\n  incomplete: " + s;
            }
            throw DatasetRefused("checkpoint " + std::to_string(collected.checkpoint) +
                                     " does not export as a complete dataset; nothing was "
                                     "published (--allow-partial publishes it marked partial):" +
                                     why,
                                 unsupported_.empty() ? "incomplete" : "unsupported");
        }
        if (fs::exists(fs::symlink_status(out, ec))) {
            throw DatasetRefused(out.string() + " appeared while the dataset was exported",
                                 "other");
        }
        fs::rename(staged, out, ec);
        if (ec) {
            throw DatasetRefused(
                "cannot publish the dataset at " + out.string() + ": " + ec.message(), "other");
        }
        fs::remove_all(tmp, ec);
    } catch (const DatasetRefused&) {
        fs::remove_all(tmp, ec);
        throw;
    } catch (const std::exception& e) {
        fs::remove_all(tmp, ec);
        throw DatasetRefused(
            std::string{"the export failed: "} + e.what() + "; nothing was published", "other");
    }
    r.dataset = out;
    r.complete = incomplete_.empty() && unsupported_.empty();
    r.incomplete = incomplete_;
    r.unsupported = unsupported_;
    r.participants = participants_;
    r.entries = entries_;
    for (const auto& e : fs::recursive_directory_iterator(out, ec)) {
        if (e.is_regular_file(ec)) {
            r.bytes += e.file_size(ec);
        }
    }
    r.seconds = std::chrono::duration<double>(Clock::now() - started_).count();
    return r;
}

// ---- reading ------------------------------------------------------------------

std::uint64_t parse_u64(const std::string& s, const char* what) {
    if (s.empty() || s.size() > 20 || s.find_first_not_of("0123456789") != std::string::npos) {
        throw DatasetInvalid(std::string{"dataset.json: "} + what + " is not a decimal number");
    }
    return std::stoull(s);
}

std::uint64_t uint_field(const JsonValue& v, const char* key) {
    if (!v.is_object() || !v.contains(key) || !v.at(key).is_integral_number() ||
        v.at(key).as_int() < 0) {
        throw DatasetInvalid(std::string{"dataset.json: "} + key + " is not a whole number");
    }
    return static_cast<std::uint64_t>(v.at(key).as_int());
}

std::uint8_t scope_from(const std::string& s) {
    if (s == "keyed") {
        return kKeyed;
    }
    if (s == "operator") {
        return kOperator;
    }
    if (s == "raw") {
        return kRaw;
    }
    throw DatasetInvalid("dataset.json: unknown scope '" + s + "'");
}

// Every path component under the root is a plain directory or file.
void refuse_links(const fs::path& root, const std::string& rel) {
    std::error_code ec;
    fs::path walk = root;
    for (const auto& part : fs::path{rel}) {
        walk /= part;
        if (fs::is_symlink(fs::symlink_status(walk, ec))) {
            throw DatasetInvalid(rel + ": a symbolic link is on its path");
        }
    }
}

// ---- the streaming merge ------------------------------------------------------

int compare_identity(const DatasetEntry& x, const DatasetEntry& y) {
    if (x.op != y.op) {
        return x.op < y.op ? -1 : 1;
    }
    if (const int c = x.slot.compare(y.slot); c != 0) {
        return c < 0 ? -1 : 1;
    }
    if (x.scope != y.scope) {
        return x.scope < y.scope ? -1 : 1;
    }
    if (x.key_group != y.key_group) {
        return x.key_group < y.key_group ? -1 : 1;
    }
    if (x.scope != kKeyed && x.participant != y.participant) {
        return x.participant < y.participant ? -1 : 1;
    }
    if (const int c = x.user_key.compare(y.user_key); c != 0) {
        return c < 0 ? -1 : 1;
    }
    return 0;
}

// One participant's row group, read a batch at a time.
class GroupCursor {
public:
    GroupCursor(const fs::path& file, int row_group, std::uint32_t participant)
        : participant_(participant) {
        parquet::arrow::FileReaderBuilder builder;
        check(builder.OpenFile(file.string()), "cannot open entries.parquet");
        parquet::ArrowReaderProperties props;
        props.set_batch_size(4096);
        builder.properties(props);
        check(builder.Build(&reader_), "cannot read entries.parquet");
        if (row_group < 0 || row_group >= reader_->num_row_groups()) {
            throw DatasetInvalid("entries.parquet has no row group " + std::to_string(row_group));
        }
        auto rb = reader_->GetRecordBatchReader({row_group}, {0, 1, 2, 3, 4, 5, 6});
        check(rb.status(), "cannot read a row group");
        batches_ = std::move(*rb);
        advance();
    }

    [[nodiscard]] bool done() const { return done_; }
    [[nodiscard]] const DatasetEntry& current() const { return cur_; }

    void advance() {
        while (true) {
            if (batch_ && row_ < batch_->num_rows()) {
                load(row_++);
                return;
            }
            std::shared_ptr<arrow::RecordBatch> next;
            check(batches_->ReadNext(&next), "cannot read entries.parquet");
            if (!next) {
                done_ = true;
                return;
            }
            bind(next);
        }
    }

private:
    void bind(std::shared_ptr<arrow::RecordBatch> b) {
        const auto& s = *b->schema();
        if (!s.field(0)->type()->Equals(arrow::uint64()) ||
            !s.field(1)->type()->Equals(arrow::utf8()) ||
            !s.field(2)->type()->Equals(arrow::utf8()) ||
            !s.field(3)->type()->Equals(arrow::int16()) ||
            !s.field(4)->type()->Equals(arrow::uint32()) ||
            !s.field(5)->type()->Equals(arrow::binary()) ||
            !s.field(6)->type()->Equals(arrow::binary())) {
            throw DatasetInvalid("entries.parquet does not have the dataset's columns: " +
                                 s.ToString());
        }
        batch_ = std::move(b);
        row_ = 0;
        op_ = std::static_pointer_cast<arrow::UInt64Array>(batch_->column(0));
        slot_ = std::static_pointer_cast<arrow::StringArray>(batch_->column(1));
        scope_ = std::static_pointer_cast<arrow::StringArray>(batch_->column(2));
        kg_ = std::static_pointer_cast<arrow::Int16Array>(batch_->column(3));
        part_ = std::static_pointer_cast<arrow::UInt32Array>(batch_->column(4));
        uk_ = std::static_pointer_cast<arrow::BinaryArray>(batch_->column(5));
        val_ = std::static_pointer_cast<arrow::BinaryArray>(batch_->column(6));
    }

    void load(std::int64_t i) {
        DatasetEntry e;
        e.op = op_->Value(i);
        e.slot = std::string{slot_->GetView(i)};
        e.scope = scope_from(std::string{scope_->GetView(i)});
        e.key_group = kg_->IsNull(i) ? std::int16_t{-1} : kg_->Value(i);
        e.participant = part_->Value(i);
        e.user_key = std::string{uk_->GetView(i)};
        e.value = std::string{val_->GetView(i)};
        if (e.participant != participant_) {
            throw DatasetInvalid("entries.parquet: a row of participant " +
                                 std::to_string(e.participant) + " in participant " +
                                 std::to_string(participant_) + "'s row group");
        }
        if ((e.scope == kKeyed) != (e.key_group >= 0 && e.key_group < kNumKeyGroups)) {
            throw DatasetInvalid("entries.parquet: a key group that does not match its scope");
        }
        if (started_ && compare_identity(cur_, e) >= 0) {
            throw DatasetInvalid("entries.parquet: participant " + std::to_string(participant_) +
                                 "'s rows are not in identity order");
        }
        started_ = true;
        cur_ = std::move(e);
    }

    std::uint32_t participant_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::unique_ptr<arrow::RecordBatchReader> batches_;
    std::shared_ptr<arrow::RecordBatch> batch_;
    std::int64_t row_{0};
    std::shared_ptr<arrow::UInt64Array> op_;
    std::shared_ptr<arrow::StringArray> slot_;
    std::shared_ptr<arrow::StringArray> scope_;
    std::shared_ptr<arrow::Int16Array> kg_;
    std::shared_ptr<arrow::UInt32Array> part_;
    std::shared_ptr<arrow::BinaryArray> uk_;
    std::shared_ptr<arrow::BinaryArray> val_;
    DatasetEntry cur_;
    bool started_{false};
    bool done_{false};
};

// Every participant's row group merged into one stream in identity order.
class EntryStream {
public:
    explicit EntryStream(const Dataset& d) {
        for (const auto& p : d.participants) {
            if (p.row_group < 0) {
                continue;
            }
            cursors_.push_back(std::make_unique<GroupCursor>(
                d.root / kEntriesFile, static_cast<int>(p.row_group), p.index));
        }
        for (std::size_t i = 0; i < cursors_.size(); ++i) {
            if (!cursors_[i]->done()) {
                heap_.push(i);
            }
        }
    }

    // The next entry, or nullptr at the end. Valid until the next call.
    const DatasetEntry* next() {
        if (last_ != kNone) {
            cursors_[last_]->advance();
            if (!cursors_[last_]->done()) {
                heap_.push(last_);
            }
            last_ = kNone;
        }
        if (heap_.empty()) {
            return nullptr;
        }
        last_ = heap_.top();
        heap_.pop();
        return &cursors_[last_]->current();
    }

private:
    static constexpr std::size_t kNone = static_cast<std::size_t>(-1);
    struct Greater {
        const std::vector<std::unique_ptr<GroupCursor>>* c;
        bool operator()(std::size_t x, std::size_t y) const {
            return compare_identity((*c)[x]->current(), (*c)[y]->current()) > 0;
        }
    };
    std::vector<std::unique_ptr<GroupCursor>> cursors_;
    std::priority_queue<std::size_t, std::vector<std::size_t>, Greater> heap_{Greater{&cursors_}};
    std::size_t last_{kNone};
};

// ---- rendering --------------------------------------------------------------

JsonValue side_json(const DiffSide& s) {
    JsonObject o;
    o["path"] = JsonValue{s.path};
    o["job"] = num(s.job);
    o["checkpoint"] = num(s.checkpoint);
    o["generation"] = num(s.generation);
    o["complete"] = JsonValue{s.complete};
    o["graph_sha256"] = s.graph_sha256.empty() ? JsonValue{} : JsonValue{s.graph_sha256};
    return JsonValue{std::move(o)};
}

std::string side_text(const DiffSide& s) {
    return s.path + " (job " + std::to_string(s.job) + ", checkpoint " +
           std::to_string(s.checkpoint) + ", generation " + std::to_string(s.generation) + ", " +
           (s.complete ? "complete" : "PARTIAL") + ")";
}

}  // namespace

// ---- public -----------------------------------------------------------------

const char* scope_name(std::uint8_t scope) noexcept {
    switch (scope) {
        case kKeyed:
            return "keyed";
        case kOperator:
            return "operator";
        default:
            return "raw";
    }
}

std::string hex_bytes(std::string_view bytes, std::size_t max) {
    static constexpr char kHex[] = "0123456789abcdef";
    const auto n = max == 0 ? bytes.size() : std::min(bytes.size(), max);
    std::string out;
    out.reserve(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        const auto c = static_cast<unsigned char>(bytes[i]);
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0xF]);
    }
    return out;
}

DatasetResult export_checkpoint_dataset(const DatasetOptions& options) {
    Exporter e(options);
    return e.run();
}

namespace {

Dataset read_dataset(const fs::path& root_in) {
    std::error_code ec;
    const auto root = fs::absolute(root_in, ec).lexically_normal();
    if (!fs::is_directory(fs::symlink_status(root, ec))) {
        throw DatasetInvalid(root.string() + " is not a directory (a link is not followed)");
    }
    Dataset d;
    d.root = root;
    try {
        d.doc = config::parse(read_plain_file(root / kDatasetFile, kMaxDatasetJsonBytes));
    } catch (const DatasetInvalid&) {
        throw;
    } catch (const std::exception& e) {
        throw DatasetInvalid(std::string{"dataset.json does not parse: "} + e.what());
    }
    const auto& doc = d.doc;
    if (!doc.is_object() || doc.string_or("format", "") != kDatasetFormat) {
        throw DatasetInvalid(root.string() + " is not a " + kDatasetFormat);
    }
    if (doc.int_or("version", 0) != kDatasetVersion) {
        throw DatasetInvalid("dataset.json is version " + std::to_string(doc.int_or("version", 0)) +
                             "; this build reads version " + std::to_string(kDatasetVersion));
    }
    // The bundle under it, by its own rules.
    incident::Bundle b;
    try {
        b = incident::read_bundle(root);
    } catch (const std::exception& e) {
        throw DatasetInvalid(std::string{"the dataset's bundle is refused: "} + e.what());
    }
    const auto& ej = doc.at("entries");
    if (ej.string_or("path", "") != kEntriesFile) {
        throw DatasetInvalid("dataset.json names an entry table other than entries.parquet");
    }
    refuse_links(root, kEntriesFile);
    refuse_links(root, "manifest.json");
    const auto st = fs::symlink_status(root / kEntriesFile, ec);
    if (ec || !fs::is_regular_file(st)) {
        throw DatasetInvalid("entries.parquet is missing or not a regular file");
    }
    if (fs::file_size(root / kEntriesFile, ec) != uint_field(ej, "bytes") ||
        incident::sha256_file(root / kEntriesFile) != ej.string_or("sha256", "")) {
        throw DatasetInvalid(
            "entries.parquet does not hash to what dataset.json records: the dataset was "
            "changed or damaged after it was exported");
    }
    if (incident::sha256_file(root / "manifest.json") !=
        doc.at("bundle").string_or("manifest_sha256", "")) {
        throw DatasetInvalid(
            "the bundle manifest does not hash to what dataset.json records: the dataset was "
            "changed or damaged after it was exported");
    }
    d.job = uint_field(doc.at("job"), "id");
    d.checkpoint = uint_field(doc.at("checkpoint"), "id");
    d.generation = uint_field(doc.at("checkpoint"), "generation");
    if (d.job != b.job() || d.checkpoint != b.checkpoint()) {
        throw DatasetInvalid("dataset.json and the bundle name different checkpoints");
    }
    if (doc.at("job").contains("graph_sha256") && doc.at("job").at("graph_sha256").is_string()) {
        d.graph_sha256 = doc.at("job").at("graph_sha256").as_string();
    }
    if (const auto& r = doc.at("selection").at("restored"); r.is_object()) {
        d.restored = std::make_pair(uint_field(r, "restore"), uint_field(r, "next"));
    }
    if (!doc.at("complete").is_bool()) {
        throw DatasetInvalid("dataset.json: complete is not a boolean");
    }
    d.complete = doc.at("complete").as_bool();
    d.entries = uint_field(ej, "rows");
    std::set<std::int64_t> groups;
    std::uint64_t rows = 0;
    for (const auto& p : doc.at("participants").as_array()) {
        DatasetParticipant dp;
        dp.index = static_cast<std::uint32_t>(uint_field(p, "index"));
        dp.row_group = p.at("row_group").as_int();
        dp.rows = uint_field(p, "rows");
        if (dp.row_group >= 0) {
            if (!groups.insert(dp.row_group).second) {
                throw DatasetInvalid("dataset.json: two participants name one row group");
            }
            rows += dp.rows;
        }
        d.participants.push_back(dp);
    }
    if (rows != d.entries || groups.size() != uint_field(ej, "row_groups")) {
        throw DatasetInvalid("dataset.json's participants do not account for its entry table");
    }
    for (const auto& o : doc.at("operators").as_array()) {
        const auto op = parse_u64(o.at("op_id").as_string(), "op_id");
        for (const auto& p : o.at("participants").as_array()) {
            d.operator_participants[op].insert(static_cast<std::uint32_t>(p.as_int()));
        }
        for (const auto& s : o.at("slots").as_array()) {
            const auto slot = s.at("slot").as_string();
            const auto scope = scope_from(s.at("scope").as_string());
            const auto n = uint_field(s, "rows");
            if (n > 0) {
                d.slots[SlotKey{op, slot, scope}] = n;
            }
            SlotSchema sch;
            const auto& sj = s.at("schema");
            if (sj.is_object()) {
                if (sj.contains("fingerprint") && sj.at("fingerprint").is_string()) {
                    sch.fingerprint = sj.at("fingerprint").as_string();
                }
                if (sj.contains("versions") && sj.at("versions").is_object()) {
                    for (const auto& [t, v] : sj.at("versions").as_object()) {
                        sch.versions[std::string{t}] = static_cast<std::uint32_t>(v.as_int());
                    }
                }
            }
            d.schemas[{op, slot}] = std::move(sch);
        }
    }
    if (doc.contains("graph") && doc.at("graph").is_object()) {
        for (const auto& g : doc.at("graph").at("operators").as_array()) {
            if (g.at("op_id").is_string()) {
                d.graph_ops[parse_u64(g.at("op_id").as_string(), "op_id")] =
                    static_cast<std::uint32_t>(uint_field(g, "parallelism"));
            }
        }
    }
    return d;
}

}  // namespace

Dataset open_dataset(const fs::path& root) {
    try {
        return read_dataset(root);
    } catch (const DatasetInvalid&) {
        throw;
    } catch (const std::exception& e) {
        // A field of the wrong type or missing.
        throw DatasetInvalid(std::string{"dataset.json is malformed: "} + e.what());
    }
}

void for_each_entry(const Dataset& dataset, const std::function<bool(const DatasetEntry&)>& visit) {
    EntryStream s(dataset);
    while (const auto* e = s.next()) {
        if (!visit(*e)) {
            return;
        }
    }
}

bool DatasetDiff::differences() const {
    for (const auto& s : slots) {
        if (s.schema_changed || s.status == "operator-only-in-a" ||
            s.status == "operator-only-in-b" || s.added + s.removed + s.changed > 0) {
            return true;
        }
    }
    return false;
}

int DatasetDiff::exit_code() const {
    if (!established) {
        return 3;
    }
    return differences() ? 1 : 0;
}

DatasetDiff diff_datasets(const Dataset& a, const Dataset& b, const StateDiffOptions& options) {
    const auto started = Clock::now();
    DatasetDiff r;
    r.max_samples = options.max_samples;
    const auto side = [](const Dataset& d) {
        return DiffSide{.path = d.root.string(),
                        .job = d.job,
                        .checkpoint = d.checkpoint,
                        .generation = d.generation,
                        .complete = d.complete,
                        .graph_sha256 = d.graph_sha256};
    };
    r.a = side(a);
    r.b = side(b);
    r.same_job = a.job == b.job;
    r.same_graph = !a.graph_sha256.empty() && a.graph_sha256 == b.graph_sha256;

    // Lineage: what relates the two checkpoints.
    if (!r.same_job) {
        r.lineage.push_back("different jobs (" + std::to_string(a.job) + " and " +
                            std::to_string(b.job) + "): operators are matched by id");
    }
    if (a.graph_sha256.empty() || b.graph_sha256.empty()) {
        r.lineage.push_back("a side holds no job graph, so whether the graphs match is unknown");
    } else if (!r.same_graph) {
        r.lineage.push_back(
            "the job graphs differ (a rescale, or another build of the job): operators are "
            "matched by id");
    }
    if (r.same_job && a.generation != b.generation) {
        r.lineage.push_back("different runs: generation " + std::to_string(a.generation) + " and " +
                            std::to_string(b.generation));
    }
    if (r.same_job && a.restored != b.restored && b.restored.has_value()) {
        r.lineage.push_back("b's run was restored from checkpoint " +
                            std::to_string(b.restored->first) + " (numbering from " +
                            std::to_string(b.restored->second) + ")");
    }
    if (r.same_job && a.checkpoint == b.checkpoint && a.generation == b.generation) {
        r.lineage.push_back("the same checkpoint");
    } else if (r.same_job && b.checkpoint < a.checkpoint) {
        r.lineage.push_back("b is an earlier checkpoint than a");
    }
    if (!a.complete) {
        r.established = false;
        r.reasons.push_back("a is a partial dataset");
    }
    if (!b.complete) {
        r.established = false;
        r.reasons.push_back("b is a partial dataset");
    }
    if (!r.established && !options.allow_partial) {
        r.reasons.push_back(
            "nothing was compared; --allow-partial compares a partial dataset, and the result "
            "is still not established");
        return r;
    }

    // The plan: every slot either side holds or stamps.
    std::set<std::uint64_t> ops_a;
    std::set<std::uint64_t> ops_b;
    std::set<SlotKey> keys;
    for (const auto& [k, n] : a.slots) {
        keys.insert(k);
        ops_a.insert(std::get<0>(k));
    }
    for (const auto& [k, n] : b.slots) {
        keys.insert(k);
        ops_b.insert(std::get<0>(k));
    }
    for (const auto& [k, s] : a.schemas) {
        ops_a.insert(k.first);
    }
    for (const auto& [k, s] : b.schemas) {
        ops_b.insert(k.first);
    }
    for (const auto& [op, par] : a.graph_ops) {
        ops_a.insert(op);
    }
    for (const auto& [op, par] : b.graph_ops) {
        ops_b.insert(op);
    }
    std::map<SlotKey, SlotComparison> plan;
    const auto schema_of = [](const Dataset& d, std::uint64_t op, const std::string& slot) {
        const auto it = d.schemas.find({op, slot});
        return it == d.schemas.end() ? SlotSchema{} : it->second;
    };
    const auto describe = [](const SlotSchema& s) {
        std::string out = s.fingerprint ? "fingerprint " + *s.fingerprint : "no fingerprint";
        for (const auto& [t, v] : s.versions) {
            out += ", " + t + " v" + std::to_string(v);
        }
        return out;
    };
    for (const auto& k : keys) {
        const auto& [op, slot, scope] = k;
        SlotComparison c;
        c.op = op;
        c.slot = slot;
        c.scope = scope_name(scope);
        if (const auto it = a.slots.find(k); it != a.slots.end()) {
            c.entries_a = it->second;
        }
        if (const auto it = b.slots.find(k); it != b.slots.end()) {
            c.entries_b = it->second;
        }
        const auto sa = schema_of(a, op, slot);
        const auto sb = schema_of(b, op, slot);
        if (!ops_b.contains(op)) {
            c.status = "operator-only-in-a";
            c.note = "operator " + std::to_string(op) + " is not in b";
        } else if (!ops_a.contains(op)) {
            c.status = "operator-only-in-b";
            c.note = "operator " + std::to_string(op) + " is not in a";
        } else if (scope != kKeyed &&
                   (a.operator_participants.count(op) == 0 ||
                    b.operator_participants.count(op) == 0 ||
                    a.operator_participants.at(op) != b.operator_participants.at(op) ||
                    (a.graph_ops.contains(op) && b.graph_ops.contains(op) &&
                     a.graph_ops.at(op) != b.graph_ops.at(op)))) {
            const auto set_text = [&](const Dataset& d) {
                std::string s;
                if (const auto it = d.operator_participants.find(op);
                    it != d.operator_participants.end()) {
                    for (const auto p : it->second) {
                        s += (s.empty() ? "" : ",") + std::to_string(p);
                    }
                }
                return "{" + s + "}";
            };
            c.status = "subtasks-changed";
            c.note = "not comparable: the operator's subtasks changed (" + set_text(a) + " -> " +
                     set_text(b) + "), and its " + c.scope +
                     " rows belong to the subtask that wrote them";
        } else if (!(sa == sb)) {
            c.schema_changed = true;
            if (options.raw_bytes) {
                c.status = "raw-compared";
                c.note = "schema changed (" + describe(sa) + " -> " + describe(sb) +
                         "); compared as raw bytes, which says nothing about the values' meaning";
            } else {
                c.status = "schema-changed";
                c.note = "not compared: schema changed (" + describe(sa) + " -> " + describe(sb) +
                         "); --raw-bytes compares the stored bytes, labelled raw";
            }
        } else {
            c.status = "compared";
            if (!r.same_graph && !sa.fingerprint && sa.versions.empty()) {
                c.note =
                    "neither side stamps this slot's schema and the graphs differ: compared as "
                    "stored bytes, whose encoding is not recorded";
            }
        }
        if (!c.compared() && c.status != "operator-only-in-a" && c.status != "operator-only-in-b") {
            r.established = false;
            r.reasons.push_back("operator " + std::to_string(op) + " slot " + slot + " (" +
                                c.scope + ") was not compared: " + c.note);
        }
        plan.emplace(k, std::move(c));
    }
    // Slots a schema stamp names on both sides but neither holds an entry of.
    for (const auto& [k, s] : a.schemas) {
        const auto key = SlotKey{k.first, k.second, kKeyed};
        bool held = false;
        for (std::uint8_t sc = 0; sc < 3; ++sc) {
            held = held || plan.contains(SlotKey{k.first, k.second, sc});
        }
        if (held || !b.schemas.contains(k) || b.schemas.at(k) == s) {
            continue;
        }
        SlotComparison c;
        c.op = k.first;
        c.slot = k.second;
        c.scope = "keyed";
        c.schema_changed = true;
        c.status = options.raw_bytes ? "raw-compared" : "schema-changed";
        c.note = "schema changed (" + describe(s) + " -> " + describe(b.schemas.at(k)) +
                 "); the slot holds no entry on either side";
        plan.emplace(key, std::move(c));
    }

    // The merge.
    EntryStream sa(a);
    EntryStream sb(b);
    const DatasetEntry* ea = sa.next();
    const DatasetEntry* eb = sb.next();
    SlotComparison* cur = nullptr;
    SlotKey cur_key;
    const auto slot_for = [&](const DatasetEntry& e) -> SlotComparison* {
        SlotKey k{e.op, e.slot, e.scope};
        if (cur == nullptr || k != cur_key) {
            const auto it = plan.find(k);
            if (it == plan.end()) {
                throw DatasetInvalid("an entry of a slot dataset.json does not list: operator " +
                                     std::to_string(e.op) + " slot " + e.slot);
            }
            cur = &it->second;
            cur_key = std::move(k);
        }
        return cur;
    };
    const auto sample =
        [&](SlotComparison& c, const char* kind, const DatasetEntry* x, const DatasetEntry* y) {
            if (c.samples.size() >= options.max_samples) {
                return;
            }
            const auto& e = x != nullptr ? *x : *y;
            DiffSample s;
            s.kind = kind;
            s.key_group = e.key_group;
            s.user_key_hex = hex_bytes(e.user_key, kSampleBytes);
            s.user_key_bytes = e.user_key.size();
            if (x != nullptr) {
                s.participant_a = x->participant;
                s.value_a_hex = hex_bytes(x->value, kSampleBytes);
                s.value_a_bytes = x->value.size();
            }
            if (y != nullptr) {
                s.participant_b = y->participant;
                s.value_b_hex = hex_bytes(y->value, kSampleBytes);
                s.value_b_bytes = y->value.size();
            }
            c.samples.push_back(std::move(s));
        };
    while (ea != nullptr || eb != nullptr) {
        if (r.entries_read >= options.max_entries ||
            ((r.entries_read & 1023U) == 0 && Clock::now() - started > options.timeout)) {
            r.limit_reached = true;
            r.established = false;
            r.reasons.push_back(
                r.entries_read >= options.max_entries
                    ? "the entry limit (--max-entries=" + std::to_string(options.max_entries) +
                          ") was reached; the counts cover only the entries read"
                    : "the time limit (--timeout-s) was reached; the counts cover only the "
                      "entries read");
            break;
        }
        const int c = ea == nullptr ? 1 : eb == nullptr ? -1 : compare_identity(*ea, *eb);
        if (c < 0) {
            auto* s = slot_for(*ea);
            ++r.entries_read;
            if (s->compared()) {
                ++s->removed;
                sample(*s, "removed", ea, nullptr);
            }
            ea = sa.next();
        } else if (c > 0) {
            auto* s = slot_for(*eb);
            ++r.entries_read;
            if (s->compared()) {
                ++s->added;
                sample(*s, "added", nullptr, eb);
            }
            eb = sb.next();
        } else {
            auto* s = slot_for(*ea);
            r.entries_read += 2;
            if (s->compared()) {
                if (ea->value == eb->value) {
                    ++s->unchanged;
                } else {
                    ++s->changed;
                    sample(*s, "changed", ea, eb);
                }
                if (ea->participant != eb->participant) {
                    ++s->relocated;
                }
            }
            ea = sa.next();
            eb = sb.next();
        }
    }
    for (auto& [k, c] : plan) {
        r.slots.push_back(std::move(c));
    }
    return r;
}

std::string DatasetDiff::render_json() const {
    JsonObject o;
    o["format"] = JsonValue{std::string{kStateDiffFormat}};
    o["version"] = JsonValue{kStateDiffVersion};
    o["a"] = side_json(a);
    o["b"] = side_json(b);
    JsonObject lj;
    lj["same_job"] = JsonValue{same_job};
    lj["same_graph"] = JsonValue{same_graph};
    JsonArray notes;
    for (const auto& l : lineage) {
        notes.push_back(JsonValue{l});
    }
    lj["notes"] = JsonValue{std::move(notes)};
    o["lineage"] = JsonValue{std::move(lj)};
    o["established"] = JsonValue{established};
    JsonArray rs;
    for (const auto& s : reasons) {
        rs.push_back(JsonValue{s});
    }
    o["reasons"] = JsonValue{std::move(rs)};
    o["limit_reached"] = JsonValue{limit_reached};
    o["entries_read"] = num(entries_read);
    o["differences"] = JsonValue{differences()};
    o["exit"] = JsonValue{static_cast<std::int64_t>(exit_code())};
    std::array<std::uint64_t, 5> t{};
    JsonArray sl;
    for (const auto& s : slots) {
        JsonObject j;
        j["op_id"] = JsonValue{std::to_string(s.op)};
        j["slot"] = JsonValue{s.slot};
        j["scope"] = JsonValue{s.scope};
        j["status"] = JsonValue{s.status};
        j["note"] = s.note.empty() ? JsonValue{} : JsonValue{s.note};
        j["schema_changed"] = JsonValue{s.schema_changed};
        j["comparison"] = JsonValue{std::string{s.status == "raw-compared" ? "raw-bytes"
                                                : s.compared()             ? "bytes"
                                                                           : "none"}};
        j["entries_a"] = num(s.entries_a);
        j["entries_b"] = num(s.entries_b);
        j["added"] = num(s.added);
        j["removed"] = num(s.removed);
        j["changed"] = num(s.changed);
        j["unchanged"] = num(s.unchanged);
        j["relocated"] = num(s.relocated);
        t[0] += s.added;
        t[1] += s.removed;
        t[2] += s.changed;
        t[3] += s.unchanged;
        t[4] += s.relocated;
        JsonArray samples;
        for (const auto& x : s.samples) {
            JsonObject sj;
            sj["kind"] = JsonValue{x.kind};
            if (s.status == "raw-compared") {
                sj["raw"] = JsonValue{true};
            }
            sj["key_group"] =
                x.key_group >= 0 ? JsonValue{static_cast<std::int64_t>(x.key_group)} : JsonValue{};
            sj["participant_a"] = x.participant_a ? num(*x.participant_a) : JsonValue{};
            sj["participant_b"] = x.participant_b ? num(*x.participant_b) : JsonValue{};
            sj["user_key_hex"] = JsonValue{x.user_key_hex};
            sj["user_key_bytes"] = num(x.user_key_bytes);
            sj["value_a_hex"] = x.value_a_hex ? JsonValue{*x.value_a_hex} : JsonValue{};
            sj["value_a_bytes"] = x.value_a_hex ? num(x.value_a_bytes) : JsonValue{};
            sj["value_b_hex"] = x.value_b_hex ? JsonValue{*x.value_b_hex} : JsonValue{};
            sj["value_b_bytes"] = x.value_b_hex ? num(x.value_b_bytes) : JsonValue{};
            samples.push_back(JsonValue{std::move(sj)});
        }
        j["samples"] = JsonValue{std::move(samples)};
        j["samples_truncated"] = JsonValue{s.added + s.removed + s.changed >
                                           static_cast<std::uint64_t>(s.samples.size())};
        sl.push_back(JsonValue{std::move(j)});
    }
    JsonObject tj;
    tj["added"] = num(t[0]);
    tj["removed"] = num(t[1]);
    tj["changed"] = num(t[2]);
    tj["unchanged"] = num(t[3]);
    tj["relocated"] = num(t[4]);
    o["totals"] = JsonValue{std::move(tj)};
    o["max_samples"] = num(max_samples);
    o["sample_bytes"] = num(kSampleBytes);
    o["slots"] = JsonValue{std::move(sl)};
    return JsonValue{std::move(o)}.serialize(2) + "\n";
}

std::string DatasetDiff::render_text() const {
    std::ostringstream out;
    out << "a: " << side_text(a) << "\n";
    out << "b: " << side_text(b) << "\n";
    for (const auto& l : lineage) {
        out << "lineage: " << l << "\n";
    }
    for (const auto& s : slots) {
        const bool raw = s.status == "raw-compared";
        out << "\nop " << s.op << " slot " << s.slot << " (" << s.scope << ")"
            << (raw ? " [RAW BYTES]" : "") << ": ";
        if (s.compared()) {
            out << "+" << s.added << " -" << s.removed << " ~" << s.changed << " =" << s.unchanged;
            if (s.relocated > 0) {
                out << ", " << s.relocated << " relocated (owner changed, not a change)";
            }
            out << "\n";
        } else {
            out << s.status << "\n";
        }
        if (!s.note.empty()) {
            out << "  " << s.note << "\n";
        }
        for (const auto& x : s.samples) {
            out << "  " << (raw ? "raw " : "") << x.kind;
            if (x.key_group >= 0) {
                out << " kg " << x.key_group;
            }
            out << " key " << x.user_key_hex << " (" << x.user_key_bytes << " bytes)";
            if (x.value_a_hex) {
                out << " a=" << *x.value_a_hex << " (" << x.value_a_bytes << " bytes)";
            }
            if (x.value_b_hex) {
                out << " b=" << *x.value_b_hex << " (" << x.value_b_bytes << " bytes)";
            }
            out << "\n";
        }
        if (s.added + s.removed + s.changed > s.samples.size()) {
            out << "  (" << s.samples.size() << " of " << (s.added + s.removed + s.changed)
                << " differences shown)\n";
        }
    }
    out << "\n";
    if (!established) {
        out << "result: NOT ESTABLISHED (exit 3)\n";
        for (const auto& s : reasons) {
            out << "  " << s << "\n";
        }
    } else if (differences()) {
        out << "result: differences (exit 1)\n";
    } else {
        out << "result: no differences (exit 0)\n";
    }
    return out.str();
}

}  // namespace clink::state_processor
