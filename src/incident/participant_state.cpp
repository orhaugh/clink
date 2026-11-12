// A participant's checkpoint in a bundle, whatever backend wrote it
// (include/clink/incident/bundle.hpp, "a participant's checkpoint").

#include <atomic>
#include <cstring>
#include <fstream>
#include <map>
#include <mutex>
#include <random>
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "clink/incident/bundle.hpp"
#include "clink/state/external_materialization_store.hpp"
#include "clink/state/snapshot_canonicalise.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace clink::incident {

namespace {

namespace fs = std::filesystem;

std::mutex& readers_mutex() {
    static std::mutex m;
    return m;
}

std::map<std::string, DirectoryStateReader>& readers() {
    static std::map<std::string, DirectoryStateReader> r;
    return r;
}

std::vector<std::byte> read_bytes(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot read " + p.string());
    }
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::vector<std::byte> out(raw.size());
    if (!raw.empty()) {
        std::memcpy(out.data(), raw.data(), raw.size());
    }
    return out;
}

// A changelog snapshot's handles name files under the job's state root as it
// was on the machine that wrote them; the bundle holds them at the same place
// under its own state root.
class BundleMaterializationStore final : public ExternalMaterializationStore {
public:
    BundleMaterializationStore(fs::path bundle_root, std::string original_root)
        : bundle_root_(std::move(bundle_root)), original_root_(std::move(original_root)) {}

    // A replay materialises again; that copy is kept here, never in the bundle.
    std::string write(CheckpointId id, std::span<const std::byte> bytes) override {
        const auto handle =
            "replay:" + std::to_string(id.value()) + ":" + std::to_string(written_.size());
        written_[handle].assign(bytes.begin(), bytes.end());
        return handle;
    }

    std::vector<std::byte> read(const std::string& handle) override {
        if (const auto it = written_.find(handle); it != written_.end()) {
            return it->second;
        }
        if (original_root_.empty() || !handle.starts_with(original_root_ + "/")) {
            throw StateUnsupported("a materialisation handle outside the job's state root: " +
                                   handle);
        }
        const auto rel = handle.substr(original_root_.size() + 1);
        if (!safe_relative_path(rel)) {
            throw StateUnsupported("a materialisation handle that is not a plain path: " + handle);
        }
        return read_bytes(bundle_root_ / rel);
    }

    [[nodiscard]] std::string description() const override {
        return "the bundle's copies of " + original_root_;
    }

private:
    fs::path bundle_root_;  // the bundle-side state root
    std::string original_root_;
    std::map<std::string, std::vector<std::byte>> written_;
};

}  // namespace

void register_directory_state_reader(const std::string& backend, DirectoryStateReader reader) {
    std::lock_guard lock(readers_mutex());
    readers()[backend] = std::move(reader);
}

std::vector<ParticipantState> participant_states(const Bundle& b) {
    std::vector<ParticipantState> out;
    const auto& sel = b.manifest.at("selection");
    if (sel.contains("participant_state") && sel.at("participant_state").is_array()) {
        for (const auto& p : sel.at("participant_state").as_array()) {
            if (!p.is_object() || !p.contains("index") || !p.at("index").is_integral_number()) {
                continue;
            }
            ParticipantState s;
            s.index = static_cast<std::uint32_t>(p.at("index").as_int());
            s.backend = p.string_or("backend", "file");
            s.path = p.string_or("path", "");
            out.push_back(std::move(s));
        }
        return out;
    }
    // An older bundle: the file backend's layout for every participant.
    const auto state_rel = sel.string_or("state_root", "");
    const auto generation = sel.int_or("generation", 0);
    if (state_rel.empty() || generation <= 0 || !sel.contains("participants") ||
        !sel.at("participants").is_array()) {
        return out;
    }
    for (const auto& v : sel.at("participants").as_array()) {
        if (!v.is_integral_number()) {
            continue;
        }
        const auto idx = static_cast<std::uint32_t>(v.as_int());
        out.push_back(ParticipantState{
            .index = idx,
            .backend = "file",
            .path = state_dir_for(state_rel, static_cast<std::uint32_t>(generation), idx) +
                    "/checkpoint-" + std::to_string(b.checkpoint()) + ".snap"});
    }
    return out;
}

std::vector<std::string> changelog_materialization_handles(const std::vector<std::byte>& bytes) {
    std::vector<std::string> out;
    if (bytes.empty()) {
        return out;
    }
    auto buf = std::make_shared<arrow::Buffer>(reinterpret_cast<const std::uint8_t*>(bytes.data()),
                                               static_cast<std::int64_t>(bytes.size()));
    auto reader =
        arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buf));
    if (!reader.ok()) {
        throw std::runtime_error("not an Arrow IPC stream: " + reader.status().ToString());
    }
    const auto schema = (*reader)->schema();
    if (schema->num_fields() != 4 || schema->field(0)->name() != "row_kind" ||
        !schema->field(0)->type()->Equals(arrow::uint8()) ||
        !schema->field(3)->type()->Equals(arrow::binary())) {
        throw std::runtime_error("not a changelog snapshot: " + schema->ToString());
    }
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        const auto st = (*reader)->ReadNext(&batch);
        if (!st.ok()) {
            throw std::runtime_error("the changelog snapshot does not decode: " + st.ToString());
        }
        if (!batch) {
            break;
        }
        const auto kinds = std::static_pointer_cast<arrow::UInt8Array>(batch->column(0));
        const auto values = std::static_pointer_cast<arrow::BinaryArray>(batch->column(3));
        for (std::int64_t i = 0; i < batch->num_rows(); ++i) {
            if (kinds->Value(i) == 3) {
                out.emplace_back(values->GetView(i));
            }
        }
    }
    return out;
}

std::vector<std::byte> canonical_participant_state(const Bundle& b, const ParticipantState& p) {
    if (p.backend == "file") {
        return canonicalise_state_snapshot(read_bytes(b.root / p.path));
    }
    if (p.backend == "changelog") {
        const auto& sel = b.manifest.at("selection");
        auto store = std::make_shared<BundleMaterializationStore>(
            b.root / sel.string_or("state_root", ""), sel.string_or("state_root_original", ""));
        try {
            return canonicalise_state_snapshot(read_bytes(b.root / p.path), store);
        } catch (const StateUnsupported&) {
            throw;
        }
    }
    DirectoryStateReader reader;
    {
        std::lock_guard lock(readers_mutex());
        if (const auto it = readers().find(p.backend); it != readers().end()) {
            reader = it->second;
        }
    }
    if (!reader) {
        throw StateUnsupported("this build cannot read a " + p.backend +
                               " checkpoint (a clink linked with " + p.backend + " can)");
    }
    // A scratch copy: the reader opens a database there, which may write.
    static std::atomic<std::uint64_t> seq{0};
    std::error_code ec;
    const auto scratch =
        fs::temp_directory_path() / ("clink-participant-" + std::to_string(::getpid()) + "-" +
                                     std::to_string(seq.fetch_add(1)));
    fs::create_directories(scratch);
    try {
        for (const auto& a : b.artifacts) {
            if (a.path.starts_with(p.path + "/")) {
                const auto dest = scratch / a.path.substr(p.path.size() + 1);
                fs::create_directories(dest.parent_path());
                fs::copy_file(b.root / a.path, dest, fs::copy_options::overwrite_existing);
            }
        }
        auto bytes = reader(scratch);
        fs::remove_all(scratch, ec);
        return bytes;
    } catch (...) {
        fs::remove_all(scratch, ec);
        throw;
    }
}

}  // namespace clink::incident
