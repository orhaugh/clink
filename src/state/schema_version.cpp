#include "clink/state/schema_version.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

namespace clink {

namespace {

// Auto-migration helpers -------------------------------------------------

// Is `from` an integer type that promotes losslessly to `to`? Returns
// true for same-width integer with same signedness, and for widenings
// (int8 -> int16/32/64, etc.). Does NOT permit signed/unsigned
// crossover (semantic mismatch risk).
bool is_non_narrowing_integer_promotion(const arrow::DataType& from, const arrow::DataType& to) {
    if (from.Equals(to)) {
        return true;
    }
    using arrow::Type;
    auto width_signed = [](Type::type id) -> int {
        switch (id) {
            case Type::INT8:
                return 8;
            case Type::INT16:
                return 16;
            case Type::INT32:
                return 32;
            case Type::INT64:
                return 64;
            default:
                return 0;
        }
    };
    auto width_unsigned = [](Type::type id) -> int {
        switch (id) {
            case Type::UINT8:
                return 8;
            case Type::UINT16:
                return 16;
            case Type::UINT32:
                return 32;
            case Type::UINT64:
                return 64;
            default:
                return 0;
        }
    };
    const int fs = width_signed(from.id());
    const int ts = width_signed(to.id());
    if (fs > 0 && ts > 0) {
        return ts >= fs;
    }
    const int fu = width_unsigned(from.id());
    const int tu = width_unsigned(to.id());
    if (fu > 0 && tu > 0) {
        return tu >= fu;
    }
    return false;
}

// Check that `from` schema is additively-compatible with `to` schema.
// Rules:
//   - Every field in `from` must exist in `to` with the same name.
//   - The field's type must equal or be a non-narrowing integer
//     promotion of the `from` type.
//   - Fields in `to` that don't exist in `from` MUST be nullable
//     (we'll fill them with nulls during migration).
//   - Field removal disallowed (data loss).
bool schemas_additively_compatible(const arrow::Schema& from, const arrow::Schema& to) {
    for (int i = 0; i < from.num_fields(); ++i) {
        const auto& old_field = from.field(i);
        const auto new_field_idx = to.GetFieldIndex(old_field->name());
        if (new_field_idx < 0) {
            return false;  // field removed
        }
        const auto& new_field = to.field(new_field_idx);
        if (!is_non_narrowing_integer_promotion(*old_field->type(), *new_field->type())) {
            return false;
        }
    }
    // New fields not present in old must be nullable.
    for (int i = 0; i < to.num_fields(); ++i) {
        const auto& new_field = to.field(i);
        if (from.GetFieldIndex(new_field->name()) >= 0) {
            continue;  // exists in both, already checked above
        }
        if (!new_field->nullable()) {
            return false;
        }
    }
    return true;
}

[[noreturn]] void throw_arrow(const std::string& where, const arrow::Status& s) {
    throw std::runtime_error("clink::StateMigrationRegistry " + where + ": " + s.ToString());
}

// Read input bytes as an Arrow IPC stream and project to the target
// schema. Each new field that doesn't exist in the input is filled
// with nulls; integer-typed fields that widen are cast losslessly.
std::vector<std::byte> arrow_migrate_bytes(std::span<const std::byte> input,
                                           const arrow::Schema& from_schema,
                                           const arrow::Schema& to_schema) {
    if (input.empty()) {
        // Empty payload: encode an empty record batch under the new
        // schema. Callers that store explicit empties round-trip.
        auto sink_result = arrow::io::BufferOutputStream::Create();
        if (!sink_result.ok())
            throw_arrow("empty_create_sink", sink_result.status());
        auto sink = *sink_result;
        const auto schema_ptr = std::shared_ptr<arrow::Schema>(
            const_cast<arrow::Schema*>(&to_schema), [](arrow::Schema*) {});
        auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema_ptr);
        if (!writer_result.ok())
            throw_arrow("empty_make_writer", writer_result.status());
        auto writer = *writer_result;
        if (auto s = writer->Close(); !s.ok())
            throw_arrow("empty_close_writer", s);
        auto buf_result = sink->Finish();
        if (!buf_result.ok())
            throw_arrow("empty_finish", buf_result.status());
        auto buf = *buf_result;
        std::vector<std::byte> out(static_cast<std::size_t>(buf->size()));
        if (buf->size() > 0) {
            std::memcpy(out.data(), buf->data(), static_cast<std::size_t>(buf->size()));
        }
        return out;
    }

    auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(input.data()),
                                                  static_cast<int64_t>(input.size()));
    auto reader_input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(reader_input);
    if (!reader_result.ok())
        throw_arrow("auto_migrate (open reader)", reader_result.status());
    auto reader = *reader_result;

    // Validate the input schema actually matches the registered
    // from_schema. We can't auto-migrate if the data on disk is for
    // a different schema than the one the user registered.
    if (!reader->schema()->Equals(from_schema, /*check_metadata=*/false)) {
        throw std::runtime_error(
            "clink::StateMigrationRegistry::migrate: input Arrow schema does not match the "
            "registered from-schema for this version");
    }

    auto sink_result = arrow::io::BufferOutputStream::Create();
    if (!sink_result.ok())
        throw_arrow("auto_migrate (create sink)", sink_result.status());
    auto sink = *sink_result;
    const auto to_schema_ptr = std::shared_ptr<arrow::Schema>(
        const_cast<arrow::Schema*>(&to_schema), [](arrow::Schema*) {});
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, to_schema_ptr);
    if (!writer_result.ok())
        throw_arrow("auto_migrate (make writer)", writer_result.status());
    auto writer = *writer_result;

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        if (auto s = reader->ReadNext(&batch); !s.ok()) {
            throw_arrow("auto_migrate (read batch)", s);
        }
        if (!batch)
            break;

        const auto num_rows = batch->num_rows();
        std::vector<std::shared_ptr<arrow::Array>> projected;
        projected.reserve(to_schema.num_fields());
        for (int j = 0; j < to_schema.num_fields(); ++j) {
            const auto& new_field = to_schema.field(j);
            const auto old_idx = from_schema.GetFieldIndex(new_field->name());
            if (old_idx < 0) {
                // New field absent in old schema: fill with nulls.
                auto null_array_result = arrow::MakeArrayOfNull(new_field->type(), num_rows);
                if (!null_array_result.ok())
                    throw_arrow("auto_migrate (null array)", null_array_result.status());
                projected.push_back(*null_array_result);
                continue;
            }
            auto old_col = batch->column(old_idx);
            if (old_col->type()->Equals(*new_field->type())) {
                projected.push_back(old_col);
                continue;
            }
            // Cast to the wider type. Arrow's Cast kernel handles the
            // integer-widening cases we accept under
            // is_non_narrowing_integer_promotion.
            auto cast_result = arrow::compute::Cast(arrow::Datum(old_col), new_field->type());
            if (!cast_result.ok())
                throw_arrow("auto_migrate (cast)", cast_result.status());
            projected.push_back(cast_result->make_array());
        }

        auto new_batch = arrow::RecordBatch::Make(to_schema_ptr, num_rows, projected);
        if (auto s = writer->WriteRecordBatch(*new_batch); !s.ok()) {
            throw_arrow("auto_migrate (write batch)", s);
        }
    }
    if (auto s = writer->Close(); !s.ok())
        throw_arrow("auto_migrate (close writer)", s);

    auto buf_result = sink->Finish();
    if (!buf_result.ok())
        throw_arrow("auto_migrate (finish)", buf_result.status());
    auto buf = *buf_result;
    std::vector<std::byte> out(static_cast<std::size_t>(buf->size()));
    if (buf->size() > 0) {
        std::memcpy(out.data(), buf->data(), static_cast<std::size_t>(buf->size()));
    }
    return out;
}

}  // namespace

void StateVersionMap::set(OperatorId op,
                          std::string state_type,
                          std::uint32_t version,
                          std::string slot) {
    if (state_type.find('\n') != std::string::npos || state_type.find('|') != std::string::npos) {
        throw std::invalid_argument(
            "StateVersionMap::set: state_type must not contain '\\n' or '|' (delimiters reserved "
            "for on-disk packing)");
    }
    if (slot.find('\n') != std::string::npos || slot.find('|') != std::string::npos) {
        throw std::invalid_argument(
            "StateVersionMap::set: slot must not contain '\\n' or '|' (delimiters reserved "
            "for on-disk packing)");
    }
    Key key{op, std::move(state_type)};
    // Enforce the 1:1 (op, state_type) <-> slot invariant the migrator
    // relies on. Re-binding the SAME state_type to a different non-empty
    // slot would silently drop the first slot's stamp (the map is keyed
    // on (op, state_type)), so the migrator would never migrate it. Fail
    // fast instead. A pure version-bump re-set (same slot, or going
    // empty<->named) is still allowed.
    if (auto it = entries_.find(key); it != entries_.end()) {
        const std::string& existing = it->second.slot;
        if (!existing.empty() && !slot.empty() && existing != slot) {
            throw std::invalid_argument("StateVersionMap::set: state_type '" + key.state_type +
                                        "' is already bound to slot '" + existing +
                                        "' on this operator; give each "
                                        "keyed-state slot a distinct state_type tag");
        }
    }
    entries_[key] = Stamp{version, std::move(slot)};
}

std::optional<std::uint32_t> StateVersionMap::get(OperatorId op,
                                                  const std::string& state_type) const {
    auto it = entries_.find(Key{op, state_type});
    if (it == entries_.end())
        return std::nullopt;
    return it->second.version;
}

std::vector<StateVersionEntry> StateVersionMap::entries() const {
    std::vector<StateVersionEntry> out;
    out.reserve(entries_.size());
    for (const auto& [k, v] : entries_) {
        out.push_back(StateVersionEntry{
            .op_id = k.op, .state_type = k.state_type, .version = v.version, .slot = v.slot});
    }
    // Stable ordering so pack() output is reproducible.
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) {
        if (a.op_id.value() != b.op_id.value())
            return a.op_id.value() < b.op_id.value();
        return a.state_type < b.state_type;
    });
    return out;
}

std::string StateVersionMap::pack() const {
    std::ostringstream oss;
    bool first = true;
    for (const auto& e : entries()) {
        if (!first)
            oss << '\n';
        oss << e.op_id.value() << '|' << e.state_type << '|' << e.version;
        // Back-compatible: a slot-less entry packs as the historic
        // 3-field "<op>|<type>|<ver>" form, so old readers and existing
        // golden strings are unaffected. Only entries carrying a slot
        // emit the 4th field.
        if (!e.slot.empty()) {
            oss << '|' << e.slot;
        }
        first = false;
    }
    return oss.str();
}

StateVersionMap StateVersionMap::unpack(std::string_view packed) {
    StateVersionMap map;
    if (packed.empty())
        return map;

    std::size_t start = 0;
    while (start < packed.size()) {
        auto nl = packed.find('\n', start);
        std::string_view line =
            packed.substr(start, nl == std::string_view::npos ? packed.size() - start : nl - start);
        if (line.empty()) {
            // Skip blank lines defensively (e.g., trailing newline).
            start = nl == std::string_view::npos ? packed.size() : nl + 1;
            continue;
        }
        auto p1 = line.find('|');
        if (p1 == std::string_view::npos) {
            throw std::runtime_error("StateVersionMap::unpack: missing '|' separator in line");
        }
        auto p2 = line.find('|', p1 + 1);
        if (p2 == std::string_view::npos) {
            throw std::runtime_error("StateVersionMap::unpack: missing second '|' separator");
        }
        std::string_view op_str = line.substr(0, p1);
        std::string_view type_str = line.substr(p1 + 1, p2 - p1 - 1);
        // Optional 4th field (slot) appended by newer writers. A legacy
        // 3-field line has no third '|', so ver_str is the rest and slot
        // stays empty - back-compatible with snapshots from before slots.
        auto p3 = line.find('|', p2 + 1);
        std::string_view ver_str =
            p3 == std::string_view::npos ? line.substr(p2 + 1) : line.substr(p2 + 1, p3 - p2 - 1);
        std::string_view slot_str =
            p3 == std::string_view::npos ? std::string_view{} : line.substr(p3 + 1);
        // A well-formed line has at most 4 fields (slot forbids '|'). A
        // 5th '|' means a corrupt savepoint - surface it as the
        // documented runtime_error rather than letting set() reject the
        // pipe-bearing slot as a logic_error (invalid_argument), which
        // would escape a handler catching runtime_error.
        if (slot_str.find('|') != std::string_view::npos) {
            throw std::runtime_error("StateVersionMap::unpack: too many '|' separators in line");
        }

        std::uint64_t op_val = 0;
        auto [op_end, op_ec] =
            std::from_chars(op_str.data(), op_str.data() + op_str.size(), op_val);
        if (op_ec != std::errc{} || op_end != op_str.data() + op_str.size()) {
            throw std::runtime_error("StateVersionMap::unpack: invalid op_id");
        }
        std::uint32_t ver_val = 0;
        auto [ver_end, ver_ec] =
            std::from_chars(ver_str.data(), ver_str.data() + ver_str.size(), ver_val);
        if (ver_ec != std::errc{} || ver_end != ver_str.data() + ver_str.size()) {
            throw std::runtime_error("StateVersionMap::unpack: invalid version");
        }
        map.set(OperatorId{op_val}, std::string{type_str}, ver_val, std::string{slot_str});

        if (nl == std::string_view::npos)
            break;
        start = nl + 1;
    }
    return map;
}

void StateMigrationRegistry::register_migration(std::string state_type,
                                                std::uint32_t from_version,
                                                std::uint32_t to_version,
                                                MigrationFn fn) {
    if (from_version == to_version) {
        throw std::invalid_argument(
            "StateMigrationRegistry: from_version == to_version is not a migration");
    }
    if (!fn) {
        throw std::invalid_argument("StateMigrationRegistry: null migration function");
    }
    std::lock_guard lock(mu_);
    auto& bucket = edges_[Key{std::move(state_type), from_version}];
    auto it = std::find_if(
        bucket.begin(), bucket.end(), [&](const Edge_& e) { return e.to_version == to_version; });
    if (it != bucket.end()) {
        it->fn = std::move(fn);
    } else {
        bucket.push_back(Edge_{.to_version = to_version, .fn = std::move(fn)});
    }
}

std::vector<std::uint32_t> StateMigrationRegistry::plan_path_unlocked(const std::string& state_type,
                                                                      std::uint32_t from,
                                                                      std::uint32_t to) const {
    if (from == to) {
        return {};  // empty chain is the no-op
    }
    // BFS over versions. predecessor[v] = the version we arrived from.
    std::unordered_map<std::uint32_t, std::uint32_t> predecessor;
    std::unordered_set<std::uint32_t> visited{from};
    std::queue<std::uint32_t> q;
    q.push(from);
    while (!q.empty()) {
        const auto v = q.front();
        q.pop();
        auto it = edges_.find(Key{state_type, v});
        if (it == edges_.end())
            continue;
        for (const auto& e : it->second) {
            if (visited.count(e.to_version))
                continue;
            visited.insert(e.to_version);
            predecessor[e.to_version] = v;
            if (e.to_version == to) {
                std::vector<std::uint32_t> rev;
                std::uint32_t cur = to;
                while (cur != from) {
                    rev.push_back(cur);
                    cur = predecessor[cur];
                }
                std::reverse(rev.begin(), rev.end());
                return rev;
            }
            q.push(e.to_version);
        }
    }
    return {};
}

std::vector<std::byte> StateMigrationRegistry::migrate(const std::string& state_type,
                                                       std::uint32_t from_version,
                                                       std::uint32_t to_version,
                                                       std::span<const std::byte> input) const {
    std::lock_guard lock(mu_);

    if (from_version == to_version) {
        return std::vector<std::byte>(input.begin(), input.end());
    }

    auto path = plan_path_unlocked(state_type, from_version, to_version);
    if (path.empty()) {
        // Fall back to Arrow auto-migration when both
        // versions have registered schemas and the change is
        // additively compatible. This handles "added nullable column"
        // and "widened integer" cases without an explicit user fn.
        auto from_schema = arrow_schema_unlocked(state_type, from_version);
        auto to_schema = arrow_schema_unlocked(state_type, to_version);
        if (from_schema && to_schema && schemas_additively_compatible(*from_schema, *to_schema)) {
            return arrow_migrate_bytes(input, *from_schema, *to_schema);
        }
        std::ostringstream oss;
        oss << "StateMigrationRegistry: no migration path for state_type=\"" << state_type
            << "\" from v" << from_version << " to v" << to_version;
        throw std::runtime_error(oss.str());
    }

    // Walk the chain. At step k we have bytes valid for version prev,
    // and we invoke the registered (prev -> path[k]) migration.
    std::vector<std::byte> current(input.begin(), input.end());
    std::uint32_t prev = from_version;
    for (std::uint32_t next : path) {
        auto it = edges_.find(Key{state_type, prev});
        // Path planning guarantees the edge exists; defensive check.
        const Edge_* edge = nullptr;
        if (it != edges_.end()) {
            for (const auto& e : it->second) {
                if (e.to_version == next) {
                    edge = &e;
                    break;
                }
            }
        }
        if (!edge) {
            std::ostringstream oss;
            oss << "StateMigrationRegistry: planned edge v" << prev << " -> v" << next
                << " disappeared for state_type=\"" << state_type << "\"";
            throw std::runtime_error(oss.str());
        }
        current = edge->fn(std::span<const std::byte>(current.data(), current.size()));
        prev = next;
    }
    return current;
}

bool StateMigrationRegistry::has_path(const std::string& state_type,
                                      std::uint32_t from_version,
                                      std::uint32_t to_version) const {
    std::lock_guard lock(mu_);
    if (from_version == to_version) {
        return true;
    }
    if (!plan_path_unlocked(state_type, from_version, to_version).empty()) {
        return true;
    }
    // Auto-migration is also a valid path when both
    // versions have registered schemas and the change is additive.
    auto from_schema = arrow_schema_unlocked(state_type, from_version);
    auto to_schema = arrow_schema_unlocked(state_type, to_version);
    return from_schema && to_schema && schemas_additively_compatible(*from_schema, *to_schema);
}

std::vector<StateMigrationRegistry::Edge> StateMigrationRegistry::edges_for(
    const std::string& state_type) const {
    std::lock_guard lock(mu_);
    std::vector<Edge> out;
    for (const auto& [key, bucket] : edges_) {
        if (key.state_type != state_type)
            continue;
        for (const auto& e : bucket) {
            out.push_back(Edge{.from_version = key.from_version, .to_version = e.to_version});
        }
    }
    return out;
}

StateMigrationRegistry& StateMigrationRegistry::global() {
    static StateMigrationRegistry instance;
    return instance;
}

// --- Arrow-aware auto-migration ----------------------------------------

void StateMigrationRegistry::register_arrow_schema(std::string state_type,
                                                   std::uint32_t version,
                                                   std::shared_ptr<arrow::Schema> schema) {
    if (!schema) {
        throw std::invalid_argument("StateMigrationRegistry::register_arrow_schema: null schema");
    }
    std::lock_guard lock(mu_);
    arrow_schemas_[ArrowKey{std::move(state_type), version}] = std::move(schema);
}

std::shared_ptr<arrow::Schema> StateMigrationRegistry::arrow_schema_unlocked(
    const std::string& state_type, std::uint32_t version) const {
    auto it = arrow_schemas_.find(ArrowKey{state_type, version});
    if (it == arrow_schemas_.end())
        return nullptr;
    return it->second;
}

std::shared_ptr<arrow::Schema> StateMigrationRegistry::arrow_schema_for(
    const std::string& state_type, std::uint32_t version) const {
    std::lock_guard lock(mu_);
    return arrow_schema_unlocked(state_type, version);
}

bool StateMigrationRegistry::can_auto_arrow_migrate(const std::string& state_type,
                                                    std::uint32_t from_version,
                                                    std::uint32_t to_version) const {
    if (from_version == to_version)
        return true;
    std::lock_guard lock(mu_);
    auto from = arrow_schema_unlocked(state_type, from_version);
    auto to = arrow_schema_unlocked(state_type, to_version);
    if (!from || !to)
        return false;
    return schemas_additively_compatible(*from, *to);
}

}  // namespace clink
