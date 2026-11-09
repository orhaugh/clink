// SQL-off stub for the replay half of `clink incident-verify` and `clink
// incident-regress`. Replay rebuilds SQL Row operators, so it needs the SQL
// frontend; without it the replay checks report themselves unsupported and
// incident-regress refuses with a rebuild hint.

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "clink/incident/bundle.hpp"

namespace {

constexpr const char* kNoSql = "this clink build has no SQL support (CLINK_BUILD_SQL=OFF)";

}  // namespace

std::vector<clink::incident::Check> clink_incident_replay_checks(
    const clink::incident::Bundle& /*bundle*/,
    std::optional<std::uint64_t> /*op*/,
    std::optional<std::uint64_t> /*subtask*/,
    std::optional<std::uint64_t> /*epoch*/,
    const std::string& expected_path,
    std::size_t /*max_replays*/) {
    std::vector<clink::incident::Check> out;
    std::vector<std::string> ids = {"replay.determinism", "replay.recorded_output"};
    if (!expected_path.empty()) {
        ids.emplace_back("replay.expected_output");
    }
    for (const auto& id : ids) {
        out.push_back(clink::incident::Check{
            .id = id,
            .verdict = clink::incident::Verdict::Unsupported,
            .evidence = {},
            .reason = kNoSql,
            .next_action = "verify with a clink built with -DCLINK_BUILD_SQL=ON",
            .details = {},
            .counts = {}});
    }
    return out;
}

std::size_t clink_incident_regress(const clink::incident::Bundle& /*bundle*/,
                                   std::uint64_t /*op*/,
                                   std::uint64_t /*epoch*/,
                                   std::optional<std::uint64_t> /*subtask*/,
                                   const std::string& /*out_dir*/) {
    throw std::runtime_error(std::string{kNoSql} + "; rebuild with -DCLINK_BUILD_SQL=ON");
}
