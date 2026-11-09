// The replay half of `clink incident-verify` and `clink incident-regress`, on
// the SQL frontend's replay engine (include/clink/sql/incident_replay.hpp). A
// SQL-off build links clink_incident_replay_stub.cpp instead.

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "clink/sql/incident_replay.hpp"

std::vector<clink::incident::Check> clink_incident_replay_checks(
    const clink::incident::Bundle& bundle,
    std::optional<std::uint64_t> op,
    std::optional<std::uint64_t> subtask,
    std::optional<std::uint64_t> epoch,
    const std::string& expected_path,
    std::size_t max_replays) {
    clink::sql::IncidentReplayOptions o;
    o.op = op;
    o.subtask = subtask;
    o.epoch = epoch;
    o.expected_path = expected_path;
    o.max_replays = max_replays;
    return clink::sql::incident_replay_checks(bundle, o);
}

std::size_t clink_incident_regress(const clink::incident::Bundle& bundle,
                                   std::uint64_t op,
                                   std::uint64_t epoch,
                                   std::optional<std::uint64_t> subtask,
                                   const std::string& out_dir) {
    return clink::sql::incident_regress(bundle, op, epoch, subtask, out_dir);
}
