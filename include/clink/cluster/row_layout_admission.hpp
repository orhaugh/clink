#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/protocol.hpp"

namespace clink::cluster {

// Which Row sidecar layout a deployment may carry between its workers.
//
// The Row channel's Arrow sidecar is described by a row-schema string whose
// type codes say what each column holds. Layout 1 is every code a worker has
// always read. Layout 2 adds codes for typed timestamp columns (`ts_ms`,
// `tstz_ms`), which a worker from before protocol v3 reads as utf8: a typed
// column sent to such a worker fails the channel. So a deployment uses layout
// 2 only when it has an operator whose factory parses a layout-bearing schema
// and every worker hosting it registered at kRowLayoutV2ProtocolVersion or
// later, and the coordinator says so by adding `row_layout=2` to the params of
// those operators. A factory without the stamp resolves layout 2 codes to
// their layout 1 types. A deployment with no such operator can never send a
// typed column, so it stays on layout 1 and none of the layout 2 placement
// rules apply to it.
//
// The stamp lives only in the Deploy frames. The submitted spec, the job's
// retained graph and the HA manifest never carry it, so the job's resume
// fingerprint and everything persisted are unchanged by admission.

inline constexpr std::string_view kRowLayoutParam = "row_layout";
inline constexpr std::uint32_t kRowLayoutV1 = 1;
inline constexpr std::uint32_t kRowLayoutV2 = 2;

// The protocol version a worker declared, with a peer from before versioning
// read as the version it must have spoken.
[[nodiscard]] constexpr std::uint32_t effective_protocol_version(std::uint32_t declared) noexcept {
    return declared == 0 ? kUnversionedPeerProtocolVersion : declared;
}

struct PlannedTask;  // coordinator.hpp

struct HostingWorker {
    std::string worker_id;
    std::uint32_t protocol_version{};  // as registered; 0 = unversioned
};

struct RowLayoutAdmission {
    std::uint32_t layout{kRowLayoutV1};
    // When layout is 1 because of a worker: the first such worker by id, and
    // the version it registered at. Empty when no worker hosts the deployment.
    std::string held_back_by;
    std::uint32_t held_back_version{0};
    // When layout is 1 because the deployment has no layout-bearing
    // operator, whatever its workers registered at.
    bool no_layout_bearing_op{false};

    // "row_layout=2", "row_layout=1 (no layout-bearing operator)" or
    // "row_layout=1 (worker 'w' registered at protocol v2; layout 2 needs
    // v3)", for the coordinator's log.
    [[nodiscard]] std::string describe() const;
};

// The layout a deployment hosted on `hosts` may use, judged on the workers
// alone: 2 when there is at least one host and every host registered at
// kRowLayoutV2ProtocolVersion or later, otherwise 1. Pure; the same hosts
// always give the same answer, whatever their order.
[[nodiscard]] RowLayoutAdmission admit_row_layout(const std::vector<HostingWorker>& hosts);

// The layout a deployment of `layout_bearing_ops` layout-bearing operators
// (count_layout_bearing_ops over its tasks) hosted on `hosts` may use: 1 with
// no_layout_bearing_op set when there are none, otherwise as above. This is
// the coordinator's decision for a whole-job deploy.
[[nodiscard]] RowLayoutAdmission admit_row_layout(const std::vector<HostingWorker>& hosts,
                                                  std::size_t layout_bearing_ops);

// The operator types whose factories parse a schema that can carry layout 2
// codes, and so read the stamp: the columnar Kafka JSON decode and the born-
// columnar window and join outputs. Every other factory keeps the layout 1
// codes it has always been given, and is never stamped: some refuse params
// they do not know.
[[nodiscard]] const std::vector<std::string_view>& layout_bearing_op_types();
[[nodiscard]] bool parses_layout_bearing_schema(std::string_view op_type);

// Stamp `layout` onto the layout-bearing operators of every task in `tasks`
// (each task's OperatorChainSpec in extra_config: its ops and fused
// endpoints). Layout 1 stamps nothing, and a task whose extra_config is not a
// chain spec (a custom role's) or names no layout-bearing operator is left
// byte-for-byte as it was. Returns the number of operators stamped.
std::size_t stamp_row_layout(std::vector<DeploymentTask>& tasks, std::uint32_t layout);

// The number of operators stamp_row_layout would stamp in `tasks` for layout
// 2, without changing them.
[[nodiscard]] std::size_t count_layout_bearing_ops(const std::vector<DeploymentTask>& tasks);

// A worker a hot cutover could place a new subtask on: its registered
// protocol version and its free slots, counting those the operator's old
// subtasks give back at the teardown.
struct CutoverCandidate {
    std::string worker_id;
    std::uint32_t protocol_version{};  // as registered; 0 = unversioned
    std::uint32_t free_slots{};
};

// For a job on layout 2, whether a hot cutover's new subtasks `tasks` need a
// worker that cannot read it. They are placed (assign_task_placement) on the
// candidates that registered at kRowLayoutV2ProtocolVersion or later, as the
// cutover's deploy places them. When those seat every task, nothing is in the
// way. When they do not, the answer is the first candidate by id that
// registered earlier and has a free slot, the worker the cutover would need;
// when there is none, the shortfall is plain lack of room, the deploy's to
// report, and the answer is empty as well.
[[nodiscard]] std::optional<HostingWorker> layout_two_cutover_blocker(
    const std::vector<PlannedTask>& tasks, const std::vector<CutoverCandidate>& candidates);

}  // namespace clink::cluster
