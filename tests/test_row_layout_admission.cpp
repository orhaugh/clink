// Admission of the Row sidecar's second layout (protocol v3).
//
// A deployment carries typed timestamp columns between workers only when
// every worker hosting it reads them: a worker from before protocol v3 reads
// the new codes as utf8, and a typed column sent to it fails the channel. The
// coordinator decides per deployment and says so by stamping `row_layout=2`
// onto the layout-bearing operators in the Deploy frames it sends, never into
// the submitted spec.
//
// Three layers, each pinned here: the pure admission function (with the
// deployment's operators, and where a second-layout job's cutover may place),
// the stamp on a task's chain spec, and the coordinator choosing and stamping
// over real frames for a submit, a restart, a replan rescale and a hot
// cutover. The coordinator cases use frame-level fake workers so a worker can
// declare an older protocol version and the Deploy frames can be read as
// sent.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/row_layout_admission.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

// ----- The pure function -----

TEST(RowLayoutAdmission, TheProtocolVersionThatReadsTheSecondLayoutIsThisBuilds) {
    EXPECT_EQ(kClusterProtocolVersion, 3U);
    EXPECT_EQ(kRowLayoutV2ProtocolVersion, 3U);
    EXPECT_LE(kRowLayoutV2ProtocolVersion, kClusterProtocolVersion)
        << "a build that cannot read the second layout must never admit it";
}

TEST(RowLayoutAdmission, EveryHostAtVersionThreeOrLaterAdmitsTheSecondLayout) {
    EXPECT_EQ(admit_row_layout({{"a", 3}}).layout, kRowLayoutV2);
    EXPECT_EQ(admit_row_layout({{"a", 3}, {"b", 3}}).layout, kRowLayoutV2);
    EXPECT_EQ(admit_row_layout({{"a", 3}, {"b", 7}}).layout, kRowLayoutV2)
        << "a newer worker reads everything a v3 worker does";
    const auto admitted = admit_row_layout({{"a", 3}, {"b", 4}});
    EXPECT_TRUE(admitted.held_back_by.empty());
    EXPECT_EQ(admitted.describe(), "row_layout=2");
}

TEST(RowLayoutAdmission, OneOlderHostKeepsTheFirstLayoutAndIsNamed) {
    const auto mixed = admit_row_layout({{"w-new", 3}, {"w-old", 2}});
    EXPECT_EQ(mixed.layout, kRowLayoutV1);
    EXPECT_EQ(mixed.held_back_by, "w-old");
    EXPECT_EQ(mixed.held_back_version, 2U);
    EXPECT_EQ(mixed.describe(),
              "row_layout=1 (worker 'w-old' registered at protocol v2; layout 2 needs v3)");

    // A peer from before versioning declares 0, which reads as v1.
    const auto unversioned = admit_row_layout({{"a", 3}, {"legacy", 0}});
    EXPECT_EQ(unversioned.layout, kRowLayoutV1);
    EXPECT_EQ(unversioned.held_back_by, "legacy");
    EXPECT_EQ(unversioned.held_back_version, 1U);
}

TEST(RowLayoutAdmission, TheAnswerDoesNotDependOnTheOrderOfTheHosts) {
    const std::vector<HostingWorker> forward{{"w1", 2}, {"w2", 3}, {"w0", 1}};
    const std::vector<HostingWorker> reverse{{"w0", 1}, {"w2", 3}, {"w1", 2}};
    const auto a = admit_row_layout(forward);
    const auto b = admit_row_layout(reverse);
    EXPECT_EQ(a.layout, kRowLayoutV1);
    EXPECT_EQ(a.held_back_by, "w0") << "the first older worker by id is named";
    EXPECT_EQ(a.held_back_by, b.held_back_by);
    EXPECT_EQ(a.held_back_version, b.held_back_version);
}

TEST(RowLayoutAdmission, NoHostAdmitsNothing) {
    const auto none = admit_row_layout({});
    EXPECT_EQ(none.layout, kRowLayoutV1);
    EXPECT_TRUE(none.held_back_by.empty());
    EXPECT_EQ(none.describe(), "row_layout=1");
}

// ----- The stamp on a task's chain spec -----

namespace {

ChainOp rla_chain_op(std::string id, std::string type) {
    ChainOp op;
    op.id = std::move(id);
    op.type = std::move(type);
    op.params["k"] = "v";
    return op;
}

DeploymentTask rla_task(const OperatorChainSpec& chain) {
    DeploymentTask t;
    t.role = kGenericSubtaskRole;
    t.extra_config = chain.to_json();
    return t;
}

}  // namespace

TEST(RowLayoutStamp, OnlyTheLayoutBearingOperatorsAreStamped) {
    OperatorChainSpec chain;
    chain.ops.push_back(rla_chain_op("dec", "json_string_to_row_columnar"));
    chain.ops.push_back(rla_chain_op("flt", "filter_row_predicate"));
    chain.ops.push_back(rla_chain_op("win", "tumbling_window_row"));
    chain.fused_source = rla_chain_op("src", "kafka_source_string");
    chain.fused_sink = rla_chain_op("snk", "clickhouse_native_sink_row");
    std::vector<DeploymentTask> tasks{rla_task(chain)};

    EXPECT_EQ(stamp_row_layout(tasks, kRowLayoutV2), 2U);
    const auto out = OperatorChainSpec::from_json(tasks[0].extra_config);
    ASSERT_EQ(out.ops.size(), 3U);
    EXPECT_EQ(out.ops[0].params.at("row_layout"), "2");
    EXPECT_EQ(out.ops[1].params.count("row_layout"), 0U)
        << "a factory that does not parse a layout-bearing schema is never stamped";
    EXPECT_EQ(out.ops[2].params.at("row_layout"), "2");
    ASSERT_TRUE(out.fused_source.has_value());
    ASSERT_TRUE(out.fused_sink.has_value());
    EXPECT_EQ(out.fused_source->params.count("row_layout"), 0U);
    EXPECT_EQ(out.fused_sink->params.count("row_layout"), 0U)
        << "the native sink's strict option check refuses unknown keys";
    // Everything else in the op survives the stamp.
    EXPECT_EQ(out.ops[0].params.at("k"), "v");
    EXPECT_EQ(out.ops[2].id, "win");
}

TEST(RowLayoutStamp, EveryLayoutBearingTypeIsStampedOnAJoinOrAFusedEndpoint) {
    for (const auto type : layout_bearing_op_types()) {
        SCOPED_TRACE(std::string{type});
        EXPECT_TRUE(parses_layout_bearing_schema(type));
        OperatorChainSpec chain;
        chain.ops.push_back(rla_chain_op("op", std::string{type}));
        std::vector<DeploymentTask> tasks{rla_task(chain)};
        EXPECT_EQ(stamp_row_layout(tasks, kRowLayoutV2), 1U);
    }
    EXPECT_FALSE(parses_layout_bearing_schema("json_string_to_row"))
        << "the row bridge keeps the first layout's codes";
    EXPECT_FALSE(parses_layout_bearing_schema("parquet_row_sink"));
}

TEST(RowLayoutStamp, TheFirstLayoutAndUnrelatedTasksAreLeftByteForByte) {
    OperatorChainSpec chain;
    chain.ops.push_back(rla_chain_op("win", "tumbling_window_row"));
    OperatorChainSpec plain;
    plain.ops.push_back(rla_chain_op("flt", "filter_row_predicate"));
    DeploymentTask custom;
    custom.role = "my_custom_role";
    custom.extra_config = "tumbling_window_row=yes\nclink_attempt=2";

    std::vector<DeploymentTask> tasks{rla_task(chain), rla_task(plain), custom};
    const auto before = tasks;
    EXPECT_EQ(stamp_row_layout(tasks, kRowLayoutV1), 0U) << "the first layout is never stamped";
    for (std::size_t i = 0; i < tasks.size(); ++i) {
        EXPECT_EQ(tasks[i].extra_config, before[i].extra_config);
    }

    EXPECT_EQ(stamp_row_layout(tasks, kRowLayoutV2), 1U);
    EXPECT_NE(tasks[0].extra_config, before[0].extra_config);
    EXPECT_EQ(tasks[1].extra_config, before[1].extra_config)
        << "a chain naming no layout-bearing operator is not re-serialised";
    EXPECT_EQ(tasks[2].extra_config, before[2].extra_config)
        << "a custom role's extra_config is not a chain spec and is not touched";
}

TEST(RowLayoutStamp, CountingFindsWhatTheStampWouldStampAndChangesNothing) {
    OperatorChainSpec chain;
    chain.ops.push_back(rla_chain_op("dec", "json_string_to_row_columnar"));
    chain.ops.push_back(rla_chain_op("flt", "filter_row_predicate"));
    chain.ops.push_back(rla_chain_op("win", "tumbling_window_row"));
    OperatorChainSpec plain;
    plain.ops.push_back(rla_chain_op("flt", "filter_row_predicate"));
    DeploymentTask custom;
    custom.role = "my_custom_role";
    custom.extra_config = "tumbling_window_row=yes";

    std::vector<DeploymentTask> tasks{rla_task(chain), rla_task(plain), custom};
    const auto before = tasks;
    EXPECT_EQ(count_layout_bearing_ops(tasks), 2U);
    for (std::size_t i = 0; i < tasks.size(); ++i) {
        EXPECT_EQ(tasks[i].extra_config, before[i].extra_config) << "counting changed task " << i;
    }
    EXPECT_EQ(stamp_row_layout(tasks, kRowLayoutV2), 2U) << "the count and the stamp disagree";

    const std::vector<DeploymentTask> none{rla_task(plain), custom};
    EXPECT_EQ(count_layout_bearing_ops(none), 0U)
        << "a custom role's extra_config naming a type is not a chain spec";
}

// ----- Admission with the deployment's operators -----

TEST(RowLayoutAdmission, ADeploymentWithNoLayoutBearingOperatorStaysOnTheFirstLayout) {
    // Nothing such a deployment sends can carry a typed column, so its
    // workers do not decide anything, and none of the second layout's
    // placement rules may apply to it.
    const auto all_v3 = admit_row_layout({{"a", 3}, {"b", 3}}, 0);
    EXPECT_EQ(all_v3.layout, kRowLayoutV1);
    EXPECT_TRUE(all_v3.no_layout_bearing_op);
    EXPECT_EQ(all_v3.describe(), "row_layout=1 (no layout-bearing operator)");
    EXPECT_EQ(admit_row_layout({{"a", 3}, {"old", 2}}, 0).describe(),
              "row_layout=1 (no layout-bearing operator)");

    // With one, the workers decide, exactly as before.
    const auto admitted = admit_row_layout({{"a", 3}, {"b", 3}}, 1);
    EXPECT_EQ(admitted.layout, kRowLayoutV2);
    EXPECT_FALSE(admitted.no_layout_bearing_op);
    EXPECT_EQ(admitted.describe(), "row_layout=2");
    const auto held = admit_row_layout({{"a", 3}, {"old", 2}}, 4);
    EXPECT_EQ(held.layout, kRowLayoutV1);
    EXPECT_FALSE(held.no_layout_bearing_op);
    EXPECT_EQ(held.held_back_by, "old");
}

// ----- Where a second-layout job's cutover may place -----

namespace {

std::vector<PlannedTask> rla_new_subtasks(std::uint32_t n, std::uint32_t base) {
    std::vector<PlannedTask> tasks;
    for (std::uint32_t i = 0; i < n; ++i) {
        PlannedTask t;
        t.role = kGenericSubtaskRole;
        t.subtask_idx = base + i;
        tasks.push_back(t);
    }
    return tasks;
}

}  // namespace

TEST(RowLayoutCutover, WorkersThatReadTheSecondLayoutSeatingThePlanLeaveNothingInTheWay) {
    // The older worker sorts first and has room, so a placement over every
    // worker puts a new subtask on it. The cutover's deploy places on the
    // workers that read the layout alone, and they seat all four.
    const std::vector<CutoverCandidate> pool{{"w-a", 2, 4}, {"w-b", 3, 2}, {"w-c", 3, 2}};
    EXPECT_FALSE(layout_two_cutover_blocker(rla_new_subtasks(4, 4), pool).has_value());
}

TEST(RowLayoutCutover, AnOlderWorkerWithTheRoomThePlanNeedsIsNamed) {
    const std::vector<CutoverCandidate> pool{
        {"w-c", 3, 1}, {"w-z", 2, 4}, {"w-y", 0, 4}, {"w-x", 2, 0}};
    const auto blocker = layout_two_cutover_blocker(rla_new_subtasks(4, 4), pool);
    ASSERT_TRUE(blocker.has_value());
    EXPECT_EQ(blocker->worker_id, "w-y")
        << "the first older worker by id with a free slot; a full one has no room to offer";
    EXPECT_EQ(blocker->protocol_version, 1U) << "an unversioned worker reads as v1";
}

TEST(RowLayoutCutover, APlainLackOfRoomIsTheDeploysToReport) {
    const std::vector<CutoverCandidate> pool{{"w-a", 3, 1}, {"w-b", 2, 0}};
    EXPECT_FALSE(layout_two_cutover_blocker(rla_new_subtasks(4, 4), pool).has_value());
}

// ----- The coordinator, over real frames -----

namespace {

std::optional<std::vector<std::byte>> rla_recv_frame(network::Connection& c) {
    std::array<std::byte, 4> hdr{};
    if (!c.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (const auto b : hdr) {
        len = (len << 8) | static_cast<unsigned char>(b);
    }
    std::vector<std::byte> body(len);
    if (len > 0 && !c.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

template <typename Pred>
bool rla_await(Pred pred, std::chrono::milliseconds bound = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

// A worker that registers at a chosen protocol version, heartbeats, keeps
// every frame the coordinator sends it, and sends what the test tells it to.
class RlaFakeWorker {
public:
    RlaFakeWorker(std::uint16_t port, std::string id, std::uint32_t slots = 4)
        : id_(std::move(id)), slots_(slots) {
        conn_ = network::connect_plain("127.0.0.1", port);
    }

    [[nodiscard]] bool valid() const { return conn_ != nullptr; }
    [[nodiscard]] const std::string& id() const { return id_; }

    [[nodiscard]] bool register_at(std::uint32_t protocol_version) {
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = slots_};
        reg.protocol_version = protocol_version;
        if (!send_frame(*conn_, encode_frame(MessageKind::Register, reg))) {
            return false;
        }
        auto reply = rla_recv_frame(*conn_);
        if (!reply.has_value()) {
            return false;
        }
        MessageReader r(std::move(*reply));
        if (static_cast<MessageKind>(r.read_u8()) != MessageKind::RegisterAck) {
            return false;
        }
        if (!decode_register_ack(r).ok) {
            return false;
        }
        start_reader_();
        start_heartbeat_();
        return true;
    }

    // Take the first queued frame of `kind`, waiting up to `bound`; frames
    // of other kinds stay queued.
    [[nodiscard]] std::optional<MessageReader> await_frame(MessageKind kind,
                                                           std::chrono::milliseconds bound = 5s) {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (true) {
            {
                std::lock_guard lock(mu_);
                for (auto it = inbox_.begin(); it != inbox_.end(); ++it) {
                    if (!it->empty() && static_cast<MessageKind>((*it)[0]) == kind) {
                        MessageReader r(std::move(*it));
                        inbox_.erase(it);
                        (void)r.read_u8();
                        return r;
                    }
                }
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return std::nullopt;
            }
            std::this_thread::sleep_for(1ms);
        }
    }

    [[nodiscard]] std::optional<DeployMsg> await_deploy(std::chrono::milliseconds bound = 5s) {
        auto r = await_frame(MessageKind::Deploy, bound);
        if (!r.has_value()) {
            return std::nullopt;
        }
        return decode_deploy(*r);
    }

    // A subtask's listeners: one per upstream index in `upstream`, on ports
    // counting up from `port`. Every subtask reporting starts the job's
    // checkpoint clock; a fed task reports the listeners a cutover's rebind
    // asked for the same way.
    [[nodiscard]] bool report_listening(JobId job_id,
                                        const std::string& role,
                                        std::uint32_t subtask,
                                        const std::vector<std::uint32_t>& upstream,
                                        std::uint16_t port) {
        SubtaskListeningMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.host = "127.0.0.1";
        for (const auto idx : upstream) {
            m.edge_ports.push_back(SubtaskListeningMsg::EdgePort{
                .upstream_role = role, .upstream_subtask_idx = idx, .port = port++});
        }
        return send(encode_frame(MessageKind::SubtaskListening, m));
    }

    [[nodiscard]] bool ack_checkpoint(JobId job_id,
                                      std::uint64_t ckpt_id,
                                      const std::string& role,
                                      std::uint32_t subtask) {
        SubtaskCheckpointedMsg m;
        m.job_id = job_id;
        m.checkpoint_id = ckpt_id;
        m.role = role;
        m.subtask_idx = subtask;
        m.ok = true;
        return send(encode_frame(MessageKind::SubtaskCheckpointed, m));
    }

    [[nodiscard]] bool send_finished(JobId job_id, const std::string& role, std::uint32_t subtask) {
        SubtaskFinishedMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.had_error = false;
        return send(encode_frame(MessageKind::SubtaskFinished, m));
    }

    [[nodiscard]] bool send(const std::vector<std::byte>& frame) {
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, frame);
    }

    void close() {
        stop_.store(true, std::memory_order_release);
        if (conn_) {
            conn_->shutdown_read();
            conn_->close();
        }
        if (reader_.joinable()) {
            reader_.join();
        }
        if (heartbeat_.joinable()) {
            heartbeat_.join();
        }
    }

    ~RlaFakeWorker() { close(); }
    RlaFakeWorker(const RlaFakeWorker&) = delete;
    RlaFakeWorker& operator=(const RlaFakeWorker&) = delete;
    RlaFakeWorker(RlaFakeWorker&&) = delete;
    RlaFakeWorker& operator=(RlaFakeWorker&&) = delete;

private:
    void start_reader_() {
        reader_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                auto frame = rla_recv_frame(*conn_);
                if (!frame.has_value()) {
                    return;
                }
                std::lock_guard lock(mu_);
                inbox_.push_back(std::move(*frame));
            }
        });
    }

    void start_heartbeat_() {
        heartbeat_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                if (!send(encode_frame(MessageKind::Heartbeat, HeartbeatMsg{id_}))) {
                    return;
                }
                for (int i = 0; i < 20 && !stop_.load(std::memory_order_acquire); ++i) {
                    std::this_thread::sleep_for(10ms);
                }
            }
        });
    }

    std::string id_;
    std::uint32_t slots_;
    std::unique_ptr<network::Connection> conn_;
    std::thread reader_;
    std::thread heartbeat_;
    std::mutex send_mu_;
    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::vector<std::vector<std::byte>> inbox_;
};

// The layout-bearing type these cases deploy, as a factory that is never
// built: the workers are fakes and only read the Deploy frames. This suite
// does not link the SQL factories, so the name is free, and it is keyed on
// int64 channels, which the SQL factory never uses. It sits in the default
// registry because a hot cutover plans every job without a bundle against
// that registry, as the replan does.
const OperatorRegistry& rla_registry() {
    static const bool registered = [] {
        ensure_built_ins_registered();
        OperatorRegistry::default_instance().register_operator(
            "tumbling_window_row",
            OperatorFactory{.in = ChannelType{std::string{kChannelInt64}},
                            .out = ChannelType{std::string{kChannelInt64}},
                            .build = [](const OperatorBuildContext&) -> std::shared_ptr<void> {
                                return nullptr;  // never built: the workers are fakes
                            }});
        return true;
    }();
    (void)registered;
    return OperatorRegistry::default_instance();
}

// src -> win -> snk at parallelism 1: three tasks over two workers, so both
// host part of the job.
JobGraphSpec rla_graph(const std::filesystem::path& out) {
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "10"}};
    g.ops.push_back(src);
    OperatorSpec win;
    win.type = "tumbling_window_row";
    win.id = "win";
    win.inputs = {"src"};
    win.out_channel = std::string{kChannelInt64};
    win.params = {{"size_ms", "1000"}};
    g.ops.push_back(win);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"win"};
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(snk);
    return g;
}

// A job a hot cutover can take: `win`, of type `win_type`, keyed and with
// rescale bounds, between a source and a sink, the parallelism mismatched on
// both of its edges. Four tasks: src, two of win, snk.
JobGraphSpec rla_hot_graph(const std::string& win_type, const std::filesystem::path& out) {
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1000000"}};
    g.ops.push_back(src);
    OperatorSpec win;
    win.type = win_type;
    win.id = "win";
    win.inputs = {"src"};
    win.parallelism = 2;
    win.min_parallelism = 1;
    win.max_parallelism = 8;
    win.out_channel = std::string{kChannelInt64};
    win.key_by = "identity";
    g.ops.push_back(win);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"win"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(snk);
    return g;
}

// Whether the task's chain runs the operator `op_id`.
bool rla_runs(const DeploymentTask& t, std::string_view op_id) {
    const auto chain = OperatorChainSpec::from_json(t.extra_config);
    for (const auto& op : chain.ops) {
        if (op.id == op_id) {
            return true;
        }
    }
    return (chain.fused_source && chain.fused_source->id == op_id) ||
           (chain.fused_sink && chain.fused_sink->id == op_id);
}

// Every row_layout value stamped in the task's chain.
std::vector<std::string> rla_stamps_of(const DeploymentTask& t) {
    std::vector<std::string> out;
    const auto chain = OperatorChainSpec::from_json(t.extra_config);
    auto look = [&](const ChainOp& op) {
        if (const auto it = op.params.find("row_layout"); it != op.params.end()) {
            out.push_back(op.type + "=" + it->second);
        }
    };
    for (const auto& op : chain.ops) {
        look(op);
    }
    if (chain.fused_source) {
        look(*chain.fused_source);
    }
    if (chain.fused_sink) {
        look(*chain.fused_sink);
    }
    return out;
}

struct RlaPlaced {
    RlaFakeWorker* worker{nullptr};
    DeploymentTask task;
};

std::vector<std::string> rla_stamps(const std::vector<RlaPlaced>& placed) {
    std::vector<std::string> out;
    for (const auto& p : placed) {
        for (auto& s : rla_stamps_of(p.task)) {
            out.push_back(std::move(s));
        }
    }
    return out;
}

std::set<RlaFakeWorker*> rla_hosts(const std::vector<RlaPlaced>& placed) {
    std::set<RlaFakeWorker*> hosts;
    for (const auto& p : placed) {
        hosts.insert(p.worker);
    }
    return hosts;
}

bool rla_hosts_any(const std::vector<RlaPlaced>& placed, const RlaFakeWorker* w) {
    return std::any_of(
        placed.begin(), placed.end(), [w](const RlaPlaced& p) { return p.worker == w; });
}

std::int64_t rla_log_cursor() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() -
           1;
}

// Whether the coordinator logged a line containing `text` since `since_ms`.
bool rla_logged(std::string_view text, std::int64_t since_ms) {
    for (const auto& rec : LogBuffer::global().tail(4000, "info", since_ms, "coordinator")) {
        if (rec.message.find(text) != std::string::npos) {
            return true;
        }
    }
    return false;
}

// A coordinator and the fake workers a case registers with it. The workers
// close first, so none of their frames reaches a stopped coordinator.
class RlaCluster {
public:
    explicit RlaCluster(const std::string& tag)
        : dir_(std::filesystem::temp_directory_path() /
               ("clink_rla_" + tag + "_" + std::to_string(::getpid()))) {
        (void)rla_registry();
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }

    ~RlaCluster() {
        for (auto& w : workers_) {
            w->close();
        }
        coordinator_.stop();
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    RlaCluster(const RlaCluster&) = delete;
    RlaCluster& operator=(const RlaCluster&) = delete;
    RlaCluster(RlaCluster&&) = delete;
    RlaCluster& operator=(RlaCluster&&) = delete;

    // With an HA directory, so a case can read what a takeover would
    // recover from.
    void start(bool with_ha_dir = false) {
        if (with_ha_dir) {
            coordinator_.set_ha_dir((dir_ / "ha").string());
        }
        port_ = coordinator_.start();
    }

    Coordinator& coordinator() { return coordinator_; }
    [[nodiscard]] const std::filesystem::path& dir() const { return dir_; }

    // A worker registered at `version` with `slots` slots; nullptr when it
    // could not register.
    RlaFakeWorker* join(const std::string& id, std::uint32_t version, std::uint32_t slots) {
        auto w = std::make_unique<RlaFakeWorker>(port_, id, slots);
        if (!w->valid() || !w->register_at(version)) {
            return nullptr;
        }
        workers_.push_back(std::move(w));
        return workers_.back().get();
    }

    JobId submit(const JobGraphSpec& graph, std::uint32_t max_restarts) {
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = (dir_ / "ckpt").string();
        // The first checkpoint is triggered once every subtask listens; no
        // periodic one follows inside a case.
        ckpt.interval_ms = 600'000;
        ckpt.max_restarts_on_worker_loss = max_restarts;
        return coordinator_.submit_job(graph, rla_registry(), {}, ckpt);
    }

    // The Deploy frames for `job_id` the workers are sent within `bound`:
    // collected until the first has arrived and the rest have had a quiet
    // moment to follow, since one deploy sends every frame at once.
    std::vector<RlaPlaced> take_deploys(JobId job_id, std::chrono::milliseconds bound = 5s) {
        std::vector<RlaPlaced> out;
        const auto deadline = std::chrono::steady_clock::now() + bound;
        std::optional<std::chrono::steady_clock::time_point> quiet_until;
        while (std::chrono::steady_clock::now() < (quiet_until ? *quiet_until : deadline)) {
            for (auto& w : workers_) {
                if (auto d = w->await_deploy(0ms); d.has_value() && d->job_id == job_id) {
                    for (auto& t : d->tasks) {
                        out.push_back(RlaPlaced{w.get(), t});
                    }
                    quiet_until = std::chrono::steady_clock::now() + 300ms;
                }
            }
            std::this_thread::sleep_for(1ms);
        }
        return out;
    }

private:
    std::filesystem::path dir_;
    Coordinator coordinator_;
    std::uint16_t port_{0};
    std::vector<std::unique_ptr<RlaFakeWorker>> workers_;
};

// Every deployed subtask reports listening, which starts the checkpoint
// clock, and the first checkpoint is acked everywhere and completes: the
// restore point a cutover needs.
bool rla_complete_first_checkpoint(RlaCluster& c,
                                   JobId job_id,
                                   const std::vector<RlaPlaced>& placed) {
    std::uint16_t port = 42100;
    for (const auto& p : placed) {
        if (!p.worker->report_listening(job_id, p.task.role, p.task.subtask_idx, {0}, port++)) {
            return false;
        }
    }
    std::optional<std::uint64_t> id;
    for (auto* w : rla_hosts(placed)) {
        auto trigger = w->await_frame(MessageKind::TriggerCheckpoint);
        if (!trigger.has_value()) {
            return false;
        }
        id = decode_trigger_checkpoint(*trigger).checkpoint_id;
    }
    for (const auto& p : placed) {
        if (!p.worker->ack_checkpoint(job_id, *id, p.task.role, p.task.subtask_idx)) {
            return false;
        }
    }
    return rla_await([&] { return c.coordinator().latest_completed_checkpoint(job_id) == *id; });
}

// The drain of a stop-and-replan or a restart: each of `placed`'s workers in
// `workers` is told to cancel, and reports its subtasks finished.
bool rla_drain(JobId job_id,
               const std::vector<RlaPlaced>& placed,
               const std::vector<RlaFakeWorker*>& workers) {
    for (auto* w : workers) {
        if (!w->await_frame(MessageKind::CancelJob, 10s).has_value()) {
            ADD_FAILURE() << "worker '" << w->id() << "' was never told to cancel";
            return false;
        }
        for (const auto& p : placed) {
            if (p.worker == w && !w->send_finished(job_id, p.task.role, p.task.subtask_idx)) {
                return false;
            }
        }
    }
    return true;
}

struct RlaCutover {
    bool armed{false};                // the request took the hot path
    std::vector<RlaPlaced> deployed;  // the new subtasks, as deployed
};

// Rescale `win` to `target` and, when the request arms a hot cutover, play
// every worker's part to its deploy: each arm acked with what the worker
// hosts, the cutover checkpoint acked by every subtask, the old subtasks of
// `win` ended at it, and the sink's listeners for the new subtasks reported.
RlaCutover rla_drive_cutover(RlaCluster& c,
                             JobId job_id,
                             const std::vector<RlaPlaced>& placed,
                             std::uint32_t target) {
    RlaCutover out;
    const auto result = c.coordinator().request_operator_rescale(job_id, "win", target);
    if (!result.ok) {
        ADD_FAILURE() << "the rescale request was refused: " << result.reason;
        return out;
    }
    std::optional<std::uint64_t> cut;
    for (auto* w : rla_hosts(placed)) {
        auto arm = w->await_frame(MessageKind::BeginRescale, 1500ms);
        if (!arm.has_value()) {
            return out;
        }
        cut = decode_begin_rescale(*arm).cutover_checkpoint;
        BeginRescaleAckMsg ack;
        ack.job_id = job_id;
        ack.op_id = "win";
        ack.worker_id = w->id();
        for (const auto& p : placed) {
            if (p.worker != w) {
                continue;
            }
            if (rla_runs(p.task, "win")) {
                ++ack.armed_callbacks;
            } else if (rla_runs(p.task, "src")) {
                ++ack.armed_groups;
            } else if (rla_runs(p.task, "snk")) {
                ++ack.rebind_tasks;
            }
        }
        if (!w->send(encode_frame(MessageKind::BeginRescaleAck, ack))) {
            ADD_FAILURE() << "could not ack the arm on '" << w->id() << "'";
            return out;
        }
    }
    out.armed = true;

    for (auto* w : rla_hosts(placed)) {
        const bool triggered = rla_await([&] {
            auto frame = w->await_frame(MessageKind::TriggerCheckpoint, 0ms);
            return frame.has_value() && decode_trigger_checkpoint(*frame).checkpoint_id == *cut;
        });
        if (!triggered) {
            ADD_FAILURE() << "the cutover checkpoint never reached '" << w->id() << "'";
            return out;
        }
    }
    for (const auto& p : placed) {
        EXPECT_TRUE(p.worker->ack_checkpoint(job_id, *cut, p.task.role, p.task.subtask_idx));
    }
    for (const auto& p : placed) {
        if (rla_runs(p.task, "win")) {
            EXPECT_TRUE(p.worker->send_finished(job_id, p.task.role, p.task.subtask_idx));
        }
    }
    for (const auto& p : placed) {
        if (!rla_runs(p.task, "snk")) {
            continue;
        }
        auto rebind = p.worker->await_frame(MessageKind::CutoverRebind);
        if (!rebind.has_value()) {
            ADD_FAILURE() << "the sink was never asked to bind listeners for the new subtasks";
            return out;
        }
        const auto rb = decode_cutover_rebind(*rebind);
        EXPECT_TRUE(p.worker->report_listening(
            job_id, p.task.role, p.task.subtask_idx, rb.new_subtask_indices, 42300));
    }
    out.deployed = c.take_deploys(job_id);
    return out;
}

// Submit the graph to a coordinator whose two workers registered at the given
// versions, and read back what each was sent.
std::vector<RlaPlaced> rla_submit(std::uint32_t version_a,
                                  std::uint32_t version_b,
                                  const std::string& tag,
                                  std::string* retained_graph) {
    RlaCluster c(tag);
    c.start(/*with_ha_dir=*/true);
    auto* wa = c.join("rla-a", version_a, 4);
    auto* wb = c.join("rla-b", version_b, 4);
    EXPECT_TRUE(wa != nullptr && wb != nullptr);
    if (wa == nullptr || wb == nullptr) {
        return {};
    }
    const auto job_id = c.submit(rla_graph(c.dir() / "out.txt"), 0);
    EXPECT_GT(job_id, 0U);
    auto placed = c.take_deploys(job_id);
    EXPECT_TRUE(rla_hosts_any(placed, wa) && rla_hosts_any(placed, wb))
        << "the job is not hosted on both workers";
    if (retained_graph != nullptr) {
        std::ifstream in(c.dir() / "ha" / "jobs" / std::to_string(job_id) / "manifest.json");
        std::stringstream body;
        body << in.rdbuf();
        *retained_graph = body.str();
    }
    return placed;
}

}  // namespace

TEST(RowLayoutAdmissionCoordinator, AnAllVersionThreeDeploymentStampsTheLayoutBearingOperator) {
    std::string retained;
    const auto d = rla_submit(3, 3, "allv3", &retained);
    EXPECT_EQ(d.size(), 3U);
    EXPECT_EQ(rla_stamps(d), (std::vector<std::string>{"tumbling_window_row=2"}))
        << "exactly the window is stamped, nothing else";
    // The stamp lives in the Deploy frames only: the graph the coordinator
    // persists for a takeover (and replans from) is the one submitted.
    ASSERT_NE(retained.find("graph_json"), std::string::npos)
        << "no HA manifest was persisted to compare against";
    EXPECT_NE(retained.find("tumbling_window_row"), std::string::npos);
    EXPECT_EQ(retained.find("row_layout"), std::string::npos)
        << "the persisted spec must not change: " << retained;
}

TEST(RowLayoutAdmissionCoordinator, AWorkerAtVersionTwoKeepsTheWholeDeploymentOnTheFirstLayout) {
    const auto d = rla_submit(3, 2, "mixed", nullptr);
    EXPECT_EQ(d.size(), 3U);
    EXPECT_TRUE(rla_stamps(d).empty())
        << "a deployment hosted partly on a v2 worker must not carry the second layout on any "
           "task, the v3 worker's included";
}

TEST(RowLayoutAdmissionCoordinator, AnUnversionedWorkerKeepsTheFirstLayout) {
    const auto d = rla_submit(0, 3, "unversioned", nullptr);
    EXPECT_EQ(d.size(), 3U);
    EXPECT_TRUE(rla_stamps(d).empty());
}

// A restart redeploys the whole job and decides the layout again for the
// workers it places onto, in either direction, and stamps what it decided:
// down to the first layout when a v2 worker takes part of the job, back up to
// the second once every host reads it again.
TEST(RowLayoutAdmissionCoordinator, ARestartDecidesTheLayoutAgainForTheWorkersItRedeploysOnto) {
    RlaCluster c("restart");
    c.start();
    auto* a = c.join("rla-a", 3, 4);
    auto* b = c.join("rla-b", 3, 4);
    ASSERT_TRUE(a != nullptr && b != nullptr);
    const auto job_id = c.submit(rla_graph(c.dir() / "out.txt"), /*max_restarts=*/2);
    ASSERT_GT(job_id, 0U);
    const auto first = c.take_deploys(job_id);
    ASSERT_EQ(first.size(), 3U);
    ASSERT_EQ(rla_stamps(first), (std::vector<std::string>{"tumbling_window_row=2"}))
        << "the premise: the job starts on the second layout";

    // A v2 worker joins, hosting nothing, and b dies: the restart places the
    // job on a and the newcomer.
    auto* v2 = c.join("rla-c", 2, 4);
    ASSERT_NE(v2, nullptr);
    b->close();
    ASSERT_TRUE(rla_drain(job_id, first, {a}));
    const auto second = c.take_deploys(job_id, 10s);
    ASSERT_EQ(second.size(), 3U) << "the restart did not redeploy the whole job";
    ASSERT_TRUE(rla_hosts_any(second, v2)) << "the premise: the v2 worker hosts part of the job";
    EXPECT_TRUE(rla_stamps(second).empty())
        << "the restart kept the second layout with a v2 worker hosting part of the job";

    // The v2 worker dies and a v3 worker joins: every host reads the second
    // layout again, and the next restart stamps it.
    auto* v3 = c.join("rla-d", 3, 4);
    ASSERT_NE(v3, nullptr);
    v2->close();
    ASSERT_TRUE(rla_drain(job_id, second, {a}));
    const auto third = c.take_deploys(job_id, 10s);
    ASSERT_EQ(third.size(), 3U) << "the second restart did not redeploy the whole job";
    for (const auto& p : third) {
        ASSERT_TRUE(p.worker == a || p.worker == v3) << "placed on " << p.worker->id();
    }
    EXPECT_EQ(rla_stamps(third), (std::vector<std::string>{"tumbling_window_row=2"}))
        << "the restart onto v3 workers alone did not stamp the second layout";
}

// The hot cutover cases start the same way: a v3 worker `rla-a` with one
// slot and `rla-b` with three, so `rla-a` hosts the source and `rla-b` the
// two subtasks of `win` and the sink. The cutover's teardown gives `rla-b`
// two slots back and nothing to `rla-a`, so where the four new subtasks of
// `win` go is decided by the workers a case joins.

namespace {

struct RlaHotStart {
    JobId job_id{0};
    RlaFakeWorker* a{nullptr};
    RlaFakeWorker* b{nullptr};
    std::vector<RlaPlaced> placed;
};

RlaHotStart rla_hot_start(RlaCluster& c,
                          const std::string& win_type,
                          std::uint32_t version_a = kClusterProtocolVersion) {
    RlaHotStart s;
    c.start();
    s.a = c.join("rla-a", version_a, 1);
    s.b = c.join("rla-b", kClusterProtocolVersion, 3);
    if (s.a == nullptr || s.b == nullptr) {
        ADD_FAILURE() << "a worker could not register";
        return s;
    }
    s.job_id = c.submit(rla_hot_graph(win_type, c.dir() / "out.txt"), /*max_restarts=*/0);
    s.placed = c.take_deploys(s.job_id);
    EXPECT_EQ(s.placed.size(), 4U);
    for (const auto& p : s.placed) {
        EXPECT_EQ(p.worker == s.a, rla_runs(p.task, "src"))
            << "the premise: rla-a hosts the source and nothing else";
    }
    EXPECT_TRUE(rla_complete_first_checkpoint(c, s.job_id, s.placed))
        << "no checkpoint completed for the cutover to start from";
    return s;
}

}  // namespace

TEST(RowLayoutAdmissionCoordinator, AHotCutoverStampsItsNewSubtasksWithTheJobsSecondLayout) {
    RlaCluster c("hot_v3");
    const auto s = rla_hot_start(c, "tumbling_window_row");
    ASSERT_FALSE(::testing::Test::HasFailure());
    ASSERT_EQ(rla_stamps(s.placed).size(), 2U) << "the premise: the job runs the second layout";
    auto* joined = c.join("rla-c", 3, 4);
    ASSERT_NE(joined, nullptr);

    const auto since = rla_log_cursor();
    const auto cutover = rla_drive_cutover(c, s.job_id, s.placed, 4);
    ASSERT_TRUE(cutover.armed) << "the rescale did not take the hot path";
    ASSERT_EQ(cutover.deployed.size(), 4U);
    EXPECT_TRUE(rla_hosts_any(cutover.deployed, joined));
    for (const auto& p : cutover.deployed) {
        EXPECT_TRUE(rla_runs(p.task, "win"));
        EXPECT_EQ(rla_stamps_of(p.task), (std::vector<std::string>{"tumbling_window_row=2"}))
            << "a new subtask on '" << p.worker->id() << "' joins feeders that send the second "
            << "layout and was not stamped with it";
    }
    EXPECT_TRUE(rla_logged("row_layout=2 stamped_ops=4", since));
}

// The cutover keeps the job's layout, never one recomputed from where the new
// subtasks land. Here the job runs the first layout because its source is on
// a v2 worker, and every new subtask lands on a v3 worker: recomputed over
// them, the layout would be the second, and the new window subtasks would
// send typed columns their running feeder never sends to a sink deployed on
// the first.
TEST(RowLayoutAdmissionCoordinator,
     AHotCutoverOfAFirstLayoutJobStampsNothingOnVersionThreeWorkers) {
    RlaCluster c("hot_keep_v1");
    const auto s = rla_hot_start(c, "tumbling_window_row", /*version_a=*/2);
    ASSERT_FALSE(::testing::Test::HasFailure());
    ASSERT_TRUE(rla_stamps(s.placed).empty()) << "the premise: the job runs the first layout";
    auto* joined = c.join("rla-c", 3, 4);
    ASSERT_NE(joined, nullptr);

    const auto cutover = rla_drive_cutover(c, s.job_id, s.placed, 4);
    ASSERT_TRUE(cutover.armed) << "the rescale did not take the hot path";
    ASSERT_EQ(cutover.deployed.size(), 4U);
    for (const auto& p : cutover.deployed) {
        ASSERT_TRUE(p.worker == s.b || p.worker == joined)
            << "the premise: every new subtask lands on a v3 worker, not '" << p.worker->id()
            << "'";
        EXPECT_TRUE(rla_stamps_of(p.task).empty())
            << "the cutover stamped a layout recomputed from where the new subtasks landed";
    }
}

// A second-layout job's cutover whose new subtasks need a v2 worker's slots
// is not taken: the reason names the worker, and the replan, which decides
// the layout again for every task, takes the request onto the first layout.
TEST(RowLayoutAdmissionCoordinator, AHotCutoverNeedingAVersionTwoWorkerTakesTheReplan) {
    RlaCluster c("hot_refused");
    const auto s = rla_hot_start(c, "tumbling_window_row");
    ASSERT_FALSE(::testing::Test::HasFailure());
    ASSERT_EQ(rla_stamps(s.placed).size(), 2U) << "the premise: the job runs the second layout";
    auto* v2 = c.join("rla-c", 2, 4);
    ASSERT_NE(v2, nullptr);

    const auto since = rla_log_cursor();
    const auto result = c.coordinator().request_operator_rescale(s.job_id, "win", 4);
    ASSERT_TRUE(result.ok) << result.reason;
    EXPECT_TRUE(rla_logged("hot cutover not taken for job_id=" + std::to_string(s.job_id) +
                               " op_id=win (the job runs row layout 2 and the new subtasks of "
                               "'win' need the free slots of worker 'rla-c', which registered "
                               "at protocol v2; layout 2 needs v3)",
                           since));
    EXPECT_FALSE(s.b->await_frame(MessageKind::BeginRescale, 300ms).has_value())
        << "a cutover was armed onto a worker that cannot read the job's layout";

    ASSERT_TRUE(rla_drain(s.job_id, s.placed, {s.a, s.b}));
    const auto replanned = c.take_deploys(s.job_id, 10s);
    ASSERT_EQ(replanned.size(), 6U) << "the replan did not redeploy the job at the new parallelism";
    ASSERT_TRUE(rla_hosts_any(replanned, v2))
        << "the premise: the replan placed onto the v2 worker";
    EXPECT_TRUE(rla_stamps(replanned).empty())
        << "the replan kept the second layout with a v2 worker hosting part of the job";
    EXPECT_TRUE(
        rla_logged("row_layout=1 (worker 'rla-c' registered at protocol v2; layout 2 "
                   "needs v3) stamped_ops=0",
                   since));
}

// A v2 worker with free slots does not stop a second-layout job's cutover
// when the workers that read the layout have room for every new subtask:
// `rla-0` sorts first, so a placement over every worker would give it one,
// but the deploy places on the readers alone.
TEST(RowLayoutAdmissionCoordinator, AHotCutoverSeatsTheJobOnTheWorkersThatReadItsLayout) {
    RlaCluster c("hot_readers");
    const auto s = rla_hot_start(c, "tumbling_window_row");
    ASSERT_FALSE(::testing::Test::HasFailure());
    auto* v2 = c.join("rla-0", 2, 4);
    auto* v3 = c.join("rla-c", 3, 4);
    ASSERT_TRUE(v2 != nullptr && v3 != nullptr);

    const auto cutover = rla_drive_cutover(c, s.job_id, s.placed, 4);
    ASSERT_TRUE(cutover.armed) << "the cutover was refused although v3 workers could seat it";
    ASSERT_EQ(cutover.deployed.size(), 4U);
    EXPECT_FALSE(rla_hosts_any(cutover.deployed, v2)) << "a new subtask went to the v2 worker";
    for (const auto& p : cutover.deployed) {
        EXPECT_EQ(rla_stamps_of(p.task), (std::vector<std::string>{"tumbling_window_row=2"}));
    }
}

// A job with no layout-bearing operator can never send a typed column, so
// it runs the first layout on any workers and none of the second layout's
// placement rules applies: its cutover goes hot onto a v2 worker.
TEST(RowLayoutAdmissionCoordinator, AJobWithNoLayoutBearingOperatorCutsOverOntoAVersionTwoWorker) {
    RlaCluster c("hot_unstamped");
    const auto since = rla_log_cursor();
    const auto s = rla_hot_start(c, "identity_int64");
    ASSERT_FALSE(::testing::Test::HasFailure());
    EXPECT_TRUE(rla_logged("job_id=" + std::to_string(s.job_id) +
                               " row_layout=1 (no layout-bearing operator) stamped_ops=0",
                           since));
    auto* v2 = c.join("rla-c", 2, 4);
    ASSERT_NE(v2, nullptr);

    const auto cutover = rla_drive_cutover(c, s.job_id, s.placed, 4);
    ASSERT_TRUE(cutover.armed) << "the cutover was held to the second layout's placement rules";
    ASSERT_EQ(cutover.deployed.size(), 4U);
    EXPECT_TRUE(rla_hosts_any(cutover.deployed, v2))
        << "the premise: the v2 worker takes new subtasks";
    EXPECT_TRUE(rla_stamps(cutover.deployed).empty());
}
