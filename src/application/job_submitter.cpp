#include "clink/application/job_submitter.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/network_socket.hpp"

namespace clink::application {

namespace {

using namespace clink::cluster;
using namespace clink::network;

std::optional<std::vector<std::byte>> read_frame_with_timeout(int fd, int timeout_ms) {
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;
    const int rc = ::poll(&pfd, 1, timeout_ms);
    if (rc <= 0) {
        return std::nullopt;
    }
    std::array<std::byte, 4> hdr{};
    if (!NetworkSocket::recv_all(fd, hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (std::size_t i = 0; i < hdr.size(); ++i) {
        len = (len << 8) | static_cast<unsigned char>(hdr[i]);
    }
    std::vector<std::byte> body(len);
    if (len > 0 && !NetworkSocket::recv_all(fd, body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

}  // namespace

JobSubmitter::JobSubmitter(std::string coordinator_host, std::uint16_t coordinator_port)
    : coordinator_host_(std::move(coordinator_host)), coordinator_port_(coordinator_port) {}

SubmitResult JobSubmitter::submit(const std::string& graph_json,
                                  const std::vector<std::string>& plugin_paths,
                                  const SubmitOptions& opts) const {
    SubmitResult result;

    std::vector<PluginBinary> plugins;
    plugins.reserve(plugin_paths.size());
    for (const auto& p : plugin_paths) {
        try {
            plugins.push_back(make_plugin_binary_from_file(p));
        } catch (const std::exception& e) {
            result.reject_message = std::string{"plugin load failed for "} + p + ": " + e.what();
            return result;
        }
    }

    const int fd = NetworkSocket::connect_to(coordinator_host_, coordinator_port_);
    if (fd < 0) {
        result.reject_message = "connect_to(" + coordinator_host_ + ":" +
                                std::to_string(coordinator_port_) + ") failed";
        return result;
    }
    // RAII for the socket; close on every exit path.
    struct FdCloser {
        int fd;
        ~FdCloser() {
            if (fd >= 0) {
                NetworkSocket::close(fd);
            }
        }
    } closer{fd};

    auto send_one = [fd](MessageKind kind, const auto& msg) -> bool {
        const auto frame = encode_frame(kind, msg);
        return NetworkSocket::send_all(fd, frame.data(), frame.size());
    };

    if (!send_one(MessageKind::HelloClient, HelloClientMsg{})) {
        result.reject_message = "HelloClient send failed";
        return result;
    }
    SubmitJobMsg sj;
    sj.graph_json = graph_json;
    sj.plugins = std::move(plugins);
    sj.checkpoint = opts.checkpoint;
    if (!send_one(MessageKind::SubmitJob, sj)) {
        result.reject_message = "SubmitJob send failed";
        return result;
    }

    auto ack_frame = read_frame_with_timeout(fd, static_cast<int>(opts.ack_timeout.count()));
    if (!ack_frame.has_value()) {
        result.reject_message = "timed out waiting for SubmitJobAck";
        return result;
    }
    MessageReader ack_reader(std::move(*ack_frame));
    const auto ack_kind = static_cast<MessageKind>(ack_reader.read_u8());
    if (ack_kind != MessageKind::SubmitJobAck) {
        result.reject_message =
            "unexpected reply kind " + std::to_string(static_cast<int>(ack_kind));
        return result;
    }
    const auto ack = decode_submit_job_ack(ack_reader);
    if (!ack.ok) {
        result.reject_message = ack.message;
        return result;
    }
    result.job_id = ack.job_id;

    if (!opts.wait_for_completion || opts.wait_timeout.count() <= 0) {
        result.ok = true;
        return result;
    }

    const int wait_ms = static_cast<int>(opts.wait_timeout.count() * 1000);
    auto done_frame = read_frame_with_timeout(fd, wait_ms);
    if (!done_frame.has_value()) {
        result.reject_message = "timed out waiting for JobCompleted";
        return result;
    }
    MessageReader done_reader(std::move(*done_frame));
    const auto done_kind = static_cast<MessageKind>(done_reader.read_u8());
    if (done_kind != MessageKind::JobCompleted) {
        result.reject_message =
            "unexpected completion kind " + std::to_string(static_cast<int>(done_kind));
        return result;
    }
    const auto done = decode_job_completed(done_reader);
    result.completed = true;
    result.errors = done.errors;
    result.ok = done.ok;
    return result;
}

JobSubmitter::ListResult JobSubmitter::list_jobs(std::chrono::milliseconds timeout) const {
    ListResult result;

    const int fd = NetworkSocket::connect_to(coordinator_host_, coordinator_port_);
    if (fd < 0) {
        result.error = "connect_to(" + coordinator_host_ + ":" + std::to_string(coordinator_port_) +
                       ") failed";
        return result;
    }
    struct FdCloser {
        int fd;
        ~FdCloser() {
            if (fd >= 0) {
                NetworkSocket::close(fd);
            }
        }
    } closer{fd};

    auto send_one = [fd](MessageKind kind, const auto& msg) -> bool {
        const auto frame = encode_frame(kind, msg);
        return NetworkSocket::send_all(fd, frame.data(), frame.size());
    };
    if (!send_one(MessageKind::HelloClient, HelloClientMsg{})) {
        result.error = "HelloClient send failed";
        return result;
    }
    if (!send_one(MessageKind::ListJobs, ListJobsMsg{})) {
        result.error = "ListJobs send failed";
        return result;
    }
    auto ack_frame = read_frame_with_timeout(fd, static_cast<int>(timeout.count()));
    if (!ack_frame.has_value()) {
        result.error = "timed out waiting for ListJobsAck";
        return result;
    }
    MessageReader reader(std::move(*ack_frame));
    const auto kind = static_cast<MessageKind>(reader.read_u8());
    if (kind != MessageKind::ListJobsAck) {
        result.error = "unexpected reply kind " + std::to_string(static_cast<int>(kind));
        return result;
    }
    const auto ack = decode_list_jobs_ack(reader);
    result.ok = true;
    result.jobs.reserve(ack.jobs.size());
    for (const auto& j : ack.jobs) {
        result.jobs.push_back(JobListing{
            .job_id = j.job_id,
            .total_subtasks = j.total_subtasks,
            .completed_subtasks = j.completed_subtasks,
            .completion_signalled = j.completion_signalled,
        });
    }
    return result;
}

}  // namespace clink::application
