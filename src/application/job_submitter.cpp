#include "clink/application/job_submitter.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <utility>
#include <vector>

#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/plugin_cache.hpp"
#include "clink/cluster/plugin_loader.hpp"  // summarise_manifest_diff
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/network_socket.hpp"

namespace clink::application {

namespace {

using namespace clink::cluster;
using namespace clink::network;

// Why a frame read failed, not merely that it did.
//
// Every one of these used to collapse into nullopt, and every caller
// reported it as "timed out". A coordinator that CLOSED the connection
// therefore looked identical to one that was simply slow - and the message
// sent a reader looking at timeouts. That cost real time on F39, where a
// "timed out waiting for JobCompleted" arrived inside a three-second test
// that had asked for a fifteen-second wait: the wait had not expired, the
// connection had gone.
enum class ReadFailure {
    Timeout,       // poll() expired with nothing to read
    PollError,     // poll() itself failed
    Closed,        // peer closed, or the read died mid-frame
    OversizeFrame  // length prefix beyond kMaxFrameBytes
};

const char* describe(ReadFailure f) {
    switch (f) {
        case ReadFailure::Timeout:
            return "timed out";
        case ReadFailure::PollError:
            return "poll failed";
        case ReadFailure::Closed:
            return "connection closed by the coordinator";
        case ReadFailure::OversizeFrame:
            return "oversize frame (is this a clink coordinator port?)";
    }
    return "unknown";
}

std::optional<std::vector<std::byte>> read_frame_with_timeout(int fd,
                                                              int timeout_ms,
                                                              ReadFailure* why = nullptr) {
    const auto fail = [why](ReadFailure f) {
        if (why != nullptr) {
            *why = f;
        }
        return std::nullopt;
    };
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;
    const int rc = ::poll(&pfd, 1, timeout_ms);
    if (rc == 0) {
        return fail(ReadFailure::Timeout);
    }
    if (rc < 0) {
        return fail(ReadFailure::PollError);
    }
    std::array<std::byte, 4> hdr{};
    if (!NetworkSocket::recv_all(fd, hdr.data(), hdr.size())) {
        return fail(ReadFailure::Closed);
    }
    std::uint32_t len = 0;
    for (std::size_t i = 0; i < hdr.size(); ++i) {
        len = (len << 8) | static_cast<unsigned char>(hdr[i]);
    }
    // Same bound as the Connection-based reader in frame_io.hpp: this is
    // the fd-based copy, which needs poll() for its timeout and so cannot
    // share that code, but it must not trust the length prefix either. A
    // submitter pointed at a wrong or hostile port would otherwise try to
    // allocate 4 GB from four bytes.
    if (static_cast<std::size_t>(len) > kMaxFrameBytes) {
        return fail(ReadFailure::OversizeFrame);
    }
    std::vector<std::byte> body;
    body.reserve(std::min<std::size_t>(len, kFrameReadChunkBytes));
    std::array<std::byte, kFrameReadChunkBytes> chunk{};
    std::size_t got = 0;
    while (got < len) {
        const auto want = std::min<std::size_t>(chunk.size(), len - got);
        if (!NetworkSocket::recv_all(fd, chunk.data(), want)) {
            // Died mid-body: the peer went away, not a timeout.
            return fail(ReadFailure::Closed);
        }
        body.insert(body.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(want));
        got += want;
    }
    return body;
}

// What the submitter's own dlopen of a plugin reports: the wire advert plus
// the local per-header manifest, which never ships (the coordinator returns
// ITS manifest on a rejection and the diff is computed here, client-side).
struct LocalPluginIdentity {
    PluginAbiAdvert advert;
    std::string manifest;
};

// Read the ABI identity a plugin's handshake symbols report, via a private
// dlopen. Best-effort: any failure returns nullopt and the submit proceeds
// exactly as before the preflight existed - the coordinator's load-time gate
// stays the authority. Only the constexpr-string getters are called, never
// the register hook, so no module closure can escape and the handle is safe
// to close (the strings are copied out first).
std::optional<LocalPluginIdentity> read_plugin_identity(const std::string& path,
                                                        const std::string& content_hash) {
    void* handle = ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        return std::nullopt;
    }
    auto read_str = [handle](const char* sym) -> std::string {
        using Fn = const char* (*)();
        void* p = ::dlsym(handle, sym);
        if (p == nullptr) {
            return {};
        }
        Fn fn = nullptr;
        std::memcpy(&fn, &p, sizeof(fn));
        const char* v = fn();
        return v == nullptr ? std::string{} : std::string{v};
    };
    LocalPluginIdentity id;
    id.advert.content_hash = content_hash;
    id.advert.abi_fingerprint = read_str("clink_plugin_abi_fingerprint");
    id.advert.abi_hash = read_str("clink_plugin_abi_hash");
    id.advert.target_triple = read_str("clink_plugin_target_triple");
    id.advert.toolchain = read_str("clink_plugin_toolchain");
    id.manifest = read_str("clink_plugin_abi_manifest");
    if (void* p = ::dlsym(handle, "clink_plugin_abi_version"); p != nullptr) {
        using VerFn = int (*)();
        VerFn fn = nullptr;
        std::memcpy(&fn, &p, sizeof(fn));
        id.advert.abi_version = static_cast<std::uint32_t>(fn());
    }
    ::dlclose(handle);
    if (id.advert.abi_hash.empty() && id.advert.abi_fingerprint.empty()) {
        // Not a clink plugin handshake; advertise nothing rather than a
        // guaranteed-mismatching empty identity.
        return std::nullopt;
    }
    return id;
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
    // Content-addressed submit (item 30): send hash-only REFERENCES first.
    // A coordinator whose cache holds every module admits the job with no
    // plugin bytes on the wire at all - the common case for every submit
    // after the first. One retry uploads exactly the hashes the ack names
    // missing; anything else in the reply is an ordinary rejection.
    SubmitJobMsg sj;
    sj.graph_json = graph_json;
    sj.plugins.reserve(plugins.size());
    for (const auto& plug : plugins) {
        sj.plugins.push_back(
            PluginBinary{.name = plug.name, .content_hash = plug.content_hash, .bytes = {}});
    }
    sj.checkpoint = opts.checkpoint;
    // ABI preflight material: advertise each plugin's handshake identity so an
    // incompatible plugin is refused on this references-only exchange, before
    // any bytes ship. Best-effort per plugin; the local manifests stay here
    // for naming the differing headers if the coordinator refuses.
    std::vector<std::string> local_manifests(plugin_paths.size());
    for (std::size_t i = 0; i < plugin_paths.size(); ++i) {
        if (auto id = read_plugin_identity(plugin_paths[i], plugins[i].content_hash);
            id.has_value()) {
            sj.plugin_abi_adverts.push_back(std::move(id->advert));
            local_manifests[i] = std::move(id->manifest);
        }
    }

    const auto submit_and_read_ack =
        [&](const SubmitJobMsg& msg) -> std::optional<SubmitJobAckMsg> {
        if (!send_one(MessageKind::SubmitJob, msg)) {
            result.reject_message = "SubmitJob send failed";
            return std::nullopt;
        }
        ReadFailure ack_why = ReadFailure::Timeout;
        auto ack_frame =
            read_frame_with_timeout(fd, static_cast<int>(opts.ack_timeout.count()), &ack_why);
        if (!ack_frame.has_value()) {
            result.reject_message = std::string{"no SubmitJobAck: "} + describe(ack_why);
            return std::nullopt;
        }
        MessageReader ack_reader(std::move(*ack_frame));
        const auto ack_kind = static_cast<MessageKind>(ack_reader.read_u8());
        if (ack_kind != MessageKind::SubmitJobAck) {
            result.reject_message =
                "unexpected reply kind " + std::to_string(static_cast<int>(ack_kind));
            return std::nullopt;
        }
        return decode_submit_job_ack(ack_reader);
    };

    auto ack_opt = submit_and_read_ack(sj);
    if (!ack_opt.has_value()) {
        return result;
    }
    if (!ack_opt->ok && !ack_opt->missing_plugin_hashes.empty()) {
        // The coordinator's cache lacks some referenced modules: retry once
        // with bytes for exactly those. Everything it already holds stays a
        // reference.
        for (auto& plug : sj.plugins) {
            const bool wanted =
                std::find(ack_opt->missing_plugin_hashes.begin(),
                          ack_opt->missing_plugin_hashes.end(),
                          plug.content_hash) != ack_opt->missing_plugin_hashes.end();
            if (!wanted) {
                continue;
            }
            for (const auto& full : plugins) {
                if (full.content_hash == plug.content_hash) {
                    plug.bytes = full.bytes;
                    break;
                }
            }
        }
        ack_opt = submit_and_read_ack(sj);
        if (!ack_opt.has_value()) {
            return result;
        }
    }
    const auto& ack = *ack_opt;
    if (!ack.ok) {
        result.reject_message = ack.message;
        // An ABI-preflight refusal carries the cluster's per-header manifest;
        // the plugin's own manifest is local, so the headers that differ are
        // named here rather than shipped.
        if (!ack.cluster_abi_manifest.empty()) {
            for (const auto& manifest : local_manifests) {
                if (manifest.empty()) {
                    continue;
                }
                const auto diff =
                    cluster::summarise_manifest_diff(manifest, ack.cluster_abi_manifest, 5);
                if (!diff.empty()) {
                    result.reject_message += ". " + diff;
                }
            }
        }
        return result;
    }
    result.job_id = ack.job_id;

    if (!opts.wait_for_completion || opts.wait_timeout.count() <= 0) {
        result.ok = true;
        return result;
    }

    const int wait_ms = static_cast<int>(opts.wait_timeout.count() * 1000);
    ReadFailure done_why = ReadFailure::Timeout;
    auto done_frame = read_frame_with_timeout(fd, wait_ms, &done_why);
    if (!done_frame.has_value()) {
        // Name the wait, so a message that arrives well inside it is
        // recognisable as something other than the wait expiring.
        result.reject_message = std::string{"no JobCompleted after "} +
                                std::to_string(opts.wait_timeout.count()) +
                                "s: " + describe(done_why);
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
