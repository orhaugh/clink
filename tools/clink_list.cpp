// `clink list` - ` list`. Queries a running coordinator
// over the control plane and prints one line per job (id, total /
// completed subtasks, signalled, errors).
//
// Used to live nowhere - added as part of the consolidation of the
// per-tool binaries into the single `clink` CLI.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/network_socket.hpp"

namespace {

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (argv[i] == needle) {
            return true;
        }
    }
    return false;
}

std::string get_arg(int argc, char** argv, std::string_view flag, std::string fallback = "") {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        if (s.starts_with(prefix)) {
            return s.substr(prefix.size());
        }
    }
    return fallback;
}

void usage() {
    std::cerr << "Usage: clink list --coordinator-host=<host> --coordinator-port=<port>\n";
}

}  // namespace

int clink_cmd_list(int argc, char** argv) {
    if (has_flag(argc, argv, "help")) {
        usage();
        return 0;
    }

    const auto coordinator_host = get_arg(argc, argv, "coordinator-host", "127.0.0.1");
    const auto coordinator_port_str = get_arg(argc, argv, "coordinator-port", "6123");
    const auto coordinator_port = static_cast<std::uint16_t>(std::stoi(coordinator_port_str));

    const int fd = clink::network::NetworkSocket::connect_to(coordinator_host, coordinator_port);
    if (fd < 0) {
        std::cerr << "clink list: connect_to(" << coordinator_host << ":" << coordinator_port
                  << ") failed\n";
        return 3;
    }

    // HelloClient first so the coordinator routes us to the client handler.
    {
        clink::cluster::HelloClientMsg hello;
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::HelloClient, hello);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink list: HelloClient send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 4;
        }
    }

    clink::cluster::ListJobsMsg req;
    {
        const auto frame = clink::cluster::encode_frame(clink::cluster::MessageKind::ListJobs, req);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink list: ListJobs send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 5;
        }
    }

    std::array<std::byte, 4> len_hdr{};
    if (!clink::network::NetworkSocket::recv_all(fd, len_hdr.data(), len_hdr.size())) {
        std::cerr << "clink list: short read on ack length\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    std::uint32_t body_len = 0;
    for (int i = 0; i < 4; ++i) {
        body_len =
            (body_len << 8) | static_cast<unsigned char>(len_hdr[static_cast<std::size_t>(i)]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !clink::network::NetworkSocket::recv_all(fd, body.data(), body.size())) {
        std::cerr << "clink list: short read on ack body\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    clink::cluster::MessageReader r(std::move(body));
    const auto kind = static_cast<clink::cluster::MessageKind>(r.read_u8());
    if (kind != clink::cluster::MessageKind::ListJobsAck) {
        std::cerr << "clink list: unexpected reply kind " << static_cast<int>(kind) << "\n";
        clink::network::NetworkSocket::close(fd);
        return 7;
    }
    auto ack = clink::cluster::decode_list_jobs_ack(r);
    clink::network::NetworkSocket::close(fd);

    std::cout << "JOB_ID  COMPLETED/TOTAL  SIGNALLED\n";
    for (const auto& j : ack.jobs) {
        std::cout << j.job_id << "  " << j.completed_subtasks << "/" << j.total_subtasks << "  "
                  << (j.completion_signalled ? "yes" : "no") << "\n";
    }
    return 0;
}
