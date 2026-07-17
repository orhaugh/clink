// clink_rescale_job - client CLI that asks a running Coordinator to
// rescale a job's per-role parallelism while preserving keyed state.
// The  analogue is ` modify --parallelism` plus a savepoint
// in one step; here the coordinator handles the checkpoint + drain + redeploy
// internally so the operator only has to name the new parallelism.
//
// Wire flow:
//   1. Open a TCP connection to --coordinator-host:--coordinator-port.
//   2. Send HelloClient so the coordinator routes us through handle_client_loop_.
//   3. Send a RescaleJob frame with the requested job_id + role->p map.
//   4. Block on RescaleJobAck. Print the ack and exit.
//
// v1 restrictions enforced by the coordinator (the CLI just relays the rejection):
//   * Each role's new parallelism must be a positive integer multiple
//     of the role's current parallelism (scale-up only).
//   * The job must already have at least one completed checkpoint -
//     rescale restores from there into the new subtask layout.
//
// Usage:
//   clink_rescale_job --job-id=N --role=R --parallelism=P \
//                       [--role=R2 --parallelism=P2 ...]      \
//                       [--coordinator-host=127.0.0.1] [--coordinator-port=6123]

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/cluster/messages.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/network_socket.hpp"

namespace {

std::string get_arg(int argc,
                    char** argv,
                    std::string_view flag,
                    std::string_view default_value = {}) {
    const std::string prefix = "--" + std::string{flag} + "=";
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with(prefix)) {
            return a.substr(prefix.size());
        }
    }
    return std::string{default_value};
}

bool has_flag(int argc, char** argv, std::string_view flag) {
    const std::string needle = "--" + std::string{flag};
    for (int i = 1; i < argc; ++i) {
        if (std::string{argv[i]} == needle) {
            return true;
        }
    }
    return false;
}

// Pair --role=X --parallelism=N occurrences in argv into a vector of
// (role, parallelism) entries, in argv order. A trailing --role with
// no --parallelism (or vice versa) is rejected. Repeating --role with
// the same name is permitted; the coordinator keeps the last write.
std::vector<std::pair<std::string, std::uint32_t>> parse_role_pairs(int argc, char** argv) {
    std::vector<std::pair<std::string, std::uint32_t>> out;
    std::string pending_role;
    bool have_pending = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a.starts_with("--role=")) {
            if (have_pending) {
                throw std::runtime_error(
                    "clink_rescale_job: --role specified twice in a row "
                    "without an intervening --parallelism");
            }
            pending_role = a.substr(std::string("--role=").size());
            have_pending = true;
        } else if (a.starts_with("--parallelism=")) {
            if (!have_pending) {
                throw std::runtime_error(
                    "clink_rescale_job: --parallelism without preceding "
                    "--role");
            }
            const auto p = static_cast<std::uint32_t>(
                std::stoul(a.substr(std::string("--parallelism=").size())));
            out.emplace_back(std::move(pending_role), p);
            pending_role.clear();
            have_pending = false;
        }
    }
    if (have_pending) {
        throw std::runtime_error("clink_rescale_job: trailing --role with no --parallelism");
    }
    return out;
}

void usage() {
    std::cerr << "Usage: clink rescale --job-id=N "
                 "--role=<role> --parallelism=<p> [--role=... --parallelism=...] "
                 "[--coordinator-host=127.0.0.1] [--coordinator-port=6123]\n";
}

}  // namespace

int clink_cmd_rescale(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_id_str = get_arg(argc, argv, "job-id");
    const auto coordinator_host = get_arg(argc, argv, "coordinator-host", "127.0.0.1");
    const auto coordinator_port_str = get_arg(argc, argv, "coordinator-port", "6123");

    if (job_id_str.empty()) {
        std::cerr << "clink_rescale_job: --job-id=N is required\n";
        return 2;
    }

    std::vector<std::pair<std::string, std::uint32_t>> roles;
    try {
        roles = parse_role_pairs(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 2;
    }
    if (roles.empty()) {
        std::cerr << "clink_rescale_job: at least one --role/--parallelism pair is required\n";
        return 2;
    }

    const auto job_id = static_cast<clink::cluster::JobId>(std::stoull(job_id_str));
    const auto coordinator_port = static_cast<std::uint16_t>(std::stoi(coordinator_port_str));

    const int fd = clink::network::NetworkSocket::connect_to(coordinator_host, coordinator_port);
    if (fd < 0) {
        std::cerr << "clink_rescale_job: connect_to(" << coordinator_host << ":" << coordinator_port
                  << ") failed\n";
        return 3;
    }

    // Identify as a client so the coordinator routes us through handle_client_loop_.
    {
        clink::cluster::HelloClientMsg hello;
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::HelloClient, hello);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_rescale_job: HelloClient send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 4;
        }
    }

    clink::cluster::RescaleJobMsg req;
    req.job_id = job_id;
    req.role_parallelism = std::move(roles);
    {
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::RescaleJob, req);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_rescale_job: RescaleJob send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 5;
        }
    }

    // Wait for the coordinator to ack. Frame format: 4-byte big-endian length +
    // 1-byte MessageKind + body. Same framing as clink_cancel_job.
    std::array<std::byte, 4> len_hdr{};
    if (!clink::network::NetworkSocket::recv_all(fd, len_hdr.data(), len_hdr.size())) {
        std::cerr << "clink_rescale_job: short read on ack length\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    std::uint32_t body_len = 0;
    for (int i = 0; i < 4; ++i) {
        body_len = (body_len << 8) | static_cast<unsigned char>(len_hdr[i]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !clink::network::NetworkSocket::recv_all(fd, body.data(), body.size())) {
        std::cerr << "clink_rescale_job: short read on ack body\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    clink::cluster::MessageReader r(std::move(body));
    const auto kind = static_cast<clink::cluster::MessageKind>(r.read_u8());
    if (kind != clink::cluster::MessageKind::RescaleJobAck) {
        std::cerr << "clink_rescale_job: unexpected reply kind " << static_cast<int>(kind) << "\n";
        clink::network::NetworkSocket::close(fd);
        return 7;
    }
    auto ack = clink::cluster::decode_rescale_job_ack(r);
    clink::network::NetworkSocket::close(fd);

    std::cout << "rescale: job_id=" << ack.job_id << " ok=" << ack.ok << " message=\""
              << ack.message << "\"\n";
    return ack.ok ? 0 : 8;
}
