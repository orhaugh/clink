// clink_savepoint - trigger a synchronous savepoint on a running
// job and print the (dir, id) handle the user can feed back to
// clink_submit_job's --restore-from-dir / --restore-from-checkpoint-
// id. Mirrors ` savepoint <jobid>` for the common case where
// the operator just wants a named, addressable snapshot.
//
// v1 returns the in-place checkpoint location - physical relocation
// to a portable path is the operator's responsibility (rsync, S3
// copy). The handle is a stable filesystem path + integer id that
// both the coordinator and any future clink_submit_job can address.
//
// Wire flow:
//   1. HelloClient frames us as a client.
//   2. Savepoint(job_id, timeout) → coordinator triggers a checkpoint and
//      blocks until every subtask acks.
//   3. SavepointAck carries (ok, checkpoint_id, checkpoint_dir,
//      message). On ok=true we print one line:
//        savepoint: job_id=N ok=1 dir=/path id=42
//      which is parseable by shell scripts.
//
// Usage:
//   clink_savepoint --job-id=N [--timeout-s=30] \
//                     [--coordinator-host=127.0.0.1] [--coordinator-port=6123]

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

void usage() {
    std::cerr << "Usage: clink savepoint --job-id=N [--timeout-s=30] "
                 "[--coordinator-host=127.0.0.1] [--coordinator-port=6123]\n";
}

}  // namespace

int clink_cmd_savepoint(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_id_str = get_arg(argc, argv, "job-id");
    const auto coordinator_host = get_arg(argc, argv, "coordinator-host", "127.0.0.1");
    const auto coordinator_port_str = get_arg(argc, argv, "coordinator-port", "6123");
    const auto timeout_s_str = get_arg(argc, argv, "timeout-s", "30");

    if (job_id_str.empty()) {
        std::cerr << "clink_savepoint: --job-id=N is required\n";
        return 2;
    }
    const auto job_id = static_cast<clink::cluster::JobId>(std::stoull(job_id_str));
    const auto coordinator_port = static_cast<std::uint16_t>(std::stoi(coordinator_port_str));
    const auto timeout_s = static_cast<std::int64_t>(std::stoll(timeout_s_str));

    const int fd = clink::network::NetworkSocket::connect_to(coordinator_host, coordinator_port);
    if (fd < 0) {
        std::cerr << "clink_savepoint: connect_to(" << coordinator_host << ":" << coordinator_port
                  << ") failed\n";
        return 3;
    }

    {
        clink::cluster::HelloClientMsg hello;
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::HelloClient, hello);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_savepoint: HelloClient send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 4;
        }
    }

    clink::cluster::SavepointMsg req;
    req.job_id = job_id;
    req.timeout_ms = timeout_s * 1000;
    {
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::Savepoint, req);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_savepoint: Savepoint send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 5;
        }
    }

    std::array<std::byte, 4> len_hdr{};
    if (!clink::network::NetworkSocket::recv_all(fd, len_hdr.data(), len_hdr.size())) {
        std::cerr << "clink_savepoint: short read on ack length\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    std::uint32_t body_len = 0;
    for (int i = 0; i < 4; ++i) {
        body_len = (body_len << 8) | static_cast<unsigned char>(len_hdr[i]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !clink::network::NetworkSocket::recv_all(fd, body.data(), body.size())) {
        std::cerr << "clink_savepoint: short read on ack body\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    clink::cluster::MessageReader r(std::move(body));
    const auto kind = static_cast<clink::cluster::MessageKind>(r.read_u8());
    if (kind != clink::cluster::MessageKind::SavepointAck) {
        std::cerr << "clink_savepoint: unexpected reply kind " << static_cast<int>(kind) << "\n";
        clink::network::NetworkSocket::close(fd);
        return 7;
    }
    auto ack = clink::cluster::decode_savepoint_ack(r);
    clink::network::NetworkSocket::close(fd);

    std::cout << "savepoint: job_id=" << ack.job_id << " ok=" << ack.ok << " dir=\""
              << ack.checkpoint_dir << "\""
              << " id=" << ack.checkpoint_id << " message=\"" << ack.message << "\"\n";
    return ack.ok ? 0 : 8;
}
