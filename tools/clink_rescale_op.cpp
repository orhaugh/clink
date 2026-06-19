// clink_rescale_op - Phase 29d-4 client CLI: ask a running JobManager
// to rescale ONE operator within a job to a new parallelism.
// Mirrors clink_rescale_job's wire flow but targets a single operator
// (via its OperatorSpec.id) rather than a whole role list.
//
// Usage:
//   clink rescale-op --job-id=N --op=<op_id> --parallelism=<p> \
//                    [--jm-host=127.0.0.1] [--jm-port=6123]
//
// The JM validates against the operator's Phase 29a min/max bounds
// (set in the OperatorSpec at submit time) and rejects out-of-range
// requests, requests equal to the current parallelism, and requests
// against operators that already have a rescale in progress.

#include <array>
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
    std::cerr << "Usage: clink rescale-op --job-id=N --op=<op_id> --parallelism=<p> "
                 "[--jm-host=127.0.0.1] [--jm-port=6123]\n";
}

}  // namespace

int clink_cmd_rescale_op(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_id_str = get_arg(argc, argv, "job-id");
    const auto op_id = get_arg(argc, argv, "op");
    const auto parallelism_str = get_arg(argc, argv, "parallelism");
    const auto jm_host = get_arg(argc, argv, "jm-host", "127.0.0.1");
    const auto jm_port_str = get_arg(argc, argv, "jm-port", "6123");

    if (job_id_str.empty() || op_id.empty() || parallelism_str.empty()) {
        std::cerr << "clink_rescale_op: --job-id, --op, and --parallelism are all required\n";
        return 2;
    }

    const auto job_id = static_cast<clink::cluster::JobId>(std::stoull(job_id_str));
    const auto new_parallelism = static_cast<std::uint32_t>(std::stoul(parallelism_str));
    const auto jm_port = static_cast<std::uint16_t>(std::stoi(jm_port_str));

    const int fd = clink::network::NetworkSocket::connect_to(jm_host, jm_port);
    if (fd < 0) {
        std::cerr << "clink_rescale_op: connect_to(" << jm_host << ":" << jm_port << ") failed\n";
        return 3;
    }

    {
        clink::cluster::HelloClientMsg hello;
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::HelloClient, hello);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_rescale_op: HelloClient send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 4;
        }
    }

    clink::cluster::RescaleOperatorMsg req;
    req.job_id = job_id;
    req.op_id = op_id;
    req.new_parallelism = new_parallelism;
    {
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::RescaleOperator, req);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_rescale_op: RescaleOperator send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 5;
        }
    }

    // Wait for the JM to ack. Frame: 4-byte big-endian length + 1-byte
    // MessageKind + body. Same framing as the other CLI tools.
    std::array<std::byte, 4> len_hdr{};
    if (!clink::network::NetworkSocket::recv_all(fd, len_hdr.data(), len_hdr.size())) {
        std::cerr << "clink_rescale_op: short read on ack length\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    std::uint32_t body_len = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        body_len = (body_len << 8) | static_cast<unsigned char>(len_hdr[i]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !clink::network::NetworkSocket::recv_all(fd, body.data(), body.size())) {
        std::cerr << "clink_rescale_op: short read on ack body\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    clink::cluster::MessageReader r(std::move(body));
    const auto kind = static_cast<clink::cluster::MessageKind>(r.read_u8());
    if (kind != clink::cluster::MessageKind::RescaleOperatorAck) {
        std::cerr << "clink_rescale_op: unexpected reply kind " << static_cast<int>(kind) << "\n";
        clink::network::NetworkSocket::close(fd);
        return 7;
    }
    auto ack = clink::cluster::decode_rescale_operator_ack(r);
    clink::network::NetworkSocket::close(fd);

    std::cout << "rescale-op: job_id=" << ack.job_id << " op=" << op_id << " ok=" << ack.ok
              << " accepted_target=" << ack.accepted_target << " message=\"" << ack.message
              << "\"\n";
    return ack.ok ? 0 : 1;
}
