// clink_stop_job - stop a running job GRACEFULLY and print the checkpoint to
// resume from.
//
// The difference from `clink cancel`, which is the whole reason this exists: a
// cancel is abrupt. It stops the subtasks where they are, so every record
// processed since the last completed checkpoint is discarded and replays when
// the job is resubmitted. That is consistent with the delivery guarantee, but it
// is not a drain, and it gives an operator upgrading a job no clean point to
// resume from.
//
// A stop tells every source to stop producing and then run its end-of-input
// path: flush, take a coordinator-coordinated final checkpoint, and block until
// the sinks have committed it. Only then does the subtask report finished, so the
// job ends as a SUCCESS with its tail durable. The printed checkpoint id is what
// to resubmit from.
//
// Usage:
//   clink stop --job-id=N [--timeout-s=60] \
//              [--coordinator-host=127.0.0.1] [--coordinator-port=6123]
//
// The output line is parseable:
//   stop: job_id=1 ok=1 savepoint_checkpoint_id=42 message="..."
//
// Resume with:
//   clink run --job=... --restore-from-dir=<checkpoint dir> \
//             --restore-from-checkpoint-id=42

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "clink/cluster/client_handshake.hpp"
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
    std::cerr << "Usage: clink stop --job-id=N [--timeout-s=60] "
                 "[--coordinator-host=127.0.0.1] [--coordinator-port=6123]\n"
                 "\n"
                 "Stops the job gracefully: sources stop producing, the tail is committed at a\n"
                 "final checkpoint, and the job finishes as a success. Prints the checkpoint id\n"
                 "to resubmit from. Use `clink cancel` to stop abruptly instead - that discards\n"
                 "everything since the last completed checkpoint.\n";
}

}  // namespace

int clink_cmd_stop_job(int argc, char** argv) {
    if (has_flag(argc, argv, "help") || argc < 2) {
        usage();
        return argc < 2 ? 1 : 0;
    }

    const auto job_id_str = get_arg(argc, argv, "job-id");
    const auto coordinator_host = get_arg(argc, argv, "coordinator-host", "127.0.0.1");
    const auto coordinator_port_str = get_arg(argc, argv, "coordinator-port", "6123");
    // Generous by default: the wait covers the drain, the final checkpoint and
    // the sinks committing it, not just a round trip.
    const auto timeout_s_str = get_arg(argc, argv, "timeout-s", "60");

    if (job_id_str.empty()) {
        std::cerr << "clink_stop_job: --job-id=N is required\n";
        return 2;
    }
    const auto job_id = static_cast<clink::cluster::JobId>(std::stoull(job_id_str));
    const auto coordinator_port = static_cast<std::uint16_t>(std::stoi(coordinator_port_str));
    const auto timeout_s = static_cast<std::int64_t>(std::stoll(timeout_s_str));

    const int fd = clink::network::NetworkSocket::connect_to(coordinator_host, coordinator_port);
    if (fd < 0) {
        std::cerr << "clink_stop_job: connect_to(" << coordinator_host << ":" << coordinator_port
                  << ") failed\n";
        return 3;
    }

    {
        clink::cluster::HelloClientMsg hello;
        const auto frame =
            clink::cluster::encode_frame(clink::cluster::MessageKind::HelloClient, hello);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_stop_job: HelloClient send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 4;
        }
    }

    clink::cluster::StopJobMsg req;
    req.job_id = job_id;
    req.timeout_ms = static_cast<std::uint64_t>(timeout_s) * 1000;
    {
        const auto frame = clink::cluster::encode_frame(clink::cluster::MessageKind::StopJob, req);
        if (!clink::network::NetworkSocket::send_all(fd, frame.data(), frame.size())) {
            std::cerr << "clink_stop_job: StopJob send failed\n";
            clink::network::NetworkSocket::close(fd);
            return 5;
        }
    }

    std::array<std::byte, 4> len_hdr{};
    if (!clink::network::NetworkSocket::recv_all(fd, len_hdr.data(), len_hdr.size())) {
        std::cerr << "clink_stop_job: short read on ack length\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    std::uint32_t body_len = 0;
    for (int i = 0; i < 4; ++i) {
        body_len = (body_len << 8) | static_cast<unsigned char>(len_hdr[i]);
    }
    std::vector<std::byte> body(body_len);
    if (body_len > 0 && !clink::network::NetworkSocket::recv_all(fd, body.data(), body.size())) {
        std::cerr << "clink_stop_job: short read on ack body\n";
        clink::network::NetworkSocket::close(fd);
        return 6;
    }
    clink::cluster::MessageReader r(std::move(body));
    const auto kind = static_cast<clink::cluster::MessageKind>(r.read_u8());
    if (kind != clink::cluster::MessageKind::StopJobAck) {
        // A refused handshake arrives as a SubmitJobAck rather than the ack this
        // tool asked for. Report why rather than reporting the number.
        if (const auto why = clink::cluster::protocol_rejection_message(kind, r)) {
            std::cerr << "clink_stop_job: coordinator refused the connection: " << *why << "\n";
        } else {
            std::cerr << "clink_stop_job: unexpected reply kind " << static_cast<int>(kind) << "\n";
        }
        clink::network::NetworkSocket::close(fd);
        return 7;
    }
    auto ack = clink::cluster::decode_stop_job_ack(r);
    clink::network::NetworkSocket::close(fd);

    std::cout << "stop: job_id=" << ack.job_id << " ok=" << ack.ok
              << " savepoint_checkpoint_id=" << ack.savepoint_checkpoint_id << " message=\""
              << ack.message << "\"\n";
    return ack.ok ? 0 : 8;
}
