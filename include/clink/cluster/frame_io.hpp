#pragma once

// Length-prefixed frame IO for the control plane.
//
// This used to be three copies of the same twelve lines - one in
// coordinator.cpp, one in worker.cpp, one in job_submitter.cpp - and all
// three shared the same defect, which is the usual reason to have one copy
// rather than three.
//
// The defect: the 4-byte length prefix was trusted. `std::vector<std::byte>
// body(len)` allocated and zeroed up to 4 GB in response to four bytes from
// an unauthenticated peer, before receiving any of the body. Anything that
// could reach the control port could make the coordinator try to allocate
// 4 GB, repeatedly, by sending `FF FF FF FF`.
//
// Two things fix it, and both are needed:
//
//   * A HARD CAP on frame size, so a nonsense length is refused outright
//     rather than attempted.
//   * INCREMENTAL reads, so memory tracks the bytes the peer has actually
//     sent. A cap alone still leaves the amplification - four bytes
//     claiming 256 MB would still allocate 256 MB up front. Reading in
//     chunks means an attacker must send a byte to cost a byte.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "clink/runtime/network/connection.hpp"

namespace clink::cluster {

// Largest control frame this build will accept.
//
// It has to clear the biggest legitimate frame by a wide margin, and that
// is a Deploy or SubmitJob carrying plugin `.so` bytes - a debug-built
// plugin runs to tens of megabytes. 256 MiB leaves room for several of
// those in one frame while still refusing a length that can only be
// nonsense. Operators shipping something larger raise it explicitly rather
// than discovering there was never a limit.
inline constexpr std::size_t kMaxFrameBytes = std::size_t{256} * 1024 * 1024;

// Read granularity. Frames are almost always a few hundred bytes, so the
// first read usually completes the frame; the chunking only matters for
// the plugin-carrying ones, where it is what bounds memory to what has
// actually arrived.
inline constexpr std::size_t kFrameReadChunkBytes = std::size_t{64} * 1024;

// Read one length-prefixed frame, returning the payload without the
// 4-byte header.
//
// nullopt means the connection ended, the peer sent a length above
// `max_bytes`, or the body did not arrive. All three are terminal for the
// connection: after an over-long length there is no way to know where the
// next frame starts, so the caller must close rather than resynchronise.
[[nodiscard]] inline std::optional<std::vector<std::byte>> read_frame(
    network::Connection& conn, std::size_t max_bytes = kMaxFrameBytes) {
    std::array<std::byte, 4> hdr{};
    if (!conn.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (const auto b : hdr) {
        len = (len << 8) | static_cast<unsigned char>(b);
    }
    if (len == 0) {
        return std::vector<std::byte>{};
    }
    if (static_cast<std::size_t>(len) > max_bytes) {
        return std::nullopt;
    }

    // Grow with the data rather than reserving `len` up front, so a peer
    // that claims 256 MB and sends nothing costs nothing.
    std::vector<std::byte> body;
    body.reserve(std::min<std::size_t>(len, kFrameReadChunkBytes));
    std::array<std::byte, kFrameReadChunkBytes> chunk{};
    std::size_t got = 0;
    while (got < len) {
        const auto want = std::min<std::size_t>(chunk.size(), len - got);
        if (!conn.recv_all(chunk.data(), want)) {
            return std::nullopt;
        }
        body.insert(body.end(), chunk.begin(), chunk.begin() + static_cast<std::ptrdiff_t>(want));
        got += want;
    }
    return body;
}

[[nodiscard]] inline bool send_frame(network::Connection& conn,
                                     const std::vector<std::byte>& frame) {
    return conn.send_all(frame.data(), frame.size());
}

}  // namespace clink::cluster
