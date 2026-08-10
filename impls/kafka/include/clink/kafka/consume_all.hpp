#pragma once

// consume_all_committed - drain every COMMITTED record currently in a topic.
//
// A read_committed consumer assigned to every partition from the beginning,
// reading until each partition reports EOF at its last stable offset. That
// makes it the external observer the exactly-once suites need: records inside
// an open or aborted transaction are invisible to it, exactly as they are to
// any correctly configured downstream consumer, and an ABANDONED transaction
// shows up as missing tail records rather than as phantom output. Also handy
// as a debugging probe against a transactional sink.
//
// No librdkafka types in the interface, so callers need only link
// clink::kafka.

#include <chrono>
#include <string>
#include <vector>

namespace clink::kafka {

// Payloads of every committed record in `topic`, in partition order then
// offset order. Throws std::runtime_error on connection/consume errors or if
// the drain does not reach EOF on every partition within `timeout` (an open
// transaction pinning the last stable offset looks like exactly that).
std::vector<std::string> consume_all_committed(
    const std::string& brokers,
    const std::string& topic,
    std::chrono::milliseconds timeout = std::chrono::seconds{30});

// Same drain with an explicit isolation level ("read_committed" or
// "read_uncommitted"). The uncommitted view includes records of open and
// aborted transactions - the difference between the two views is the
// diagnostic when committed output goes missing: records absent from BOTH
// were never produced; records visible only uncommitted sit in a
// transaction that never committed.
std::vector<std::string> consume_all(const std::string& brokers,
                                     const std::string& topic,
                                     const std::string& isolation,
                                     std::chrono::milliseconds timeout = std::chrono::seconds{30});

}  // namespace clink::kafka
