#include "native/error_class.hpp"

#include <algorithm>
#include <array>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>

#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>

#include "native/errors.hpp"

namespace clink::clickhouse::native {

static_assert(static_cast<std::size_t>(FailureClass::Unclassified) + 1 == kFailureClasses,
              "kFailureClasses must count every FailureClass");

namespace {

namespace ch = ::clickhouse;

// Newer than the pinned client's error_codes.h, so it has no name there.
constexpr int kServerOverloaded = 745;

// Retried by phase: nothing can have been written when the failure came before
// any block was sent, and the INSERT is in doubt when it came after.
constexpr std::array<int, 12> kRetriedByPhase = {
    ch::TOO_MANY_SIMULTANEOUS_QUERIES,
    ch::TABLE_IS_READ_ONLY,
    ch::NO_ZOOKEEPER,
    kServerOverloaded,
    ch::TIMEOUT_EXCEEDED,
    // What an unreachable shard most likely returns under foreground
    // Distributed inserts.
    ch::ALL_CONNECTION_TRIES_FAILED,
    ch::ABORTED,
    ch::UNKNOWN_STATUS_OF_INSERT,
    ch::SOCKET_TIMEOUT,
    ch::NETWORK_ERROR,
    ch::QUERY_WAS_CANCELLED,
    ch::KEEPER_EXCEPTION,
};

// Under quorum inserts these pass once enough replicas are back, so they are
// retried, but by phase like the codes above. Nothing guarantees the INSERT
// wrote nothing: some server lines check the quorum again on every chunk the
// server squashes the INSERT's blocks into, by which time the parts of
// earlier chunks may be committed, and on every line the commit of a later
// part can fail on the quorum once an earlier part is in the table.
constexpr std::array<int, 3> kQuorumRetried = {
    ch::TOO_FEW_LIVE_REPLICAS,
    ch::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,
    ch::READONLY,
};

// A statement or a value the server will never take, or a table, column,
// database or setting that is not there. Retrying them for the whole window
// would only delay the same failure.
constexpr std::array<int, 14> kPermanentInsert = {
    ch::CANNOT_PARSE_TEXT,
    ch::NOT_FOUND_COLUMN_IN_BLOCK,
    ch::NO_SUCH_COLUMN_IN_TABLE,
    ch::BAD_ARGUMENTS,
    ch::ILLEGAL_COLUMN,
    ch::UNKNOWN_IDENTIFIER,
    ch::NOT_IMPLEMENTED,
    ch::TYPE_MISMATCH,
    ch::UNKNOWN_TABLE,
    ch::SYNTAX_ERROR,
    ch::CANNOT_CONVERT_TYPE,
    ch::UNKNOWN_DATABASE,
    ch::UNKNOWN_SETTING,
    ch::INCORRECT_DATA,
};

// Credentials and grants: permanent, and reported as an access problem.
constexpr std::array<int, 4> kPermanentAccess = {
    ch::UNKNOWN_USER,
    ch::DATABASE_ACCESS_DENIED,
    ch::ACCESS_DENIED,
    ch::AUTHENTICATION_FAILED,
};

template <std::size_t N>
bool contains(const std::array<int, N>& codes, int code) {
    return std::find(codes.begin(), codes.end(), code) != codes.end();
}

constexpr std::string_view kVerifyFailedPrefix = "Failed to verify SSL connection";
constexpr std::string_view kTooManyPartitionsPrefix = "Too many partitions for single INSERT block";

// The server's own message, once the wrappers in front of it are gone. The
// display text leads with the exception's name, and an error a Distributed
// table forwards from a shard leads with "Received from <address>. " and then
// the shard's display text.
std::string_view server_message_core(std::string_view text) {
    constexpr std::string_view kName = "DB::Exception: ";
    constexpr std::string_view kForwarded = "Received from ";
    for (;;) {
        if (text.starts_with(kName)) {
            text.remove_prefix(kName.size());
            continue;
        }
        if (text.starts_with(kForwarded)) {
            const auto end = text.find(". ");
            if (end == std::string_view::npos) {
                return text;
            }
            text.remove_prefix(end + 2);
            continue;
        }
        return text;
    }
}

// A NativeSinkError owns its code, and the Failure and the Decision built from
// it outlive the exception, so the code is copied into storage that lives for
// the process. There are a few dozen codes at most. Deliberately leaked, so a
// detached writer that fails during exit never reads a destroyed set.
const char* intern_code(const std::string& code) {
    static auto* const mu = new std::mutex;
    static auto* const codes = new std::set<std::string, std::less<>>;
    const std::lock_guard<std::mutex> lock(*mu);
    return codes->insert(code).first->c_str();
}

// In doubt once anything of the attempt may have reached the server.
bool past_send(const Failure& f) {
    return f.after_send || f.phase == Phase::Send || f.phase == Phase::End;
}

FailureClass by_phase(const Failure& f) {
    return past_send(f) ? FailureClass::InDoubt : FailureClass::TransientNotWritten;
}

FailureClass classify_server(const Failure& f, bool quorum_inserts) {
    const int code = f.code;
    if (code == ch::TOO_MANY_PARTS) {
        // Matched by prefix, not by substring: the text for too many parts in
        // all partitions in total also contains "partitions". That one, like
        // too many parts in a partition, is merge back-pressure, because
        // merges can bring the count down. Only the per-INSERT partition
        // limit cannot pass with time.
        return server_message_core(f.message).starts_with(kTooManyPartitionsPrefix)
                   ? FailureClass::Permanent
                   : FailureClass::MergeBackpressure;
    }
    if (code == ch::TOO_MANY_PARTITIONS) {
        return FailureClass::Permanent;
    }
    if (code == ch::MEMORY_LIMIT_EXCEEDED) {
        return FailureClass::Resource;
    }
    if (quorum_inserts && contains(kQuorumRetried, code)) {
        return by_phase(f);
    }
    if (code == ch::READONLY || contains(kPermanentInsert, code) ||
        contains(kPermanentAccess, code)) {
        return FailureClass::Permanent;
    }
    if (contains(kRetriedByPhase, code)) {
        return by_phase(f);
    }
    return FailureClass::Unclassified;
}

const char* permanent_server_code(int code) {
    if (code == ch::TOO_MANY_PARTS || code == ch::TOO_MANY_PARTITIONS) {
        return code::kTooManyPartitions;
    }
    if (code == ch::READONLY || contains(kPermanentAccess, code)) {
        return code::kAccessDenied;
    }
    return code::kInsertFailed;
}

const char* permanent_code(const Failure& f) {
    switch (f.signal) {
        case Signal::TlsVerify:
            return code::kTlsVerifyFailed;
        case Signal::Unimplemented:
        case Signal::HeaderDrift:
            return code::kHeaderDrift;
        case Signal::Conversion:
            return code::kConversionFailed;
        case Signal::Server:
            return permanent_server_code(f.code);
        case Signal::Other:
            return f.sink_code != nullptr ? f.sink_code : code::kInsertFailed;
        case Signal::Transport:
        case Signal::Tls:
        case Signal::Protocol:
        case Signal::Validation:
        case Signal::BadOptional:
            break;
    }
    return code::kInsertFailed;
}

constexpr Decision kRetry{Action::Retry, nullptr};

constexpr Decision fail(const char* fail_code) {
    return Decision{Action::Fail, fail_code};
}

}  // namespace

Failure to_failure(std::exception_ptr e, Phase phase, bool after_send) {
    Failure f;
    f.phase = phase;
    f.after_send = after_send;
    if (!e) {
        f.message = "no exception in flight";
        return f;
    }
    // Our own errors first, then the client's types, then the standard ones
    // they would otherwise be caught as. A ConversionError is told apart by
    // its code.
    try {
        std::rethrow_exception(e);
    } catch (const NativeSinkError& x) {
        if (x.code() == code::kHeaderDrift) {
            f.signal = Signal::HeaderDrift;
        } else if (x.code() == code::kConversionFailed) {
            f.signal = Signal::Conversion;
        } else {
            f.signal = Signal::Other;
        }
        f.message = x.what();
        f.sink_code = intern_code(x.code());
    } catch (const ch::ServerException& x) {
        f.signal = Signal::Server;
        f.code = x.GetCode();
        f.message = x.what();
    } catch (const ch::OpenSSLError& x) {
        f.message = x.what();
        f.signal = std::string_view(f.message).starts_with(kVerifyFailedPrefix) ? Signal::TlsVerify
                                                                                : Signal::Tls;
    } catch (const ch::ProtocolError& x) {
        f.signal = Signal::Protocol;
        f.message = x.what();
    } catch (const ch::CompressionError& x) {
        // A frame that does not decompress is a broken stream, like any other
        // undecodable packet.
        f.signal = Signal::Protocol;
        f.message = x.what();
    } catch (const ch::UnimplementedError& x) {
        f.signal = Signal::Unimplemented;
        f.message = x.what();
    } catch (const ch::ValidationError& x) {
        // The client refusing a call in its current state: a defect in how it
        // was driven, which a fresh client clears.
        f.signal = Signal::Validation;
        f.message = std::string("client state error: ") + x.what();
    } catch (const std::system_error& x) {
        f.signal = Signal::Transport;
        f.message = x.what();
    } catch (const std::bad_optional_access& x) {
        f.signal = Signal::BadOptional;
        f.message = x.what();
    } catch (const std::exception& x) {
        // AssertionError, bad_alloc and anything else: nothing a retry fixes.
        f.signal = Signal::Other;
        f.message = x.what();
    } catch (...) {
        f.signal = Signal::Other;
        f.message = "unknown exception";
    }
    return f;
}

FailureClass classify(const Failure& f, bool quorum_inserts) {
    switch (f.signal) {
        case Signal::Server:
            return classify_server(f, quorum_inserts);
        case Signal::Transport:
        case Signal::Tls:
        case Signal::Protocol:
            return by_phase(f);
        case Signal::Unimplemented:
            // At Begin it is the header naming a column type the client cannot
            // build, which no retry changes. Elsewhere it is a packet the
            // client does not know, which a new connection need not repeat.
            return f.phase == Phase::Begin ? FailureClass::Permanent : by_phase(f);
        case Signal::Validation:
        case Signal::BadOptional:
            return FailureClass::ClientDefect;
        case Signal::TlsVerify:
        case Signal::Conversion:
        case Signal::HeaderDrift:
        case Signal::Other:
            return FailureClass::Permanent;
    }
    return FailureClass::Permanent;
}

const char* to_string(FailureClass c) {
    switch (c) {
        case FailureClass::TransientNotWritten:
            return "transient";
        case FailureClass::InDoubt:
            return "in_doubt";
        case FailureClass::MergeBackpressure:
            return "merge_backpressure";
        case FailureClass::Resource:
            return "resource";
        case FailureClass::Permanent:
            return "permanent";
        case FailureClass::ClientDefect:
            return "client_defect";
        case FailureClass::Unclassified:
            return "unclassified";
    }
    return "unknown";
}

Decision action_for(FailureClass c, const Failure& f, const AttemptState& s) {
    switch (c) {
        case FailureClass::TransientNotWritten:
        case FailureClass::InDoubt:
        case FailureClass::MergeBackpressure:
            return kRetry;
        case FailureClass::Resource:
            // Halving a large INSERT eases the server's memory. Below the
            // threshold further halving gains nothing against server-wide
            // pressure, so the INSERT waits it out instead.
            return s.rows > kSplitMinRows ? Decision{Action::SplitHalves, nullptr} : kRetry;
        case FailureClass::Unclassified:
            // Bounds the cost of a permanent code nobody listed, without
            // failing on a transient one at its first appearance.
            return s.unclassified_attempts < kUnclassifiedRetries ? kRetry
                                                                  : fail(code::kInsertFailed);
        case FailureClass::ClientDefect:
            if (f.signal == Signal::Validation) {
                return s.validation_attempts < kValidationRetries ? kRetry
                                                                  : fail(code::kInsertFailed);
            }
            return kRetry;
        case FailureClass::Permanent:
            return fail(permanent_code(f));
    }
    return fail(code::kInsertFailed);
}

}  // namespace clink::clickhouse::native
