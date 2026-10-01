#include <cerrno>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>
#include <clickhouse/server_exception.h>
#include <clickhouse/version.h>
#include <gtest/gtest.h>

#include "native/error_class.hpp"
#include "native/errors.hpp"

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
#include "native/vendor/clickhouse/base/sslsocket.h"
static_assert(CLICKHOUSE_CPP_VERSION_MAJOR == 2 && CLICKHOUSE_CPP_VERSION_MINOR == 6 &&
                  CLICKHOUSE_CPP_VERSION_PATCH == 2,
              "the vendored sslsocket.h is the 2.6.2 copy");
#endif

namespace clink::clickhouse::native {
namespace {

constexpr Phase kAllPhases[] = {
    Phase::Connect, Phase::Metadata, Phase::Begin, Phase::Send, Phase::End};
constexpr Phase kBeforeSendPhases[] = {Phase::Connect, Phase::Metadata, Phase::Begin};
constexpr Phase kAfterSendPhases[] = {Phase::Send, Phase::End};

// The real 26.8 texts, as the server formats them.
const std::string kEcTooManyPartitionsText =
    "Too many partitions for single INSERT block (more than 100). The limit is controlled by "
    "'max_partitions_per_insert_block' setting. Large number of partitions is a common "
    "misconception. It will lead to severe negative performance impact, including slow server "
    "startup, slow INSERT queries and slow SELECT queries. Recommended total number of "
    "partitions for a table is under 1000..10000. Please note, that partitioning is not "
    "intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries "
    "fast). Partitions are intended for data manipulation (DROP PARTITION, etc).";
const std::string kEcPartsInTotalText =
    "Too many parts (100000) in all partitions in total in table 'default.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. This indicates wrong choice of partition key. "
    "The threshold can be modified with 'max_parts_in_total' setting in <merge_tree> element "
    "in config.xml or with per-table setting.";
const std::string kEcPartsInPartitionText =
    "Too many parts (3000 with average size of 1.20 MiB) in table 'default.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. Merges are processing significantly slower "
    "than inserts";
const std::string kEcInactivePartsText =
    "Too many inactive parts (1000) in table 'default.events "
    "(5d7c1f6e-2a7b-4c3d-9e8f-0a1b2c3d4e5f)'. Parts cleaning are processing significantly "
    "slower than inserts";

// A server error as the client raises it: the display text leads with the
// exception's name.
::clickhouse::ServerException ec_server_error(int code, const std::string& message) {
    return ::clickhouse::ServerException(std::make_shared<::clickhouse::Exception>(
        ::clickhouse::Exception{code, "DB::Exception", "DB::Exception: " + message, ""}));
}

// Throws `ex` and maps it from inside the catch block, as the sink does.
template <typename Ex>
Failure ec_caught(const Ex& ex, Phase phase, bool after_send = false) {
    try {
        throw ex;
    } catch (...) {
        return to_failure(std::current_exception(), phase, after_send);
    }
}

Failure ec_server(int code, Phase phase, bool after_send = false, const std::string& text = "x") {
    return ec_caught(ec_server_error(code, text), phase, after_send);
}

// Send always follows a started send_block; End follows the INSERT's blocks.
bool ec_sent(Phase phase) {
    return phase == Phase::Send || phase == Phase::End;
}

FailureClass ec_class(const Failure& f, bool quorum = false) {
    return classify(f, quorum);
}

Decision ec_decide(const Failure& f, bool quorum = false, AttemptState s = {}) {
    return action_for(classify(f, quorum), f, s);
}

void ec_expect_fails_with(const Failure& f, const char* code, bool quorum = false) {
    EXPECT_EQ(classify(f, quorum), FailureClass::Permanent) << f.message;
    const Decision d = ec_decide(f, quorum);
    EXPECT_EQ(d.action, Action::Fail) << f.message;
    ASSERT_NE(d.fail_code, nullptr) << f.message;
    EXPECT_STREQ(d.fail_code, code) << f.message;
}

// --- to_failure ---------------------------------------------------------------

TEST(NativeErrorClass, AServerErrorKeepsItsCodeAndDisplayText) {
    const Failure f = ec_server(202, Phase::Begin, false, "Too many simultaneous queries");
    EXPECT_EQ(f.signal, Signal::Server);
    EXPECT_EQ(f.phase, Phase::Begin);
    EXPECT_EQ(f.code, 202);
    EXPECT_EQ(f.message, "DB::Exception: Too many simultaneous queries");
    EXPECT_FALSE(f.after_send);
    EXPECT_EQ(f.sink_code, nullptr);
}

TEST(NativeErrorClass, ThePhaseAndTheSendFlagAreTheCallersOwn) {
    const Failure f = ec_caught(
        std::system_error(ECONNRESET, std::generic_category(), "closed"), Phase::Send, true);
    EXPECT_EQ(f.signal, Signal::Transport);
    EXPECT_EQ(f.phase, Phase::Send);
    EXPECT_TRUE(f.after_send);
    EXPECT_NE(f.message.find("closed"), std::string::npos) << f.message;
}

TEST(NativeErrorClass, NoExceptionInFlightIsPermanent) {
    const Failure f = to_failure(nullptr, Phase::End, true);
    EXPECT_EQ(f.signal, Signal::Other);
    EXPECT_EQ(f.message, "no exception in flight");
    ec_expect_fails_with(f, code::kInsertFailed);
}

// --- std::system_error --------------------------------------------------------

TEST(NativeErrorClass, ASystemErrorBeforeSendIsTransientNotWritten) {
    const std::vector<std::system_error> errors = {
        std::system_error(ECONNREFUSED, std::generic_category(), "fail to connect"),
        std::system_error(ETIMEDOUT, std::generic_category(), "fail to connect"),
        std::system_error(ECONNRESET, std::generic_category(), "closed"),
        std::system_error(ECONNABORTED, std::generic_category()),
    };
    for (const auto& e : errors) {
        for (const Phase p : kBeforeSendPhases) {
            const Failure f = ec_caught(e, p);
            EXPECT_EQ(f.signal, Signal::Transport);
            EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten) << e.what();
            const Decision d = ec_decide(f);
            EXPECT_EQ(d.action, Action::Retry);
            EXPECT_EQ(d.fail_code, nullptr);
        }
    }
}

TEST(NativeErrorClass, ASystemErrorAtSendOrEndIsInDoubt) {
    for (const Phase p : kAfterSendPhases) {
        // The attempt deadline surfaces as ETIMEDOUT from the wrapper streams.
        const Failure timed_out =
            ec_caught(std::system_error(ETIMEDOUT, std::generic_category()), p, true);
        EXPECT_EQ(ec_class(timed_out), FailureClass::InDoubt);
        EXPECT_EQ(ec_decide(timed_out).action, Action::Retry);

        const Failure closed =
            ec_caught(std::system_error(ECONNRESET, std::generic_category(), "closed"), p, true);
        EXPECT_EQ(ec_class(closed), FailureClass::InDoubt);
    }
}

TEST(NativeErrorClass, PhaseAloneMakesSendAndEndInDoubt) {
    // Even when the caller did not mark a send, nothing past Begin is treated
    // as unwritten.
    for (const Phase p : kAfterSendPhases) {
        const Failure f = ec_caught(std::system_error(EPIPE, std::generic_category()), p, false);
        EXPECT_EQ(ec_class(f), FailureClass::InDoubt);
    }
}

// --- OpenSSLError -------------------------------------------------------------

TEST(NativeErrorClass, ACertificateVerifyFailureIsPermanent) {
    const ::clickhouse::OpenSSLError e(
        "Failed to verify SSL connection, X509_v error: 20 unable to get local issuer "
        "certificate\nServer certificate: subject=CN=clickhouse");
    const Failure f = ec_caught(e, Phase::Connect);
    EXPECT_EQ(f.signal, Signal::TlsVerify);
    ec_expect_fails_with(f, code::kTlsVerifyFailed);
}

TEST(NativeErrorClass, AVerifyTextLaterInTheMessageIsNotAVerifyFailure) {
    // Only the client's own prefix counts: a certificate subject quoted in
    // another error must not turn a handshake failure into a permanent one.
    const ::clickhouse::OpenSSLError e(
        "OpenSSL error: 1 : sslv3 alert handshake failure\nServer certificate: "
        "subject=CN=Failed to verify SSL connection");
    const Failure f = ec_caught(e, Phase::Connect);
    EXPECT_EQ(f.signal, Signal::Tls);
    EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten);
}

TEST(NativeErrorClass, AnyOtherTlsErrorIsRetriedByPhase) {
    const ::clickhouse::OpenSSLError e("OpenSSL error: 1 : sslv3 alert handshake failure");
    for (const Phase p : kBeforeSendPhases) {
        const Failure f = ec_caught(e, p);
        EXPECT_EQ(f.signal, Signal::Tls);
        EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
    for (const Phase p : kAfterSendPhases) {
        const Failure f = ec_caught(e, p, true);
        EXPECT_EQ(ec_class(f), FailureClass::InDoubt);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
}

TEST(NativeErrorClass, ACaTheTransportCouldNotLoadStaysAnOptionError) {
    // The transport refuses a socket factory it cannot build, naming the key;
    // the failure keeps that code wherever it is classified.
    const Failure f = ec_caught(
        NativeSinkError(code::kOptionInvalid,
                        "tls_ca_file `/etc/clink/ca.pem` could not be loaded: OpenSSL error: "
                        "-2147483646 : Unknown SSL error"),
        Phase::Connect);
    EXPECT_EQ(f.signal, Signal::Other);
    ASSERT_NE(f.sink_code, nullptr);
    EXPECT_STREQ(f.sink_code, "clickhouse.option_invalid");
    ec_expect_fails_with(f, code::kOptionInvalid);
}

#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
TEST(NativeErrorClass, TheRealFactoryConstructorFailureNeedsTheTransportsRefusal) {
    ::clickhouse::ClientOptions opts;
    opts.SetSSLOptions(::clickhouse::ClientOptions::SSLOptions()
                           .SetPathToCAFiles({"/nonexistent/clink-native-test/ca.pem"})
                           .SetUseDefaultCALocations(false));
    std::optional<::clickhouse::OpenSSLError> thrown;
    try {
        const ::clickhouse::SSLSocketFactory factory(opts);
    } catch (const ::clickhouse::OpenSSLError& e) {
        thrown.emplace(e);
    }
    ASSERT_TRUE(thrown.has_value()) << "the CA load did not fail";

    // Bare, the factory's error looks like any TLS failure on a connection,
    // which is retried: neither its type nor its phase tells them apart. So
    // the transport must turn it into a refusal before it reaches here.
    const Failure bare = ec_caught(*thrown, Phase::Connect);
    EXPECT_EQ(bare.signal, Signal::Tls);
    EXPECT_EQ(ec_class(bare), FailureClass::TransientNotWritten);

    const Failure refused = ec_caught(
        NativeSinkError(code::kOptionInvalid,
                        std::string("tls_ca_file could not be loaded: ") + thrown->what()),
        Phase::Connect);
    EXPECT_NE(refused.message.find(thrown->what()), std::string::npos) << refused.message;
    ec_expect_fails_with(refused, code::kOptionInvalid);
}
#endif

// --- ProtocolError and CompressionError ---------------------------------------

TEST(NativeErrorClass, AProtocolErrorBeforeSendIsTransient) {
    // The handshake's failure, at Connect.
    const Failure handshake =
        ec_caught(::clickhouse::ProtocolError("fail to connect to clickhouse-1"), Phase::Connect);
    EXPECT_EQ(handshake.signal, Signal::Protocol);
    EXPECT_EQ(ec_class(handshake), FailureClass::TransientNotWritten);

    const ::clickhouse::ProtocolError e("fail to receive data packet");
    for (const Phase p : {Phase::Metadata, Phase::Begin}) {
        const Failure f = ec_caught(e, p);
        EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
}

TEST(NativeErrorClass, AProtocolErrorAtSendOrEndIsInDoubt) {
    const ::clickhouse::ProtocolError e(
        "unexpected packet from server while receiving end of query, expected (expected "
        "Exception, EndOfStream or Log, got: 1)");
    const Failure at_send = ec_caught(e, Phase::Send, true);
    EXPECT_EQ(at_send.signal, Signal::Protocol);
    EXPECT_EQ(ec_class(at_send), FailureClass::InDoubt);
    const Failure at_end = ec_caught(e, Phase::End, true);
    EXPECT_EQ(ec_class(at_end), FailureClass::InDoubt);
}

TEST(NativeErrorClass, ACompressionErrorIsAProtocolFailure) {
    const ::clickhouse::CompressionError e("data was corrupted");
    const Failure before = ec_caught(e, Phase::Metadata);
    EXPECT_EQ(before.signal, Signal::Protocol);
    EXPECT_EQ(before.message, "data was corrupted");
    EXPECT_EQ(ec_class(before), FailureClass::TransientNotWritten);
    const Failure after = ec_caught(e, Phase::End, true);
    EXPECT_EQ(ec_class(after), FailureClass::InDoubt);
}

// --- UnimplementedError ------------------------------------------------------

TEST(NativeErrorClass, AnUnimplementedErrorAtBeginIsHeaderDrift) {
    const Failure f = ec_caught(
        ::clickhouse::UnimplementedError("unsupported column type: Dynamic"), Phase::Begin);
    EXPECT_EQ(f.signal, Signal::Unimplemented);
    ec_expect_fails_with(f, code::kHeaderDrift);
}

TEST(NativeErrorClass, AnUnknownPacketIsRetriedByPhase) {
    const ::clickhouse::UnimplementedError e("unimplemented 14");
    for (const Phase p : {Phase::Connect, Phase::Metadata}) {
        const Failure f = ec_caught(e, p);
        EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
    for (const Phase p : kAfterSendPhases) {
        const Failure f = ec_caught(e, p, true);
        EXPECT_EQ(ec_class(f), FailureClass::InDoubt);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
}

// --- server codes ------------------------------------------------------------

TEST(NativeErrorClass, TransientServerCodesAreRetriedByPhase) {
    for (const int code : {202, 242, 225, 745, 159, 279, 236, 319, 209, 210, 394, 999}) {
        for (const Phase p : kBeforeSendPhases) {
            const Failure f = ec_server(code, p);
            EXPECT_EQ(ec_class(f), FailureClass::TransientNotWritten) << code;
            EXPECT_EQ(ec_decide(f).action, Action::Retry) << code;
        }
        for (const Phase p : kAfterSendPhases) {
            const Failure f = ec_server(code, p, true);
            EXPECT_EQ(ec_class(f), FailureClass::InDoubt) << code;
            EXPECT_EQ(ec_decide(f).action, Action::Retry) << code;
        }
    }
}

TEST(NativeErrorClass, AnUnreachableShardAndAnAbortBeforeAndAfterSend) {
    for (const int code : {279, 236}) {
        EXPECT_EQ(ec_class(ec_server(code, Phase::Metadata)), FailureClass::TransientNotWritten)
            << code;
        EXPECT_EQ(ec_class(ec_server(code, Phase::Begin)), FailureClass::TransientNotWritten)
            << code;
        EXPECT_EQ(ec_class(ec_server(code, Phase::Send, true)), FailureClass::InDoubt) << code;
        EXPECT_EQ(ec_class(ec_server(code, Phase::End, true)), FailureClass::InDoubt) << code;
    }
}

TEST(NativeErrorClass, QuorumFailuresAreTransientUnderQuorumInserts) {
    for (const int code : {285, 286, 164}) {
        for (const Phase p : kAllPhases) {
            const Failure f = ec_server(code, p, ec_sent(p));
            EXPECT_EQ(classify(f, true), FailureClass::TransientNotWritten) << code;
            EXPECT_EQ(ec_decide(f, true).action, Action::Retry) << code;
        }
    }
}

TEST(NativeErrorClass, ReadOnlyWithoutQuorumIsAnAccessFailure) {
    for (const Phase p : kAllPhases) {
        ec_expect_fails_with(ec_server(164, p, ec_sent(p)), code::kAccessDenied, false);
    }
}

TEST(NativeErrorClass, QuorumCodesWithoutQuorumAreUnclassified) {
    for (const int code : {285, 286}) {
        EXPECT_EQ(classify(ec_server(code, Phase::End, true), false), FailureClass::Unclassified)
            << code;
    }
}

TEST(NativeErrorClass, TooManyPartitionsForOneInsertIsPermanent) {
    for (const Phase p : kAllPhases) {
        const Failure f = ec_server(252, p, ec_sent(p), kEcTooManyPartitionsText);
        ec_expect_fails_with(f, code::kTooManyPartitions);
    }
}

TEST(NativeErrorClass, TooManyPartsInAllPartitionsIsMergeBackpressure) {
    // The text contains "partitions", which is why the rule is a prefix.
    ASSERT_NE(kEcPartsInTotalText.find("partitions"), std::string::npos);
    for (const Phase p : kAllPhases) {
        const Failure f = ec_server(252, p, ec_sent(p), kEcPartsInTotalText);
        EXPECT_EQ(ec_class(f), FailureClass::MergeBackpressure);
        EXPECT_EQ(ec_decide(f).action, Action::Retry);
    }
}

TEST(NativeErrorClass, TooManyPartsInAPartitionIsMergeBackpressure) {
    for (const auto& text : {kEcPartsInPartitionText, kEcInactivePartsText, std::string("")}) {
        const Failure f = ec_server(252, Phase::End, true, text);
        EXPECT_EQ(ec_class(f), FailureClass::MergeBackpressure) << text;
        EXPECT_EQ(ec_decide(f).action, Action::Retry) << text;
    }
}

TEST(NativeErrorClass, ThePartitionsPrefixIsMatchedWithoutTheExceptionName) {
    // A bare message, and the text a Distributed table forwards from a shard.
    const Failure bare =
        ec_caught(::clickhouse::ServerException(std::make_shared<::clickhouse::Exception>(
                      ::clickhouse::Exception{252, "DB::Exception", kEcTooManyPartitionsText, ""})),
                  Phase::End,
                  true);
    EXPECT_EQ(ec_class(bare), FailureClass::Permanent);

    const Failure forwarded =
        ec_server(252,
                  Phase::End,
                  true,
                  "Received from shard-2:9000. DB::Exception: " + kEcTooManyPartitionsText);
    ec_expect_fails_with(forwarded, code::kTooManyPartitions);

    const Failure forwarded_parts = ec_server(
        252, Phase::End, true, "Received from shard-2:9000. DB::Exception: " + kEcPartsInTotalText);
    EXPECT_EQ(ec_class(forwarded_parts), FailureClass::MergeBackpressure);
}

TEST(NativeErrorClass, ThePartitionsTextInsideAnotherMessageIsNotTheRule) {
    const Failure f = ec_server(
        252, Phase::End, true, "Cannot insert: Too many partitions for single INSERT block");
    EXPECT_EQ(ec_class(f), FailureClass::MergeBackpressure);
}

TEST(NativeErrorClass, TooManyPartitionsCodeIsPermanent) {
    ec_expect_fails_with(ec_server(565, Phase::End, true), code::kTooManyPartitions);
}

TEST(NativeErrorClass, MemoryLimitIsAResourceFailure) {
    for (const Phase p : kAllPhases) {
        EXPECT_EQ(ec_class(ec_server(241, p, ec_sent(p))), FailureClass::Resource);
    }
}

TEST(NativeErrorClass, PermanentServerCodesFailTheInsert) {
    for (const int code : {6, 16, 36, 44, 48, 53, 62, 70, 117, 10, 47, 60, 81, 115}) {
        for (const Phase p : kAllPhases) {
            ec_expect_fails_with(ec_server(code, p, ec_sent(p)), code::kInsertFailed);
        }
    }
}

TEST(NativeErrorClass, CredentialAndGrantCodesFailAsAccessDenied) {
    for (const int code : {192, 291, 497, 516}) {
        for (const Phase p : kAllPhases) {
            ec_expect_fails_with(ec_server(code, p, ec_sent(p)), code::kAccessDenied);
        }
        // Quorum makes no difference to them.
        ec_expect_fails_with(ec_server(code, Phase::End, true), code::kAccessDenied, true);
    }
}

TEST(NativeErrorClass, AnyOtherServerCodeIsUnclassified) {
    for (const int code : {0, 1, 32, 1000, 99999}) {
        for (const Phase p : kAllPhases) {
            EXPECT_EQ(ec_class(ec_server(code, p, ec_sent(p))), FailureClass::Unclassified) << code;
        }
    }
}

// --- client defects and our own errors -----------------------------------------

TEST(NativeErrorClass, BadOptionalAccessIsAClientDefect) {
    for (const Phase p : kAllPhases) {
        const Failure f = ec_caught(std::bad_optional_access(), p, ec_sent(p));
        EXPECT_EQ(f.signal, Signal::BadOptional);
        EXPECT_EQ(ec_class(f), FailureClass::ClientDefect);
    }
}

TEST(NativeErrorClass, AValidationErrorIsAClientStateError) {
    for (const Phase p : kAllPhases) {
        const Failure f = ec_caught(
            ::clickhouse::ValidationError("cannot execute query while executing another operation"),
            p,
            ec_sent(p));
        EXPECT_EQ(f.signal, Signal::Validation);
        EXPECT_EQ(f.message,
                  "client state error: cannot execute query while executing another operation");
        EXPECT_EQ(ec_class(f), FailureClass::ClientDefect);
    }
}

TEST(NativeErrorClass, AConversionErrorIsPermanent) {
    const Failure f =
        ec_caught(ConversionError("email", 3, "null into non-Nullable String"), Phase::Send, true);
    EXPECT_EQ(f.signal, Signal::Conversion);
    ASSERT_NE(f.sink_code, nullptr);
    EXPECT_STREQ(f.sink_code, "clickhouse.conversion_failed");
    EXPECT_NE(f.message.find("column `email`, row 3"), std::string::npos) << f.message;
    ec_expect_fails_with(f, code::kConversionFailed);
}

TEST(NativeErrorClass, HeaderDriftFromThePlanCheckIsPermanent) {
    const Failure f = ec_caught(
        NativeSinkError(code::kHeaderDrift, "column `b`: plan Int64, server Nullable(Int64)"),
        Phase::Begin);
    EXPECT_EQ(f.signal, Signal::HeaderDrift);
    ec_expect_fails_with(f, code::kHeaderDrift);
}

TEST(NativeErrorClass, OurOwnRefusalKeepsItsCode) {
    // A re-probe on a new client refusing the target must fail with that code,
    // not be renamed to a generic insert failure.
    const Failure f =
        ec_caught(NativeSinkError(code::kTargetAsyncInsert, "async_insert=1 on the server default"),
                  Phase::Metadata);
    EXPECT_EQ(f.signal, Signal::Other);
    ASSERT_NE(f.sink_code, nullptr);
    EXPECT_STREQ(f.sink_code, "clickhouse.target_async_insert");
    ec_expect_fails_with(f, code::kTargetAsyncInsert);
}

TEST(NativeErrorClass, AKeptCodeOutlivesTheException) {
    const char* kept = nullptr;
    {
        const std::string code = "clickhouse.server_settings_unsupported";
        kept = ec_caught(NativeSinkError(code, "missing a setting"), Phase::Metadata).sink_code;
    }
    ASSERT_NE(kept, nullptr);
    EXPECT_STREQ(kept, "clickhouse.server_settings_unsupported");
    // The same code maps to the same storage every time.
    EXPECT_EQ(ec_caught(NativeSinkError(code::kServerSettingsUnsupported, "again"), Phase::Begin)
                  .sink_code,
              kept);
}

TEST(NativeErrorClass, AssertionsAllocationFailuresAndAnythingElseArePermanent) {
    const Failure assertion = ec_caught(
        ::clickhouse::AssertionError("Failed to write too big chunk at once"), Phase::Send, true);
    EXPECT_EQ(assertion.signal, Signal::Other);
    ec_expect_fails_with(assertion, code::kInsertFailed);

    ec_expect_fails_with(ec_caught(std::bad_alloc(), Phase::Send, true), code::kInsertFailed);
    ec_expect_fails_with(ec_caught(std::runtime_error("boom"), Phase::Begin), code::kInsertFailed);
    ec_expect_fails_with(ec_caught(std::logic_error("bad state"), Phase::Connect),
                         code::kInsertFailed);

    const Failure non_std = ec_caught(42, Phase::End, true);
    EXPECT_EQ(non_std.signal, Signal::Other);
    EXPECT_EQ(non_std.message, "unknown exception");
    ec_expect_fails_with(non_std, code::kInsertFailed);
}

TEST(NativeErrorClass, ClassLabelsAreTheMetricValues) {
    EXPECT_STREQ(to_string(FailureClass::TransientNotWritten), "transient");
    EXPECT_STREQ(to_string(FailureClass::InDoubt), "in_doubt");
    EXPECT_STREQ(to_string(FailureClass::MergeBackpressure), "merge_backpressure");
    EXPECT_STREQ(to_string(FailureClass::Resource), "resource");
    EXPECT_STREQ(to_string(FailureClass::Permanent), "permanent");
    EXPECT_STREQ(to_string(FailureClass::ClientDefect), "client_defect");
    EXPECT_STREQ(to_string(FailureClass::Unclassified), "unclassified");
    EXPECT_EQ(static_cast<std::size_t>(FailureClass::Unclassified) + 1, kFailureClasses);
}

// --- action_for, on its own ----------------------------------------------------

Failure ec_plain(Signal signal, Phase phase, int code = 0) {
    Failure f;
    f.signal = signal;
    f.phase = phase;
    f.code = code;
    f.after_send = ec_sent(phase);
    return f;
}

TEST(NativeActionFor, AMemoryLimitAboveAThousandRowsSplits) {
    const Failure f = ec_plain(Signal::Server, Phase::End, 241);
    AttemptState s;
    s.rows = 1001;
    const Decision split = action_for(FailureClass::Resource, f, s);
    EXPECT_EQ(split.action, Action::SplitHalves);
    EXPECT_EQ(split.fail_code, nullptr);

    s.rows = 4000;
    EXPECT_EQ(action_for(FailureClass::Resource, f, s).action, Action::SplitHalves);
}

TEST(NativeActionFor, AMemoryLimitAtAThousandRowsOrFewerRetries) {
    const Failure f = ec_plain(Signal::Server, Phase::End, 241);
    for (const std::uint64_t rows :
         {std::uint64_t{1000}, std::uint64_t{500}, std::uint64_t{1}, std::uint64_t{0}}) {
        AttemptState s;
        s.rows = rows;
        const Decision d = action_for(FailureClass::Resource, f, s);
        EXPECT_EQ(d.action, Action::Retry) << rows;
        EXPECT_EQ(d.fail_code, nullptr) << rows;
    }
}

TEST(NativeActionFor, UnclassifiedRetriesUntilThreeEarlierFailuresThenFails) {
    const Failure f = ec_plain(Signal::Server, Phase::End, 1000);
    for (const std::uint32_t earlier : {0U, 1U, 2U}) {
        AttemptState s;
        s.unclassified_attempts = earlier;
        EXPECT_EQ(action_for(FailureClass::Unclassified, f, s).action, Action::Retry) << earlier;
    }
    for (const std::uint32_t earlier : {3U, 4U, 100U}) {
        AttemptState s;
        s.unclassified_attempts = earlier;
        const Decision d = action_for(FailureClass::Unclassified, f, s);
        EXPECT_EQ(d.action, Action::Fail) << earlier;
        ASSERT_NE(d.fail_code, nullptr);
        EXPECT_STREQ(d.fail_code, code::kInsertFailed);
    }
}

TEST(NativeActionFor, AValidationErrorRetriesOnceThenFails) {
    const Failure f = ec_plain(Signal::Validation, Phase::Send);
    AttemptState first;
    EXPECT_EQ(action_for(FailureClass::ClientDefect, f, first).action, Action::Retry);

    AttemptState second;
    second.validation_attempts = 1;
    const Decision d = action_for(FailureClass::ClientDefect, f, second);
    EXPECT_EQ(d.action, Action::Fail);
    ASSERT_NE(d.fail_code, nullptr);
    EXPECT_STREQ(d.fail_code, code::kInsertFailed);
}

TEST(NativeActionFor, BadOptionalAccessAlwaysRetries) {
    const Failure f = ec_plain(Signal::BadOptional, Phase::Send);
    AttemptState s;
    s.validation_attempts = 5;
    s.unclassified_attempts = 5;
    EXPECT_EQ(action_for(FailureClass::ClientDefect, f, s).action, Action::Retry);
}

TEST(NativeActionFor, RetryableClassesIgnoreTheCaps) {
    AttemptState s;
    s.rows = 1'000'000;
    s.unclassified_attempts = 50;
    s.validation_attempts = 50;
    for (const FailureClass c : {FailureClass::TransientNotWritten,
                                 FailureClass::InDoubt,
                                 FailureClass::MergeBackpressure}) {
        const Decision d = action_for(c, ec_plain(Signal::Transport, Phase::End), s);
        EXPECT_EQ(d.action, Action::Retry) << to_string(c);
        EXPECT_EQ(d.fail_code, nullptr) << to_string(c);
    }
}

TEST(NativeActionFor, EveryPermanentRowFailsWithItsCode) {
    struct Row {
        Failure failure;
        const char* code;
    };
    Failure kept = ec_plain(Signal::Other, Phase::Metadata);
    kept.sink_code = code::kTargetEngineUnsupported;
    const std::vector<Row> rows = {
        {ec_plain(Signal::TlsVerify, Phase::Connect), code::kTlsVerifyFailed},
        {ec_plain(Signal::Unimplemented, Phase::Begin), code::kHeaderDrift},
        {ec_plain(Signal::HeaderDrift, Phase::Begin), code::kHeaderDrift},
        {ec_plain(Signal::Conversion, Phase::Send), code::kConversionFailed},
        {ec_plain(Signal::Server, Phase::End, 252), code::kTooManyPartitions},
        {ec_plain(Signal::Server, Phase::End, 565), code::kTooManyPartitions},
        {ec_plain(Signal::Server, Phase::Begin, 164), code::kAccessDenied},
        {ec_plain(Signal::Server, Phase::Connect, 192), code::kAccessDenied},
        {ec_plain(Signal::Server, Phase::Metadata, 291), code::kAccessDenied},
        {ec_plain(Signal::Server, Phase::Begin, 497), code::kAccessDenied},
        {ec_plain(Signal::Server, Phase::Connect, 516), code::kAccessDenied},
        {ec_plain(Signal::Server, Phase::Begin, 60), code::kInsertFailed},
        {ec_plain(Signal::Server, Phase::End, 6), code::kInsertFailed},
        {ec_plain(Signal::Server, Phase::Begin, 115), code::kInsertFailed},
        {ec_plain(Signal::Other, Phase::Send), code::kInsertFailed},
        {kept, code::kTargetEngineUnsupported},
    };
    for (const auto& row : rows) {
        const Decision d = action_for(FailureClass::Permanent, row.failure, AttemptState{});
        EXPECT_EQ(d.action, Action::Fail) << row.code;
        ASSERT_NE(d.fail_code, nullptr) << row.code;
        EXPECT_STREQ(d.fail_code, row.code);
    }
}

TEST(NativeActionFor, TheOpenLoopShapeRetriesAnOutageAndRefusesACredential) {
    // The opener passes no INSERT state at all.
    const Failure refused =
        ec_caught(std::system_error(ECONNREFUSED, std::generic_category(), "fail to connect"),
                  Phase::Connect);
    EXPECT_EQ(action_for(classify(refused, false), refused, AttemptState{}).action, Action::Retry);

    const Failure memory = ec_server(241, Phase::Metadata);
    EXPECT_EQ(action_for(classify(memory, false), memory, AttemptState{}).action, Action::Retry);

    const Failure auth = ec_server(516, Phase::Connect, false, "default: Authentication failed");
    const Decision d = action_for(classify(auth, false), auth, AttemptState{});
    EXPECT_EQ(d.action, Action::Fail);
    EXPECT_STREQ(d.fail_code, code::kAccessDenied);
}

}  // namespace
}  // namespace clink::clickhouse::native
