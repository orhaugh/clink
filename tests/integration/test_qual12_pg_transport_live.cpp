// QUAL-12's Postgres transport rows, against REAL servers.
//
// postgres_tls.hpp has existed and been wired into all three Postgres
// sinks, asserting the ESTABLISHED transport rather than the requested
// one - and nothing proved it. Its three declared outcomes
// (refusals.json: pg.asked_for_tls_and_did_not_get_it,
// pg.silent_downgrade_is_stated, pg.encrypted_is_recorded) are exactly
// the kind of security property that reads as obviously-correct in code
// review and can still be wrong: PQsslInUse could be misread, the
// sslmode parser could mis-tokenise, or a build without SSL support
// could make every connection unencrypted while the code path that
// notices never runs.
//
// So each row runs against a real server: one Postgres with TLS OFF (the
// downgrade cases) and one with a self-signed certificate and TLS ON
// (the encrypted case). Nothing here is hermetic; if Docker or openssl
// is unavailable the rows report UNEXERCISED, which the campaign's
// summariser treats as no evidence rather than as a pass.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <libpq-fe.h>
#include <string>

#include <gtest/gtest.h>

#include "clink/connectors/postgres_tls.hpp"
#include "clink/runtime/log_buffer.hpp"

#include "docker_postgres.hpp"

namespace {

bool docker_available() {
    return std::system("docker info > /dev/null 2>&1") == 0;
}

// A self-signed cert + key the server can read. Postgres refuses a key
// group/world-readable, and runs as uid 999 in the official image, so
// the files are made 0600 and owned by that uid inside the container's
// bind mount.
bool make_server_cert(const std::filesystem::path& dir) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const std::string cmd =
        "openssl req -new -x509 -days 1 -nodes -text "
        "-out " +
        (dir / "server.crt").string() + " -keyout " + (dir / "server.key").string() +
        " -subj '/CN=127.0.0.1' > /dev/null 2>&1";
    if (std::system(cmd.c_str()) != 0) {
        return false;
    }
    std::filesystem::permissions(
        dir / "server.key",
        std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
        ec);
    // The official image runs postgres as uid 999; the key must be
    // readable by it or the server refuses to start with ssl on.
    const std::string chown =
        "chmod 600 " + (dir / "server.key").string() + " && " + "docker run --rm -v " +
        dir.string() + ":/certs alpine:3 " +
        "sh -c 'chown 999:999 /certs/server.key /certs/server.crt' > /dev/null 2>&1";
    return std::system(chown.c_str()) == 0;
}

// Capture what the connector logged while asserting the transport.
std::string capture_assert(const std::string& conninfo, bool& threw) {
    // Read the ring the engine's own logging writes into, so the check is
    // on what an operator would actually see rather than on a return
    // value the connector never exposes.
    const auto before = clink::LogBuffer::global().tail(1000, "").size();
    threw = false;
    PGconn* conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        // libpq refused before clink saw the connection: for the
        // demands-encryption row that IS the refusal, and the caller
        // treats it as one.
        const std::string msg = PQerrorMessage(conn) != nullptr ? PQerrorMessage(conn) : "";
        PQfinish(conn);
        threw = true;
        return msg;
    }
    std::string logged;
    try {
        clink::connectors::pg::assert_no_silent_downgrade(conn, conninfo, "qual12.pg");
    } catch (const std::exception& e) {
        threw = true;
        logged = e.what();
    }
    PQfinish(conn);
    if (!threw) {
        const auto recs = clink::LogBuffer::global().tail(1000, "");
        for (std::size_t i = before; i < recs.size(); ++i) {
            logged += recs[i].level + " " + recs[i].source + " " + recs[i].message + "\n";
        }
    }
    return logged;
}

}  // namespace

// Row pg.asked_for_tls_and_did_not_get_it -> REFUSE.
//
// The subtlety that a first version of this test got wrong: pointing
// sslmode=require at a plaintext server proves nothing about clink,
// because LIBPQ refuses before clink ever sees a connection - the test
// passed with clink's own check disabled. The guard exists precisely for
// the case where libpq does NOT refuse (a future default, a build
// without SSL support), so the check must be given what it is designed
// to catch: an ESTABLISHED, unencrypted connection whose conninfo
// demands encryption. assert_no_silent_downgrade takes the conninfo
// separately from the connection, which is exactly what makes that
// testable - and what makes the invariant clink's own rather than a
// dependency's.
TEST(Qual12PgTransportLive, AnEstablishedUnencryptedConnectionThatDemandedTlsIsRefused) {
    if (!docker_available()) {
        GTEST_SKIP() << "docker unavailable: row pg.asked_for_tls_and_did_not_get_it UNEXERCISED";
    }
    clink::test::DockerPostgres pg;  // TLS off: the stock image serves plaintext only
    PGconn* conn = PQconnectdb(pg.conninfo().c_str());  // connects plaintext, no sslmode
    ASSERT_EQ(PQstatus(conn), CONNECTION_OK) << PQerrorMessage(conn);
    ASSERT_EQ(PQsslInUse(conn), 0) << "fixture is not plaintext; the row would prove nothing";
    bool threw = false;
    try {
        clink::connectors::pg::assert_no_silent_downgrade(
            conn, pg.conninfo() + " sslmode=require", "qual12.pg");
    } catch (const std::exception&) {
        threw = true;
    }
    PQfinish(conn);
    EXPECT_TRUE(threw) << "an unencrypted connection whose conninfo demanded TLS was accepted";
}

// And the same fact end to end, as an operator would hit it: libpq
// itself refuses too, so the misconfiguration never yields a usable
// plaintext connection by either route.
TEST(Qual12PgTransportLive, RequiringTlsFromAPlaintextServerNeverConnects) {
    if (!docker_available()) {
        GTEST_SKIP() << "docker unavailable: row pg.asked_for_tls_and_did_not_get_it UNEXERCISED";
    }
    clink::test::DockerPostgres pg;
    PGconn* conn = PQconnectdb((pg.conninfo() + " sslmode=require").c_str());
    const bool refused = PQstatus(conn) != CONNECTION_OK;
    PQfinish(conn);
    EXPECT_TRUE(refused) << "a plaintext server accepted an sslmode=require connection";
}

// Row pg.silent_downgrade_is_stated -> WARN (never silent, never refused).
TEST(Qual12PgTransportLive, AnUnrequestedPlaintextConnectionIsStatedNotSilent) {
    if (!docker_available()) {
        GTEST_SKIP() << "docker unavailable: row pg.silent_downgrade_is_stated UNEXERCISED";
    }
    clink::test::DockerPostgres pg;
    bool threw = false;
    // No sslmode at all: libpq's default is `prefer`, which tries TLS and
    // falls back to plaintext SILENTLY. That fallback is legitimate - and
    // must not be silent.
    const auto logged = capture_assert(pg.conninfo(), threw);
    EXPECT_FALSE(threw) << "plaintext with no sslmode requested must not be refused: " << logged;
    EXPECT_NE(logged.find("sslmode"), std::string::npos)
        << "the downgrade was not stated, or did not name the option that fixes it: " << logged;
}

// Row pg.encrypted_is_recorded -> ACCEPT, and the fact is recorded.
TEST(Qual12PgTransportLive, AnEncryptedConnectionIsAcceptedAndRecorded) {
    if (!docker_available()) {
        GTEST_SKIP() << "docker unavailable: row pg.encrypted_is_recorded UNEXERCISED";
    }
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_q12_pgcert_" + std::to_string(::getpid()));
    if (!make_server_cert(dir)) {
        GTEST_SKIP() << "openssl unavailable: row pg.encrypted_is_recorded UNEXERCISED";
    }
    clink::test::DockerPostgres::Options opts;
    opts.postgres_args = {"-c",
                          "ssl=on",
                          "-c",
                          "ssl_cert_file=/certs/server.crt",
                          "-c",
                          "ssl_key_file=/certs/server.key"};
    opts.extra_docker_args = "-v " + dir.string() + ":/certs";
    clink::test::DockerPostgres pg{opts};
    bool threw = false;
    const auto logged = capture_assert(pg.conninfo() + " sslmode=require", threw);
    EXPECT_FALSE(threw) << "an encrypted connection was refused: " << logged;
    EXPECT_NE(logged.find("TLS-encrypted"), std::string::npos)
        << "the established transport was not recorded: " << logged;
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
