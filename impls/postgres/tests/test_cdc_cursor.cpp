// The CDC source's position and acknowledgement rules (cdc_cursor.hpp),
// without a server: a checkpoint inside a transaction replays exactly, a
// resume that does not get the interrupted transaction back refuses, a
// keepalive moves the position only between transactions, and nothing is
// confirmable until the checkpoint that captured it completes.

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "cdc_cursor.hpp"

namespace cdc = clink::postgres::cdc;

namespace {

// One transaction as the server sends it: Begin (identified by its commit
// LSN), `changes` changes, Commit ending at `end`.
struct Txn {
    std::uint64_t id;
    int changes;
    std::uint64_t end;
};

// Feed transactions through a cursor, collecting (txn id, change index) for
// every change the cursor lets through. Stops after `stop_after` changes have
// been processed in total, mid-transaction if that is where it falls.
std::vector<std::pair<std::uint64_t, int>> feed(cdc::TxnCursor& cur,
                                                const std::vector<Txn>& txns,
                                                int stop_after = -1) {
    std::vector<std::pair<std::uint64_t, int>> emitted;
    int processed = 0;
    for (const auto& t : txns) {
        (void)cur.on_begin(t.id);
        for (int i = 0; i < t.changes; ++i) {
            if (stop_after >= 0 && processed == stop_after) {
                return emitted;
            }
            ++processed;
            if (cur.on_change()) {
                emitted.emplace_back(t.id, i);
            }
        }
        if (stop_after >= 0 && processed == stop_after && &t == &txns.back()) {
            return emitted;
        }
        cur.on_commit(t.end);
    }
    return emitted;
}

}  // namespace

TEST(CdcCursor, ACheckpointInsideATransactionReplaysExactly) {
    const std::vector<Txn> history = {{100, 2, 110}, {200, 5, 210}, {300, 1, 310}};

    // First run: stop after 4 changes - both of T100 and two of T200 - and
    // checkpoint there.
    cdc::TxnCursor first;
    first.start(50, std::nullopt);
    const auto before = feed(first, history, 4);
    const cdc::CursorState state = first.state(cdc::Phase::Stream);
    EXPECT_EQ(state.resume_lsn, 110U) << "the resume position is T100's end";
    ASSERT_TRUE(state.in_flight);
    EXPECT_EQ(state.txn_id, 200U);
    EXPECT_EQ(state.txn_changes, 2U);

    // Restore: the server resends everything committed from 110 - T200 first.
    cdc::TxnCursor second;
    second.start(state.resume_lsn, std::make_pair(state.txn_id, state.txn_changes));
    const std::vector<Txn> resent = {{200, 5, 210}, {300, 1, 310}};
    const auto after = feed(second, resent);

    // Together the two runs emit every change exactly once.
    std::vector<std::pair<std::uint64_t, int>> all = before;
    all.insert(all.end(), after.begin(), after.end());
    const std::vector<std::pair<std::uint64_t, int>> want = {
        {100, 0}, {100, 1}, {200, 0}, {200, 1}, {200, 2}, {200, 3}, {200, 4}, {300, 0}};
    EXPECT_EQ(all, want);
    EXPECT_EQ(second.resume_lsn(), 310U);
}

TEST(CdcCursor, TheInterruptedTransactionsBeginIsNotEmittedTwice) {
    cdc::TxnCursor cur;
    cur.start(110, std::make_pair(std::uint64_t{200}, std::uint64_t{0}));
    EXPECT_FALSE(cur.on_begin(200)) << "its Begin was emitted before the checkpoint";
    EXPECT_TRUE(cur.on_change());
    cur.on_commit(210);
    EXPECT_TRUE(cur.on_begin(300));
}

TEST(CdcCursor, AResumeThatDoesNotGetTheInterruptedTransactionRefuses) {
    // The slot was confirmed or advanced past T200, so the server starts with
    // a later transaction. Carrying on would skip the rest of T200.
    cdc::TxnCursor cur;
    cur.start(110, std::make_pair(std::uint64_t{200}, std::uint64_t{2}));
    EXPECT_THROW((void)cur.on_begin(300), cdc::HistoryUnavailable);
}

TEST(CdcCursor, AnInterruptedTransactionThatComesBackShorterRefuses) {
    cdc::TxnCursor cur;
    cur.start(110, std::make_pair(std::uint64_t{200}, std::uint64_t{3}));
    EXPECT_FALSE(cur.on_begin(200));
    EXPECT_FALSE(cur.on_change());
    EXPECT_FALSE(cur.on_change());
    EXPECT_THROW(cur.on_commit(210), cdc::HistoryUnavailable);
}

TEST(CdcCursor, AKeepaliveMovesThePositionOnlyBetweenTransactions) {
    cdc::TxnCursor cur;
    cur.start(100, std::nullopt);
    cur.on_keepalive(150);
    EXPECT_EQ(cur.resume_lsn(), 150U);
    (void)cur.on_begin(400);
    (void)cur.on_change();
    cur.on_keepalive(390);  // sent while T400 is being sent: not a resume point
    EXPECT_EQ(cur.state(cdc::Phase::Stream).resume_lsn, 150U);
    cur.on_commit(410);
    EXPECT_EQ(cur.resume_lsn(), 410U);
    cur.on_keepalive(405);  // never backwards
    EXPECT_EQ(cur.resume_lsn(), 410U);

    // A restart that still expects its interrupted transaction ignores
    // keepalives until that transaction is back.
    cdc::TxnCursor resumed;
    resumed.start(410, std::make_pair(std::uint64_t{500}, std::uint64_t{1}));
    resumed.on_keepalive(450);
    EXPECT_EQ(resumed.resume_lsn(), 410U);
}

TEST(CdcCursor, ALostConnectionTurnsTheOpenTransactionIntoTheNextResumesSkip) {
    cdc::TxnCursor cur;
    cur.start(100, std::nullopt);
    (void)cur.on_begin(200);
    (void)cur.on_change();
    (void)cur.on_change();
    const auto interrupted = cur.interrupted();
    ASSERT_TRUE(interrupted.has_value());
    EXPECT_EQ(interrupted->first, 200U);
    EXPECT_EQ(interrupted->second, 2U);
    cur.start(cur.resume_lsn(), interrupted);
    EXPECT_FALSE(cur.on_begin(200));
    EXPECT_FALSE(cur.on_change());
    EXPECT_FALSE(cur.on_change());
    EXPECT_TRUE(cur.on_change()) << "the third change was never emitted";
}

TEST(CdcCursor, ProtocolViolationsAreErrors) {
    cdc::TxnCursor cur;
    cur.start(100, std::nullopt);
    EXPECT_THROW((void)cur.on_change(), std::runtime_error);
    EXPECT_THROW(cur.on_commit(110), std::runtime_error);
    (void)cur.on_begin(200);
    EXPECT_THROW((void)cur.on_begin(300), std::runtime_error);
}

TEST(CdcCursor, NothingIsConfirmableUntilItsCheckpointCompletes) {
    cdc::AckLedger ledger;
    ledger.record(1, 100);
    ledger.record(2, 200);
    ledger.record(3, 300);
    EXPECT_EQ(ledger.confirmable(), 0U);
    ledger.abort(2);
    EXPECT_EQ(ledger.confirmable(), 0U) << "an aborted checkpoint confirms nothing";
    ledger.complete(1);
    EXPECT_EQ(ledger.confirmable(), 100U);
    // A completion covers every earlier checkpoint still pending.
    ledger.record(4, 400);
    ledger.complete(4);
    EXPECT_EQ(ledger.confirmable(), 400U);
    EXPECT_EQ(ledger.pending(), 0U);
    // A late or repeated notification never moves it backwards.
    ledger.complete(1);
    ledger.complete(3);
    EXPECT_EQ(ledger.confirmable(), 400U);
}

TEST(CdcCursor, TheCheckpointEncodingRoundTripsAndRefusesUnknownVersions) {
    cdc::CursorState s;
    s.phase = cdc::Phase::Snapshot;
    s.resume_lsn = 0x1122334455667788ULL;
    s.in_flight = true;
    s.txn_id = 0x99;
    s.txn_changes = 7;
    const auto bytes = cdc::encode_cursor(s);
    const std::vector<char> v(bytes.begin(), bytes.end());
    const auto back = cdc::decode_cursor(v);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->phase, cdc::Phase::Snapshot);
    EXPECT_EQ(back->resume_lsn, s.resume_lsn);
    EXPECT_TRUE(back->in_flight);
    EXPECT_EQ(back->txn_id, 0x99U);
    EXPECT_EQ(back->txn_changes, 7U);

    std::vector<char> future = v;
    future[0] = 3;
    EXPECT_FALSE(cdc::decode_cursor(future).has_value());
    EXPECT_FALSE(cdc::decode_cursor(std::vector<char>(v.begin(), v.begin() + 10)).has_value());
}
