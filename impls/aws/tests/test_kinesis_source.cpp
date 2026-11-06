// The Kinesis source against a model of the service (kinesis_model.hpp): the
// production KinesisSource, its ownership, discovery, lineage, read loop,
// failure handling and checkpoint rows, with the service's behaviour scripted.
// Rescale cases restore through the runtime's own state backend factory. The
// LocalStack cases are in test_kinesis_live.cpp.

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/aws/kinesis_source.hpp"
#include "clink/cluster/rescale_dispatch.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"
#if defined(CLINK_AWS_TESTS_HAVE_ROCKSDB)
#include "clink/rocksdb/install.hpp"
#endif

#include "kinesis_model.hpp"

namespace {

using namespace std::chrono_literals;
using clink::CheckpointId;
using clink::Emitter;
using clink::InMemoryStateBackend;
using clink::OperatorId;
using clink::StateBackend;
using clink::StreamElement;
using clink::aws::classify_kinesis_error;
using clink::aws::decode_kinesis_shard_checkpoint;
using clink::aws::decode_kinesis_subtask_checkpoint;
using clink::aws::encode_kinesis_shard_checkpoint;
using clink::aws::encode_kinesis_subtask_checkpoint;
using clink::aws::kinesis_shard_owner;
using clink::aws::KinesisCall;
using clink::aws::KinesisFault;
using clink::aws::KinesisReadFrom;
using clink::aws::KinesisShardCheckpoint;
using clink::aws::KinesisSource;
using clink::aws::KinesisSourceOptions;
using clink::aws::KinesisSubtaskCheckpoint;
using clink::aws::test::KinesisModel;
using clink::aws::test::now_ms;
using clink::aws::test::service_error;
using clink::aws::test::shard_name;
using clink::aws::test::shard_number_owned_by;
using Status = KinesisShardCheckpoint::Status;

const OperatorId kOp{7};

KinesisSourceOptions options(const std::shared_ptr<KinesisModel>& model,
                             std::uint32_t subtask = 0,
                             std::uint32_t parallelism = 1,
                             const std::string& initial_position = "trim_horizon") {
    KinesisSourceOptions o;
    o.stream = "orders";
    o.initial_position = initial_position;
    o.subtask_idx = subtask;
    o.parallelism = parallelism;
    o.service = model;
    o.max_records_per_poll = 100;
    o.poll_interval = 2ms;
    o.read_spacing = 0ms;
    o.discovery_interval = 100ms;
    o.retry_timeout = 2s;
    o.max_backoff = 5ms;
    return o;
}

// One subtask under test: the source, what it emitted, and its emitter.
struct Reader {
    explicit Reader(KinesisSourceOptions o)
        : source(std::make_unique<KinesisSource>(std::move(o))) {}
    std::unique_ptr<KinesisSource> source;
    std::vector<std::string> got;
    Emitter<std::string> emitter{[this](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                got.push_back(r.value());
            }
        }
        return true;
    }};
    void turn() { source->produce(emitter); }
    [[nodiscard]] Status status_of(const std::string& id) const {
        for (const auto& s : source->shard_statuses()) {
            if (s.shard_id == id) {
                return s.status;
            }
        }
        ADD_FAILURE() << "shard " << id << " is not tracked";
        return Status::Waiting;
    }
    [[nodiscard]] bool tracks(const std::string& id) const {
        const auto all = source->shard_statuses();
        return std::any_of(all.begin(), all.end(), [&](const auto& s) { return s.shard_id == id; });
    }
};

// Turns every reader until `done` holds or the limit passes; returns `done`.
template <typename Done>
bool run_until(std::vector<Reader*> readers, Done done, std::chrono::milliseconds limit = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        for (auto* r : readers) {
            r->turn();
        }
    }
    return true;
}

std::multiset<std::string> as_multiset(const std::vector<std::string>& v) {
    return {v.begin(), v.end()};
}

std::size_t index_of(const std::vector<std::string>& v, const std::string& x) {
    return static_cast<std::size_t>(std::find(v.begin(), v.end(), x) - v.begin());
}

// Writes `n` records "<tag>-<i>" to a shard.
void fill(KinesisModel& m, const std::string& shard, const std::string& tag, int n) {
    for (int i = 0; i < n; ++i) {
        m.put(shard, tag + "-" + std::to_string(i));
    }
}

std::string expect_throw_message(const std::function<void()>& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    ADD_FAILURE() << "expected an exception";
    return {};
}

// ---- ownership, classification, rows, configuration -------------------------

TEST(KinesisShardOwnership, EveryShardHasExactlyOneOwnerWhateverTheListingOrder) {
    std::vector<std::string> ids;
    for (int n = 0; n < 200; ++n) {
        ids.push_back(shard_name(n));
    }
    for (std::uint32_t p = 1; p <= 7; ++p) {
        std::vector<std::size_t> per_subtask(p, 0);
        for (const auto& id : ids) {
            const auto owner = kinesis_shard_owner(id, p);
            ASSERT_LT(owner, p);
            ++per_subtask[owner];
        }
        // A function of the id alone: reversing the list changes nothing.
        std::vector<std::string> reversed(ids.rbegin(), ids.rend());
        for (const auto& id : reversed) {
            EXPECT_EQ(kinesis_shard_owner(id, p), kinesis_shard_owner(std::string{id}, p));
        }
        for (std::uint32_t s = 0; s < p; ++s) {
            EXPECT_GT(per_subtask[s], 200 / p / 3) << "subtask " << s << " of " << p << " starved";
        }
    }
    EXPECT_EQ(kinesis_shard_owner("anything", 1), 0U);
}

TEST(KinesisErrorClassification, MapsServiceCodesToHandling) {
    using K = KinesisCall;
    EXPECT_EQ(
        classify_kinesis_error(K::GetRecords, "ProvisionedThroughputExceededException", 400, true),
        KinesisFault::Throttled);
    EXPECT_EQ(classify_kinesis_error(K::ListShards, "LimitExceededException", 400, false),
              KinesisFault::Throttled);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "KMSThrottlingException", 400, false),
              KinesisFault::Throttled);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "", 429, false), KinesisFault::Throttled);
    EXPECT_EQ(
        classify_kinesis_error(
            K::GetRecords, "com.amazonaws.kinesis.v20131202#ExpiredIteratorException", 400, false),
        KinesisFault::ExpiredIterator);
    EXPECT_EQ(classify_kinesis_error(K::ListShards, "ExpiredNextTokenException", 400, false),
              KinesisFault::ExpiredNextToken);
    EXPECT_EQ(classify_kinesis_error(K::ListShards, "ResourceNotFoundException", 400, false),
              KinesisFault::Permanent);
    EXPECT_EQ(classify_kinesis_error(K::GetShardIterator, "ResourceNotFoundException", 400, false),
              KinesisFault::PositionUnavailable);
    EXPECT_EQ(classify_kinesis_error(K::GetShardIterator, "InvalidArgumentException", 400, false),
              KinesisFault::PositionUnavailable);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "InvalidArgumentException", 400, false),
              KinesisFault::ExpiredIterator);
    EXPECT_EQ(classify_kinesis_error(K::ListShards, "InvalidArgumentException", 400, false),
              KinesisFault::Permanent);
    for (const char* code : {"AccessDeniedException",
                             "UnrecognizedClientException",
                             "InvalidSignatureException",
                             "KMSAccessDeniedException",
                             "KMSDisabledException",
                             "ValidationException"}) {
        EXPECT_EQ(classify_kinesis_error(K::GetRecords, code, 400, true), KinesisFault::Permanent)
            << code;
    }
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "InternalFailure", 500, true),
              KinesisFault::Transient);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "ServiceUnavailable", 503, true),
              KinesisFault::Transient);
    EXPECT_EQ(classify_kinesis_error(K::ListShards, "ResourceInUseException", 400, false),
              KinesisFault::Transient);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "NoResponse", 0, true),
              KinesisFault::Transient);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "SomethingNew", 502, false),
              KinesisFault::Transient);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "SomethingNew", 400, false),
              KinesisFault::Permanent);
    EXPECT_EQ(classify_kinesis_error(K::GetRecords, "SomethingNew", 400, true),
              KinesisFault::Transient);
}

TEST(KinesisCheckpointRows, RoundTripAndRefuseAnotherFormat) {
    KinesisShardCheckpoint row;
    row.status = Status::Waiting;
    row.parent = "shardId-000000000001";
    row.adjacent_parent = "shardId-000000000002";
    row.starting_sequence = "4967";
    row.start_timestamp_ms = 1730000000123;
    const auto back = decode_kinesis_shard_checkpoint(encode_kinesis_shard_checkpoint(row));
    EXPECT_EQ(back.status, Status::Waiting);
    EXPECT_EQ(back.parent, row.parent);
    EXPECT_EQ(back.adjacent_parent, row.adjacent_parent);
    EXPECT_EQ(back.starting_sequence, "4967");
    EXPECT_EQ(back.start_timestamp_ms, row.start_timestamp_ms);
    EXPECT_TRUE(back.last_sequence.empty());

    KinesisShardCheckpoint reading;
    reading.status = Status::Reading;
    reading.last_sequence = "49679966097759955104421939506464168651684368584854732802";
    const auto r2 = decode_kinesis_shard_checkpoint(encode_kinesis_shard_checkpoint(reading));
    EXPECT_EQ(r2.last_sequence, reading.last_sequence);
    EXPECT_FALSE(r2.start_timestamp_ms.has_value());

    KinesisSubtaskCheckpoint sub;
    sub.parallelism = 3;
    sub.initial_position = "latest";
    sub.start_ms = 42;
    sub.stream = "arn:aws:kinesis:eu-west-1:123456789012:stream/orders";
    const auto s2 = decode_kinesis_subtask_checkpoint(encode_kinesis_subtask_checkpoint(sub));
    EXPECT_EQ(s2.parallelism, 3U);
    EXPECT_EQ(s2.initial_position, "latest");
    EXPECT_EQ(s2.start_ms, 42);
    EXPECT_EQ(s2.stream, sub.stream);

    EXPECT_THROW(decode_kinesis_shard_checkpoint("v2|reading|1|||||"), std::runtime_error);
    EXPECT_THROW(decode_kinesis_shard_checkpoint("v1|paused|1||||"), std::runtime_error);
    EXPECT_THROW(decode_kinesis_shard_checkpoint("v1|reading|1|"), std::runtime_error);
    EXPECT_THROW(decode_kinesis_subtask_checkpoint("v1|0|latest|1|s"), std::runtime_error);
    EXPECT_THROW(decode_kinesis_subtask_checkpoint("v9|1|latest|1|s"), std::runtime_error);
}

TEST(KinesisSourceConfig, RejectsInvalidSettings) {
    const auto model = std::make_shared<KinesisModel>();
    const auto rejects = [&](auto change) {
        auto o = options(model);
        change(o);
        EXPECT_THROW(KinesisSource{std::move(o)}, std::runtime_error);
    };
    rejects([](KinesisSourceOptions& o) { o.stream.clear(); });
    rejects([](KinesisSourceOptions& o) { o.initial_position = "lastest"; });
    rejects([](KinesisSourceOptions& o) { o.parallelism = 0; });
    rejects([](KinesisSourceOptions& o) {
        o.parallelism = 2;
        o.subtask_idx = 2;
    });
    rejects([](KinesisSourceOptions& o) { o.max_records_per_poll = 0; });
    rejects([](KinesisSourceOptions& o) { o.max_records_per_poll = 10001; });
    rejects([](KinesisSourceOptions& o) { o.discovery_interval = 10ms; });
    rejects([](KinesisSourceOptions& o) { o.retry_timeout = 0ms; });
    rejects([](KinesisSourceOptions& o) { o.list_page_size = -1; });
    auto ok = options(model, 1, 2, "latest");
    EXPECT_NO_THROW(KinesisSource{std::move(ok)});
    KinesisSource unbounded{options(model)};
    EXPECT_FALSE(unbounded.is_bounded());
}

// ---- discovery and ownership ------------------------------------------------

// A subtask that owns nothing at first still lists the stream, and reads a
// shard it comes to own as soon as the next listing names it.
TEST(KinesisSourceDiscovery, AnEmptySubtaskDiscoversShardsItComesToOwn) {
    const auto model = std::make_shared<KinesisModel>();
    const int a = shard_number_owned_by(0, 2, 0);
    model->create(shard_name(a));
    fill(*model, shard_name(a), "a", 3);
    Reader empty{options(model, 1, 2)};
    empty.source->open();
    ASSERT_TRUE(run_until({&empty}, [&] { return empty.source->listings() >= 1; }));
    EXPECT_TRUE(empty.source->shard_statuses().empty());

    // The parent closes; one child is subtask 1's.
    const int left = shard_number_owned_by(1, 2, a + 1);
    const int right = shard_number_owned_by(0, 2, left + 1);
    model->split(shard_name(a), shard_name(left), shard_name(right));
    fill(*model, shard_name(left), "left", 4);
    ASSERT_TRUE(run_until({&empty}, [&] { return empty.got.size() == 4; }))
        << "the subtask never read the shard it came to own";
    EXPECT_EQ(empty.got, model->data_of(shard_name(left)));
    EXPECT_GE(empty.source->listings(), 2U);
}

// Discovery runs on its interval while an owned shard keeps returning records:
// it does not wait for an idle round or for an owned shard to end.
TEST(KinesisSourceDiscovery, ListsWhileExistingShardsStayBusy) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->create(shard_name(1));
    Reader r{options(model)};
    r.source->open();
    int fed = 0;
    const auto feed = [&] { model->put(shard_name(0), "busy-" + std::to_string(fed++)); };
    ASSERT_TRUE(run_until({&r}, [&] {
        feed();
        return r.source->listings() >= 1;
    }));
    // Shard 1 is split while shard 0 never goes quiet.
    model->split(shard_name(1), shard_name(2), shard_name(3));
    model->put(shard_name(2), "child-2");
    model->put(shard_name(3), "child-3");
    ASSERT_TRUE(run_until({&r}, [&] {
        feed();
        return std::count(r.got.begin(), r.got.end(), "child-2") == 1 &&
               std::count(r.got.begin(), r.got.end(), "child-3") == 1;
    })) << "children of a reshard were not discovered while another shard was busy";
    EXPECT_EQ(r.status_of(shard_name(1)), Status::Drained);
    EXPECT_EQ(r.status_of(shard_name(0)), Status::Reading);
}

// Small pages, a different order on every listing, and subtasks that list at
// different moments: every shard is read by exactly one subtask.
TEST(KinesisSourceDiscovery, PaginatedReorderedListingsKeepOwnershipExclusive) {
    const auto model = std::make_shared<KinesisModel>();
    constexpr int kShards = 23;
    for (int n = 0; n < kShards; ++n) {
        model->create(shard_name(n));
        model->put(shard_name(n), "only-" + std::to_string(n));
    }
    model->page_size = 2;
    int calls = 0;
    model->reorder = [&](std::vector<clink::aws::KinesisShard>& v) {
        // A different permutation per listing.
        std::rotate(v.begin(), v.begin() + (++calls * 5) % static_cast<int>(v.size()), v.end());
        if (calls % 2 == 0) {
            std::reverse(v.begin(), v.end());
        }
    };
    std::vector<std::unique_ptr<Reader>> readers;
    std::vector<Reader*> all;
    for (std::uint32_t i = 0; i < 3; ++i) {
        readers.push_back(std::make_unique<Reader>(options(model, i, 3)));
        readers.back()->source->open();
        all.push_back(readers.back().get());
    }
    ASSERT_TRUE(run_until(all, [&] {
        std::size_t total = 0;
        for (auto* r : all) {
            total += r->got.size();
        }
        return total >= kShards && all[0]->source->listings() >= 3;
    }));
    std::multiset<std::string> read;
    std::set<std::string> owned;
    for (std::uint32_t i = 0; i < 3; ++i) {
        for (const auto& s : all[i]->source->shard_statuses()) {
            EXPECT_TRUE(owned.insert(s.shard_id).second) << s.shard_id << " owned twice";
            EXPECT_EQ(kinesis_shard_owner(s.shard_id, 3), i);
        }
        read.insert(all[i]->got.begin(), all[i]->got.end());
    }
    EXPECT_EQ(owned.size(), static_cast<std::size_t>(kShards));
    EXPECT_EQ(read, as_multiset(model->all_data()));
    EXPECT_GT(model->list_calls(), 3U * 3U) << "small pages should take several calls a listing";
}

// A drained shard that leaves the listing is forgotten after two listings and
// never read again; one that leaves with records unread fails the source.
TEST(KinesisSourceDiscovery, ShardRemovalNeitherDuplicatesNorSilentlyDrops) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "p", 5);
    model->split(shard_name(0), shard_name(1), shard_name(2));
    fill(*model, shard_name(1), "c1", 2);
    Reader r{options(model)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 7; }));
    model->expire(shard_name(0));
    const auto before = r.source->listings();
    ASSERT_TRUE(run_until({&r}, [&] { return !r.tracks(shard_name(0)); }));
    EXPECT_GE(r.source->listings(), before + 2);
    // Nothing read twice, and the expired parent was not picked up again.
    EXPECT_EQ(as_multiset(r.got).count("p-0"), 1U);
    InMemoryStateBackend backend;
    r.source->snapshot_offset(backend, kOp, CheckpointId{1});
    std::size_t rows = 0;
    backend.scan_operator_state(kOp, [&](StateBackend::KeyView k, StateBackend::ValueView) {
        rows += std::string_view{k}.find(shard_name(0)) != std::string_view::npos ? 1U : 0U;
    });
    EXPECT_EQ(rows, 0U) << "a forgotten shard kept its checkpoint row";

    // A child with unread records leaves the stream: the source stops.
    model->put(shard_name(2), "late");
    model->expire(shard_name(2));
    const auto msg = expect_throw_message([&] { run_until({&r}, [] { return false; }, 2s); });
    EXPECT_NE(msg.find(shard_name(2)), std::string::npos) << msg;
    EXPECT_NE(msg.find("cannot continue"), std::string::npos) << msg;
}

// ---- lineage ---------------------------------------------------------------------

// Parallelism 1: a split's children wait until the parent is read to its end,
// even when the parent's backlog takes many reads.
TEST(KinesisSourceLineage, SplitChildrenWaitForASlowParent) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "parent", 50);
    model->split(shard_name(0), shard_name(1), shard_name(2));
    fill(*model, shard_name(1), "left", 5);
    fill(*model, shard_name(2), "right", 5);
    auto o = options(model);
    o.max_records_per_poll = 3;  // the parent drains over 17 reads
    Reader r{std::move(o)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.source->listings() >= 1; }));
    EXPECT_EQ(r.status_of(shard_name(1)), Status::Waiting);
    EXPECT_EQ(r.status_of(shard_name(2)), Status::Waiting);
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 60; }));
    const auto last_parent = index_of(r.got, "parent-49");
    for (const char* first_child : {"left-0", "right-0"}) {
        EXPECT_LT(last_parent, index_of(r.got, first_child))
            << first_child << " was emitted before the parent was read to its end";
    }
    // Within each shard, the stream's order.
    for (const std::string tag : {"parent", "left", "right"}) {
        std::vector<std::string> mine;
        std::copy_if(r.got.begin(), r.got.end(), std::back_inserter(mine), [&](const auto& v) {
            return v.rfind(tag + "-", 0) == 0;
        });
        EXPECT_EQ(mine,
                  model->data_of(tag == "parent" ? shard_name(0)
                                 : tag == "left" ? shard_name(1)
                                                 : shard_name(2)));
    }
}

// Parallelism 1: a merge's child waits for both parents, the second of which
// is slow.
TEST(KinesisSourceLineage, MergeChildWaitsForBothParents) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->create(shard_name(1));
    fill(*model, shard_name(0), "a", 2);
    fill(*model, shard_name(1), "b", 40);
    model->merge(shard_name(0), shard_name(1), shard_name(2));
    fill(*model, shard_name(2), "merged", 3);
    auto o = options(model);
    o.max_records_per_poll = 2;
    Reader r{std::move(o)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 45; }));
    EXPECT_LT(index_of(r.got, "a-1"), index_of(r.got, "merged-0"));
    EXPECT_LT(index_of(r.got, "b-39"), index_of(r.got, "merged-0"))
        << "the merged shard was read before its slower parent ended";
}

// A child whose parents have left the stream before the source started is
// read at once: there is nothing left to wait for.
TEST(KinesisSourceLineage, AParentAbsentFromTheListingDoesNotHoldItsChild) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->create(shard_name(1));
    model->merge(shard_name(0), shard_name(1), shard_name(2));
    fill(*model, shard_name(2), "m", 3);
    model->expire(shard_name(0));
    model->expire(shard_name(1));
    Reader r{options(model)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 3; }));
    EXPECT_EQ(r.status_of(shard_name(2)), Status::Reading);
}

// Parallelism 2, split: a child the parent's owner also owns waits for the
// parent; a child another subtask owns is not held back. Every record is read
// once, by its shard's owner.
TEST(KinesisSourceLineage, SplitWithChildrenOnBothSubtasks) {
    const auto model = std::make_shared<KinesisModel>();
    const int parent = shard_number_owned_by(0, 2, 0);
    const int same = shard_number_owned_by(0, 2, parent + 1);
    const int other = shard_number_owned_by(1, 2, parent + 1);
    model->create(shard_name(parent));
    fill(*model, shard_name(parent), "parent", 30);
    model->split(shard_name(parent), shard_name(same), shard_name(other));
    fill(*model, shard_name(same), "same", 3);
    fill(*model, shard_name(other), "other", 3);
    auto o0 = options(model, 0, 2);
    o0.max_records_per_poll = 2;
    Reader r0{std::move(o0)};
    Reader r1{options(model, 1, 2)};
    r0.source->open();
    r1.source->open();
    // Subtask 1 reads its child while subtask 0 is still on the parent.
    ASSERT_TRUE(run_until({&r0, &r1}, [&] { return r1.got.size() == 3; }));
    EXPECT_LT(r0.got.size(), 30U) << "the test needs the parent unfinished here";
    EXPECT_EQ(r0.status_of(shard_name(same)), Status::Waiting);
    ASSERT_TRUE(run_until({&r0, &r1}, [&] { return r0.got.size() == 33; }));
    EXPECT_LT(index_of(r0.got, "parent-29"), index_of(r0.got, "same-0"));
    std::multiset<std::string> read(r0.got.begin(), r0.got.end());
    read.insert(r1.got.begin(), r1.got.end());
    EXPECT_EQ(read, as_multiset(model->all_data()));
    EXPECT_FALSE(r1.tracks(shard_name(parent)));
    EXPECT_FALSE(r1.tracks(shard_name(same)));
}

// Parallelism 2, merge with parents on different subtasks: the child's owner
// waits for the parent it owns and not for the other subtask's.
TEST(KinesisSourceLineage, MergeWithParentsOnDifferentSubtasks) {
    const auto model = std::make_shared<KinesisModel>();
    const int mine = shard_number_owned_by(0, 2, 0);
    const int theirs = shard_number_owned_by(1, 2, 0);
    const int child = shard_number_owned_by(0, 2, std::max(mine, theirs) + 1);
    model->create(shard_name(mine));
    model->create(shard_name(theirs));
    fill(*model, shard_name(mine), "mine", 20);
    fill(*model, shard_name(theirs), "theirs", 20);
    model->merge(shard_name(mine), shard_name(theirs), shard_name(child));
    fill(*model, shard_name(child), "child", 4);
    auto o0 = options(model, 0, 2);
    o0.max_records_per_poll = 2;
    auto o1 = options(model, 1, 2);
    o1.max_records_per_poll = 1;
    Reader r0{std::move(o0)};
    Reader r1{std::move(o1)};
    r0.source->open();
    r1.source->open();
    ASSERT_TRUE(run_until({&r0, &r1}, [&] { return r0.got.size() == 24 && r1.got.size() == 20; }));
    EXPECT_LT(index_of(r0.got, "mine-19"), index_of(r0.got, "child-0"));
    std::multiset<std::string> read(r0.got.begin(), r0.got.end());
    read.insert(r1.got.begin(), r1.got.end());
    EXPECT_EQ(read, as_multiset(model->all_data()));
}

// ---- checkpoints and restore -----------------------------------------------------

struct Run {
    std::vector<std::string> got;
    std::vector<std::string> statuses;
};

// Checkpoints a source at a lineage transition, restores a new one from it and
// reads on: together the two runs emit every record once, in each shard's
// order, and the restored run resumes each shard where the first left it.
TEST(KinesisSourceRestore, RestartAtEachParentChildTransition) {
    enum class At { ParentReading, ParentDrained, ChildReading, ChildDrained };
    for (const auto at :
         {At::ParentReading, At::ParentDrained, At::ChildReading, At::ChildDrained}) {
        SCOPED_TRACE(static_cast<int>(at));
        const auto model = std::make_shared<KinesisModel>();
        model->create(shard_name(0));
        fill(*model, shard_name(0), "p", 10);
        model->split(shard_name(0), shard_name(1), shard_name(2));
        fill(*model, shard_name(1), "l", 10);
        fill(*model, shard_name(2), "r", 10);
        model->close(shard_name(1));
        auto o = options(model);
        o.max_records_per_poll = 4;
        Reader first{o};
        first.source->open();
        const auto reached = [&] {
            switch (at) {
                case At::ParentReading:
                    return first.got.size() >= 4;
                case At::ParentDrained:
                    return first.source->listings() >= 1 &&
                           first.status_of(shard_name(0)) == Status::Drained;
                case At::ChildReading:
                    return std::count_if(first.got.begin(), first.got.end(), [](const auto& v) {
                               return v[0] == 'l';
                           }) >= 4;
                case At::ChildDrained:
                    return first.source->listings() >= 1 &&
                           first.status_of(shard_name(1)) == Status::Drained;
            }
            return false;
        };
        ASSERT_TRUE(run_until({&first}, reached));
        InMemoryStateBackend backend;
        first.source->snapshot_offset(backend, kOp, CheckpointId{1});

        Reader second{o};
        ASSERT_TRUE(second.source->restore_offset(backend, kOp));
        second.source->open();
        ASSERT_TRUE(
            run_until({&second}, [&] { return first.got.size() + second.got.size() >= 30; }));
        // Keep reading a little: nothing more may arrive.
        run_until({&second}, [] { return false; }, 150ms);
        std::multiset<std::string> all(first.got.begin(), first.got.end());
        all.insert(second.got.begin(), second.got.end());
        EXPECT_EQ(all, as_multiset(model->all_data())) << "a record was lost or read twice";
        if (at == At::ParentReading) {
            // The restored run finishes the parent before either child.
            const auto last_parent = index_of(second.got, "p-9");
            ASSERT_LT(last_parent, second.got.size());
            EXPECT_LT(last_parent, index_of(second.got, "l-0"));
            EXPECT_LT(last_parent, index_of(second.got, "r-0"));
        }
    }
}

// A checkpoint never holds a position past the records the runtime was handed:
// a failed hand-off leaves the position where it was.
TEST(KinesisSourceRestore, PositionsNeverPassRecordsHandedDownstream) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "x", 6);
    auto o = options(model);
    o.max_records_per_poll = 3;
    KinesisSource source{o};
    std::vector<std::string> handed;
    bool refuse = false;
    Emitter<std::string> emitter{[&](StreamElement<std::string> e) {
        if (refuse) {
            throw std::runtime_error("downstream closed");
        }
        for (const auto& r : e.as_data()) {
            handed.push_back(r.value());
        }
        return true;
    }};
    source.open();
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (handed.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        source.produce(emitter);
    }
    ASSERT_EQ(handed.size(), 3U);
    refuse = true;
    EXPECT_THROW(
        {
            while (std::chrono::steady_clock::now() < deadline) {
                source.produce(emitter);
            }
        },
        std::runtime_error);
    InMemoryStateBackend backend;
    source.snapshot_offset(backend, kOp, CheckpointId{1});
    Reader resumed{o};
    ASSERT_TRUE(resumed.source->restore_offset(backend, kOp));
    resumed.source->open();
    ASSERT_TRUE(run_until({&resumed}, [&] { return resumed.got.size() == 3; }));
    EXPECT_EQ(resumed.got, (std::vector<std::string>{"x-3", "x-4", "x-5"}))
        << "the failed batch was skipped by the checkpoint";
}

// Latest mode: a fresh start skips what the stream held before it; records
// arriving after it are read, including on a child discovered later.
TEST(KinesisSourceRestore, LatestFreshStartSkipsEarlierRecordsOnly) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->put(shard_name(0), "before", now_ms() - 60'000);
    Reader r{options(model, 0, 1, "latest")};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.source->listings() >= 1; }));
    model->put(shard_name(0), "after-0");
    model->split(shard_name(0), shard_name(1), shard_name(2));
    model->put(shard_name(1), "child-0");
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 2; }));
    EXPECT_EQ(r.got, (std::vector<std::string>{"after-0", "child-0"}));
    const auto mints = model->mints();
    ASSERT_FALSE(mints.empty());
    EXPECT_EQ(mints.front().from.kind, KinesisReadFrom::Kind::AtTimestamp);
}

// Latest mode, restored from a checkpoint taken before any record: the
// restored run starts at the first run's start time, not the tip, so records
// that arrived in between are read.
TEST(KinesisSourceRestore, LatestRestoreBeforeTheFirstRecordKeepsTheStartTime) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->put(shard_name(0), "before", now_ms() - 60'000);
    InMemoryStateBackend backend;
    {
        Reader first{options(model, 0, 1, "latest")};
        first.source->open();
        ASSERT_TRUE(run_until({&first}, [&] { return first.source->listings() >= 1; }));
        run_until({&first}, [] { return false; }, 50ms);
        ASSERT_TRUE(first.got.empty());
        first.source->snapshot_offset(backend, kOp, CheckpointId{1});
    }
    // Written while the job was down.
    model->put(shard_name(0), "during-outage");
    std::this_thread::sleep_for(20ms);
    Reader second{options(model, 0, 1, "latest")};
    ASSERT_TRUE(second.source->restore_offset(backend, kOp));
    second.source->open();
    ASSERT_TRUE(run_until({&second}, [&] { return second.got.size() == 1; }));
    run_until({&second}, [] { return false; }, 50ms);
    EXPECT_EQ(second.got, (std::vector<std::string>{"during-outage"}));
}

// An iterator that expires before the shard's first record is minted again at
// the shard's start (latest: the start time), never at the tip.
TEST(KinesisSourceRestore, IteratorExpiryBeforeTheFirstRecordDoesNotSkip) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    Reader r{options(model, 0, 1, "latest")};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return model->mints().size() >= 1; }));
    run_until({&r}, [] { return false; }, 30ms);
    model->put(shard_name(0), "arrived-while-expired-0");
    model->put(shard_name(0), "arrived-while-expired-1");
    model->expire_iterators();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 2; }));
    const auto mints = model->mints();
    ASSERT_GE(mints.size(), 2U);
    EXPECT_EQ(mints.back().from.kind, KinesisReadFrom::Kind::AtTimestamp);
    EXPECT_EQ(mints.back().from.timestamp_ms, mints.front().from.timestamp_ms);
}

// After the first record, an expired iterator is minted AT the last record
// read, which the service returns first and the source drops.
TEST(KinesisSourceRestore, IteratorExpiryAfterTheFirstRecordResumesAtIt) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "e", 3);
    Reader r{options(model)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 3; }));
    model->expire_iterators();
    fill(*model, shard_name(0), "f", 2);
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 5; }));
    run_until({&r}, [] { return false; }, 50ms);
    EXPECT_EQ(r.got, (std::vector<std::string>{"e-0", "e-1", "e-2", "f-0", "f-1"}));
    const auto mints = model->mints();
    EXPECT_EQ(mints.back().from.kind, KinesisReadFrom::Kind::AtSequence);
}

// A saved position that has left the stream (retention) fails the restore,
// whether the service refuses the sequence number or silently starts later.
TEST(KinesisSourceRestore, RetentionLossOfASavedPositionFailsClearly) {
    for (const auto mode :
         {KinesisModel::MissingSequence::Refuse, KinesisModel::MissingSequence::FromTrimHorizon}) {
        SCOPED_TRACE(static_cast<int>(mode));
        const auto model = std::make_shared<KinesisModel>();
        model->missing_sequence = mode;
        model->create(shard_name(0));
        fill(*model, shard_name(0), "k", 4);
        auto o = options(model);
        o.max_records_per_poll = 2;
        InMemoryStateBackend backend;
        {
            Reader first{o};
            first.source->open();
            ASSERT_TRUE(run_until({&first}, [&] { return first.got.size() == 2; }));
            first.source->snapshot_offset(backend, kOp, CheckpointId{1});
        }
        model->trim(shard_name(0), 3);  // k-0..k-2 gone, k-3 left
        Reader second{o};
        ASSERT_TRUE(second.source->restore_offset(backend, kOp));
        second.source->open();
        const auto msg =
            expect_throw_message([&] { run_until({&second}, [] { return false; }, 2s); });
        EXPECT_NE(msg.find("retention"), std::string::npos) << msg;
        EXPECT_TRUE(second.got.empty()) << "records were emitted past a gap";
    }
}

// A restored shard the listing no longer names: drained is fine (it is
// forgotten), unread is a loss the source refuses.
TEST(KinesisSourceRestore, MissingShardsAtRestore) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->create(shard_name(1));
    fill(*model, shard_name(0), "a", 2);
    model->close(shard_name(0));
    fill(*model, shard_name(1), "b", 4);
    auto o = options(model);
    o.max_records_per_poll = 2;
    InMemoryStateBackend backend;
    {
        Reader first{o};
        first.source->open();
        ASSERT_TRUE(run_until({&first}, [&] {
            return first.source->listings() >= 1 &&
                   first.status_of(shard_name(0)) == Status::Drained && first.got.size() >= 4;
        }));
        first.source->snapshot_offset(backend, kOp, CheckpointId{1});
    }
    model->expire(shard_name(0));
    {
        Reader drained_gone{o};
        drained_gone.source->restore_offset(backend, kOp);
        drained_gone.source->open();
        ASSERT_TRUE(run_until({&drained_gone}, [&] {
            return drained_gone.source->listings() >= 2 && !drained_gone.tracks(shard_name(0));
        }));
    }
    model->expire(shard_name(1));
    Reader unread_gone{o};
    unread_gone.source->restore_offset(backend, kOp);
    unread_gone.source->open();
    const auto msg =
        expect_throw_message([&] { run_until({&unread_gone}, [] { return false; }, 2s); });
    EXPECT_NE(msg.find(shard_name(1)), std::string::npos) << msg;
}

TEST(KinesisSourceRestore, ARecreatedStreamOrAnotherStreamIsRefused) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "a", 2);
    InMemoryStateBackend backend;
    {
        Reader first{options(model)};
        first.source->open();
        ASSERT_TRUE(run_until({&first}, [&] { return first.got.size() == 2; }));
        first.source->snapshot_offset(backend, kOp, CheckpointId{1});
    }
    auto other = options(model);
    other.stream = "payments";
    Reader wrong_stream{other};
    wrong_stream.source->restore_offset(backend, kOp);
    EXPECT_THROW(wrong_stream.source->open(), std::runtime_error);

    model->recreate();
    model->put(shard_name(0), "new-stream");
    Reader recreated{options(model)};
    recreated.source->restore_offset(backend, kOp);
    recreated.source->open();
    const auto msg =
        expect_throw_message([&] { run_until({&recreated}, [] { return false; }, 2s); });
    EXPECT_NE(msg.find("created again"), std::string::npos) << msg;
}

// An earlier release's rows (one sequence number per shard) restore at
// parallelism 1, resuming at each saved record; above 1 they are refused.
TEST(KinesisSourceRestore, EarlierReleaseCheckpoints) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->create(shard_name(1));
    std::vector<std::string> seqs;
    for (int i = 0; i < 4; ++i) {
        seqs.push_back(model->put(shard_name(0), "a-" + std::to_string(i)));
    }
    model->put(shard_name(1), "b-0", now_ms() - 60'000);
    InMemoryStateBackend legacy;
    legacy.put_operator_state(kOp,
                              "__kinesis_seq__:" + shard_name(0),
                              StateBackend::ValueView{seqs[1].data(), seqs[1].size()});
    // latest: shard 1 has no row and no start time, so it starts at its oldest record.
    Reader r{options(model, 0, 1, "latest")};
    ASSERT_TRUE(r.source->restore_offset(legacy, kOp));
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 3; }));
    run_until({&r}, [] { return false; }, 50ms);
    EXPECT_EQ(as_multiset(r.got), (std::multiset<std::string>{"a-2", "a-3", "b-0"}));

    Reader parallel{options(model, 0, 2)};
    ASSERT_TRUE(parallel.source->restore_offset(legacy, kOp));
    const auto msg = expect_throw_message([&] { parallel.source->open(); });
    EXPECT_NE(msg.find("earlier Kinesis source"), std::string::npos) << msg;

    InMemoryStateBackend future;
    const std::string v2 = "v2|reading|1|||||";
    future.put_operator_state(
        kOp, "__kinesis_shard__:" + shard_name(0), StateBackend::ValueView{v2.data(), v2.size()});
    Reader newer{options(model)};
    EXPECT_THROW(newer.source->restore_offset(future, kOp), std::runtime_error);
}

// ---- rescale through the runtime's restore -------------------------------------

std::filesystem::path scratch_dir(const std::string& tag) {
    auto p = std::filesystem::temp_directory_path() /
             ("clink_kinesis_" + std::to_string(::getpid()) + "_" + tag);
    std::filesystem::remove_all(p);
    std::filesystem::create_directories(p);
    return p;
}

// Runs `old_p` subtasks part way, checkpoints each into `uri`, restores
// `new_p` subtasks through the factory with the coordinator's parent mapping,
// and reads on. Returns every record each run emitted.
std::multiset<std::string> rescale_through_backend(const std::shared_ptr<KinesisModel>& model,
                                                   const std::string& from_uri,
                                                   const std::string& to_uri,
                                                   std::uint32_t old_p,
                                                   std::uint32_t new_p) {
    std::multiset<std::string> emitted;
    for (std::uint32_t i = 0; i < old_p; ++i) {
        clink::StateBackendSpec spec;
        spec.uri = from_uri;
        spec.subtask_idx = i;
        auto built = clink::StateBackendFactory::default_instance().build(spec);
        auto o = options(model, i, old_p);
        o.max_records_per_poll = 3;
        Reader r{o};
        r.source->open();
        // Each subtask reads part of what it owns.
        run_until({&r}, [&] { return r.got.size() >= 3 || r.source->listings() >= 3; });
        r.source->snapshot_offset(*built.backend, kOp, CheckpointId{9});
        (void)built.backend->snapshot(CheckpointId{9});
        emitted.insert(r.got.begin(), r.got.end());
    }
    std::vector<std::unique_ptr<Reader>> readers;
    std::vector<std::unique_ptr<clink::BuiltStateBackend>> keep;
    for (std::uint32_t i = 0; i < new_p; ++i) {
        const auto mapping = clink::cluster::rescale_parent_mapping(old_p, new_p, i);
        if (!mapping.ok) {
            ADD_FAILURE() << mapping.error;
            return emitted;
        }
        clink::StateBackendSpec spec;
        spec.uri = to_uri;
        spec.subtask_idx = i;
        spec.restore_uri = from_uri;
        spec.restore_checkpoint_id = 9;
        spec.restore_from_subtask_idx = mapping.parent_idx;
        spec.restore_from_parent_count = mapping.parent_count;
        spec.generation = 2;
        auto built = std::make_unique<clink::BuiltStateBackend>(
            clink::StateBackendFactory::default_instance().build(spec));
        if (built->restore_from.has_value()) {
            const auto [first, last] = clink::key_group_range_for_subtask(i, new_p);
            built->backend->restore(*built->restore_from, clink::KeyGroupRange{first, last});
        }
        auto r = std::make_unique<Reader>(options(model, i, new_p));
        r->source->restore_offset(*built->backend, kOp);
        r->source->open();  // throws when this subtask lacks rows it needs
        readers.push_back(std::move(r));
        keep.push_back(std::move(built));
    }
    std::vector<Reader*> all;
    for (auto& r : readers) {
        all.push_back(r.get());
    }
    const auto total = model->all_data().size();
    run_until(all, [&] {
        std::size_t n = emitted.size();
        for (auto* r : all) {
            n += r->got.size();
        }
        return n >= total;
    });
    run_until(all, [] { return false; }, 100ms);
    for (auto* r : all) {
        emitted.insert(r->got.begin(), r->got.end());
    }
    return emitted;
}

std::shared_ptr<KinesisModel> eight_shards() {
    auto model = std::make_shared<KinesisModel>();
    for (int n = 0; n < 8; ++n) {
        model->create(shard_name(n));
        fill(*model, shard_name(n), "s" + std::to_string(n), 7);
    }
    return model;
}

// file:// hands every new subtask every previous subtask's operator rows, so
// each shard's new owner resumes it exactly where its old owner stopped.
TEST(KinesisSourceRescale, FileBackendCarriesEveryPositionToItsNewOwner) {
    for (const auto& [old_p, new_p] :
         std::vector<std::pair<std::uint32_t, std::uint32_t>>{{2, 4}, {3, 1}, {1, 4}, {4, 2}}) {
        SCOPED_TRACE(std::to_string(old_p) + "->" + std::to_string(new_p));
        const auto model = eight_shards();
        const auto dir = scratch_dir("file_" + std::to_string(old_p) + "_" + std::to_string(new_p));
        const auto emitted = rescale_through_backend(model,
                                                     "file://" + (dir / "a").string(),
                                                     "file://" + (dir / "b").string(),
                                                     old_p,
                                                     new_p);
        EXPECT_EQ(emitted, as_multiset(model->all_data()));
        std::filesystem::remove_all(dir);
    }
}

#if defined(CLINK_AWS_TESTS_HAVE_ROCKSDB)
// rocksdb:// hands a new subtask only the previous subtasks mapped to it (the
// runtime rescales by integer factors). Scaling 1 to 2 hands each new subtask
// the only previous one, and 2 to 1 hands it both: those work. Scaling 2 to 4
// hands each new subtask one of two: refused at open, before anything is read.
TEST(KinesisSourceRescale, RocksDbBackendRescalesOnlyWhenEveryRowArrives) {
    clink::rocksdb::install();
    for (const auto& [old_p, new_p] :
         std::vector<std::pair<std::uint32_t, std::uint32_t>>{{1, 2}, {2, 1}}) {
        SCOPED_TRACE(std::to_string(old_p) + "->" + std::to_string(new_p));
        const auto model = eight_shards();
        const auto dir = scratch_dir("rocks_ok_" + std::to_string(old_p));
        const auto emitted = rescale_through_backend(model,
                                                     "rocksdb://" + (dir / "a").string(),
                                                     "rocksdb://" + (dir / "b").string(),
                                                     old_p,
                                                     new_p);
        EXPECT_EQ(emitted, as_multiset(model->all_data()));
        std::filesystem::remove_all(dir);
    }
    {
        const auto model = eight_shards();
        const auto dir = scratch_dir("rocks_refused");
        const auto msg = expect_throw_message([&] {
            (void)rescale_through_backend(model,
                                          "rocksdb://" + (dir / "a").string(),
                                          "rocksdb://" + (dir / "b").string(),
                                          2,
                                          4);
        });
        EXPECT_NE(msg.find("without the rows of previous subtask"), std::string::npos) << msg;
        std::filesystem::remove_all(dir);
    }
}
#endif

// ---- failures ---------------------------------------------------------------------

// Throttling and transient failures are retried with a backoff and nothing is
// lost; a failure that outlasts the retry budget fails the task naming it.
TEST(KinesisSourceFailures, ThrottlingAndTransientFailuresAreRetriedThenBounded) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    fill(*model, shard_name(0), "t", 5);
    int throttles = 3;
    int list_failures = 2;
    model->fault = [&](KinesisCall call, const std::string&) {
        if (call == KinesisCall::ListShards && list_failures > 0) {
            --list_failures;
            return service_error(call, "InternalFailure", 500);
        }
        if (call == KinesisCall::GetRecords && throttles > 0) {
            --throttles;
            return service_error(call, "ProvisionedThroughputExceededException");
        }
        return clink::aws::KinesisCallError{};
    };
    Reader r{options(model)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 5; }));
    EXPECT_EQ(throttles, 0);
    EXPECT_EQ(list_failures, 0);
    EXPECT_EQ(r.got, model->data_of(shard_name(0)));

    // Throttled for longer than the budget.
    model->fault = [](KinesisCall call, const std::string&) {
        return call == KinesisCall::GetRecords
                   ? service_error(call, "ProvisionedThroughputExceededException")
                   : clink::aws::KinesisCallError{};
    };
    model->put(shard_name(0), "more");
    auto o = options(model);
    o.retry_timeout = 150ms;
    Reader bounded{o};
    bounded.source->open();
    const auto msg = expect_throw_message([&] { run_until({&bounded}, [] { return false; }, 3s); });
    EXPECT_NE(msg.find("ProvisionedThroughputExceededException"), std::string::npos) << msg;
    EXPECT_NE(msg.find("has failed for"), std::string::npos) << msg;
}

// A permanent failure (denied access, a missing stream) fails at once with a
// message that says what to check, instead of retrying it.
TEST(KinesisSourceFailures, PermanentFailuresFailAtOnce) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    model->put(shard_name(0), "x");
    model->fault = [](KinesisCall call, const std::string&) {
        return call == KinesisCall::GetRecords ? service_error(call, "AccessDeniedException")
                                               : clink::aws::KinesisCallError{};
    };
    Reader denied{options(model)};
    denied.source->open();
    const auto started = std::chrono::steady_clock::now();
    const auto msg = expect_throw_message([&] { run_until({&denied}, [] { return false; }, 3s); });
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s) << "a permanent failure was retried";
    EXPECT_NE(msg.find("AccessDeniedException"), std::string::npos) << msg;
    EXPECT_NE(msg.find("kinesis:GetRecords"), std::string::npos) << msg;

    model->fault = [](KinesisCall call, const std::string&) {
        return call == KinesisCall::ListShards ? service_error(call, "ResourceNotFoundException")
                                               : clink::aws::KinesisCallError{};
    };
    Reader missing{options(model)};
    missing.source->open();
    const auto msg2 =
        expect_throw_message([&] { run_until({&missing}, [] { return false; }, 3s); });
    EXPECT_NE(msg2.find("stream exists"), std::string::npos) << msg2;
}

// A listing whose NextToken expires restarts from the first page.
TEST(KinesisSourceFailures, AnExpiredListingTokenRestartsTheListing) {
    const auto model = std::make_shared<KinesisModel>();
    for (int n = 0; n < 6; ++n) {
        model->create(shard_name(n));
        model->put(shard_name(n), "v" + std::to_string(n));
    }
    model->page_size = 2;
    bool expired_once = false;
    int list_calls = 0;
    // The hook runs under the model's lock: it keeps its own count.
    model->fault = [&](KinesisCall call, const std::string&) {
        if (call == KinesisCall::ListShards && ++list_calls == 2 && !expired_once) {
            expired_once = true;
            return service_error(call, "ExpiredNextTokenException");
        }
        return clink::aws::KinesisCallError{};
    };
    Reader r{options(model)};
    r.source->open();
    ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == 6; }));
    EXPECT_TRUE(expired_once);
}

// ---- cancellation and bounded state ------------------------------------------------

TEST(KinesisSourceCancel, CancelInterruptsWaitsAndCallsInFlight) {
    const auto model = std::make_shared<KinesisModel>();
    model->create(shard_name(0));
    auto o = options(model);
    o.poll_interval = 10s;  // a caught-up shard would wait this long
    Reader idle{o};
    idle.source->open();
    ASSERT_TRUE(run_until({&idle}, [&] { return model->get_records_calls(shard_name(0)) >= 1; }));
    std::thread canceller([&] {
        std::this_thread::sleep_for(30ms);
        idle.source->cancel();
    });
    const auto started = std::chrono::steady_clock::now();
    while (idle.source->produce(idle.emitter)) {
    }
    canceller.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started, 1s);

    // A GetRecords that does not return until the call is aborted.
    const auto hung = std::make_shared<KinesisModel>();
    hung->create(shard_name(0));
    hung->hang_get_records = true;
    Reader blocked{options(hung)};
    blocked.source->open();
    std::thread canceller2([&] {
        std::this_thread::sleep_for(50ms);
        blocked.source->cancel();
    });
    const auto started2 = std::chrono::steady_clock::now();
    while (blocked.source->produce(blocked.emitter)) {
    }
    canceller2.join();
    EXPECT_LT(std::chrono::steady_clock::now() - started2, 2s);
}

// Many reshards over a long run: tracked shards and checkpoint rows stay
// bounded by the stream's listing, because drained shards that have left the
// stream are forgotten.
TEST(KinesisSourceState, TrackedShardsAndRowsStayBoundedAcrossManyReshards) {
    const auto model = std::make_shared<KinesisModel>();
    int next = 0;
    std::string open_shard = model->create(shard_name(next++));
    Reader r{options(model)};
    r.source->open();
    std::size_t expected = 0;
    std::size_t most_tracked = 0;
    InMemoryStateBackend backend;
    for (int round = 0; round < 25; ++round) {
        model->put(open_shard, "r" + std::to_string(round));
        ++expected;
        const auto left = shard_name(next++);
        const auto right = shard_name(next++);
        model->split(open_shard, left, right);
        model->close(right);
        // The parent and the closed sibling leave the stream after a while.
        if (round >= 2) {
            model->expire(shard_name(next - 5));
            model->expire(shard_name(next - 6));
        }
        open_shard = left;
        ASSERT_TRUE(run_until({&r}, [&] { return r.got.size() == expected; }));
        run_until({&r}, [] { return false; }, 220ms);  // two listings
        most_tracked = std::max(most_tracked, r.source->shard_statuses().size());
        r.source->snapshot_offset(
            backend, kOp, CheckpointId{static_cast<std::uint64_t>(round + 1)});
    }
    EXPECT_LE(most_tracked, 8U) << "drained shards that left the stream were kept";
    std::size_t rows = 0;
    backend.scan_operator_state(kOp,
                                [&](StateBackend::KeyView, StateBackend::ValueView) { ++rows; });
    EXPECT_LE(rows, 9U);
}

}  // namespace
