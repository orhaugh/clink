// CasManifest (DISAGG-6): the per-checkpoint content-addressed manifest codec.
// A decode that silently mis-parses = a wrong restore, so this pins the
// round-trip, the deterministic (sorted, byte-identical) encode, and tolerant
// rejection of malformed / wrong-version input.

#include <string>

#include <gtest/gtest.h>

#include "clink/s3/cas_manifest.hpp"

using clink::s3::CasManifest;
using clink::s3::CasManifestEntry;

namespace {

const std::string kH1 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const std::string kH2 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const std::string kH3 = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";

}  // namespace

TEST(CasManifest, RoundTrip) {
    CasManifest m;
    m.checkpoint_id = 88;
    m.subtask = 3;
    m.entries = {{"000007.sst", kH1, 4096}, {"CURRENT", kH2, 16}, {"MANIFEST-000005", kH3, 512}};

    const auto decoded = CasManifest::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->checkpoint_id, 88u);
    EXPECT_EQ(decoded->subtask, 3u);
    ASSERT_EQ(decoded->entries.size(), 3u);
    // entries come back sorted by name
    EXPECT_EQ(decoded->entries[0].name, "000007.sst");
    EXPECT_EQ(decoded->entries[0].hash, kH1);
    EXPECT_EQ(decoded->entries[0].size, 4096u);
    EXPECT_EQ(decoded->entries[1].name, "CURRENT");
    EXPECT_EQ(decoded->entries[2].name, "MANIFEST-000005");
}

TEST(CasManifest, EncodeIsDeterministicRegardlessOfInputOrder) {
    CasManifest a;
    a.checkpoint_id = 1;
    a.entries = {{"c.sst", kH1, 1}, {"a.sst", kH2, 2}, {"b.sst", kH3, 3}};
    CasManifest b;
    b.checkpoint_id = 1;
    b.entries = {{"a.sst", kH2, 2}, {"b.sst", kH3, 3}, {"c.sst", kH1, 1}};
    // Same set, different insertion order -> byte-identical manifest (sorted).
    EXPECT_EQ(a.encode(), b.encode());
}

TEST(CasManifest, EmptyEntries) {
    CasManifest m;
    m.checkpoint_id = 5;
    m.subtask = 0;
    const auto decoded = CasManifest::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->checkpoint_id, 5u);
    EXPECT_TRUE(decoded->entries.empty());
}

TEST(CasManifest, RejectsWrongFormatTag) {
    EXPECT_FALSE(
        CasManifest::decode("clink-cas-manifest-v2\nid 1\nsubtask 0\nentries 0\n").has_value());
    EXPECT_FALSE(CasManifest::decode("garbage").has_value());
    EXPECT_FALSE(CasManifest::decode("").has_value());
}

TEST(CasManifest, RejectsMalformedEntries) {
    // bad hash (not 64-hex)
    EXPECT_FALSE(
        CasManifest::decode("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries 1\nzzz 10 a.sst\n")
            .has_value());
    // non-numeric size
    EXPECT_FALSE(CasManifest::decode("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries 1\n" + kH1 +
                                     " big a.sst\n")
                     .has_value());
    // entry-count mismatch (declares 2, supplies 1) = truncated manifest
    EXPECT_FALSE(CasManifest::decode("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries 2\n" + kH1 +
                                     " 10 a.sst\n")
                     .has_value());
    // missing name
    EXPECT_FALSE(
        CasManifest::decode("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries 1\n" + kH1 + " 10 \n")
            .has_value());
}

TEST(CasManifest, RejectsPathTraversalNames) {
    // A name that is not a single safe path component must be rejected: fetch
    // reconstructs <staging>/<name>, so "../x" / "/abs" / "sub/x" would write
    // outside the staging dir on restore.
    for (const char* bad : {"../escape", "/abs/path", "sub/dir.sst", "..", ".", "a\\b"}) {
        const std::string text =
            std::string("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries 1\n") + kH1 + " 10 " +
            bad + "\n";
        EXPECT_FALSE(CasManifest::decode(text).has_value()) << "must reject name: " << bad;
    }
    // A normal flat RocksDB filename is still accepted.
    EXPECT_TRUE(CasManifest::decode(std::string("clink-cas-manifest-v1\nid 1\nsubtask 0\nentries "
                                                "1\n") +
                                    kH1 + " 10 000123.sst\n")
                    .has_value());
}

TEST(CasManifest, RejectsBadHeader) {
    EXPECT_FALSE(
        CasManifest::decode("clink-cas-manifest-v1\nid x\nsubtask 0\nentries 0\n").has_value());
    EXPECT_FALSE(CasManifest::decode("clink-cas-manifest-v1\nid 1\nsubtask 0\n").has_value());
}
