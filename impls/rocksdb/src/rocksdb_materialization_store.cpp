#include "clink/rocksdb/rocksdb_materialization_store.hpp"

#include <stdexcept>
#include <utility>

#ifdef CLINK_HAS_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#endif

namespace clink::rocksdb {

#ifdef CLINK_HAS_ROCKSDB

struct RocksDbMaterializationStore::Impl {
    std::unique_ptr<::rocksdb::DB> db;
};

RocksDbMaterializationStore::RocksDbMaterializationStore(std::filesystem::path path)
    : impl_(std::make_unique<Impl>()), path_(std::move(path)) {
    std::error_code ec;
    std::filesystem::create_directories(path_, ec);
    ::rocksdb::Options opts;
    opts.create_if_missing = true;
    std::unique_ptr<::rocksdb::DB> db_owned;
    auto status = ::rocksdb::DB::Open(opts, path_.string(), &db_owned);
    if (!status.ok()) {
        throw std::runtime_error("RocksDbMaterializationStore::open failed: " + status.ToString() +
                                 " (path=" + path_.string() + ")");
    }
    impl_->db = std::move(db_owned);
}

RocksDbMaterializationStore::~RocksDbMaterializationStore() = default;

std::string RocksDbMaterializationStore::write(clink::CheckpointId id,
                                               std::span<const std::byte> bytes) {
    const std::string key = "mat:" + std::to_string(id.value());
    ::rocksdb::Slice value(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    auto status = impl_->db->Put(::rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("RocksDbMaterializationStore::write Put failed: " +
                                 status.ToString());
    }
    return key;
}

std::vector<std::byte> RocksDbMaterializationStore::read(const std::string& handle) {
    std::string value;
    auto status = impl_->db->Get(::rocksdb::ReadOptions(), handle, &value);
    if (!status.ok()) {
        throw std::runtime_error("RocksDbMaterializationStore::read Get failed for handle='" +
                                 handle + "': " + status.ToString());
    }
    std::vector<std::byte> out(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(value[i]));
    }
    return out;
}

void RocksDbMaterializationStore::erase(const std::string& handle) {
    auto status = impl_->db->Delete(::rocksdb::WriteOptions(), handle);
    (void)status;  // best-effort cleanup
}

std::string RocksDbMaterializationStore::description() const {
    return "rocksdb://" + path_.string();
}

#else  // !CLINK_HAS_ROCKSDB

struct RocksDbMaterializationStore::Impl {};

RocksDbMaterializationStore::RocksDbMaterializationStore(std::filesystem::path) : impl_(nullptr) {
    throw std::runtime_error(
        "RocksDbMaterializationStore: built without CLINK_HAS_ROCKSDB; "
        "configure with -DCLINK_BUILD_ROCKSDB=ON");
}

RocksDbMaterializationStore::~RocksDbMaterializationStore() = default;

std::string RocksDbMaterializationStore::write(clink::CheckpointId, std::span<const std::byte>) {
    throw std::runtime_error("RocksDbMaterializationStore: unavailable in this build");
}

std::vector<std::byte> RocksDbMaterializationStore::read(const std::string&) {
    throw std::runtime_error("RocksDbMaterializationStore: unavailable in this build");
}

void RocksDbMaterializationStore::erase(const std::string&) {}

std::string RocksDbMaterializationStore::description() const {
    return "rocksdb (unavailable)";
}

#endif

}  // namespace clink::rocksdb
