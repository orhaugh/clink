#include "clink/cassandra/cassandra_upsert_sink.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "clink/cassandra/cassandra_rules.hpp"
#include "clink/runtime/logging.hpp"

#include "sink_core.hpp"

namespace clink::cassandra {

namespace {

detail::SinkCoreOptions core_options(CassandraUpsertSink::Options& o) {
    const auto refuse = [&o](const std::string& why) {
        throw std::runtime_error(o.name + ": " + why);
    };
    if (o.max_buffered_records < 1 || o.max_buffered_records > detail::kMaxBufferedRecordsLimit) {
        refuse("'max_buffered_records' must be 1 to " +
               std::to_string(detail::kMaxBufferedRecordsLimit) + "; it is " +
               std::to_string(o.max_buffered_records));
    }
    if (o.max_buffered_bytes < detail::kMinBytesBound ||
        o.max_buffered_bytes > detail::kMaxBytesBound) {
        refuse("'max_buffered_bytes' must be " + std::to_string(detail::kMinBytesBound) + " to " +
               std::to_string(detail::kMaxBytesBound) + "; it is " +
               std::to_string(o.max_buffered_bytes));
    }
    return detail::checked_options(o.name,
                                   o.conn,
                                   o.keyspace,
                                   o.table,
                                   o.key_columns,
                                   /*keyed=*/true,
                                   o.max_in_flight,
                                   o.max_in_flight_bytes,
                                   std::move(o.service));
}

}  // namespace

struct CassandraUpsertSink::Impl {
    Impl(detail::SinkCoreOptions o, std::size_t max_records, std::size_t max_bytes)
        : core(std::move(o)), max_buffered_records(max_records), max_buffered_bytes(max_bytes) {}

    // The latest record for one row, waiting to be written.
    struct Entry {
        bool is_delete{false};
        std::string insert_json;
        std::vector<std::string> key_json;
        std::uint64_t seq{0};  // the position of the record that set it
        std::size_t bytes{0};
    };

    detail::SinkCore core;
    std::size_t max_buffered_records;
    std::size_t max_buffered_bytes;
    std::unordered_map<std::string, Entry> buffer;  // identity -> latest record
    std::size_t buffer_bytes{0};
    std::uint64_t seq{0};

    void clear_buffer() {
        buffer.clear();
        buffer_bytes = 0;
        core.note_buffered(0);
    }

    // Hands every buffered row to the server, in the order of the records that
    // set them. Waits only for room in the window, not for acknowledgements.
    void write_out(RuntimeContext* rt) {
        if (buffer.empty()) {
            return;
        }
        core.reach("before_write");
        std::vector<Entry> rows;
        rows.reserve(buffer.size());
        for (auto& [identity, entry] : buffer) {
            (void)identity;
            rows.push_back(std::move(entry));
        }
        clear_buffer();
        std::sort(
            rows.begin(), rows.end(), [](const Entry& a, const Entry& b) { return a.seq < b.seq; });
        for (std::size_t i = 0; i < rows.size(); ++i) {
            core.note_buffered(rows.size() - i - 1);
            auto& r = rows[i];
            CassandraWrite w;
            w.kind = r.is_delete ? CassandraWrite::Kind::Delete : CassandraWrite::Kind::Insert;
            w.json = std::move(r.insert_json);
            w.key_json = std::move(r.key_json);
            core.submit(rt, std::move(w), r.bytes);
        }
        core.note_buffered(0);
    }

    void add(RuntimeContext* rt, CassandraUpsertRecord rec) {
        std::size_t bytes = rec.identity.size();
        if (rec.is_delete) {
            for (const auto& k : rec.key_json) {
                bytes += k.size();
            }
        } else {
            bytes += rec.insert_json.size();
        }
        auto it = buffer.find(rec.identity);
        const std::size_t count = buffer.size() + (it == buffer.end() ? 1 : 0);
        const std::size_t total =
            buffer_bytes - (it == buffer.end() ? 0 : it->second.bytes) + bytes;
        if (!buffer.empty() && (count > max_buffered_records || total > max_buffered_bytes)) {
            write_out(rt);
            it = buffer.end();
        }
        Entry e;
        e.is_delete = rec.is_delete;
        e.insert_json = std::move(rec.insert_json);
        e.key_json = std::move(rec.key_json);
        e.seq = ++seq;
        e.bytes = bytes;
        if (it == buffer.end()) {
            buffer.emplace(std::move(rec.identity), std::move(e));
        } else {
            buffer_bytes -= it->second.bytes;
            it->second = std::move(e);
        }
        buffer_bytes += bytes;
        core.note_buffered(buffer.size());
        if (buffer_bytes > max_buffered_bytes) {
            // A record larger than the bound, buffered alone: written at once.
            write_out(rt);
        }
    }
};

CassandraUpsertSink::CassandraUpsertSink(Options opts) {
    const std::size_t max_records = opts.max_buffered_records;
    const std::size_t max_bytes = opts.max_buffered_bytes;
    auto core = core_options(opts);
    impl_ = std::make_unique<Impl>(std::move(core), max_records, max_bytes);
}

CassandraUpsertSink::~CassandraUpsertSink() = default;

void CassandraUpsertSink::open() {
    impl_->clear_buffer();
    impl_->seq = 0;
    impl_->core.open(runtime());
}

void CassandraUpsertSink::on_data(const Batch<std::string>& batch) {
    auto& core = impl_->core;
    core.check_usable();
    std::vector<CassandraUpsertRecord> prepared;
    prepared.reserve(batch.size());
    std::size_t i = 0;
    for (const auto& rec : batch) {
        ++i;
        try {
            prepared.push_back(prepare_cassandra_upsert_record(rec.value(), core.key()));
        } catch (const CassandraRecordError& e) {
            const std::size_t buffered = impl_->buffer.size();
            impl_->clear_buffer();
            core.fail("record " + std::to_string(i) + " of a batch of " +
                          std::to_string(batch.size()) + " is refused: " + e.what() +
                          ". Nothing of that batch was buffered or written",
                      buffered);
        }
    }
    auto* rt = runtime();
    for (auto& p : prepared) {
        impl_->add(rt, std::move(p));
    }
}

void CassandraUpsertSink::on_barrier(CheckpointBarrier /*b*/) {
    flush();
}

void CassandraUpsertSink::flush() {
    auto& core = impl_->core;
    core.check_usable();
    auto* rt = runtime();
    impl_->write_out(rt);
    if (core.in_flight() == 0) {
        return;
    }
    core.reach("in_flight");
    core.drain(rt);
    core.reach("after_write");
}

void CassandraUpsertSink::close() {
    if (!impl_->buffer.empty()) {
        clink::log::warn("cassandra.sink",
                         impl_->core.name() + ": closing with " +
                             std::to_string(impl_->buffer.size()) +
                             " buffered row(s) not written; no barrier covers them");
    }
    impl_->clear_buffer();
    impl_->core.close();
}

std::string CassandraUpsertSink::name() const {
    return impl_->core.name();
}

bool CassandraUpsertSink::failed() const {
    return impl_->core.failed();
}

std::size_t CassandraUpsertSink::buffered_rows() const {
    return impl_->buffer.size();
}

std::size_t CassandraUpsertSink::in_flight() const {
    return impl_->core.in_flight();
}

}  // namespace clink::cassandra
