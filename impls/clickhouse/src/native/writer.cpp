#include "native/writer.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native {

struct Writer::Core {};

Writer::Writer(WriterConfig /*config*/,
               std::unique_ptr<InsertTransport> /*transport*/,
               TokenSource /*tokens*/) {
    not_implemented("Writer");
}

Writer::~Writer() = default;

void Writer::submit(Chunk /*chunk*/) {
    not_implemented("Writer::submit");
}

void Writer::flush(std::uint64_t /*checkpoint_id*/) {
    not_implemented("Writer::flush");
}

void Writer::finish() {
    not_implemented("Writer::finish");
}

void Writer::abort() noexcept {}

WriterStats Writer::stats() const {
    not_implemented("Writer::stats");
}

std::size_t Writer::queue_bytes() const noexcept {
    return 0;
}

}  // namespace clink::clickhouse::native
