#include "fake_transport.hpp"

#include "native/errors.hpp"

namespace clink::clickhouse::native::testing {

struct FakeServer::Impl {};
struct FakeTransport::Impl {};

FakeServer::FakeServer() : impl_(std::make_unique<Impl>()) {}
FakeServer::~FakeServer() = default;

void FakeServer::set_version(std::uint64_t, std::uint64_t, std::uint64_t) {
    not_implemented("FakeServer::set_version");
}
void FakeServer::set_settings(std::vector<std::pair<std::string, std::string>>) {
    not_implemented("FakeServer::set_settings");
}
void FakeServer::set_merge_tree_setting(const std::string&, const std::string&) {
    not_implemented("FakeServer::set_merge_tree_setting");
}
void FakeServer::add_table(FakeTable) {
    not_implemented("FakeServer::add_table");
}
void FakeServer::add_cluster(const std::string&, std::vector<FakeServer*>) {
    not_implemented("FakeServer::add_cluster");
}
void FakeServer::set_unreadable_replica(const std::string&, std::size_t) {
    not_implemented("FakeServer::set_unreadable_replica");
}
void FakeServer::alter_column_type(const std::string&, const std::string&, const std::string&) {
    not_implemented("FakeServer::alter_column_type");
}
void FakeServer::inject(Fault) {
    not_implemented("FakeServer::inject");
}
void FakeServer::set_down(bool) {
    not_implemented("FakeServer::set_down");
}
std::vector<LandedBlock> FakeServer::landed(const std::string&) const {
    not_implemented("FakeServer::landed");
}
std::uint64_t FakeServer::rows(const std::string&) const {
    not_implemented("FakeServer::rows");
}
std::vector<std::string> FakeServer::statements() const {
    not_implemented("FakeServer::statements");
}
std::size_t FakeServer::abandoned_mid_insert() const {
    not_implemented("FakeServer::abandoned_mid_insert");
}
std::size_t FakeServer::destroyed_mid_insert() const {
    not_implemented("FakeServer::destroyed_mid_insert");
}
std::size_t FakeServer::connects() const {
    not_implemented("FakeServer::connects");
}

FakeTransport::FakeTransport(std::shared_ptr<FakeServer>) {
    not_implemented("FakeTransport");
}
FakeTransport::~FakeTransport() = default;

void FakeTransport::connect(const Endpoint&) {
    not_implemented("FakeTransport::connect");
}
bool FakeTransport::connected() const noexcept {
    return false;
}
const ServerIdentity& FakeTransport::server() const {
    not_implemented("FakeTransport::server");
}
ResultSet FakeTransport::select(MetaQuery, const std::string&) {
    not_implemented("FakeTransport::select");
}
std::vector<HeaderColumn> FakeTransport::begin_insert(const std::string&) {
    not_implemented("FakeTransport::begin_insert");
}
void FakeTransport::send_block(const ::clickhouse::Block&) {
    not_implemented("FakeTransport::send_block");
}
void FakeTransport::end_insert() {
    not_implemented("FakeTransport::end_insert");
}
void FakeTransport::abandon() noexcept {}
void FakeTransport::interrupt() noexcept {}
void FakeTransport::set_deadline(std::optional<std::chrono::steady_clock::time_point>) noexcept {}
TransportCounters FakeTransport::counters() const noexcept {
    return {};
}

TransportFactory fake_factory(std::shared_ptr<FakeServer>) {
    not_implemented("fake_factory");
}

std::unique_ptr<InsertTransport> faulty(std::unique_ptr<InsertTransport>,
                                        std::shared_ptr<std::deque<Fault>>) {
    not_implemented("faulty");
}

}  // namespace clink::clickhouse::native::testing
