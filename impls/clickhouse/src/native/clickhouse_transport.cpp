#include "native/clickhouse_transport.hpp"

#include <cerrno>
#include <cstddef>
#include <optional>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

#include <clickhouse/columns/itemview.h>
#include <clickhouse/exceptions.h>
#include <clickhouse/types/types.h>
#include <sys/stat.h>

#include "native/errors.hpp"
#include "native/socket_factory.hpp"

namespace clink::clickhouse::native {

namespace {

constexpr std::string_view kPrefix = "clickhouse_native_sink: ";

[[noreturn]] void refuse(const char* code, const std::string& message) {
    throw NativeSinkError(code, std::string(kPrefix) + message);
}

#if !defined(CLINK_CLICKHOUSE_NATIVE_TLS)
[[noreturn]] void refuse_tls_unavailable() {
    refuse(code::kTlsUnavailable,
           "secure='true' is not available: this build of the native sink was compiled without "
           "TLS support");
}
#endif

[[noreturn]] void throw_interrupted() {
    throw std::system_error(ECONNABORTED,
                            std::system_category(),
                            "clickhouse native sink: the transport was interrupted");
}

void require_timeout(std::chrono::milliseconds value, const char* key) {
    if (value.count() < 1) {
        refuse(code::kOptionInvalid,
               "option '" + std::string(key) + "' must be at least 1 ms; got " +
                   std::to_string(value.count()));
    }
}

::clickhouse::CompressionMethod client_compression(Compression compression) {
    switch (compression) {
        case Compression::None:
            return ::clickhouse::CompressionMethod::None;
        case Compression::Lz4:
            return ::clickhouse::CompressionMethod::LZ4;
        case Compression::Zstd:
            // The client compresses ZSTD at its own fixed fast level; there is
            // no level to pass.
            return ::clickhouse::CompressionMethod::ZSTD;
    }
    return ::clickhouse::CompressionMethod::LZ4;
}

// Why `path` cannot serve as a CA, or nothing when it can. OpenSSL only
// records a CA directory when the socket factory is built and looks inside it
// during the handshake, so without this a missing or unsearchable directory
// would open a socket and fail as a verify error that names no key, or pass
// unnoticed with tls_verify off. A file is checked the same way, so that its
// refusal does not depend on what OpenSSL makes of a path it cannot open.
std::optional<std::string> ca_path_problem(const std::string& path, bool directory) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0) {
        const int err = errno;
        if (err == ENOENT || err == ENOTDIR) {
            return std::string("does not exist");
        }
        return "cannot be reached: " + std::system_category().message(err);
    }
    const bool is_directory = S_ISDIR(st.st_mode);
    if (directory && !is_directory) {
        return std::string("is not a directory");
    }
    if (!directory && is_directory) {
        return std::string("is a directory; a directory of CA certificates goes in 'tls_ca_dir'");
    }
    // OpenSSL opens the certificates in a directory by their hashed names and
    // never lists it, so a directory needs search permission, not read.
    if (::access(path.c_str(), directory ? X_OK : R_OK) != 0) {
        const int err = errno;
        return std::string(directory ? "cannot be searched: " : "cannot be read: ") +
               std::system_category().message(err);
    }
    return std::nullopt;
}

void require_ca_path(const char* key, const std::string& path, bool directory) {
    if (path.empty()) {
        return;
    }
    if (const std::optional<std::string> problem = ca_path_problem(path, directory)) {
        refuse(code::kOptionInvalid,
               "option '" + std::string(key) + "' ('" + path + "') " + *problem);
    }
}

// Which key a CA that OpenSSL refused came from. Both paths have passed
// require_ca_path by then, and OpenSSL loads nothing from a directory until
// the handshake, so with both keys set the file is the one it refused.
std::string ca_refusal(const TlsOptions& tls, const std::string& openssl_text) {
    std::string subject;
    if (!tls.ca_file.empty()) {
        subject = "option 'tls_ca_file' ('" + tls.ca_file + "')";
    } else if (!tls.ca_dir.empty()) {
        subject = "option 'tls_ca_dir' ('" + tls.ca_dir + "')";
    } else {
        return "secure='true': OpenSSL could not set up the default CA locations: " + openssl_text;
    }
    return subject + " could not be loaded by OpenSSL: " + openssl_text;
}

bool is_string_type(const ::clickhouse::Type& type) {
    return type.GetCode() == ::clickhouse::Type::String;
}

bool is_nullable_string_type(const ::clickhouse::Type& type) {
    return type.GetCode() == ::clickhouse::Type::Nullable &&
           is_string_type(*type.As<::clickhouse::NullableType>()->GetNestedType());
}

// The column types a metadata result may carry. Every metadata SELECT casts
// what it selects to String, so anything else means the query or the server
// is not what the sink expects.
bool is_text_type(const ::clickhouse::Type& type) {
    switch (type.GetCode()) {
        case ::clickhouse::Type::String:
            return true;
        case ::clickhouse::Type::Nullable:
            return is_nullable_string_type(type);
        case ::clickhouse::Type::LowCardinality: {
            const auto nested = type.As<::clickhouse::LowCardinalityType>()->GetNestedType();
            return is_string_type(*nested) || is_nullable_string_type(*nested);
        }
        default:
            return false;
    }
}

// GetItem resolves the dictionary of a LowCardinality column and the null map
// of a Nullable one; a NULL comes back as a Void item.
std::string cell_text(const ::clickhouse::Column& column, std::size_t row) {
    const ::clickhouse::ItemView item = column.GetItem(row);
    if (item.type == ::clickhouse::Type::Void) {
        return {};
    }
    return std::string(item.AsBinaryData());
}

}  // namespace

::clickhouse::ClientOptions make_client_options(const SinkOptions& options,
                                                const Endpoint& endpoint,
                                                const std::optional<CaLocation>& fallback_ca) {
    require_timeout(options.connect_timeout, "connect_timeout_ms");
    require_timeout(options.send_timeout, "send_timeout_ms");
    require_timeout(options.receive_timeout, "receive_timeout_ms");

    ::clickhouse::ClientOptions opts;
    // One endpoint and no host: the client would otherwise put host:port in
    // front of the list and fail over by itself, always from the first entry
    // and never on a TLS error. The sink rotates endpoints itself.
    opts.SetEndpoints({::clickhouse::Endpoint{endpoint.host, endpoint.port}})
        .SetDefaultDatabase(options.database)
        .SetUser(options.user)
        .SetPassword(options.password)
        .SetRethrowException(true)
        .SetPingBeforeQuery(false)
        .SetSendRetries(1)
        .SetConnectionConnectTimeout(options.connect_timeout)
        .SetConnectionSendTimeout(options.send_timeout)
        .SetConnectionRecvTimeout(options.receive_timeout)
        .SetCompressionMethod(client_compression(options.compression));

    if (options.tls.enabled) {
#if defined(CLINK_CLICKHOUSE_NATIVE_TLS)
        // The client ignores these once it is given a socket factory; the
        // inner SSLSocketFactory reads them from here instead.
        ::clickhouse::ClientOptions::SSLOptions ssl;
        ssl.SetUseSNI(true).SetSkipVerification(!options.tls.verify);
        if (!options.tls.ca_file.empty()) {
            ssl.SetPathToCAFiles({options.tls.ca_file});
        }
        if (!options.tls.ca_dir.empty()) {
            ssl.SetPathToCADirectory(options.tls.ca_dir);
        }
        // A named CA is the only one trusted: the system store would widen it.
        if (!options.tls.ca_file.empty() || !options.tls.ca_dir.empty()) {
            ssl.SetUseDefaultCALocations(false);
        } else if (fallback_ca) {
            // The linked OpenSSL looks where this system keeps no CAs (a
            // static OpenSSL built elsewhere); the defaults stay on as well.
            if (fallback_ca->directory) {
                ssl.SetPathToCADirectory(fallback_ca->path);
            } else {
                ssl.SetPathToCAFiles({fallback_ca->path});
            }
        }
        opts.SetSSLOptions(ssl);
#else
        (void)fallback_ca;
        refuse_tls_unavailable();
#endif
    }
    return opts;
}

std::vector<HeaderColumn> header_columns(const ::clickhouse::Block& header) {
    std::vector<HeaderColumn> out;
    out.reserve(header.GetColumnCount());
    for (::clickhouse::Block::Iterator it(header); it.IsValid(); it.Next()) {
        out.push_back(HeaderColumn{it.Name(), it.Type()->GetName()});
    }
    return out;
}

void append_result_block(const ::clickhouse::Block& block, ResultSet& out) {
    const std::size_t columns = block.GetColumnCount();
    if (columns == 0) {
        return;
    }
    if (out.columns.empty()) {
        out.columns.reserve(columns);
        for (::clickhouse::Block::Iterator it(block); it.IsValid(); it.Next()) {
            out.columns.push_back(it.Name());
        }
    } else {
        bool same = out.columns.size() == columns;
        for (std::size_t c = 0; same && c < columns; ++c) {
            same = block.GetColumnName(c) == out.columns[c];
        }
        if (!same) {
            throw ::clickhouse::ProtocolError(
                "clickhouse native sink: a metadata result changed its columns between blocks");
        }
    }
    // Checked on the header block too, which has no rows, so a wrong type is
    // reported even for an empty result.
    for (::clickhouse::Block::Iterator it(block); it.IsValid(); it.Next()) {
        if (!is_text_type(*it.Type())) {
            throw ::clickhouse::ProtocolError("clickhouse native sink: metadata column `" +
                                              it.Name() + "` is " + it.Type()->GetName() +
                                              "; only String, Nullable(String) and "
                                              "LowCardinality(String) are read");
        }
    }
    const std::size_t rows = block.GetRowCount();
    out.rows.reserve(out.rows.size() + rows);
    for (std::size_t r = 0; r < rows; ++r) {
        std::vector<std::string> row;
        row.reserve(columns);
        for (std::size_t c = 0; c < columns; ++c) {
            row.push_back(cell_text(*block[c], r));
        }
        out.rows.push_back(std::move(row));
    }
}

ClickHouseTransport::ClickHouseTransport(SinkOptions options)
    : options_(std::move(options)), fallback_ca_(fallback_ca_location(options_.tls)) {
#if !defined(CLINK_CLICKHOUSE_NATIVE_TLS)
    if (options_.tls.enabled) {
        refuse_tls_unavailable();
    }
#endif
}

ClickHouseTransport::~ClickHouseTransport() {
    abandon();
}

void ClickHouseTransport::connect(const Endpoint& endpoint) {
    {
        const std::lock_guard<std::mutex> lock(mu_);
        if (interrupted_) {
            throw_interrupted();
        }
        if (client_) {
            return;
        }
    }

    const ::clickhouse::ClientOptions opts = make_client_options(options_, endpoint, fallback_ca_);
    if (options_.tls.enabled) {
        require_ca_path("tls_ca_dir", options_.tls.ca_dir, true);
        require_ca_path("tls_ca_file", options_.tls.ca_file, false);
    }
    auto control = std::make_shared<SocketControl>();
    {
        // Installed under the lock interrupt() takes, and only after a second
        // look at the flag: an interrupt either lands before this block and
        // fails the connect here, or after it and poisons this very control,
        // so it can never fall on the old one and be lost.
        const std::lock_guard<std::mutex> lock(mu_);
        if (interrupted_) {
            throw_interrupted();
        }
        if (control_) {
            retired_written_ += control_->bytes_written();
            retired_read_ += control_->bytes_read();
        }
        control->set_deadline(deadline_);
        control_ = control;
    }

    std::unique_ptr<CountingSocketFactory> factory;
    try {
        factory = std::make_unique<CountingSocketFactory>(opts, options_.tls.enabled, control);
    } catch (const ::clickhouse::OpenSSLError& e) {
        control->poison();
        refuse(code::kOptionInvalid, ca_refusal(options_.tls, e.what()));
    }

    std::unique_ptr<::clickhouse::Client> client;
    try {
        // The client connects and handshakes in its constructor.
        client = std::make_unique<::clickhouse::Client>(opts, std::move(factory));
    } catch (...) {
        control->poison();
        throw;
    }
    if (control->poisoned()) {
        // The interrupt came after the handshake. The client is idle and its
        // socket already shut down, so dropping it commits nothing.
        client.reset();
        throw_interrupted();
    }

    const ::clickhouse::ServerInfo& info = client->GetServerInfo();
    server_ = ServerIdentity{info.display_name,
                             info.version_major,
                             info.version_minor,
                             info.version_patch,
                             info.revision,
                             endpoint};
    client_ = std::move(client);
    const std::lock_guard<std::mutex> lock(mu_);
    ++connects_;
}

bool ClickHouseTransport::connected() const noexcept {
    return client_ != nullptr;
}

const ServerIdentity& ClickHouseTransport::server() const {
    return server_;
}

ResultSet ClickHouseTransport::select(MetaQuery /*kind*/, const std::string& sql) {
    ResultSet out;
    client().Select(sql,
                    [&out](const ::clickhouse::Block& block) { append_result_block(block, out); });
    return out;
}

std::vector<HeaderColumn> ClickHouseTransport::begin_insert(const std::string& sql) {
    return header_columns(client().BeginInsert(sql));
}

void ClickHouseTransport::send_block(const ::clickhouse::Block& block) {
    client().SendInsertBlock(block);
}

void ClickHouseTransport::end_insert() {
    client().EndInsert();
}

void ClickHouseTransport::abandon() noexcept {
    std::shared_ptr<SocketControl> control;
    {
        const std::lock_guard<std::mutex> lock(mu_);
        control = control_;
    }
    // Poison first. The client's destructor ends an open INSERT by sending the
    // end-of-data marker, which would commit every block already sent; on a
    // poisoned socket that first write fails, the destructor swallows it, and
    // the server discards the INSERT.
    if (control) {
        control->poison();
    }
    client_.reset();
}

void ClickHouseTransport::interrupt() noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    interrupted_ = true;
    if (control_) {
        control_->poison();
    }
}

void ClickHouseTransport::set_deadline(
    std::optional<std::chrono::steady_clock::time_point> deadline) noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    deadline_ = deadline;
    if (control_) {
        control_->set_deadline(deadline);
    }
}

TransportCounters ClickHouseTransport::counters() const noexcept {
    const std::lock_guard<std::mutex> lock(mu_);
    TransportCounters out;
    out.bytes_written = retired_written_;
    out.bytes_read = retired_read_;
    if (control_) {
        out.bytes_written += control_->bytes_written();
        out.bytes_read += control_->bytes_read();
    }
    out.connects = connects_;
    return out;
}

::clickhouse::Client& ClickHouseTransport::client() {
    if (!client_) {
        throw std::system_error(ENOTCONN,
                                std::system_category(),
                                "clickhouse native sink: the transport is not connected");
    }
    return *client_;
}

std::unique_ptr<InsertTransport> make_clickhouse_transport(const SinkOptions& options) {
    return std::make_unique<ClickHouseTransport>(options);
}

}  // namespace clink::clickhouse::native
