#pragma once

// PostgresJsonSink2PC - exactly-once Postgres sink via two-phase commit
// (PREPARE TRANSACTION / COMMIT PREPARED). The XA adopter of the generic
// CommittingSink base: rows since the last barrier are INSERTed into an open
// transaction; at the barrier the transaction is PREPAREd under a deterministic
// global id (the committable), and the framework COMMIT PREPAREs it once the
// checkpoint is globally durable (ROLLBACK PREPAREs on abort). A prepared
// transaction survives the session, so a crash between PREPARE and COMMIT does
// not lose it: on restart the base COMMIT PREPAREs any gid in the restored
// pending set, and on_open() ROLLBACK PREPAREs any of OUR prepared transactions
// NOT in that set (their checkpoint never became durable).
//
// Each input record is a JSON-object string (row_to_json_string on the SQL
// path). `columns` is the authoritative projection; a column absent from / null
// in a row becomes SQL NULL. Rows are written as batched multi-row INSERTs
// inside the transaction.
//
// Requires the server's max_prepared_transactions > 0 (it defaults to 0, which
// disables PREPARE TRANSACTION); a PREPARE against a server with it disabled
// fails the checkpoint loudly.
//
// The global id is "clink_<uid>_sub<N>_<ckpt>" (uid sanitised to [A-Za-z0-9_],
// falling back to the operator id). It is unique per (operator, subtask,
// checkpoint); reusing the same operator uid across two jobs writing to the same
// server is a misconfiguration (their gids collide, and reconciliation could
// roll back the other job's prepared transaction).

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <libpq-fe.h>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "clink/connectors/committing_sink.hpp"
#include "clink/connectors/postgres_json_sink.hpp"  // PgconnDeleter
#include "clink/connectors/postgres_sql.hpp"
#include "clink/metrics/connector_metrics.hpp"

namespace clink {

struct PostgresJsonSink2PCOptions {
    std::string conninfo;              // libpq conninfo (required)
    std::string table;                 // target table (required)
    std::vector<std::string> columns;  // authoritative projection (required)
    std::uint32_t subtask_idx{0};
    std::size_t batch_records{1000};  // flush pending into the txn at this many rows
    std::size_t max_bytes{0};         // byte-based flush threshold (0 = off)
    std::string name{"postgres_2pc_sink"};
};

class PostgresJsonSink2PC final : public CommittingSink<std::string, std::string> {
public:
    explicit PostgresJsonSink2PC(PostgresJsonSink2PCOptions opts)
        : CommittingSink<std::string, std::string>(opts.subtask_idx), opts_(std::move(opts)) {
        if (opts_.conninfo.empty()) {
            throw std::runtime_error(opts_.name + ": 'conninfo' is required");
        }
        if (opts_.table.empty()) {
            throw std::runtime_error(opts_.name + ": 'table' is required");
        }
        if (opts_.columns.empty()) {
            throw std::runtime_error(opts_.name + ": 'columns' is required (the projection)");
        }
        if (opts_.batch_records == 0) {
            opts_.batch_records = 1;
        }
        // Fail fast on bad identifiers.
        (void)pgsql::quote_ident(opts_.table);
        for (const auto& c : opts_.columns) {
            (void)pgsql::quote_ident(c);
        }
    }

    void on_open() override {
        connect_();
        reconcile_orphans_();
    }

    void write(const Batch<std::string>& batch) override {
        for (const auto& rec : batch) {
            pending_bytes_ += rec.value().size();
            pending_.push_back(rec.value());
            if (pending_.size() >= opts_.batch_records ||
                (opts_.max_bytes > 0 && pending_bytes_ >= opts_.max_bytes)) {
                flush_pending_into_txn_();
            }
        }
    }

    // Flush the remainder into the open transaction and PREPARE it. Returns the
    // gid to finalise, or nullopt when no rows flowed this interval (nothing to
    // commit - no empty prepared transaction is left behind).
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        flush_pending_into_txn_();
        if (!in_txn_) {
            return std::nullopt;
        }
        const std::string gid = gtxid_(checkpoint_id);
        exec_("PREPARE TRANSACTION '" + gid + "'");
        in_txn_ = false;
        return gid;
    }

    // COMMIT PREPARED. Idempotent: a gid already gone from pg_prepared_xacts
    // (committed on a prior attempt) is a no-op.
    bool commit(const std::string& gid) override {
        if (gid_is_prepared_(gid)) {
            const auto t0 = std::chrono::steady_clock::now();
            exec_("COMMIT PREPARED '" + gid + "'");
            const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
            clink::metrics::connector::commit_latency_observe("postgres",
                                                              static_cast<std::uint64_t>(dt));
        }
        return true;
    }

    // ROLLBACK PREPARED. Idempotent.
    void abort(const std::string& gid) override {
        if (gid_is_prepared_(gid)) {
            exec_("ROLLBACK PREPARED '" + gid + "'");
        }
    }

    std::string serialize(const std::string& gid) const override { return gid; }
    std::string deserialize(std::string_view bytes) const override { return std::string(bytes); }

    void close() override {
        if (conn_) {
            if (in_txn_) {
                try {
                    exec_("ROLLBACK");  // an unprepared interval is discarded
                } catch (...) {
                }
                in_txn_ = false;
            }
            conn_.reset();
        }
    }

    std::string name() const override { return opts_.name; }

private:
    void connect_() {
        PGconn* c = PQconnectdb(opts_.conninfo.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            const std::string err = PQerrorMessage(c);
            PQfinish(c);
            throw std::runtime_error(opts_.name + ": connect failed: " + err);
        }
        conn_.reset(c);
    }

    // Run a command, throwing on failure. Used for BEGIN / INSERT / PREPARE /
    // COMMIT PREPARED / ROLLBACK PREPARED.
    void exec_(const std::string& sql) {
        PGresult* r = PQexec(conn_.get(), sql.c_str());
        const auto st = r != nullptr ? PQresultStatus(r) : PGRES_FATAL_ERROR;
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const std::string err =
                r != nullptr ? PQresultErrorMessage(r) : std::string(PQerrorMessage(conn_.get()));
            PQclear(r);
            clink::metrics::connector::error_inc("postgres", "sink");
            throw std::runtime_error(opts_.name + ": " + sql + " failed: " + err);
        }
        PQclear(r);
    }

    // Charset-correct escaping of a string VALUE for inside single quotes.
    std::string escape_(std::string_view in) {
        std::string out(in.size() * 2 + 1, '\0');
        int err = 0;
        const std::size_t len =
            PQescapeStringConn(conn_.get(), out.data(), in.data(), in.size(), &err);
        out.resize(len);
        return out;
    }

    void flush_pending_into_txn_() {
        if (pending_.empty()) {
            return;
        }
        if (!conn_) {
            throw std::runtime_error(opts_.name + ": write before open()");
        }
        if (!in_txn_) {
            exec_("BEGIN");
            in_txn_ = true;
        }
        const std::string sql =
            pgsql::build_insert_sql(opts_.table,
                                    opts_.columns,
                                    /*on_conflict=*/"",
                                    /*conflict_columns=*/{},
                                    /*update_columns=*/{},
                                    pending_,
                                    [this](std::string_view s) { return escape_(s); });
        exec_(sql);
        clink::metrics::connector::records_out_inc("postgres", pending_.size());
        clink::metrics::connector::bytes_out_inc("postgres", pending_bytes_);
        pending_.clear();
        pending_bytes_ = 0;
    }

    // Sanitised operator identity: uid if set, else "op<id>"; non-alnum -> '_'.
    std::string ident_() const {
        std::string s =
            this->uid().empty() ? ("op" + std::to_string(this->id().value())) : this->uid();
        for (char& c : s) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || c == '_';
            if (!ok) {
                c = '_';
            }
        }
        return s;
    }
    std::string our_prefix_() const {
        return "clink_" + ident_() + "_sub" + std::to_string(this->subtask_idx()) + "_";
    }
    std::string gtxid_(std::uint64_t ckpt) const { return our_prefix_() + std::to_string(ckpt); }

    bool gid_is_prepared_(const std::string& gid) {
        const char* params[1] = {gid.c_str()};
        PGresult* r = PQexecParams(conn_.get(),
                                   "SELECT 1 FROM pg_prepared_xacts WHERE gid = $1",
                                   1,
                                   nullptr,
                                   params,
                                   nullptr,
                                   nullptr,
                                   0);
        if (r == nullptr || PQresultStatus(r) != PGRES_TUPLES_OK) {
            const std::string err =
                r != nullptr ? PQresultErrorMessage(r) : std::string(PQerrorMessage(conn_.get()));
            PQclear(r);
            throw std::runtime_error(opts_.name + ": pg_prepared_xacts lookup failed: " + err);
        }
        const bool present = PQntuples(r) > 0;
        PQclear(r);
        return present;
    }

    // Our prepared transactions still on the server (filtered by our exact
    // prefix so we never touch another subtask's or job's transactions).
    std::vector<std::string> our_prepared_gids_() {
        std::vector<std::string> out;
        const std::string prefix = our_prefix_();
        PGresult* r = PQexec(conn_.get(), "SELECT gid FROM pg_prepared_xacts");
        if (r != nullptr && PQresultStatus(r) == PGRES_TUPLES_OK) {
            for (int i = 0; i < PQntuples(r); ++i) {
                std::string gid = PQgetvalue(r, i, 0);
                if (gid.rfind(prefix, 0) == 0) {
                    out.push_back(std::move(gid));
                }
            }
        }
        PQclear(r);
        return out;
    }

    // Roll back any of our prepared transactions whose checkpoint never became
    // durable (prepared, but the gid is not in the restored pending set - e.g. a
    // crash between PREPARE and the checkpoint snapshot). The framework's
    // recover_all_() commits the ones that ARE in the set.
    void reconcile_orphans_() {
        const auto keep_vec = this->pending_committables();
        const std::set<std::string> keep(keep_vec.begin(), keep_vec.end());
        for (const auto& gid : our_prepared_gids_()) {
            if (keep.count(gid) == 0) {
                exec_("ROLLBACK PREPARED '" + gid + "'");
            }
        }
    }

    PostgresJsonSink2PCOptions opts_;
    std::unique_ptr<PGconn, PgconnDeleter> conn_;
    std::vector<std::string> pending_;
    std::size_t pending_bytes_{0};
    bool in_txn_{false};
};

}  // namespace clink
