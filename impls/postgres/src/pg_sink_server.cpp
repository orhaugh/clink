// The PostgreSQL sinks' connection to a real server. See pg_sink_server.hpp.

#include "clink/postgres/pg_sink_server.hpp"

#include <array>
#include <cstdlib>
#include <libpq-fe.h>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/connectors/postgres_sql.hpp"
#include "clink/connectors/postgres_tls.hpp"

namespace clink::postgres {

namespace {

std::string first_line(const char* text) {
    std::string s = text != nullptr ? text : "";
    if (const auto nl = s.find('\n'); nl != std::string::npos) {
        s.resize(nl);
    }
    return s;
}

int to_int(const char* s) {
    return s != nullptr ? static_cast<int>(std::strtol(s, nullptr, 10)) : 0;
}

// A typmod's parts, as PostgreSQL encodes them for numeric, character and
// time types.
void apply_typmod(PgColumn& c, int typmod) {
    if (typmod < 0) {
        return;
    }
    if (c.type_name == "numeric" && typmod >= 4) {
        const int v = typmod - 4;
        int scale = v & 0x7ff;
        if ((scale & 0x400) != 0) {
            scale -= 0x800;  // PostgreSQL 15's negative scales
        }
        c.numeric_constrained = true;
        c.numeric_scale = scale;
    } else if ((c.type_name == "varchar" || c.type_name == "bpchar") && typmod >= 4) {
        c.char_length = typmod - 4;
    } else if (c.type_name == "timestamp" || c.type_name == "timestamptz" ||
               c.type_name == "time" || c.type_name == "timetz") {
        c.time_precision = typmod;
    }
}

class RealPgServer final : public PgSinkServer {
public:
    RealPgServer(const std::string& conninfo, const PgConnectSettings& s, const std::string& who) {
        const auto connect_timeout = std::to_string(s.connect_timeout_s);
        const auto idle = std::to_string(s.keepalives_idle_s);
        const auto interval = std::to_string(s.keepalives_interval_s);
        const auto count = std::to_string(s.keepalives_count);
        const auto user_timeout = std::to_string(s.tcp_user_timeout_ms);
        // The settings come before the conninfo, which libpq expands in place of
        // dbname, so whatever the conninfo says overrides them.
        const std::array<const char*, 8> keys = {"connect_timeout",
                                                 "keepalives",
                                                 "keepalives_idle",
                                                 "keepalives_interval",
                                                 "keepalives_count",
                                                 "tcp_user_timeout",
                                                 "dbname",
                                                 nullptr};
        const std::array<const char*, 8> values = {connect_timeout.c_str(),
                                                   "1",
                                                   idle.c_str(),
                                                   interval.c_str(),
                                                   count.c_str(),
                                                   user_timeout.c_str(),
                                                   conninfo.c_str(),
                                                   nullptr};
        conn_ = PQconnectdbParams(keys.data(), values.data(), /*expand_dbname=*/1);
        if (PQstatus(conn_) != CONNECTION_OK) {
            const auto err = first_line(PQerrorMessage(conn_));
            PQfinish(conn_);
            conn_ = nullptr;
            throw std::runtime_error(who + ": connect failed: " + err);
        }
        try {
            clink::connectors::pg::assert_no_silent_downgrade(conn_, conninfo, who.c_str());
        } catch (...) {
            PQfinish(conn_);
            conn_ = nullptr;
            throw;
        }
        cancel_ = PQgetCancel(conn_);
    }

    ~RealPgServer() override {
        if (cancel_ != nullptr) {
            PQfreeCancel(cancel_);
        }
        if (conn_ != nullptr) {
            PQfinish(conn_);
        }
    }

    RealPgServer(const RealPgServer&) = delete;
    RealPgServer& operator=(const RealPgServer&) = delete;

    PgOutcome exec(std::string_view sql) override {
        const std::string text{sql};
        return outcome_(PQexec(conn_, text.c_str()));
    }

    PgOutcome exec_params(std::string_view sql,
                          const std::vector<std::optional<std::string>>& params) override {
        const std::string text{sql};
        std::vector<const char*> values;
        values.reserve(params.size());
        for (const auto& p : params) {
            values.push_back(p.has_value() ? p->c_str() : nullptr);
        }
        return outcome_(PQexecParams(conn_,
                                     text.c_str(),
                                     static_cast<int>(values.size()),
                                     /*paramTypes=*/nullptr,
                                     values.data(),
                                     /*paramLengths=*/nullptr,
                                     /*paramFormats=*/nullptr,
                                     /*resultFormat=*/0));
    }

    std::string escape(std::string_view value) override {
        std::string out((value.size() * 2) + 1, '\0');
        int err = 0;
        const auto len = PQescapeStringConn(conn_, out.data(), value.data(), value.size(), &err);
        if (err != 0) {
            throw RecordRefused("a string the connection's encoding cannot carry");
        }
        out.resize(len);
        return out;
    }

    PgTable describe(const std::string& table) override {
        PgTable t;
        const auto regclass = "'" + escape(pgsql::quote_table_ident(table)) + "'";
        auto rel = query_(
            "SELECT c.oid, c.relkind FROM pg_catalog.pg_class c WHERE c.oid = "
            "pg_catalog.to_regclass(" +
            regclass + ")");
        if (PQntuples(rel.get()) == 0) {
            return t;
        }
        t.exists = true;
        const std::string oid = PQgetvalue(rel.get(), 0, 0);
        const char* kind = PQgetvalue(rel.get(), 0, 1);
        t.kind = kind != nullptr && *kind != '\0' ? *kind : '\0';
        auto cols = query_(
            "SELECT a.attname, bt.typname, pg_catalog.format_type(a.atttypid, a.atttypmod), "
            "CASE WHEN t.typtype = 'd' THEN t.typtypmod ELSE a.atttypmod END, a.attgenerated, "
            "a.attidentity, COALESCE(co.collisdeterministic, true), COALESCE(co.collname, '') "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
            "JOIN pg_catalog.pg_type bt ON bt.oid = CASE WHEN t.typtype = 'd' THEN "
            "t.typbasetype ELSE t.oid END "
            "LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation "
            "WHERE a.attrelid = " +
            oid + " AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum");
        for (int r = 0; r < PQntuples(cols.get()); ++r) {
            PgColumn c;
            c.name = PQgetvalue(cols.get(), r, 0);
            c.type_name = PQgetvalue(cols.get(), r, 1);
            c.type = PQgetvalue(cols.get(), r, 2);
            apply_typmod(c, to_int(PQgetvalue(cols.get(), r, 3)));
            c.generated = std::string{PQgetvalue(cols.get(), r, 4)} == "s";
            c.identity_always = std::string{PQgetvalue(cols.get(), r, 5)} == "a";
            c.collation_deterministic = std::string{PQgetvalue(cols.get(), r, 6)} == "t";
            const std::string collation = PQgetvalue(cols.get(), r, 7);
            c.collation = collation == "default" ? std::string{} : collation;
            t.columns.push_back(std::move(c));
        }
        // A unique index's key columns: its INCLUDE columns are carried, not
        // compared.
        auto idx = query_(
            "SELECT i.relname, x.indisprimary, x.indpred IS NOT NULL, x.indexprs IS NOT NULL, "
            "pg_catalog.array_to_string(ARRAY(SELECT a.attname FROM "
            "pg_catalog.unnest(x.indkey::int2[]) WITH ORDINALITY k(attnum, ord) JOIN "
            "pg_catalog.pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum WHERE "
            "k.ord <= x.indnkeyatts ORDER BY k.ord), E'\\x1f') "
            "FROM pg_catalog.pg_index x JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid "
            "WHERE x.indrelid = " +
            oid + " AND x.indisunique AND x.indisvalid ORDER BY i.relname");
        for (int r = 0; r < PQntuples(idx.get()); ++r) {
            PgUniqueIndex u;
            u.name = PQgetvalue(idx.get(), r, 0);
            u.primary = std::string{PQgetvalue(idx.get(), r, 1)} == "t";
            u.partial = std::string{PQgetvalue(idx.get(), r, 2)} == "t";
            u.expression = std::string{PQgetvalue(idx.get(), r, 3)} == "t";
            const std::string list = PQgetvalue(idx.get(), r, 4);
            std::size_t from = 0;
            while (from <= list.size() && !list.empty()) {
                const auto sep = list.find('\x1f', from);
                u.columns.push_back(list.substr(from, sep - from));
                if (sep == std::string::npos) {
                    break;
                }
                from = sep + 1;
            }
            t.unique_indexes.push_back(std::move(u));
        }
        return t;
    }

    void cancel() noexcept override {
        if (cancel_ != nullptr) {
            std::array<char, 256> err{};
            (void)PQcancel(cancel_, err.data(), static_cast<int>(err.size()));
        }
    }

private:
    struct ResultDeleter {
        void operator()(PGresult* r) const noexcept { PQclear(r); }
    };
    using ResultPtr = std::unique_ptr<PGresult, ResultDeleter>;

    PgOutcome outcome_(PGresult* raw) {
        const ResultPtr r(raw);
        const auto status = r ? PQresultStatus(r.get()) : PGRES_FATAL_ERROR;
        PgOutcome o;
        if (r && (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK)) {
            o.tag = PQcmdStatus(r.get());
            return o;
        }
        PgError e;
        if (r) {
            const char* code = PQresultErrorField(r.get(), PG_DIAG_SQLSTATE);
            const char* primary = PQresultErrorField(r.get(), PG_DIAG_MESSAGE_PRIMARY);
            e.sqlstate = code != nullptr ? code : "";
            e.message = primary != nullptr ? primary : first_line(PQresultErrorMessage(r.get()));
        }
        if (e.message.empty()) {
            e.message = first_line(PQerrorMessage(conn_));
        }
        e.lost = PQstatus(conn_) != CONNECTION_OK;
        o.error = std::move(e);
        return o;
    }

    // A catalogue query the sink cannot do without: its failure is the open's.
    ResultPtr query_(const std::string& sql) {
        ResultPtr r(PQexec(conn_, sql.c_str()));
        if (!r || PQresultStatus(r.get()) != PGRES_TUPLES_OK) {
            const char* primary =
                r ? PQresultErrorField(r.get(), PG_DIAG_MESSAGE_PRIMARY) : nullptr;
            throw std::runtime_error(
                "reading the table's definition failed: " +
                (primary != nullptr ? std::string{primary} : first_line(PQerrorMessage(conn_))));
        }
        return r;
    }

    PGconn* conn_{nullptr};
    PGcancel* cancel_{nullptr};
};

}  // namespace

std::unique_ptr<PgSinkServer> connect_pg_sink_server(const std::string& conninfo,
                                                     const PgConnectSettings& settings,
                                                     const std::string& who) {
    return std::make_unique<RealPgServer>(conninfo, settings, who);
}

}  // namespace clink::postgres
