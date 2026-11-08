// The MySQL sinks' connection to a real server. See mysql_sink_server.hpp.

#include "clink/mysql/mysql_sink_server.hpp"

#include <cctype>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <utility>

namespace clink::mysql {

namespace {

std::string lower(std::string s) {
    for (auto& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string cell(MYSQL_ROW row, unsigned i) {
    return row[i] != nullptr ? std::string{row[i]} : std::string{};
}

class RealServer final : public MysqlSinkServer {
public:
    explicit RealServer(const ConnectOptions& o) : conn_(o) {}

    std::optional<ServerError> exec(std::string_view sql) override {
        MYSQL* h = conn_.native();
        if (mysql_real_query(h, sql.data(), static_cast<unsigned long>(sql.size())) != 0) {
            return error_();
        }
        // None of the sinks' statements returns rows; drain any that does.
        if (mysql_field_count(h) > 0) {
            if (MYSQL_RES* res = mysql_store_result(h); res != nullptr) {
                mysql_free_result(res);
            }
        }
        return std::nullopt;
    }

    std::vector<ServerWarning> warnings() override {
        std::vector<ServerWarning> out;
        if (mysql_warning_count(conn_.native()) == 0) {
            return out;
        }
        Result r = conn_.query("SHOW WARNINGS");
        while (MYSQL_ROW row = r.fetch_row()) {
            out.push_back(ServerWarning{
                cell(row, 0),
                static_cast<unsigned>(std::strtoul(row[1] != nullptr ? row[1] : "0", nullptr, 10)),
                cell(row, 2)});
        }
        return out;
    }

    std::string escape(std::string_view value) override { return conn_.escape(value); }

    TableSchema describe(const std::string& table) override {
        TableSchema s;
        std::string schema_sql = "DATABASE()";
        std::string name = table;
        if (const auto dot = table.find('.'); dot != std::string::npos) {
            schema_sql = "'" + conn_.escape(table.substr(0, dot)) + "'";
            name = table.substr(dot + 1);
        }
        const std::string where =
            "TABLE_SCHEMA = " + schema_sql + " AND TABLE_NAME = '" + conn_.escape(name) + "'";
        {
            Result r = conn_.query(
                "SELECT t.TABLE_TYPE, t.ENGINE, e.TRANSACTIONS FROM information_schema.TABLES t "
                "LEFT JOIN information_schema.ENGINES e ON e.ENGINE = t.ENGINE WHERE t." +
                where);
            if (MYSQL_ROW row = r.fetch_row()) {
                s.exists = true;
                s.view = cell(row, 0) == "VIEW";
                s.engine = cell(row, 1);
                s.transactional = cell(row, 2) == "YES";
            }
        }
        if (!s.exists) {
            return s;
        }
        // PAD_ATTRIBUTE is MySQL 8.0's; without it a collation pads.
        std::optional<Result> cols;
        const std::string select =
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.EXTRA, c.DATETIME_PRECISION, "
            "c.COLLATION_NAME";
        const std::string from =
            " FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON "
            "co.COLLATION_NAME = c.COLLATION_NAME WHERE c." +
            where + " ORDER BY c.ORDINAL_POSITION";
        try {
            cols.emplace(conn_.query(select + ", co.PAD_ATTRIBUTE" + from));
        } catch (const std::runtime_error&) {
            cols.emplace(conn_.query(select + ", 'PAD SPACE'" + from));
        }
        while (MYSQL_ROW row = cols->fetch_row()) {
            ColumnInfo c;
            c.name = cell(row, 0);
            c.data_type = lower(cell(row, 1));
            c.column_type = lower(cell(row, 2));
            const auto extra = lower(cell(row, 3));
            c.generated = extra.find("virtual generated") != std::string::npos ||
                          extra.find("stored generated") != std::string::npos;
            c.datetime_precision =
                row[4] != nullptr ? static_cast<int>(std::strtol(row[4], nullptr, 10)) : -1;
            c.collation = cell(row, 5);
            c.collation_no_pad = cell(row, 6) == "NO PAD";
            s.columns.push_back(std::move(c));
        }
        Result idx = conn_.query(
            "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS WHERE "
            "NON_UNIQUE = 0 AND " +
            where + " ORDER BY INDEX_NAME, SEQ_IN_INDEX");
        while (MYSQL_ROW row = idx.fetch_row()) {
            const auto index_name = cell(row, 0);
            if (s.unique_indexes.empty() || s.unique_indexes.back().name != index_name) {
                UniqueIndex u;
                u.name = index_name;
                u.primary = index_name == "PRIMARY";
                s.unique_indexes.push_back(std::move(u));
            }
            auto& u = s.unique_indexes.back();
            // A functional key part has no column: it names an expression.
            u.columns.push_back(row[1] != nullptr ? cell(row, 1) : std::string{"(an expression)"});
            u.prefix = u.prefix || row[2] != nullptr;
        }
        return s;
    }

    std::uint64_t max_allowed_packet() override {
        Result r = conn_.query("SELECT @@session.max_allowed_packet");
        MYSQL_ROW row = r.fetch_row();
        if (row == nullptr || row[0] == nullptr) {
            throw std::runtime_error("the server reports no max_allowed_packet");
        }
        return std::strtoull(row[0], nullptr, 10);
    }

    void cancel() noexcept override { (void)mariadb_cancel(conn_.native()); }

private:
    ServerError error_() {
        MYSQL* h = conn_.native();
        return ServerError{mysql_errno(h),
                           mysql_sqlstate(h) != nullptr ? mysql_sqlstate(h) : "",
                           mysql_error(h) != nullptr ? mysql_error(h) : ""};
    }

    Connection conn_;
};

}  // namespace

std::unique_ptr<MysqlSinkServer> connect_mysql_sink_server(const ConnectOptions& o) {
    return std::make_unique<RealServer>(o);
}

}  // namespace clink::mysql
