// The MySQL CDC source's pure rules: the checkpoint row, event classes, QUERY
// statement classes, TABLE_MAP optional metadata and the binlog-type check.
// See mysql_cdc_rules.hpp.

#include "clink/mysql/mysql_cdc_rules.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <limits>

namespace clink::mysql {

namespace {

constexpr std::string_view kMagic = "MYCDC";
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kMaxFileName = 255;
constexpr std::size_t kMaxSchemas = kMaxCheckpointDefinitions;
constexpr std::size_t kMaxSchemaKey = 1024;
constexpr std::size_t kMaxFingerprint = kMaxDefinitionBytes;

void put_u32(std::string& b, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
}
void put_u64(std::string& b, std::uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
}
void put_str(std::string& b, std::string_view s) {
    put_u32(b, static_cast<std::uint32_t>(s.size()));
    b.append(s);
}

// Reads the fields of a checkpoint row, failing on anything short.
class Reader {
public:
    explicit Reader(std::string_view b) : b_(b) {}

    std::uint64_t u(int bytes, const char* what) {
        if (b_.size() - pos_ < static_cast<std::size_t>(bytes)) {
            throw MysqlCdcCheckpointError("the row ends inside its " + std::string{what});
        }
        std::uint64_t v = 0;
        for (int i = 0; i < bytes; ++i) {
            v |= static_cast<std::uint64_t>(
                     static_cast<unsigned char>(b_[pos_ + static_cast<std::size_t>(i)]))
                 << (8 * i);
        }
        pos_ += static_cast<std::size_t>(bytes);
        return v;
    }
    std::string str(const char* what, std::size_t max) {
        const auto n = u(4, what);
        if (n > max) {
            throw MysqlCdcCheckpointError("its " + std::string{what} + " is " + std::to_string(n) +
                                          " bytes, more than " + std::to_string(max));
        }
        if (b_.size() - pos_ < n) {
            throw MysqlCdcCheckpointError("the row ends inside its " + std::string{what});
        }
        std::string s{b_.substr(pos_, static_cast<std::size_t>(n))};
        pos_ += static_cast<std::size_t>(n);
        return s;
    }
    [[nodiscard]] std::size_t left() const { return b_.size() - pos_; }

private:
    std::string_view b_;
    std::size_t pos_{0};
};

void check_coordinate(const std::string& file, std::uint64_t pos) {
    if (!valid_binlog_file_name(file)) {
        throw MysqlCdcCheckpointError("its binlog file name is not one MySQL writes");
    }
    if (pos < 4 || pos > (std::numeric_limits<std::uint64_t>::max() >> 1)) {
        throw MysqlCdcCheckpointError("its binlog position " + std::to_string(pos) +
                                      " is not a position in a binlog file");
    }
}

// ---- statement lexing ------------------------------------------------------------------

struct Token {
    enum class Kind { End, Word, Quoted, Punct, Other };
    Kind kind{Kind::End};
    std::string text;  // a Word as written; a Quoted identifier unquoted; a Punct's char
};

class Lexer {
public:
    explicit Lexer(std::string_view s) : s_(s) {}

    Token next() {
        skip();
        if (p_ >= s_.size()) {
            return {};
        }
        const char c = s_[p_];
        if (c == '`') {
            std::string out;
            ++p_;
            while (p_ < s_.size()) {
                if (s_[p_] == '`') {
                    if (p_ + 1 < s_.size() && s_[p_ + 1] == '`') {
                        out.push_back('`');
                        p_ += 2;
                        continue;
                    }
                    ++p_;
                    return {Token::Kind::Quoted, out};
                }
                out.push_back(s_[p_++]);
            }
            return {Token::Kind::Other, out};  // unterminated
        }
        if (c == '\'' || c == '"') {
            ++p_;
            while (p_ < s_.size() && s_[p_] != c) {
                p_ += s_[p_] == '\\' ? 2U : 1U;
            }
            // Past the closing quote, or at the end of a literal that has none
            // (a trailing backslash would otherwise step past the text).
            p_ = std::min(p_ + 1, s_.size());
            return {Token::Kind::Other, ""};
        }
        if (word_char(c)) {
            const auto start = p_;
            while (p_ < s_.size() && word_char(s_[p_])) {
                ++p_;
            }
            return {Token::Kind::Word, std::string{s_.substr(start, p_ - start)}};
        }
        ++p_;
        return {Token::Kind::Punct, std::string(1, c)};
    }

    Token peek() {
        const auto saved = p_;
        auto t = next();
        p_ = saved;
        return t;
    }

private:
    static bool word_char(char c) {
        const auto u = static_cast<unsigned char>(c);
        return std::isalnum(u) != 0 || c == '_' || c == '$' || u >= 0x80;
    }

    // Whitespace and comments. A versioned comment's body (/*!NNNNN ... */) is
    // text the server executes, so it is read as statement text.
    void skip() {
        for (;;) {
            while (p_ < s_.size() && std::isspace(static_cast<unsigned char>(s_[p_])) != 0) {
                ++p_;
            }
            if (s_.substr(p_, 3) == "/*!") {
                p_ += 3;
                while (p_ < s_.size() && std::isdigit(static_cast<unsigned char>(s_[p_])) != 0) {
                    ++p_;
                }
                continue;
            }
            if (s_.substr(p_, 2) == "*/") {
                p_ += 2;
                continue;
            }
            if (s_.substr(p_, 2) == "/*") {
                const auto end = s_.find("*/", p_ + 2);
                p_ = end == std::string_view::npos ? s_.size() : end + 2;
                continue;
            }
            if (s_.substr(p_, 2) == "--" &&
                (p_ + 2 >= s_.size() ||
                 std::isspace(static_cast<unsigned char>(s_[p_ + 2])) != 0)) {
                const auto end = s_.find('\n', p_);
                p_ = end == std::string_view::npos ? s_.size() : end + 1;
                continue;
            }
            if (p_ < s_.size() && s_[p_] == '#') {
                const auto end = s_.find('\n', p_);
                p_ = end == std::string_view::npos ? s_.size() : end + 1;
                continue;
            }
            return;
        }
    }

    std::string_view s_;
    std::size_t p_{0};
};

std::string upper(std::string_view s) {
    std::string out{s};
    for (auto& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

bool is_word(const Token& t, std::string_view w) {
    return t.kind == Token::Kind::Word && upper(t.text) == w;
}

// Reads [db.]table at the lexer's position.
std::optional<std::pair<std::string, std::string>> table_name(Lexer& lx, std::string_view db) {
    const auto first = lx.next();
    if (first.kind != Token::Kind::Word && first.kind != Token::Kind::Quoted) {
        return std::nullopt;
    }
    if (const auto dot = lx.peek(); dot.kind == Token::Kind::Punct && dot.text == ".") {
        (void)lx.next();
        const auto second = lx.next();
        if (second.kind != Token::Kind::Word && second.kind != Token::Kind::Quoted) {
            return std::nullopt;
        }
        return std::make_pair(first.text, second.text);
    }
    return std::make_pair(std::string{db}, first.text);
}

void skip_words(Lexer& lx, std::initializer_list<std::string_view> words) {
    for (;;) {
        const auto t = lx.peek();
        bool matched = false;
        for (const auto w : words) {
            if (is_word(t, w)) {
                matched = true;
            }
        }
        if (!matched) {
            return;
        }
        (void)lx.next();
    }
}

QueryStatement unknown(QueryStatement::Kind kind) {
    QueryStatement q;
    q.kind = kind;
    q.targets_known = false;
    return q;
}

QueryStatement one(QueryStatement::Kind kind,
                   QueryStatement::Effect effect,
                   const std::optional<std::pair<std::string, std::string>>& t) {
    if (!t) {
        return unknown(kind);
    }
    QueryStatement q;
    q.kind = kind;
    q.targets.push_back({effect, t->first, t->second});
    return q;
}

// What one clause of an ALTER TABLE does to the table, as far as the source
// needs to know.
enum class AlterClause {
    Columns,     // the columns may change (and any clause the parser does not know)
    Layoutless,  // indexes, constraints, options, partitioning, column defaults
    Rename,      // RENAME [TO|AS] name: the table moves
    Rows,        // rows change without row events (partitions or tablespaces)
};

bool word_in(const Token& t, std::initializer_list<std::string_view> words) {
    for (const auto w : words) {
        if (is_word(t, w)) {
            return true;
        }
    }
    return false;
}

AlterClause classify_alter_clause(const std::vector<Token>& c) {
    if (c.empty()) {
        return AlterClause::Layoutless;
    }
    const Token none{};
    const auto at = [&](std::size_t i) -> const Token& { return i < c.size() ? c[i] : none; };
    const auto& w = c[0];
    if (is_word(w, "RENAME")) {
        if (is_word(at(1), "COLUMN")) {
            return AlterClause::Columns;
        }
        return word_in(at(1), {"INDEX", "KEY"}) ? AlterClause::Layoutless : AlterClause::Rename;
    }
    if (is_word(w, "ADD")) {
        if (word_in(at(1),
                    {"INDEX",
                     "KEY",
                     "UNIQUE",
                     "PRIMARY",
                     "FULLTEXT",
                     "SPATIAL",
                     "CONSTRAINT",
                     "FOREIGN",
                     "CHECK",
                     "PARTITION"})) {
            return AlterClause::Layoutless;
        }
        return AlterClause::Columns;
    }
    if (is_word(w, "DROP")) {
        if (word_in(at(1), {"INDEX", "KEY", "PRIMARY", "FOREIGN", "CHECK", "CONSTRAINT"})) {
            return AlterClause::Layoutless;
        }
        return is_word(at(1), "PARTITION") ? AlterClause::Rows : AlterClause::Columns;
    }
    if (is_word(w, "ALTER")) {
        if (word_in(at(1), {"INDEX", "CHECK", "CONSTRAINT"})) {
            return AlterClause::Layoutless;
        }
        // ALTER [COLUMN] c SET DEFAULT | DROP DEFAULT | SET VISIBLE | SET INVISIBLE
        const std::size_t name = is_word(at(1), "COLUMN") ? 2 : 1;
        const auto& verb = at(name + 1);
        const auto& what = at(name + 2);
        if ((is_word(verb, "SET") && word_in(what, {"DEFAULT", "VISIBLE", "INVISIBLE"})) ||
            (is_word(verb, "DROP") && is_word(what, "DEFAULT"))) {
            return AlterClause::Layoutless;
        }
        return AlterClause::Columns;
    }
    if (word_in(w, {"TRUNCATE", "EXCHANGE", "DISCARD", "IMPORT"})) {
        return AlterClause::Rows;
    }
    if (word_in(w, {"MODIFY", "CHANGE", "CONVERT"})) {
        return AlterClause::Columns;
    }
    if (word_in(w,
                {// partitioning that keeps every row
                 "PARTITION",
                 "REMOVE",
                 "COALESCE",
                 "REORGANIZE",
                 "REBUILD",
                 "ANALYZE",
                 "CHECK",
                 "OPTIMIZE",
                 "REPAIR",
                 "UPGRADE",
                 // the statement's execution
                 "ALGORITHM",
                 "LOCK",
                 "FORCE",
                 "ORDER",
                 "WITH",
                 "WITHOUT",
                 "ENABLE",
                 "DISABLE",
                 // table options; DEFAULT CHARACTER SET changes no existing column
                 "DEFAULT",
                 "CHARACTER",
                 "CHARSET",
                 "COLLATE",
                 "ENGINE",
                 "AUTO_INCREMENT",
                 "AVG_ROW_LENGTH",
                 "CHECKSUM",
                 "COMMENT",
                 "COMPRESSION",
                 "CONNECTION",
                 "DATA",
                 "INDEX",
                 "DELAY_KEY_WRITE",
                 "ENCRYPTION",
                 "INSERT_METHOD",
                 "KEY_BLOCK_SIZE",
                 "MAX_ROWS",
                 "MIN_ROWS",
                 "PACK_KEYS",
                 "PASSWORD",
                 "ROW_FORMAT",
                 "STATS_AUTO_RECALC",
                 "STATS_PERSISTENT",
                 "STATS_SAMPLE_PAGES",
                 "TABLESPACE",
                 "UNION",
                 "SECONDARY_ENGINE",
                 "SECONDARY_ENGINE_ATTRIBUTE",
                 "ENGINE_ATTRIBUTE",
                 "AUTOEXTEND_SIZE",
                 "SECONDARY_LOAD",
                 "SECONDARY_UNLOAD"})) {
        return AlterClause::Layoutless;
    }
    return AlterClause::Columns;
}

QueryStatement classify_alter(Lexer& lx, std::string_view db) {
    skip_words(lx, {"ONLINE", "OFFLINE", "IGNORE"});
    if (!is_word(lx.next(), "TABLE")) {
        return {};  // ALTER DATABASE, VIEW, USER, EVENT...: no captured rows
    }
    const auto t = table_name(lx, db);
    if (!t) {
        return unknown(QueryStatement::Kind::Ddl);
    }
    // The clauses, split at the commas outside parentheses.
    std::vector<std::vector<Token>> clauses(1);
    int depth = 0;
    for (auto tok = lx.next(); tok.kind != Token::Kind::End; tok = lx.next()) {
        if (tok.kind == Token::Kind::Punct && tok.text == "(") {
            ++depth;
        } else if (tok.kind == Token::Kind::Punct && tok.text == ")") {
            --depth;
        } else if (depth == 0 && tok.kind == Token::Kind::Punct && tok.text == ",") {
            clauses.emplace_back();
            continue;
        }
        clauses.back().push_back(std::move(tok));
    }
    bool columns = false;
    bool rows = false;
    std::optional<std::pair<std::string, std::string>> renamed_to;
    std::vector<std::pair<std::string, std::string>> exchanged;
    for (const auto& c : clauses) {
        // EXCHANGE PARTITION p WITH TABLE [db.]x swaps rows with x too.
        if (!c.empty() && is_word(c[0], "EXCHANGE")) {
            for (std::size_t i = 0; i + 2 < c.size(); ++i) {
                if (!is_word(c[i], "WITH") || !is_word(c[i + 1], "TABLE")) {
                    continue;
                }
                const auto& a = c[i + 2];
                if (i + 4 < c.size() && c[i + 3].kind == Token::Kind::Punct &&
                    c[i + 3].text == ".") {
                    exchanged.emplace_back(a.text, c[i + 4].text);
                } else {
                    exchanged.emplace_back(std::string{db}, a.text);
                }
                break;
            }
        }
        switch (classify_alter_clause(c)) {
            case AlterClause::Columns:
                columns = true;
                break;
            case AlterClause::Rows:
                rows = true;
                break;
            case AlterClause::Rename: {
                // RENAME [TO|AS] [db.]name
                const std::size_t i = word_in(c.size() > 1 ? c[1] : Token{}, {"TO", "AS"}) ? 2 : 1;
                const auto name_token = [&](std::size_t k) {
                    return k < c.size() &&
                           (c[k].kind == Token::Kind::Word || c[k].kind == Token::Kind::Quoted);
                };
                if (!name_token(i)) {
                    return unknown(QueryStatement::Kind::Ddl);
                }
                if (i + 1 < c.size() && c[i + 1].kind == Token::Kind::Punct &&
                    c[i + 1].text == ".") {
                    if (!name_token(i + 2)) {
                        return unknown(QueryStatement::Kind::Ddl);
                    }
                    renamed_to = std::make_pair(c[i].text, c[i + 2].text);
                } else {
                    renamed_to = std::make_pair(std::string{db}, c[i].text);
                }
                break;
            }
            case AlterClause::Layoutless:
                break;
        }
    }
    QueryStatement q;
    q.kind = QueryStatement::Kind::Ddl;
    if (rows) {
        q.targets.push_back({QueryStatement::Effect::Truncate, t->first, t->second});
    }
    for (const auto& x : exchanged) {
        q.targets.push_back({QueryStatement::Effect::Truncate, x.first, x.second});
    }
    if (renamed_to) {
        q.targets.push_back({QueryStatement::Effect::Rename, t->first, t->second});
        q.targets.push_back(
            {QueryStatement::Effect::Create, renamed_to->first, renamed_to->second});
        return q;
    }
    QueryStatement::Target alter{QueryStatement::Effect::Alter, t->first, t->second};
    alter.columns = columns;
    q.targets.push_back(std::move(alter));
    return q;
}

QueryStatement classify_create(Lexer& lx, std::string_view db) {
    skip_words(lx, {"OR", "REPLACE"});
    if (is_word(lx.peek(), "TEMPORARY")) {
        return {};  // a session's temporary table is never captured
    }
    skip_words(lx, {"UNIQUE", "FULLTEXT", "SPATIAL"});
    const auto what = lx.next();
    if (is_word(what, "TABLE")) {
        if (is_word(lx.peek(), "IF")) {
            skip_words(lx, {"IF", "NOT", "EXISTS"});
        }
        return one(QueryStatement::Kind::Ddl, QueryStatement::Effect::Create, table_name(lx, db));
    }
    if (is_word(what, "INDEX")) {
        (void)lx.next();  // the index's name
        if (!is_word(lx.next(), "ON")) {
            return unknown(QueryStatement::Kind::Ddl);
        }
        auto q = one(QueryStatement::Kind::Ddl, QueryStatement::Effect::Alter, table_name(lx, db));
        for (auto& target : q.targets) {
            target.columns = false;
        }
        return q;
    }
    return {};  // DATABASE, VIEW, TRIGGER, PROCEDURE, USER...
}

QueryStatement classify_drop(Lexer& lx, std::string_view db) {
    if (is_word(lx.peek(), "TEMPORARY")) {
        return {};
    }
    const auto what = lx.next();
    if (is_word(what, "TABLE") || is_word(what, "TABLES")) {
        if (is_word(lx.peek(), "IF")) {
            skip_words(lx, {"IF", "EXISTS"});
        }
        QueryStatement q;
        q.kind = QueryStatement::Kind::Ddl;
        for (;;) {
            const auto t = table_name(lx, db);
            if (!t) {
                return unknown(QueryStatement::Kind::Ddl);
            }
            q.targets.push_back({QueryStatement::Effect::Drop, t->first, t->second});
            if (const auto sep = lx.peek(); sep.kind == Token::Kind::Punct && sep.text == ",") {
                (void)lx.next();
                continue;
            }
            return q;
        }
    }
    if (is_word(what, "INDEX")) {
        (void)lx.next();  // the index's name
        if (!is_word(lx.next(), "ON")) {
            return unknown(QueryStatement::Kind::Ddl);
        }
        auto q = one(QueryStatement::Kind::Ddl, QueryStatement::Effect::Alter, table_name(lx, db));
        for (auto& target : q.targets) {
            target.columns = false;
        }
        return q;
    }
    if (is_word(what, "DATABASE") || is_word(what, "SCHEMA")) {
        if (is_word(lx.peek(), "IF")) {
            skip_words(lx, {"IF", "EXISTS"});
        }
        const auto name = lx.next();
        if (name.kind != Token::Kind::Word && name.kind != Token::Kind::Quoted) {
            return unknown(QueryStatement::Kind::Ddl);
        }
        QueryStatement q;
        q.kind = QueryStatement::Kind::Ddl;
        q.targets.push_back({QueryStatement::Effect::DropDatabase, name.text, ""});
        return q;
    }
    return {};
}

// RENAME TABLE a TO b [, c TO d]. A name both renamed away and renamed into in
// one statement (the swap online schema-change tools make) keeps a table
// under that name, perhaps a different definition: an Alter. A name only
// renamed away is a Rename; one only renamed into, a Create.
QueryStatement classify_rename(Lexer& lx, std::string_view db) {
    if (!is_word(lx.next(), "TABLE")) {
        return {};  // RENAME USER
    }
    std::vector<std::pair<std::string, std::string>> from;
    std::vector<std::pair<std::string, std::string>> to;
    for (;;) {
        const auto a = table_name(lx, db);
        if (!a || !is_word(lx.next(), "TO")) {
            return unknown(QueryStatement::Kind::Ddl);
        }
        const auto b = table_name(lx, db);
        if (!b) {
            return unknown(QueryStatement::Kind::Ddl);
        }
        from.push_back(*a);
        to.push_back(*b);
        if (const auto sep = lx.peek(); sep.kind == Token::Kind::Punct && sep.text == ",") {
            (void)lx.next();
            continue;
        }
        break;
    }
    QueryStatement q;
    q.kind = QueryStatement::Kind::Ddl;
    const auto in = [](const auto& v, const auto& x) {
        return std::find(v.begin(), v.end(), x) != v.end();
    };
    for (const auto& f : from) {
        q.targets.push_back(
            {in(to, f) ? QueryStatement::Effect::Alter : QueryStatement::Effect::Rename,
             f.first,
             f.second});
    }
    for (const auto& t : to) {
        if (!in(from, t)) {
            q.targets.push_back({QueryStatement::Effect::Create, t.first, t.second});
        }
    }
    return q;
}

QueryStatement classify_dml(std::string_view first, Lexer& lx, std::string_view db) {
    using K = QueryStatement::Kind;
    using E = QueryStatement::Effect;
    if (first == "INSERT" || first == "REPLACE") {
        skip_words(lx, {"LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO"});
        return one(K::Dml, E::Write, table_name(lx, db));
    }
    if (first == "UPDATE") {
        skip_words(lx, {"LOW_PRIORITY", "IGNORE"});
        const auto t = table_name(lx, db);
        const auto after = lx.peek();
        if (!t || (after.kind == Token::Kind::Punct && after.text == ",") ||
            is_word(after, "JOIN") || is_word(after, "INNER") || is_word(after, "LEFT") ||
            is_word(after, "RIGHT") || is_word(after, "CROSS") || is_word(after, "NATURAL") ||
            is_word(after, "STRAIGHT_JOIN")) {
            return unknown(K::Dml);
        }
        return one(K::Dml, E::Write, t);
    }
    if (first == "DELETE") {
        skip_words(lx, {"LOW_PRIORITY", "QUICK", "IGNORE"});
        if (!is_word(lx.next(), "FROM")) {
            return unknown(K::Dml);  // DELETE t1, t2 FROM ...
        }
        const auto t = table_name(lx, db);
        const auto after = lx.peek();
        if (!t || (after.kind == Token::Kind::Punct && after.text == ",") ||
            is_word(after, "USING") || is_word(after, "JOIN") || is_word(after, "INNER") ||
            is_word(after, "LEFT")) {
            return unknown(K::Dml);
        }
        return one(K::Dml, E::Write, t);
    }
    // LOAD DATA|XML ... INTO TABLE t
    for (auto tok = lx.next(); tok.kind != Token::Kind::End; tok = lx.next()) {
        if (is_word(tok, "INTO") && is_word(lx.peek(), "TABLE")) {
            (void)lx.next();
            return one(K::Dml, E::Write, table_name(lx, db));
        }
    }
    return unknown(K::Dml);
}

// MySQL's packed (length-encoded) integer.
bool read_lenenc(const std::uint8_t* d, std::size_t size, std::size_t& p, std::uint64_t& out) {
    if (p >= size) {
        return false;
    }
    const auto first = d[p++];
    int bytes = 0;
    if (first < 0xFB) {
        out = first;
        return true;
    }
    if (first == 0xFC) {
        bytes = 2;
    } else if (first == 0xFD) {
        bytes = 3;
    } else if (first == 0xFE) {
        bytes = 8;
    } else {
        return false;
    }
    if (size - p < static_cast<std::size_t>(bytes)) {
        return false;
    }
    out = 0;
    for (int i = 0; i < bytes; ++i) {
        out |= static_cast<std::uint64_t>(d[p + static_cast<std::size_t>(i)]) << (8 * i);
    }
    p += static_cast<std::size_t>(bytes);
    return true;
}

bool read_lenenc_string(const std::uint8_t* d, std::size_t size, std::size_t& p, std::string& out) {
    std::uint64_t n = 0;
    if (!read_lenenc(d, size, p, n) || n > size - p) {
        return false;
    }
    out.assign(reinterpret_cast<const char*>(d + p), static_cast<std::size_t>(n));
    p += static_cast<std::size_t>(n);
    return true;
}

}  // namespace

// ---- the checkpoint row ---------------------------------------------------------------

bool valid_binlog_file_name(std::string_view name) {
    if (name.empty() || name.size() > kMaxFileName) {
        return false;
    }
    for (const char c : name) {
        const auto u = static_cast<unsigned char>(c);
        if (std::isalnum(u) == 0 && c != '_' && c != '.' && c != '-') {
            return false;
        }
    }
    const auto dot = name.rfind('.');
    if (dot == std::string_view::npos || dot == 0 || name.size() - dot - 1 < 6) {
        return false;
    }
    return std::all_of(name.begin() + static_cast<std::ptrdiff_t>(dot) + 1, name.end(), [](char c) {
        return std::isdigit(static_cast<unsigned char>(c)) != 0;
    });
}

bool valid_server_uuid(std::string_view uuid) {
    if (uuid.size() != 36) {
        return false;
    }
    for (std::size_t i = 0; i < uuid.size(); ++i) {
        const bool hyphen = i == 8 || i == 13 || i == 18 || i == 23;
        if (hyphen ? uuid[i] != '-' : std::isxdigit(static_cast<unsigned char>(uuid[i])) == 0) {
            return false;
        }
    }
    return true;
}

std::string encode_mysql_cdc_checkpoint(const MysqlCdcCheckpoint& cp) {
    // A row the decoder would refuse is never written: the checkpoint fails
    // instead of leaving one no restore can read.
    if (cp.file.size() > kMaxFileName || cp.server_uuid.size() > 64 ||
        cp.schemas.size() > kMaxSchemas) {
        throw MysqlCdcCheckpointError("the checkpoint exceeds the row's limits");
    }
    for (const auto& [table, fingerprint] : cp.schemas) {
        if (table.size() > kMaxSchemaKey || fingerprint.size() > kMaxFingerprint) {
            throw MysqlCdcCheckpointError("the definition of " + table +
                                          " exceeds the row's limits");
        }
    }
    std::string b;
    b.append(kMagic);
    b.push_back(static_cast<char>(kVersion));
    b.push_back(static_cast<char>(cp.snapshot_unfinished ? 1 : 0));
    put_str(b, cp.file);
    put_u64(b, cp.pos);
    put_str(b, cp.server_uuid);
    put_u32(b, static_cast<std::uint32_t>(cp.schemas.size()));
    for (const auto& [table, fingerprint] : cp.schemas) {
        put_str(b, table);
        put_str(b, fingerprint);
    }
    return b;
}

MysqlCdcCheckpoint decode_mysql_cdc_checkpoint(std::string_view bytes) {
    MysqlCdcCheckpoint cp;
    if (bytes.substr(0, kMagic.size()) != kMagic) {
        // The earlier format: u32 file length, the file, u64 position.
        cp.version = 0;
        Reader r(bytes);
        cp.file = r.str("binlog file name", kMaxFileName);
        cp.pos = r.u(8, "binlog position");
        if (r.left() != 0) {
            throw MysqlCdcCheckpointError("it has " + std::to_string(r.left()) +
                                          " bytes after its position");
        }
        check_coordinate(cp.file, cp.pos);
        return cp;
    }
    Reader r(bytes.substr(kMagic.size()));
    const auto version = r.u(1, "version");
    if (version != kVersion) {
        throw MysqlCdcCheckpointError("it is version " + std::to_string(version) +
                                      ", which this release does not read");
    }
    const auto phase = r.u(1, "phase");
    if (phase > 1) {
        throw MysqlCdcCheckpointError("its phase is " + std::to_string(phase));
    }
    cp.snapshot_unfinished = phase == 1;
    cp.file = r.str("binlog file name", kMaxFileName);
    cp.pos = r.u(8, "binlog position");
    check_coordinate(cp.file, cp.pos);
    cp.server_uuid = r.str("server uuid", 64);
    if (!cp.server_uuid.empty() && !valid_server_uuid(cp.server_uuid)) {
        throw MysqlCdcCheckpointError("its server uuid is not a UUID");
    }
    const auto schemas = r.u(4, "schema count");
    if (schemas > kMaxSchemas) {
        throw MysqlCdcCheckpointError("it lists " + std::to_string(schemas) + " schemas");
    }
    for (std::uint64_t i = 0; i < schemas; ++i) {
        auto table = r.str("schema's table", kMaxSchemaKey);
        auto fingerprint = r.str("schema", kMaxFingerprint);
        if (table.empty() || table.find('.') == std::string::npos) {
            throw MysqlCdcCheckpointError("a schema names no db.table");
        }
        if (fingerprint.empty() ||
            (fingerprint.front() != kUnknownDefinition && !parse_schema_fingerprint(fingerprint))) {
            throw MysqlCdcCheckpointError("the definition it records for " + table +
                                          " is malformed");
        }
        if (!cp.schemas.emplace(std::move(table), std::move(fingerprint)).second) {
            throw MysqlCdcCheckpointError("it lists a table's schema twice");
        }
    }
    if (r.left() != 0) {
        throw MysqlCdcCheckpointError("it has " + std::to_string(r.left()) +
                                      " bytes after its last field");
    }
    return cp;
}

// ---- binlog events ----------------------------------------------------------------------

BinlogEventClass classify_binlog_event(int type, std::string* why) {
    const auto unsupported = [&](const char* reason) {
        if (why != nullptr) {
            *why = reason;
        }
        return BinlogEventClass::Unsupported;
    };
    switch (type) {
        case 2:  // QUERY_EVENT
            return BinlogEventClass::Query;
        case 3:  // STOP_EVENT
            return BinlogEventClass::Stop;
        case 4:  // ROTATE_EVENT
            return BinlogEventClass::Rotate;
        case 16:  // XID_EVENT
            return BinlogEventClass::Xid;
        case 19:  // TABLE_MAP_EVENT
            return BinlogEventClass::TableMap;
        case 23:   // WRITE_ROWS_EVENT_V1
        case 24:   // UPDATE_ROWS_EVENT_V1
        case 25:   // DELETE_ROWS_EVENT_V1
        case 30:   // WRITE_ROWS_EVENT
        case 31:   // UPDATE_ROWS_EVENT
        case 32:   // DELETE_ROWS_EVENT
        case 166:  // WRITE_ROWS_COMPRESSED_EVENT_V1 (MariaDB; the library inflates it)
        case 167:  // UPDATE_ROWS_COMPRESSED_EVENT_V1
        case 168:  // DELETE_ROWS_COMPRESSED_EVENT_V1
        case 169:  // WRITE_ROWS_COMPRESSED_EVENT
        case 170:  // UPDATE_ROWS_COMPRESSED_EVENT
        case 171:  // DELETE_ROWS_COMPRESSED_EVENT
            return BinlogEventClass::Rows;
        case 27:  // HEARTBEAT_LOG_EVENT
        case 41:  // HEARTBEAT_LOG_EVENT_V2
            return BinlogEventClass::Heartbeat;
        case 33:   // GTID_LOG_EVENT
        case 34:   // ANONYMOUS_GTID_LOG_EVENT
        case 162:  // GTID_EVENT (MariaDB)
            return BinlogEventClass::TransactionStart;
        case 5:    // INTVAR_EVENT
        case 13:   // RAND_EVENT
        case 14:   // USER_VAR_EVENT
        case 15:   // FORMAT_DESCRIPTION_EVENT
        case 28:   // IGNORABLE_LOG_EVENT
        case 29:   // ROWS_QUERY_LOG_EVENT
        case 35:   // PREVIOUS_GTIDS_LOG_EVENT
        case 36:   // TRANSACTION_CONTEXT_EVENT
        case 37:   // VIEW_CHANGE_EVENT
        case 160:  // ANNOTATE_ROWS_EVENT (MariaDB)
        case 161:  // BINLOG_CHECKPOINT_EVENT (MariaDB)
        case 163:  // GTID_LIST_EVENT (MariaDB)
            return BinlogEventClass::Informational;
        case 26:  // INCIDENT_EVENT
            if (why != nullptr) {
                *why = "the master logged an incident: events it should have written are missing";
            }
            return BinlogEventClass::Fatal;
        case 20:
        case 21:
        case 22:
            return unsupported("a pre-GA rows event, which this source does not decode");
        case 6:
        case 8:
        case 9:
        case 10:
        case 11:
        case 12:
        case 17:
        case 18:
            return unsupported(
                "a LOAD DATA event of statement-based logging, whose rows are not in the binlog");
        case 38:
            return unsupported(
                "XA PREPARE: the rows of an XA transaction are logged at prepare and may be "
                "rolled back later");
        case 39:
            return unsupported(
                "a partial JSON update (binlog_row_value_options=PARTIAL_JSON), whose after "
                "image holds only the changed parts");
        case 40:
            return unsupported(
                "a compressed transaction payload (binlog_transaction_compression=ON), which this "
                "source does not decompress");
        case 164:
            return unsupported("MariaDB binlog encryption, which this source does not read");
        case 165:
            return unsupported("a compressed QUERY event (MariaDB log_bin_compress)");
        default:
            return unsupported("an event type this source does not know");
    }
}

bool is_v1_update_rows_event(int type) {
    return type == 24 || type == 167;
}

// ---- QUERY-event statements ---------------------------------------------------------------

QueryStatement classify_query(std::string_view statement, std::string_view db) {
    using K = QueryStatement::Kind;
    Lexer lx(statement);
    const auto first_tok = lx.next();
    if (first_tok.kind != Token::Kind::Word) {
        return {};
    }
    const auto first = upper(first_tok.text);
    QueryStatement q;
    if (first == "BEGIN") {
        q.kind = K::Begin;
        return q;
    }
    if (first == "START") {
        q.kind = is_word(lx.next(), "TRANSACTION") ? K::Begin : K::Other;
        return q;
    }
    if (first == "COMMIT") {
        q.kind = K::Commit;
        return q;
    }
    if (first == "ROLLBACK") {
        skip_words(lx, {"WORK"});
        q.kind = is_word(lx.peek(), "TO") ? K::InTransaction : K::Rollback;
        return q;
    }
    if (first == "SAVEPOINT" || first == "RELEASE") {
        q.kind = K::InTransaction;
        return q;
    }
    if (first == "XA") {
        const auto what = lx.next();
        if (is_word(what, "START") || is_word(what, "BEGIN")) {
            q.kind = K::XaStart;
            return q;
        }
        if (is_word(what, "COMMIT")) {
            // XA COMMIT xid ONE PHASE commits a transaction that was never
            // prepared, and closes the group its XA START opened.
            for (auto t = lx.next(); t.kind != Token::Kind::End; t = lx.next()) {
                if (is_word(t, "ONE") && is_word(lx.peek(), "PHASE")) {
                    q.kind = K::Commit;
                    return q;
                }
            }
        }
        q.kind = K::InTransaction;
        return q;
    }
    if (first == "ALTER") {
        return classify_alter(lx, db);
    }
    if (first == "CREATE") {
        return classify_create(lx, db);
    }
    if (first == "DROP") {
        return classify_drop(lx, db);
    }
    if (first == "RENAME") {
        return classify_rename(lx, db);
    }
    if (first == "TRUNCATE") {
        skip_words(lx, {"TABLE"});
        return one(K::Ddl, QueryStatement::Effect::Truncate, table_name(lx, db));
    }
    if (first == "INSERT" || first == "REPLACE" || first == "UPDATE" || first == "DELETE" ||
        first == "LOAD") {
        return classify_dml(first, lx, db);
    }
    return q;
}

// ---- TABLE_MAP optional metadata ------------------------------------------------------------

std::vector<std::string> parse_column_names(const std::uint8_t* data,
                                            std::size_t size,
                                            std::uint32_t columns) {
    std::vector<std::string> names;
    if (data == nullptr) {
        return names;
    }
    std::size_t p = 0;
    while (p < size) {
        std::string name;
        if (!read_lenenc_string(data, size, p, name)) {
            return {};
        }
        names.push_back(std::move(name));
    }
    if (names.size() != columns) {
        return {};
    }
    return names;
}

std::optional<std::vector<std::vector<std::string>>> parse_str_value_lists(const std::uint8_t* data,
                                                                           std::size_t size) {
    std::vector<std::vector<std::string>> lists;
    if (data == nullptr) {
        return lists;
    }
    std::size_t p = 0;
    while (p < size) {
        std::uint64_t count = 0;
        if (!read_lenenc(data, size, p, count) || count > size - p) {
            return std::nullopt;
        }
        std::vector<std::string> values;
        for (std::uint64_t i = 0; i < count; ++i) {
            std::string v;
            if (!read_lenenc_string(data, size, p, v)) {
                return std::nullopt;
            }
            values.push_back(std::move(v));
        }
        lists.push_back(std::move(values));
    }
    return lists;
}

// ---- schema checks ---------------------------------------------------------------------

bool binlog_type_matches(std::uint8_t binlog_type,
                         std::uint8_t metadata0,
                         std::string_view data_type) {
    const auto dt = data_type;
    const auto any = [&](std::initializer_list<std::string_view> names) {
        return std::find(names.begin(), names.end(), dt) != names.end();
    };
    switch (binlog_type) {
        case 1:
            return dt == "tinyint";
        case 2:
            return dt == "smallint";
        case 9:
            return dt == "mediumint";
        case 3:
            return any({"int", "integer"});
        case 8:
            return dt == "bigint";
        case 4:
            return dt == "float";
        case 5:
            return any({"double", "real", "double precision"});
        case 0:
        case 246:
            return any({"decimal", "numeric"});
        case 13:
            return dt == "year";
        case 10:
        case 14:
            return dt == "date";
        case 11:
        case 19:
            return dt == "time";
        case 12:
        case 18:
            return dt == "datetime";
        case 7:
        case 17:
            return dt == "timestamp";
        case 15:
        case 253:
            return any({"varchar", "varbinary"});
        case 254:
            // CHAR and BINARY, or ENUM and SET, whose real type is the first
            // metadata byte.
            if (metadata0 == 247) {
                return dt == "enum";
            }
            if (metadata0 == 248) {
                return dt == "set";
            }
            return any({"char", "binary"});
        case 247:
            return dt == "enum";
        case 248:
            return dt == "set";
        case 252:
            return any({"tinyblob",
                        "blob",
                        "mediumblob",
                        "longblob",
                        "tinytext",
                        "text",
                        "mediumtext",
                        "longtext"});
        case 245:
            return dt == "json";
        case 255:
            return any({"geometry",
                        "point",
                        "linestring",
                        "polygon",
                        "multipoint",
                        "multilinestring",
                        "multipolygon",
                        "geometrycollection",
                        "geomcollection"});
        case 16:
            return dt == "bit";
        default:
            return false;
    }
}

std::string schema_fingerprint(const std::vector<ResolvedColumnSpec>& columns) {
    std::string out;
    const auto field = [&](std::string_view s) {
        out += std::to_string(s.size());
        out.push_back(':');
        out.append(s);
    };
    for (const auto& c : columns) {
        field(c.name);
        field(c.data_type);
        out.push_back(c.is_unsigned ? 'u' : 's');
        out += std::to_string(c.labels.size());
        out.push_back('[');
        for (const auto& l : c.labels) {
            field(l);
        }
        out.push_back(']');
    }
    return out;
}

std::optional<std::vector<ResolvedColumnSpec>> parse_schema_fingerprint(std::string_view text) {
    std::size_t p = 0;
    // A decimal length, ':' and that many bytes.
    const auto field = [&](std::string& out) {
        std::size_t n = 0;
        std::size_t digits = 0;
        while (p < text.size() && std::isdigit(static_cast<unsigned char>(text[p])) != 0) {
            if (digits == 9 || (digits > 0 && n == 0)) {
                return false;  // too long, or a leading zero
            }
            n = n * 10 + static_cast<std::size_t>(text[p] - '0');
            ++digits;
            ++p;
        }
        if (digits == 0 || p >= text.size() || text[p] != ':' || n > text.size() - p - 1) {
            return false;
        }
        ++p;
        out.assign(text.substr(p, n));
        p += n;
        return true;
    };
    std::vector<ResolvedColumnSpec> columns;
    while (p < text.size()) {
        ResolvedColumnSpec c;
        if (!field(c.name) || !field(c.data_type) || p >= text.size() ||
            (text[p] != 'u' && text[p] != 's')) {
            return std::nullopt;
        }
        c.is_unsigned = text[p++] == 'u';
        std::size_t labels = 0;
        std::size_t digits = 0;
        while (p < text.size() && std::isdigit(static_cast<unsigned char>(text[p])) != 0) {
            if (digits == 6 || (digits > 0 && labels == 0)) {
                return std::nullopt;
            }
            labels = labels * 10 + static_cast<std::size_t>(text[p] - '0');
            ++digits;
            ++p;
        }
        if (digits == 0 || p >= text.size() || text[p] != '[') {
            return std::nullopt;
        }
        ++p;
        for (std::size_t i = 0; i < labels; ++i) {
            std::string label;
            if (!field(label)) {
                return std::nullopt;
            }
            c.labels.push_back(std::move(label));
        }
        if (p >= text.size() || text[p] != ']') {
            return std::nullopt;
        }
        ++p;
        columns.push_back(std::move(c));
    }
    if (columns.empty()) {
        return std::nullopt;
    }
    return columns;
}

ResolvedColumnSpec resolve_column(const InformationSchemaColumn& c) {
    const auto lower = [](std::string_view in) {
        std::string out{in};
        for (auto& ch : out) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        return out;
    };
    ResolvedColumnSpec s;
    s.name = c.name;
    s.data_type = lower(c.data_type);
    const auto type = lower(c.column_type);
    if (s.data_type == "enum" || s.data_type == "set") {
        // The members of "enum('a','b')"; MySQL doubles an embedded quote.
        const auto lp = c.column_type.find('(');
        std::size_t i = lp == std::string::npos ? c.column_type.size() : lp + 1;
        while (i < c.column_type.size() && c.column_type[i] != ')') {
            if (c.column_type[i] != '\'') {
                ++i;
                continue;
            }
            ++i;
            std::string label;
            while (i < c.column_type.size()) {
                if (c.column_type[i] == '\'') {
                    if (i + 1 < c.column_type.size() && c.column_type[i + 1] == '\'') {
                        label.push_back('\'');
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                label.push_back(c.column_type[i]);
                ++i;
            }
            s.labels.push_back(std::move(label));
        }
    } else {
        s.is_unsigned = type.find("unsigned") != std::string::npos;
    }
    return s;
}

}  // namespace clink::mysql
