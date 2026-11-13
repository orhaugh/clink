// Holds a Relate store open for writing, as another job or tool would, until
// its standard input closes or it is killed. Prints "holding" once the store
// is open, or "refused <reason>" and exits 1 when it cannot open it.
//
//   clink_relate_store_holder <store>

#include <array>
#include <cstdio>
#include <relate.h>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: clink_relate_store_holder <store>\n");
        return 2;
    }
    relate_database db = nullptr;
    char* err = nullptr;
    if (relate_database_open(argv[1], &db, &err) != RELATE_OK) {
        std::printf("refused %s\n", err != nullptr ? err : "");
        relate_free_string(err);
        return 1;
    }
    std::printf("holding\n");
    std::fflush(stdout);
    std::array<char, 64> buf{};
    while (std::fread(buf.data(), 1, buf.size(), stdin) > 0) {
    }
    relate_database_close(db);
    return 0;
}
