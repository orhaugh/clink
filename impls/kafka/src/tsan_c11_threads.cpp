// C11 threads for librdkafka under ThreadSanitizer.
//
// librdkafka starts its threads with C11 thrd_create and synchronises them
// with mtx_* and cnd_*. glibc implements those by calling its own pthread
// internals directly, not through the public pthread_* symbols, and libtsan
// intercepts only the public ones. So TSan never registers librdkafka's
// threads, and the first allocation on one faults inside the TSan runtime
// ("SEGV on unknown address", then "nested bug in the same thread"). Any test
// that opened a producer, a consumer or a mock cluster crashed that way, and
// every such test had to be excluded from the TSan pass by name.
//
// These definitions forward each call to the pthread function glibc itself
// would have used, now through the public entry point libtsan intercepts. A
// copy linked into the executable interposes glibc's definitions for the
// whole process, because the executable comes first in symbol lookup; a copy
// in a module loaded later with dlopen is inert, since librdkafka's references
// are already bound by then. The behaviour matches glibc 2.41 call for call:
// the same mutex type mapping, the same pthread-to-thrd_* error mapping, the
// same int result carried through thrd_join and thrd_exit, and realtime
// deadlines for the timed waits.
//
// Compiled only into ThreadSanitizer builds on Linux (impls/kafka/CMakeLists.txt).

// __GLIBC__ comes from <features.h>, not from the compiler, so it can only be
// tested after that header.
#if defined(__linux__)
#include <features.h>
#endif

#if defined(__GLIBC__) && __has_include(<threads.h>)

#include <cerrno>
#include <cstdint>
#include <ctime>
#include <new>
#include <pthread.h>
#include <threads.h>

static_assert(sizeof(thrd_t) == sizeof(pthread_t) && alignof(thrd_t) == alignof(pthread_t));
static_assert(sizeof(mtx_t) == sizeof(pthread_mutex_t) &&
              alignof(mtx_t) == alignof(pthread_mutex_t));
static_assert(sizeof(cnd_t) == sizeof(pthread_cond_t) && alignof(cnd_t) == alignof(pthread_cond_t));
static_assert(sizeof(once_flag) == sizeof(pthread_once_t) &&
              alignof(once_flag) == alignof(pthread_once_t));

// Weak, because more than one object in a link can carry these definitions:
// every consumer of clink::kafka compiles this file, so an executable that
// also links an OBJECT library consuming clink::kafka gets two copies. The
// dynamic linker binds librdkafka to a weak definition in the executable
// exactly as it would to a strong one.
#define CLINK_C11_SHIM __attribute__((weak))

namespace {

int thrd_result(int rc) {
    switch (rc) {
        case 0:
            return thrd_success;
        case ENOMEM:
            return thrd_nomem;
        case EBUSY:
            return thrd_busy;
        case ETIMEDOUT:
            return thrd_timedout;
        default:
            return thrd_error;
    }
}

struct C11ThreadStart {
    thrd_start_t func;
    void* arg;
};

void* run_c11_thread(void* p) {
    const auto* start = static_cast<C11ThreadStart*>(p);
    const thrd_start_t func = start->func;
    void* const arg = start->arg;
    delete start;
    return reinterpret_cast<void*>(static_cast<std::intptr_t>(func(arg)));
}

pthread_mutex_t* as_pthread(mtx_t* m) {
    return reinterpret_cast<pthread_mutex_t*>(m);
}

pthread_cond_t* as_pthread(cnd_t* c) {
    return reinterpret_cast<pthread_cond_t*>(c);
}

}  // namespace

extern "C" {

CLINK_C11_SHIM int thrd_create(thrd_t* thr, thrd_start_t func, void* arg) {
    auto* start = new (std::nothrow) C11ThreadStart{func, arg};
    if (start == nullptr) {
        return thrd_nomem;
    }
    const int rc = pthread_create(thr, nullptr, run_c11_thread, start);
    if (rc != 0) {
        delete start;
    }
    return thrd_result(rc);
}

CLINK_C11_SHIM int thrd_join(thrd_t thr, int* res) {
    void* out = nullptr;
    const int rc = pthread_join(thr, &out);
    if (rc == 0 && res != nullptr) {
        *res = static_cast<int>(reinterpret_cast<std::intptr_t>(out));
    }
    return thrd_result(rc);
}

CLINK_C11_SHIM int thrd_detach(thrd_t thr) {
    return thrd_result(pthread_detach(thr));
}

CLINK_C11_SHIM void thrd_exit(int res) {
    pthread_exit(reinterpret_cast<void*>(static_cast<std::intptr_t>(res)));
}

CLINK_C11_SHIM int mtx_init(mtx_t* m, int type) {
    // glibc's mapping: only the two recursive combinations are recursive, and
    // every other value, including invalid ones, is a normal mutex.
    const bool recursive =
        type == (mtx_plain | mtx_recursive) || type == (mtx_timed | mtx_recursive);
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, recursive ? PTHREAD_MUTEX_RECURSIVE : PTHREAD_MUTEX_NORMAL);
    const int rc = pthread_mutex_init(as_pthread(m), &attr);
    pthread_mutexattr_destroy(&attr);
    return thrd_result(rc);
}

CLINK_C11_SHIM int mtx_lock(mtx_t* m) {
    return thrd_result(pthread_mutex_lock(as_pthread(m)));
}

CLINK_C11_SHIM int mtx_timedlock(mtx_t* m, const struct timespec* abs_time) {
    return thrd_result(pthread_mutex_timedlock(as_pthread(m), abs_time));
}

CLINK_C11_SHIM int mtx_trylock(mtx_t* m) {
    return thrd_result(pthread_mutex_trylock(as_pthread(m)));
}

CLINK_C11_SHIM int mtx_unlock(mtx_t* m) {
    return thrd_result(pthread_mutex_unlock(as_pthread(m)));
}

CLINK_C11_SHIM void mtx_destroy(mtx_t* m) {
    pthread_mutex_destroy(as_pthread(m));
}

CLINK_C11_SHIM void call_once(once_flag* flag, void (*func)(void)) {
    pthread_once(reinterpret_cast<pthread_once_t*>(flag), func);
}

CLINK_C11_SHIM int cnd_init(cnd_t* c) {
    return thrd_result(pthread_cond_init(as_pthread(c), nullptr));
}

CLINK_C11_SHIM int cnd_signal(cnd_t* c) {
    return thrd_result(pthread_cond_signal(as_pthread(c)));
}

CLINK_C11_SHIM int cnd_broadcast(cnd_t* c) {
    return thrd_result(pthread_cond_broadcast(as_pthread(c)));
}

CLINK_C11_SHIM int cnd_wait(cnd_t* c, mtx_t* m) {
    return thrd_result(pthread_cond_wait(as_pthread(c), as_pthread(m)));
}

CLINK_C11_SHIM int cnd_timedwait(cnd_t* c, mtx_t* m, const struct timespec* abs_time) {
    return thrd_result(pthread_cond_timedwait(as_pthread(c), as_pthread(m), abs_time));
}

CLINK_C11_SHIM void cnd_destroy(cnd_t* c) {
    pthread_cond_destroy(as_pthread(c));
}

}  // extern "C"

#undef CLINK_C11_SHIM

#endif  // __GLIBC__ && <threads.h>
