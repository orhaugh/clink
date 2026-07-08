// libclink: the C ABI over EmbeddedEngine (see include/clink/embed/clink.h
// for the contract). Every entry point catches C++ exceptions at the
// boundary and reports through the per-engine last-error string; engine
// diagnostics (the EmbeddedEngine err stream) are captured into the same
// place rather than written to stderr, since this is a library.

#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <arrow/c/bridge.h>

#include "clink/embed/clink.h"
#include "clink/embed/embedded_engine.hpp"

namespace {

thread_local std::string g_open_error;

constexpr std::chrono::milliseconds kWaitSlice{200};

}  // namespace

// The opaque handle: the engine plus the captured-diagnostics buffer its
// err stream writes into, and the last-error snapshot the C API exposes.
struct clink_engine {
    std::ostringstream diag;
    std::unique_ptr<clink::embed::EmbeddedEngine> engine;
    std::mutex err_mutex;
    std::string last_error;

    // Move the freshly captured diagnostics (plus an optional prefix)
    // into last_error; clears the capture buffer.
    void set_error(const std::string& prefix) {
        std::lock_guard lk(err_mutex);
        std::string captured = diag.str();
        diag.str({});
        last_error = prefix;
        if (!captured.empty()) {
            if (!last_error.empty()) {
                last_error += ": ";
            }
            last_error += captured;
        }
    }

    void clear_error() {
        std::lock_guard lk(err_mutex);
        diag.str({});
        last_error.clear();
    }
};

extern "C" {

int32_t clink_abi_version(void) {
    return CLINK_EMBED_ABI_VERSION;
}

clink_engine* clink_engine_open(const clink_engine_options* options) {
    auto handle = std::make_unique<clink_engine>();
    clink::embed::EngineOptions opts;
    if (options != nullptr) {
        if (options->parallelism > 0) {
            opts.parallelism = options->parallelism;
        }
        if (options->state_backend_uri != nullptr) {
            opts.state_backend_uri = options->state_backend_uri;
        }
        if (options->checkpoint_dir != nullptr) {
            opts.checkpoint_dir = options->checkpoint_dir;
        }
        if (options->checkpoint_interval_ms > 0) {
            opts.checkpoint_interval_ms = options->checkpoint_interval_ms;
        }
        if (options->catalog_dir != nullptr) {
            opts.catalog_dir = options->catalog_dir;
        }
    }
    // Library mode: capture diagnostics instead of writing to stderr; rows
    // sent to a print sink still reach stdout by design.
    opts.err = &handle->diag;
    opts.out = &handle->diag;
    try {
        handle->engine = std::make_unique<clink::embed::EmbeddedEngine>(std::move(opts));
    } catch (const std::exception& e) {
        g_open_error = e.what();
        const std::string captured = handle->diag.str();
        if (!captured.empty()) {
            g_open_error += ": " + captured;
        }
        return nullptr;
    }
    g_open_error.clear();
    return handle.release();
}

const char* clink_open_error(void) {
    return g_open_error.c_str();
}

void clink_engine_close(clink_engine* engine) {
    if (engine == nullptr) {
        return;
    }
    try {
        engine->engine->cancel_all();
    } catch (...) {
    }
    delete engine;  // ~EmbeddedEngine aborts collect streams, stops TM + JM
}

int clink_exec(clink_engine* engine, const char* sql) {
    if (engine == nullptr || sql == nullptr) {
        return -1;
    }
    engine->clear_error();
    try {
        const int rc = engine->engine->execute_script(sql);
        if (rc != 0) {
            engine->set_error("");
        }
        return rc;
    } catch (const std::exception& e) {
        engine->set_error(e.what());
        return -1;
    }
}

const char* clink_last_error(clink_engine* engine) {
    if (engine == nullptr) {
        return "";
    }
    std::lock_guard lk(engine->err_mutex);
    return engine->last_error.c_str();
}

size_t clink_job_count(clink_engine* engine) {
    if (engine == nullptr) {
        return 0;
    }
    return engine->engine->job_count();
}

uint64_t clink_job_id_at(clink_engine* engine, size_t index) {
    if (engine == nullptr) {
        return 0;
    }
    const auto ids = engine->engine->job_ids();
    return index < ids.size() ? ids[index] : 0;
}

int clink_job_wait(clink_engine* engine, uint64_t job_id, int64_t timeout_ms) {
    if (engine == nullptr) {
        return -1;
    }
    engine->clear_error();
    const bool infinite = timeout_ms < 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{infinite ? 0 : timeout_ms};
    try {
        while (true) {
            if (engine->engine->await_job(job_id, kWaitSlice)) {
                return 0;
            }
            if (!infinite && std::chrono::steady_clock::now() >= deadline) {
                return 1;
            }
        }
    } catch (const std::exception& e) {
        engine->set_error(e.what());
        return -1;
    }
}

int clink_job_cancel(clink_engine* engine, uint64_t job_id) {
    if (engine == nullptr) {
        return -1;
    }
    engine->clear_error();
    try {
        engine->engine->cancel_job(job_id);
        return 0;
    } catch (const std::exception& e) {
        engine->set_error(e.what());
        return -1;
    }
}

int clink_await_all(clink_engine* engine, int64_t timeout_ms) {
    if (engine == nullptr) {
        return -1;
    }
    engine->clear_error();
    const bool infinite = timeout_ms < 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{infinite ? 0 : timeout_ms};
    try {
        for (const auto id : engine->engine->job_ids()) {
            while (!engine->engine->await_job(id, kWaitSlice)) {
                if (!infinite && std::chrono::steady_clock::now() >= deadline) {
                    return 1;
                }
            }
        }
        std::string errors;
        for (const auto id : engine->engine->job_ids()) {
            for (const auto& e : engine->engine->job_errors(id)) {
                if (!errors.empty()) {
                    errors += "; ";
                }
                errors += "job " + std::to_string(id) + ": " + e;
            }
        }
        if (!errors.empty()) {
            engine->set_error(errors);
            return -1;
        }
        return 0;
    } catch (const std::exception& e) {
        engine->set_error(e.what());
        return -1;
    }
}

void clink_cancel_all(clink_engine* engine) {
    if (engine == nullptr) {
        return;
    }
    try {
        engine->engine->cancel_all();
    } catch (...) {
    }
}

int clink_collect_stream(clink_engine* engine, const char* table, struct ArrowArrayStream* out) {
    if (engine == nullptr || table == nullptr || out == nullptr) {
        return -1;
    }
    engine->clear_error();
    try {
        auto reader = engine->engine->collect_reader(table);
        if (!reader.ok()) {
            engine->set_error(reader.status().ToString());
            return -1;
        }
        const auto st = arrow::ExportRecordBatchReader(*reader, out);
        if (!st.ok()) {
            engine->set_error(st.ToString());
            return -1;
        }
        return 0;
    } catch (const std::exception& e) {
        engine->set_error(e.what());
        return -1;
    }
}

}  // extern "C"
