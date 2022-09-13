#pragma once

#include <atomic>
#include <optional>
#include <ostream>
#include <thread>

#include <nova/util/atomic_intrusive_list.hpp>

#include <iostream>

namespace nova {
inline namespace scheduler {

struct task_base {
    task_base() = default;
    task_base(const task_base &) = delete;
    task_base &operator=(const task_base &) = delete;
    task_base(task_base &&) = delete;
    task_base &operator=(task_base &&) = delete;
    virtual ~task_base() = default;
    virtual void execute() = 0;
    task_base *next = nullptr;
};

enum class WorkerState {
    Running,
    Sleeping,
    Notified,
};

inline std::ostream &operator<<(std::ostream &os, WorkerState s) {
    switch (s) {
        case WorkerState::Running:
            return os << "R";
        case WorkerState::Sleeping:
            return os << "S";
        case WorkerState::Notified:
            return os << "N";
    }
}

template<typename Derived>
struct worker_base {
    using id_t = std::int32_t;
    inline static thread_local std::optional<id_t> this_thread_worker_id = std::nullopt;

    explicit worker_base(id_t id) : id(id) {}
    worker_base() = delete;
    worker_base(const worker_base &) = delete;

    void run() {
        if (this_thread_worker_id.has_value()) {
            throw std::runtime_error{"already run_worker worker on this thread."};
        }
        this_thread_worker_id = this->id;
        state.store(WorkerState::Running, std::memory_order_release);
        while (true) {
            while (static_cast<Derived *>(this)->execute_one()) {}
            static_cast<Derived *>(this)->try_sleep();
            if (is_stop_requested.load(std::memory_order_acquire)) {
                this_thread_worker_id = std::nullopt;
                return;
            }
        }
        this_thread_worker_id = std::nullopt;
    }

public:
    struct nop {
        void operator()(auto &&) {}
    };

    template<typename F = nop>
    bool try_wake_up(F &&before_notify = {}) {
        auto e = WorkerState::Sleeping;
        if (state.compare_exchange_strong(e, WorkerState::Notified, std::memory_order_release, std::memory_order_relaxed)) {
            before_notify(*static_cast<Derived *>(this));
            state.notify_all();
            return true;
        } else {
            return false;
        }
    }

    void stop_request() {
        is_stop_requested.store(true, std::memory_order_release);
        state.store(WorkerState::Notified, std::memory_order_release);
        state.notify_all();
    }

protected:
    void try_sleep(WorkerState e = WorkerState::Running) {
        while (state.compare_exchange_strong(e, WorkerState::Sleeping, std::memory_order_release, std::memory_order_relaxed)) {
            for (int i = 0; i < 50; ++i) {
                if (static_cast<Derived *>(this)->execute_one()) {
                    state.store(WorkerState::Running, std::memory_order_release);
                    return;
                }
                std::this_thread::yield();
            }
            state.wait(WorkerState::Sleeping, std::memory_order_acquire);
            e = WorkerState::Sleeping;
        }
        e = WorkerState::Notified;
        state.compare_exchange_strong(e, WorkerState::Running, std::memory_order_release, std::memory_order_relaxed);
    }

    const id_t id;
    std::atomic<bool> is_stop_requested = false;
    std::atomic<WorkerState> state = WorkerState::Running;
};
}// namespace scheduler
}// namespace nova