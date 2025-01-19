#pragma once

#include <atomic>
#include <optional>
#include <ostream>
#include <thread>

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
    virtual bool ready() const { return true; }
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
        default:
            return os << "?";
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
        state.store(WorkerState::Running);
        while (true) {
            while (static_cast<Derived *>(this)->execute_one()) {}
            static_cast<Derived *>(this)->try_sleep();
            if (is_stop_requested.load()) {
                this_thread_worker_id = std::nullopt;
                return;
            }
        }
        this_thread_worker_id = std::nullopt;
    }

    struct nop {
        void operator()(auto &&) {}
    };

    template<typename F = nop>
    bool try_wake_up(F &&before_notify = {}) {
        auto e = WorkerState::Sleeping;
        if (state.compare_exchange_strong(e, WorkerState::Notified, MEM_ORDER_REL, MEM_ORDER_RELAXED)) {
            before_notify(*static_cast<Derived *>(this));
            state.notify_all();
            return true;
        } else {
            return false;
        }
    }

    void force_wake_up() {
        state.store(WorkerState::Notified);
        state.notify_all();
    }

    // bool is_running() const {
    //     return state.load(MEM_ORDER_ACQ) == WorkerState::Running;
    // }

    void stop_request() {
        is_stop_requested.store(true, MEM_ORDER_REL);
        force_wake_up();
    }

protected:
    void try_sleep(WorkerState e = WorkerState::Running) {
        if (this_thread_worker_id.value() != id) {
            throw std::runtime_error{"this_thread_worker_id != id"};
        }
        if (state.compare_exchange_strong(e, WorkerState::Sleeping)) {
            for (int i = 0; i < 100; ++i) {
                if (static_cast<Derived *>(this)->execute_one()) {
                    state.store(WorkerState::Running);
                    return;
                }
                std::this_thread::yield();
            }
            state.wait(WorkerState::Sleeping);// sleep if state is still WorkerState::Sleeping
        }
        state.store(WorkerState::Running);
    }

    const id_t id;
    std::atomic<bool> is_stop_requested = false;
    std::atomic<WorkerState> state = WorkerState::Running;
};
}// namespace scheduler
}// namespace nova