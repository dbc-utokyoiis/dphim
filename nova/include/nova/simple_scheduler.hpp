#pragma once

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/util/atomic_intrusive_list.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace nova {

inline namespace scheduler {

struct simple_scheduler : scheduler_base {

    struct worker_t;
    using id_t = worker_base<worker_t>::id_t;

    explicit simple_scheduler(std::size_t worker_num = std::thread::hardware_concurrency())
        : scheduler_base(worker_num), workers(worker_num) {}

    task_base *try_steal(id_t stealer);

    void delegate(task_base *op, [[maybe_unused]] std::optional<id_t> source_worker);
    void post(task_base *op, int option) override;

private:
    void run_worker(int tid) override;
    void stop_request() override;

    std::vector<std::shared_ptr<worker_t>> workers;
    std::atomic<std::size_t> worker_count = 0;
    atomic_intrusive_list<task_base, &task_base::next> global_task_queue;
};

}// namespace scheduler
}// namespace nova