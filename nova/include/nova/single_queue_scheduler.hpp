#pragma once

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/util/atomic_intrusive_list.hpp>
#include <nova/worker.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace nova {

inline namespace scheduler {

struct single_queue_scheduler : scheduler_base {

    struct worker_t;
    using id_t = worker_base<worker_t>::id_t;

    explicit single_queue_scheduler(std::size_t worker_num = std::thread::hardware_concurrency())
        : scheduler_base(worker_num), workers(worker_num) {}

    void post(task_base *op, int option) override;

private:
    void run_worker(int tid) override;
    void stop_request() override;

    std::vector<std::shared_ptr<worker_t>> workers;
    std::atomic<int> sleeping_worker_count = 0;

    //    atomic_intrusive_list<task_base, &task_base::next> global_task_queue;
    std::queue<task_base *> global_task_queue;
    std::mutex queue_mtx;
};

}// namespace scheduler
}// namespace nova