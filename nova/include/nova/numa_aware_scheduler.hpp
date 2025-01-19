#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/util/concurrent_list.hpp>
#include <nova/util/numa_info.hpp>

namespace nova {
inline namespace scheduler {

struct numa_aware_scheduler : scheduler_base {
    struct worker;
    using id_t = worker_base<worker>::id_t;

    explicit numa_aware_scheduler(std::size_t thread_num, bool jemalloc_mem_control = false, bool interleaved = false);

    bool try_steal(id_t cpu, void (*func)(task_base *));

    void post(task_base *op, int dest_node_id) override;

    const numa_info::node_t &get_current_node();
    const numa_info &get_numa_info() { return info; }

    [[nodiscard]] std::optional<int> get_current_cpu_id() const override;
    [[nodiscard]] std::optional<int> get_current_node_id() const override;
    [[nodiscard]] std::optional<int> get_max_node_id() const override;
    [[nodiscard]] std::optional<int> get_corresponding_cpu_id(int /*node*/) const override;

private:
    void run_worker(int tid) override;
    void stop_request() override;

    numa_info info;
    std::vector<std::shared_ptr<worker>> workers;
    std::vector<std::atomic<int>> sleeping_worker_counts;// each node

    concurrent_stack<task_base *> global_task_queue;

    std::vector<concurrent_stack<task_base *>> node_local_task_queue;

    std::vector<int> tid2cpu, cpu2tid;
    bool use_mem_pool;
    bool jemalloc_mem_control;
};

}// namespace scheduler
}// namespace nova
