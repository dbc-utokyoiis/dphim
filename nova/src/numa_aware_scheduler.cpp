
#include <iostream>
#include <pthread.h>
#include <random>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/task.hpp>
#include <nova/util/circular_iterator.hpp>

namespace nova {
inline namespace scheduler {

struct numa_aware_scheduler::worker : worker_base<worker> {
    using base = worker_base<worker>;
    using base::this_thread_worker_id;
    friend numa_aware_scheduler;

    explicit worker(id_t id, numa_aware_scheduler &sched)
        : base(id), sched(&sched),
          cpus([](auto id, auto &sched) {
              std::vector<int> near_cpus, far_cpus;
              auto this_node = sched.info.cpu_to_node(id).id();
              for (auto &node: sched.info.nodes()) {
                  if (node.id() == this_node) {
                      near_cpus.insert(near_cpus.end(), node.cpus().begin(), node.cpus().end());
                  } else {
                      far_cpus.insert(far_cpus.end(), node.cpus().begin(), node.cpus().end());
                  }
              }

              static std::random_device seed_gen;
              static std::mt19937 engine(seed_gen());
              std::shuffle(near_cpus.begin(), near_cpus.end(), engine);
              std::shuffle(far_cpus.begin(), far_cpus.end(), engine);
              return std::make_pair(near_cpus, far_cpus);
          }(id, sched)),
          near_cpu_iter(cpus.first.begin(), cpus.first.end(), cpus.first.begin()),
          far_cpu_iter(cpus.second.begin(), cpus.second.end(), cpus.second.begin()) {
    }

    void post(task_base *tb) {
        task_list.push_front(tb);
    }

    void try_sleep() {
        auto node_id = sched->get_current_node_id();
        if (node_id)
            sched->sleeping_worker_counts[*node_id].fetch_add(1, std::memory_order_relaxed);
        base::try_sleep();
        if (node_id)
            sched->sleeping_worker_counts[*node_id].fetch_sub(1, std::memory_order_release);
    }

    auto execute_one() -> bool {
        if (!this_thread_worker_id) {
            throw std::runtime_error("simple_worker is executed on an unlinked thread.");
        }
        if (auto op = task_list.pop_front()) {
            op->execute();
            return true;
        }
        if (auto op = sched->try_steal(this->id)) {
            op->execute();
            return true;
        }
        return false;
    }


    atomic_intrusive_list<task_base, &task_base::next> task_list;
    numa_aware_scheduler *sched;

    std::pair<std::vector<int>, std::vector<int>> cpus; /* near, far */

    CircularIterator<std::vector<int>::const_iterator> near_cpu_iter;
    CircularIterator<std::vector<int>::const_iterator> far_cpu_iter;
};

task_base *numa_aware_scheduler::try_steal(id_t cpu) {
    const auto &this_node = info.cpu_to_node(cpu);

    if (auto *op = node_local_task_queue[this_node.id()].pop_front()) {
        return op;
    }

    if (auto *op = global_task_queue.pop_front()) {
        return op;
    }

    auto &near_cpu = workers[cpu]->near_cpu_iter;
    auto near_first = near_cpu;
    do {
        if (auto &w = workers[*near_cpu]; w && *near_cpu != cpu) {
            if (auto op = w->task_list.pop_front()) {
                return op;
            }
        }
    } while (++near_cpu != near_first);

    auto &far_cpu = workers[cpu]->far_cpu_iter;
    auto far_first = far_cpu;
    do {
        if (auto &w = workers[*far_cpu]; w && *far_cpu != cpu) {
            if (auto op = w->task_list.pop_front()) {
                return op;
            }
        }
    } while (++far_cpu != far_first);

    return nullptr;
}

void numa_aware_scheduler::post(task_base *op, int option) {

    if (option == -1) {
        int c = 0;
        for (auto &swc: sleeping_worker_counts) {
            auto cnt = swc.load(std::memory_order_acquire);
            if (cnt < 0)
                throw std::runtime_error("sleeping worker count must be positive. but current value is " + std::to_string(cnt));
            c += cnt;
        }

        if (c > 0) {
            using Iter = typename std::vector<int>::const_iterator;
            static thread_local std::vector<CircularIterator<Iter>> cpu_iters = [](auto &nodes) mutable {
                std::vector<CircularIterator<Iter>> ret;
                for (auto &node: nodes)
                    ret.emplace_back(CircularIterator<Iter>(node.cpus().begin(), node.cpus().end(), node.cpus().begin()));
                return ret;
            }(info.nodes());

            auto this_node = info.cpu_to_node(worker::this_thread_worker_id.value_or(0));
            for (auto &node: info.near_nodes(this_node.id())) {
                auto &it = cpu_iters[node];
                for (auto i = 0u; i < info.node(node).cpus().size(); ++i) {
                    auto cpu = *(it++);
                    if (auto &w = workers[cpu]) {
                        if (w->try_wake_up([op](auto &&w) { w.post(op); })) {
                            return;
                        }
                    }
                }
            }
        }

        if (auto w = worker::this_thread_worker_id) {
            workers[*w]->post(op);
            workers[*w]->try_wake_up();
        } else {
            global_task_queue.push_front(op);
            for (auto &worker: workers)
                if (worker && worker->try_wake_up())
                    return;
        }
    } else {

        auto c = sleeping_worker_counts[option].load(std::memory_order_acquire);
        if (c > 0) {
            for (auto cpu: info.node(option).cpus()) {
                if (auto &w = workers[cpu]; w && w->try_wake_up([op](auto &&w) { w.post(op); })) {
                    return;
                }
            }
        }

        if (auto w = worker::this_thread_worker_id) {
            auto this_node = info.cpu_to_node(*w);
            auto id_in_node = std::distance(this_node.cpus().begin(), std::find(this_node.cpus().begin(), this_node.cpus().end(), *w));
            auto w2 = workers[info.node(option).cpus()[id_in_node]];
            if (w2) {
                w2->post(op);
                w2->try_wake_up();
                return;
            }
        }

        node_local_task_queue[option].push_front(op);
        for (auto cpu: info.node(option).cpus()) {
            auto worker = workers[cpu];
            if (worker && worker->try_wake_up())
                return;
        }
    }
}

void numa_aware_scheduler::stop_request() {
    for (auto &w: workers) {
        if (w)
            w->stop_request();
    }
}

thread_local std::unique_ptr<memory_pool> mp = nullptr;

void numa_aware_scheduler::run_worker(int tid) {
    using namespace std::literals;
    auto cpu = tid2cpu.at(tid);

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);

    auto self_th = pthread_self();
    if (auto e = pthread_setaffinity_np(self_th, sizeof(cpuset), &cpuset); e != 0) {
        throw std::runtime_error("[pthread_setaffinity_np] "s + strerror(e));
    }

    numa_set_localalloc();
    numa_set_strict(true);

    if (use_mem_pool) {
        mp = std::unique_ptr<memory_pool>(new memory_pool({{136, cpu == 0 ? 100 : 10}, {112, 100}, {120, 100}, {320, 5000}}));
    }

    workers.at(cpu) = std::make_shared<worker>(cpu, *this);
    workers.at(cpu)->run();
}

numa_aware_scheduler::numa_aware_scheduler(std::size_t thread_num, bool use_mem_pool)
    : scheduler_base(thread_num),
      workers(numa_num_configured_cpus()),
      sleeping_worker_counts(numa_num_configured_nodes()),
      node_local_task_queue(numa_num_configured_nodes()),
      use_mem_pool(use_mem_pool) {

    // node0から埋めていく
    for (auto &node: info.nodes()) {
        for (auto cpu: node.cpus()) {
            tid2cpu.push_back(cpu);
        }
    }

    if (use_mem_pool) {
        std::cout << "use mem pool" << std::endl;
        nova::task_allocator::malloc_func = [](std::size_t n) -> void * {
            if (mp) {
                return mp->alloc(n);
            } else {
                return std::malloc(n);
            }
        };
        nova::task_allocator::free_func = [](void *p) -> void {
            if (mp) {
                mp->dealloc(p);
            } else {
                std::free(p);
            }
        };
    }
}

const numa_info::node_t &numa_aware_scheduler::get_current_node() {
    return info.cpu_to_node(worker::this_thread_worker_id.value_or(0));
}

std::optional<int> numa_aware_scheduler::get_current_cpu_id() const {
    return worker::this_thread_worker_id;
}

std::optional<int> numa_aware_scheduler::get_current_node_id() const {
    if (auto w = worker::this_thread_worker_id) {
        return info.cpu_to_node(*w).id();
    } else {
        return std::nullopt;
    }
}

std::optional<int> numa_aware_scheduler::get_max_node_id() const {
    return info.cpu_to_node(thread_num - 1).id();
}

}// namespace scheduler
}// namespace nova