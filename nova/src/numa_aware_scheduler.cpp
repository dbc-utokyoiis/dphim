#include <iostream>
#include <pthread.h>
#include <random>
#include <sstream>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/task.hpp>
#include <nova/util/circular_iterator.hpp>

#include <nova/jemalloc.hpp>

namespace nova {
inline namespace scheduler {
struct numa_aware_scheduler::worker : worker_base<worker> {
    using base = worker_base<worker>;
    using base::this_thread_worker_id;
    friend numa_aware_scheduler;

    explicit worker(id_t id, numa_aware_scheduler &sched)
        : base(id), sched(&sched),
          id_in_node([](id_t id, const numa_info::node_t &node) {
              return std::distance(node.cpu_ids().begin(),
                                   std::find(node.cpu_ids().begin(), node.cpu_ids().end(), id));
          }(id, sched.info.cpu2node(id))),
          cpus([](auto id, auto &sched) {
              std::vector<int> near_cpus, far_cpus;
              auto this_node = sched.info.cpu2node(id).id();
              for (auto &node: sched.info.nodes()) {
                  if (node.id() == this_node) {
                      near_cpus.insert(near_cpus.end(), node.cpu_ids().begin(), node.cpu_ids().end());
                  } else {
                      far_cpus.insert(far_cpus.end(), node.cpu_ids().begin(), node.cpu_ids().end());
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
        if (node_id) {
            sched->sleeping_worker_counts[*node_id].fetch_add(1, MEM_ORDER_RELAXED);
        }
        base::try_sleep();
        if (node_id) {
            sched->sleeping_worker_counts[*node_id].fetch_sub(1, MEM_ORDER_REL);
        }
    }

    auto execute_one() -> bool {
        if (!this_thread_worker_id) {
            throw std::runtime_error("This worker is executed on an unlinked thread.");
        }
        if (task_list.consume_once([](auto *op) {
                op->execute();
            }) > 0) {
            return true;
        }
        if (sched->try_steal(this->id, [](auto *op) {
                op->execute();
            })) {
            return true;
        }
        return false;
    }

    std::optional<int> get_corresponding_worker_id(int node) const {
        auto &cpu_ids = sched->info.nodes()[node].cpu_ids();
        if (id_in_node < cpu_ids.size()) {
            return cpu_ids[id_in_node];
        } else {
            return std::nullopt;
        }
    }

    concurrent_stack<task_base *> task_list;
    numa_aware_scheduler *sched;
    int id_in_node;

    std::pair<std::vector<int>, std::vector<int>> cpus; /* near, far */

    CircularIterator<std::vector<int>::const_iterator> near_cpu_iter;
    CircularIterator<std::vector<int>::const_iterator> far_cpu_iter;
};

void numa_aware_scheduler::post(task_base *op, int dest_node_id) {
    bool posted = false;
    auto check = make_raii([&] {
        if (!posted)
            std::cerr << "failed to post: dest_node_id=" << dest_node_id << std::endl;
    });

    if (!op->ready())
        throw std::runtime_error("not ready op is posted.");
    if (dest_node_id >= static_cast<int>(node_local_task_queue.size())) {
        std::stringstream ss;
        ss << "dest_node_id(" << dest_node_id << ") >= node size(" << node_local_task_queue.size() << ")";
        throw std::runtime_error(ss.str());
    }

    if (dest_node_id == -1) {
        int c = 0;
        for (auto &swc: sleeping_worker_counts) {
            auto cnt = swc.load(MEM_ORDER_ACQ);
            if (cnt < 0) {
                throw std::runtime_error(
                        "sleeping worker count must be positive. but current value is " + std::to_string(cnt));
            }
            c += cnt;
        }

        if (c > 0 && worker::this_thread_worker_id.has_value()) {
            auto this_node = info.cpu2node(worker::this_thread_worker_id.value());
            for (auto &node_id: this_node.near_node_ids()) {
                for (auto cpu_id: info.node(node_id).cpu_ids()) {
                    if (auto &w = workers.at(cpu_id);
                        w && w->try_wake_up([op](auto &&w) { w.post(op); })) {
                        posted = true;
                        return;
                    }
                }
            }
        }

        if (auto w = worker::this_thread_worker_id) {
            workers.at(*w)->post(op);
            posted = true;
            workers.at(*w)->force_wake_up();
        } else {
            global_task_queue.push_front(op);
            posted = true;
            for (auto &worker: workers) {
                if (worker && worker->try_wake_up()) {
                    return;
                }
            }
        }
    } else {
        auto c = sleeping_worker_counts.at(dest_node_id).load(MEM_ORDER_ACQ);
        if (c > 0) {// if a sleeping worker exists
            for (auto cpu: info.node(dest_node_id).cpu_ids()) {
                if (auto &w = workers.at(cpu);
                    w && w->try_wake_up([op](auto &&w) { w.post(op); })) {
                    posted = true;
                    return;
                }
            }
        }

        if (auto w = worker::this_thread_worker_id) {// if the task is posted from a worker thread
            auto worker = workers.at(*w);
            auto this_node = info.cpu2node(*w);
            auto id_in_node = worker->id_in_node;
            auto w2 = workers.at(info.node(dest_node_id).cpu_ids().at(id_in_node));
            if (w2) {
                w2->post(op);
                posted = true;
                w2->force_wake_up();
                return;
            }
        }

        node_local_task_queue.at(dest_node_id).push_front(op);
        posted = true;
        for (auto &cpu_id: info.node(dest_node_id).cpu_ids()) {
            auto w = workers.at(cpu_id);
            if (w && w->try_wake_up()) {
                return;
            }
        }
    }
}

bool numa_aware_scheduler::try_steal(id_t cpu, void (*func)(task_base *)) {
    const auto &this_node = info.cpu2node(cpu);

    if (node_local_task_queue.at(this_node.id()).consume_once(func) > 0)
        return true;

    if (global_task_queue.consume_once(func) > 0)
        return true;

    auto &near_cpu = workers.at(cpu)->near_cpu_iter;
    auto near_first = near_cpu;
    do {
        if (auto &w = workers.at(*near_cpu); w && *near_cpu != cpu) {
            if (w->task_list.consume_once(func) > 0) {
                return true;
            }
        }
    } while (++near_cpu != near_first);

    auto &far_cpu = workers.at(cpu)->far_cpu_iter;
    const auto far_first = far_cpu;
    do {
        if (auto &w = workers.at(*far_cpu); w && *far_cpu != cpu) {
            if (w->task_list.consume_once(func)) {
                return true;
            }
        }
        ++far_cpu;
    } while (far_cpu != far_first);

    return false;
}


void numa_aware_scheduler::stop_request() {
    for (auto &w: workers) {
        if (w) {
            w->stop_request();
        }
    }
}

// thread_local std::unique_ptr<memory_pool> mp = nullptr;

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

#ifdef HAS_JEMALLOC
    if (jemalloc_mem_control) {
        memory::setup_thread(cpu, info.cpu2node(cpu).id());
    }
#endif

    workers.at(cpu) = std::make_shared<worker>(cpu, *this);
    workers.at(cpu)->run();
}

numa_aware_scheduler::numa_aware_scheduler(std::size_t thread_num, bool jemalloc_mem_control, bool interleaved)
    : scheduler_base(thread_num),
      info(),
      workers(numa_num_configured_cpus()),
      sleeping_worker_counts(numa_num_configured_nodes()),
      node_local_task_queue(numa_num_configured_nodes()),
      jemalloc_mem_control(jemalloc_mem_control) {

    if (interleaved) {
        // 各numaノードに均等に割り当て
        [&] {
            while (true) {
                for (auto local_cid = 0;; ++local_cid) {
                    for (auto &node: info.nodes()) {
                        auto cpu = node.cpu_ids().at(local_cid);
                        tid2cpu.push_back(cpu);
                        if (tid2cpu.size() == thread_num) {
                            return;
                        }
                    }
                }
            }
        }();
    } else {
        // node0から埋めていく
        for (auto &node: info.nodes()) {
            for (auto cpu: node.cpu_ids()) {
                tid2cpu.push_back(cpu);
            }
        }
    }


    cpu2tid.resize(tid2cpu.size());
    for (auto tid = 0ul; tid < tid2cpu.size(); ++tid) {
        cpu2tid[tid2cpu[tid]] = tid;
    }

#ifdef HAS_JEMALLOC
    if (jemalloc_mem_control) {
        memory::setup(thread_num);
    }
#endif

    // if (use_mem_pool) {
    //     std::cout << "use mem pool" << std::endl;
    //     nova::task_allocator::malloc_func = [](std::size_t n) -> void * {
    //         if (mp) {
    //             return mp->alloc(n);
    //         } else {
    //             return std::malloc(n);
    //         }
    //     };
    //     nova::task_allocator::free_func = [](void *p) -> void {
    //         if (mp) {
    //             mp->dealloc(p);
    //         } else {
    //             std::free(p);
    //         }
    //     };
    // }
}

const numa_info::node_t &numa_aware_scheduler::get_current_node() {
    // return info.cpu_to_node(worker::this_thread_worker_id.value_or(0));
    return info.cpu2node(worker::this_thread_worker_id.value_or(0));
}

std::optional<int> numa_aware_scheduler::get_current_cpu_id() const {
    return worker::this_thread_worker_id;
}

std::optional<int> numa_aware_scheduler::get_current_node_id() const {
    if (auto w = worker::this_thread_worker_id) {
        return info.cpu2node(*w).id();
    } else {
        return std::nullopt;
    }
}

std::optional<int> numa_aware_scheduler::get_max_node_id() const {
    int max_node_id = 0;
    for (auto &w: workers) {
        if (w) {
            max_node_id = std::max(max_node_id, info.cpu2node(w->id).id());
        }
    }
    return max_node_id;
}

std::optional<int> numa_aware_scheduler::get_corresponding_cpu_id(int node) const {
    if (auto w = worker::this_thread_worker_id) {
        return workers[*w]->get_corresponding_worker_id(node);
    } else {
        return std::nullopt;
    }
}

}// namespace scheduler
}// namespace nova
