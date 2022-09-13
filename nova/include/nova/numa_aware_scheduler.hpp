#pragma once

#include <cstddef>
#include <cstring>
#include <malloc.h>
#include <memory>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <vector>

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/util/atomic_intrusive_list.hpp>
#include <nova/util/circular_view.hpp>
#include <nova/util/numa_info.hpp>

namespace nova {
inline namespace scheduler {

struct memory_pool {
    struct header_t {
        std::size_t size;
        header_t *next;
        alignas(std::max_align_t) char data[0];

        void *get_region() { return reinterpret_cast<void *>(&data[0]); }
    };

    memory_pool(std::vector<std::pair<std::size_t, std::size_t>> config) {
        for (auto [size, num]: config) {
            header_t *h = nullptr;
            for (auto i = 0ul; i < num; ++i)
                h = new_node(h, size);
            bins.emplace_back(size, h);
        }
    }

    ~memory_pool() {
        for (auto [size, p]: bins) {
            while (p) {
                auto next = p->next;
                std::free(p);
                p = next;
            }
        }
    }

    void *alloc(std::size_t n) {
        for (auto &[size, head]: bins) {
            if (size == n) {
                auto *ret = head;
                if (ret) {
                    head = ret->next;
                    return ret->get_region();
                }
            }
        }
        return new_node(nullptr, n)->get_region();
    }

    void dealloc(void *p) {
        auto *h = reinterpret_cast<header_t *>(reinterpret_cast<char *>(p) - sizeof(header_t));
        for (auto &[size, head]: bins) {
            if (size == h->size) {
                h->next = head;
                head = h;
                return;
            }
        }
        std::free(h);
    }

private:
    std::vector<std::pair<std::size_t, header_t *>> bins;

    header_t *new_node(header_t *prev, std::size_t n) {
        char *p = (char *) std::malloc(sizeof(header_t) + n);
        //  char *p = (char *) numa_alloc_local(sizeof(header_t) + n);
        std::memset(p, 0, sizeof(header_t) + n);
        auto *h = reinterpret_cast<header_t *>(p);
        h->next = prev;
        h->size = n;
        return h;
    }
};

struct numa_aware_scheduler : scheduler_base {
    struct worker;
    using id_t = worker_base<worker>::id_t;

    explicit numa_aware_scheduler(std::size_t thread_num, bool use_mem_pool);

    task_base *try_steal(id_t cpu);

    void post(task_base *op, int option) override;

    const numa_info::node_t &get_current_node();
    const numa_info &get_numa_info() { return info; }

    std::optional<int> get_current_cpu_id() const override;
    std::optional<int> get_current_node_id() const override;
    std::optional<int> get_max_node_id() const override;

private:
    void run_worker(int tid) override;
    void stop_request() override;

    numa_info info;
    std::vector<std::shared_ptr<worker>> workers;
    std::vector<std::atomic<int>> sleeping_worker_counts;// each node

    atomic_intrusive_list<task_base, &task_base::next> global_task_queue;

    std::vector<atomic_intrusive_list<task_base, &task_base::next>> node_local_task_queue;

    std::vector<int> tid2cpu;
    bool use_mem_pool;
};

}// namespace scheduler
}// namespace nova
