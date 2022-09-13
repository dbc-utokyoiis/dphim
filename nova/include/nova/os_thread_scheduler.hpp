#pragma once

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/worker.hpp>

#include <pthread.h>
#include <vector>

namespace nova {
inline namespace scheduler {

struct os_thread_scheduler : scheduler_base {

    explicit os_thread_scheduler(int thread_num)
        : scheduler_base(thread_num) {
        CPU_ZERO(&cpuset);
        for (int cpu = 0; cpu < thread_num; ++cpu)
            CPU_SET(cpu, &cpuset);

        auto self_th = pthread_self();
        if (auto e = pthread_setaffinity_np(self_th, sizeof(cpuset), &cpuset); e != 0) {
            throw std::runtime_error(std::string("[pthread_setaffinity_np] ") + strerror(e));
        }
    }

    void post(task_base *op, [[maybe_unused]] int option) override {
        pthread_attr_t attr;
        pthread_t thread;

        pthread_attr_init(&attr);
        if (auto err = pthread_attr_setstacksize(&attr, 2 * PTHREAD_STACK_MIN); err != 0) {
            std::cout << "[os_thread_scheduler] Error: " << strerror(err) << std::endl;
            std::abort();
        }
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (auto err = pthread_create(
                    &thread, &attr, [](void *arg) -> void * {
                        reinterpret_cast<task_base *>(arg)->execute();
                        return nullptr;
                    },
                    op);
            err != 0) {
            std::cout << "[os_thread_scheduler] Error: " << strerror(err) << std::endl;
            std::abort();
        }
    }

private:
    void run_worker([[maybe_unused]] int tid) override {}
    void stop_request() override {}

    cpu_set_t cpuset;
};

}// namespace scheduler
}// namespace nova