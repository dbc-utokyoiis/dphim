#pragma once

#include <nova/config.hpp>
#include <nova/worker.hpp>

#include <iostream>
#include <thread>
#include <vector>

namespace nova {
inline namespace scheduler {

inline static constexpr int OPTION_DEFAULT = -1;
inline static constexpr int OPTION_NO_AWAIT = -2;

struct scheduler_base {

    explicit scheduler_base(std::size_t thread_num)
        : thread_num(thread_num), thread_pool(thread_num) {}

    virtual ~scheduler_base() = default;

    struct [[nodiscard]] operation : nova::task_base {

        explicit operation(nova::scheduler_base *sched, int option)
            : sched(sched), coro{}, option(option) {}
        ~operation() override = default;
        auto await_ready() const noexcept { return option == OPTION_NO_AWAIT; }
        void await_suspend(nova::coro::coroutine_handle<> h) noexcept {
            coro = h;
            sched->post(this, option);
        }
        void await_resume() const noexcept {}
        void execute() override { coro.resume(); }

    private:
        nova::scheduler_base *sched;
        nova::coro::coroutine_handle<> coro;
        int option = OPTION_DEFAULT;
    };

    auto schedule(int option = OPTION_DEFAULT) -> operation {
        return operation{this, option};
    }

    void start() {
        for (auto i = 0; i < int(thread_pool.size()); ++i) {
            thread_pool.at(i) = std::thread{[this, i] {
                this->run_worker(i);
            }};
        }
    }

    void stop() {
        this->stop_request();
        for (auto &th: thread_pool) {
            if (th.joinable())
                th.join();
        }
    }

    virtual void post(task_base *task, int option) = 0;

    virtual std::optional<int> get_current_cpu_id() const { return std::nullopt; }
    virtual std::optional<int> get_current_node_id() const { return std::nullopt; }
    virtual std::optional<int> get_max_node_id() const { return std::nullopt; }

protected:
    virtual void run_worker(int cpu) = 0;
    virtual void stop_request() = 0;

    std::size_t thread_num;

private:
    std::vector<std::thread> thread_pool;
};

}// namespace scheduler
}// namespace nova