#pragma once

#include <nova/config.hpp>
#include <nova/worker.hpp>

#include <functional>
#include <iostream>
#include <thread>
#include <vector>

#include <papi.h>

namespace nova {
inline namespace scheduler {

inline static constexpr int OPTION_DEFAULT = -1;
inline static constexpr int OPTION_NO_AWAIT = -2;

struct scheduler_base {

    explicit scheduler_base(std::size_t thread_num)
        : thread_num(thread_num), thread_pool(thread_num) {}

    scheduler_base(const scheduler_base &) = delete;
    scheduler_base(scheduler_base &&) noexcept = delete;

    virtual ~scheduler_base() = default;

    struct [[nodiscard]] operation : nova::task_base {

        explicit operation(nova::scheduler_base *sched, int option)
            : sched(sched), coro{nullptr}, option(option) {}

        operation(const operation &) = delete;
        operation(operation &&other) noexcept
            : sched(other.sched),
              coro(std::exchange(other.coro, {})),
              option(other.option) {}

        ~operation() override = default;

        operation operator()() const & {
            return operation(sched, option);
        }

        auto await_ready() const noexcept { return option == OPTION_NO_AWAIT; }
        void await_suspend(nova::coro::coroutine_handle<> h) {
            coro = h;
            sched->post(this, option);
        }
        void await_resume() const noexcept {}
        void execute() override {
            coro.resume();
        }
        bool ready() const override {
            return bool(coro) && !coro.done();
        }

    private:
        nova::scheduler_base *const sched;
        nova::coro::coroutine_handle<> coro;
        const int option = OPTION_DEFAULT;
    };

    auto schedule(int option = OPTION_DEFAULT) & -> operation {
        return operation(this, option);
    }

    void start(std::function<void()> callback = nullptr) {
        for (auto i = 0; i < int(thread_pool.size()); ++i) {
            thread_pool.at(i) = std::thread{[this, i, callback] {
                if (callback)
                    callback();
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

    [[nodiscard]] virtual std::optional<int> get_current_cpu_id() const { return std::nullopt; }
    [[nodiscard]] virtual std::optional<int> get_current_node_id() const { return std::nullopt; }
    [[nodiscard]] virtual std::optional<int> get_max_node_id() const { return std::nullopt; }
    [[nodiscard]] virtual std::optional<int> get_corresponding_cpu_id(int /*node*/) const { return std::nullopt; }

protected:
    virtual void run_worker(int cpu) = 0;
    virtual void stop_request() = 0;

    std::size_t thread_num;

private:
    std::vector<std::thread> thread_pool;
};

}// namespace scheduler
}// namespace nova
