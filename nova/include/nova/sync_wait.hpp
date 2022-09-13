#pragma once

#include "nova/util/return_value_or_void.hpp"
#include "nova/util/synchronizer.hpp"
#include <nova/config.hpp>
#include <nova/type_traits.hpp>

#include <utility>

namespace nova {

template<typename T, typename Sync = synchronizer>
struct sync_wait_task;

template<typename T, typename Sync>
struct sync_wait_promise : return_value_or_void<T>, Sync {

    auto start() {
        coro::coroutine_handle<sync_wait_promise>::from_promise(*this).resume();
    }

    auto initial_suspend() -> coro::suspend_always { return {}; }

    auto final_suspend() noexcept {
        struct finish_notifier {
            auto await_ready() const noexcept { return false; }
            auto await_suspend(coro::coroutine_handle<sync_wait_promise> handle) const noexcept {
                return handle.promise().notify();
            }
            auto await_resume() const noexcept {}
        };
        return finish_notifier{};
    }

    auto get_return_object() -> sync_wait_task<T, Sync> {
        return sync_wait_task<T, Sync>{coro::coroutine_handle<sync_wait_promise>::from_promise(*this)};
    }
};

template<typename T, typename Sync>
struct sync_wait_task : coroutine_base<sync_wait_promise<T, Sync>> {
    using promise_type = sync_wait_promise<T, Sync>;
    friend promise_type;
    using coroutine_base<promise_type>::coroutine_base;

    auto start() { this->get_promise().start(); }

    auto wait() {
        this->get_promise().wait();
    }
};

template<typename Awaitable>
auto sync_wait(Awaitable &&awaitable) -> decltype(auto) {

    using awaiter_result_t = typename awaitable_traits<decltype(awaitable)>::awaiter_result_t;

    auto wait_task = [](auto &&awaitable) -> sync_wait_task<awaiter_result_t> {
        if constexpr (std::is_void_v<awaiter_result_t>) {
            co_await std::forward<decltype(awaitable)>(awaitable);
        } else {
            co_return co_await std::forward<decltype(awaitable)>(awaitable);
        }
    }(std::forward<Awaitable>(awaitable));

    wait_task.start();
    wait_task.wait();

    if constexpr (std::is_rvalue_reference_v<Awaitable &&>) {
        return std::move(wait_task.get_promise()).result();
    } else {
        return wait_task.get_promise().result();
    }
}

}// namespace nova