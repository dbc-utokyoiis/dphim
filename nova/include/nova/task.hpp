#pragma once

#include <nova/config.hpp>
#include <nova/util/return_value_or_void.hpp>

#include <iostream>
#include <numa.h>

namespace nova {

struct task_allocator {
    //    void *operator new(std::size_t n) {
    //        return malloc_func(n);
    //    }
    //
    //    void operator delete(void *p) noexcept {
    //        free_func(p);
    //    }

    inline static void *(*malloc_func)(std::size_t) = std::malloc;
    inline static void (*free_func)(void *) = std::free;
};

template<typename T = void>
struct task;

struct task_final_awaiter {

    auto await_ready() const noexcept { return false; }

    template<typename P>
    auto await_suspend(coro::coroutine_handle<P> h) noexcept {
        return h.promise().continuation;
    }

    auto await_resume() noexcept {}
};

template<typename T>
struct task_promise : return_value_or_void<T>, task_allocator {

    auto initial_suspend() -> coro::suspend_always { return {}; }

    auto final_suspend() noexcept -> task_final_awaiter { return {}; }

    auto get_return_object() -> task<T> {
        return task<T>{coro::coroutine_handle<task_promise<T>>::from_promise(*this)};
    }

private:
    friend task_final_awaiter;
    friend task<T>;
    coro::coroutine_handle<> continuation;
};

template<typename T>
struct [[nodiscard]] task : coroutine_base<task_promise<T>> {

    using promise_type = task_promise<T>;

    friend promise_type;

    using coroutine_base<promise_type>::coroutine_base;

    template<bool is_move>
    struct task_awaiter {
        explicit task_awaiter(task<T> *self)
            : self(self) {}

        auto await_ready() const noexcept {
            return !self->valid() || self->done();
        }

        auto await_suspend(coro::coroutine_handle<> awaiting) noexcept {
            self->get_promise().continuation = awaiting;
            return self->coro;
        }

        auto await_resume() -> decltype(auto) {
            if constexpr (is_move) {
                return std::move(self->get_promise()).result();
            } else {
                return self->get_promise().result();
            }
        }

    private:
        task<T> *self;
    };

    auto operator co_await() &noexcept { return task_awaiter<false>{this}; }
    auto operator co_await() &&noexcept { return task_awaiter<true>{this}; }
};

}// namespace nova