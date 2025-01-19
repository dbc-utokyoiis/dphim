#pragma once

#include <nova/config.hpp>
#include <nova/util/raii.hpp>
#include <nova/util/return_value_or_void.hpp>

#include <unordered_map>

namespace nova {

template<typename T = void, typename Alloc = void>
struct task;

struct task_final_awaiter {
    auto await_ready() const noexcept { return false; }

    template<typename P>
    auto await_suspend(coro::coroutine_handle<P> h) noexcept {
        return h.promise().continuation;
    }

    auto await_resume() noexcept {}
};

template<typename T, typename Alloc>
struct task_promise;

template<typename T>
struct task_promise<T, void> : return_value_or_void<T> {

    auto initial_suspend() -> coro::suspend_always { return {}; }

    auto final_suspend() noexcept -> task_final_awaiter { return {}; }

    auto get_return_object() -> task<T> {
        return task<T>{coro::coroutine_handle<task_promise>::from_promise(*this)};
    }

protected:
    friend task_final_awaiter;
    friend task<T, void>;
    coro::coroutine_handle<> continuation;
};

template<typename T, typename Alloc>
struct task_promise : task_promise<T, void>, Alloc {
private:
    friend task<T, Alloc>;
};

template<std::size_t N>
struct ArgNAlloc {
    template<typename... Args>
    void *operator new(std::size_t n, Args... args) {

        return std::malloc(n);
    }

    void operator delete(void *p) noexcept {
        std::free(p);
    }
};

template<typename T, typename Alloc>
struct [[nodiscard]] task : coroutine_base<task_promise<T, Alloc>> {

    using promise_type = task_promise<T, Alloc>;

    friend promise_type;

    using coroutine_base<promise_type>::coroutine_base;

    template<bool is_move>
    struct task_awaiter {
        explicit task_awaiter(task *self)
            : self(self) {}

        task_awaiter(const task_awaiter &) = delete;
        task_awaiter(task_awaiter &&other) noexcept
            : self(std::exchange(other.self, {})) {}

        auto await_ready() const noexcept {
            return !self->valid() || self->done();
        }

        auto await_suspend(coro::coroutine_handle<> awaiting) noexcept {
            self->get_promise().continuation = awaiting;
            return self->coro;
        }

        auto await_resume() -> T {
            if constexpr (is_move) {
                return std::move(self->get_promise()).result();
            } else {
                return self->get_promise().result();
            }
        }

    private:
        task_awaiter() = default;
        task *self = nullptr;
    };

    auto operator co_await() &noexcept { return task_awaiter<false>{this}; }
    auto operator co_await() &&noexcept { return task_awaiter<true>{this}; }
};

}// namespace nova