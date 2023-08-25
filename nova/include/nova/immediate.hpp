#pragma once

#include <nova/config.hpp>

namespace nova {

template<typename R>
struct immediate {

    immediate(const R &v) : value(v) {}
    immediate(R &&v) : value(std::move(v)) {}

    constexpr bool await_ready() const noexcept { return true; }

    constexpr auto await_suspend(coro::coroutine_handle<> h) const noexcept { return h; }

    R await_resume() { return std::forward<R>(value); }

private:
    R value;
};

template<>
struct immediate<void> {
    constexpr bool await_ready() const noexcept { return true; }
    constexpr auto await_suspend(coro::coroutine_handle<> h) const noexcept { return h; }
    void await_resume() {}
};

}// namespace nova