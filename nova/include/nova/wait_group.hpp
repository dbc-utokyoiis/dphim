#pragma once

#include <nova/config.hpp>

#include <atomic>

namespace nova {

struct wait_group {

    wait_group(const wait_group &) = delete;
    wait_group(wait_group &&) = delete;
    wait_group &operator=(const wait_group &) = delete;
    wait_group &operator=(wait_group &&) = delete;

    explicit wait_group(coro::coroutine_handle<> continuation = nullptr)
        : continuation(continuation), count(1) {}

    auto is_ready() const noexcept -> bool {
        return static_cast<bool>(continuation) && !continuation.done();
    }

    // return true if c is resumed immediately
    auto try_await(coro::coroutine_handle<> c) noexcept {
        this->continuation = c;
        return done();
    }

    auto add(std::size_t n = 1) noexcept -> void {
        count.fetch_add(n, std::memory_order_release);
    }

    auto done() -> bool {
        auto remain = count.fetch_sub(1, std::memory_order_acquire) - 1;
        if (remain == 0 && is_ready()) {
            continuation.resume();// *this maybe destructed from resume()
            return true;
        }
        return false;
    }

private:
    coro::coroutine_handle<> continuation;
    std::atomic<size_t> count;
};

}// namespace nova