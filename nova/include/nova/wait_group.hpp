#pragma once

#include <nova/config.hpp>

#include <atomic>

namespace nova {

struct wait_group {

    wait_group(const wait_group &) = delete;
    wait_group(wait_group &&) = delete;
    wait_group &operator=(const wait_group &) = delete;
    wait_group &operator=(wait_group &&) = delete;

    explicit wait_group(coro::coroutine_handle<> continuation = nullptr, int count = 1)
        : continuation(continuation), count(count) {}

    auto is_ready() const noexcept -> bool {
        return continuation && !continuation.done();
    }

    // return true if c is resumed immediately
    auto try_await(coro::coroutine_handle<> c) noexcept {
        this->continuation = c;
        return done();
    }

    auto add(std::size_t n = 1) noexcept -> void {
        count.fetch_add(n, MEM_ORDER_REL);
    }

    auto done() -> bool {
        auto remain = count.fetch_sub(1, MEM_ORDER_ACQ_REL) - 1;
        if (remain == 0 && is_ready()) {
            continuation.resume();// *this maybe destructed from resume()
            return true;
        }
        return false;
    }

private:
    coro::coroutine_handle<> continuation;
    std::atomic<int> count;
};

}// namespace nova