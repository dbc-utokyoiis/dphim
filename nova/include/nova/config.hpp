#pragma once

#if __has_include(<coroutine> )
#include <coroutine>
namespace nova {
namespace coro = std;
}
#elif __has_include(<experimental/coroutine>)

#include <experimental/coroutine>

namespace nova {
namespace coro = std::experimental::coroutines_v1;
}
#endif

#include <iostream>
#include <utility>

namespace nova {

struct void_t {};
inline constexpr void_t void_v{};

template<typename P>
struct coroutine_base {
    using promise_type = P;

    coroutine_base() = default;

    coroutine_base(const coroutine_base &) = delete;

    coroutine_base(coroutine_base &&other) noexcept
        : coro(std::exchange(other.coro, {})) {}

    coroutine_base &operator=(const coroutine_base &) = delete;

    coroutine_base &operator=(coroutine_base &&other) noexcept {
        coro = std::exchange(other.coro, {});
        return *this;
    }

    ~coroutine_base() {
        if (coro) {
            if (!coro.done()) {
                std::cerr << "WARNING: destruct unfinished coroutine" << std::endl;
                // std::abort();
            }
            coro.destroy();
        }
    }

    promise_type &get_promise() { return coro.promise(); }
    const promise_type &get_promise() const { return coro.promise(); }

    [[nodiscard]] bool done() const { return coro.done(); }
    [[nodiscard]] bool valid() const { return bool(coro); }

protected:
    explicit coroutine_base(coro::coroutine_handle<promise_type> coro)
        : coro(coro) {}

    coro::coroutine_handle<promise_type> coro;
};

#if 1
#define MEM_ORDER_RELAXED std::memory_order_relaxed
#define MEM_ORDER_REL std::memory_order_release
#define MEM_ORDER_ACQ std::memory_order_acquire
#define MEM_ORDER_ACQ_REL std::memory_order_acq_rel
#else
#define MEM_ORDER_RELAXED std::memory_order_seq_cst
#define MEM_ORDER_REL std::memory_order_seq_cst
#define MEM_ORDER_ACQ std::memory_order_seq_cst
#define MEM_ORDER_ACQ_REL std::memory_order_seq_cst
#endif

}// namespace nova