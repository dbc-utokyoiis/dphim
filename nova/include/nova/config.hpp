#pragma once

#if __has_include(<coroutine>)
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
            if (!coro.done())
                std::cout << "!coro.done()" << std::endl;
            coro.destroy();
        }
    }

    promise_type &get_promise() { return coro.promise(); }
    const promise_type &get_promise() const { return coro.promise(); }

    bool done() const { return coro.done(); }
    bool valid() const { return bool(coro); }

protected:
    explicit coroutine_base(coro::coroutine_handle<promise_type> coro)
        : coro(coro) {}

    coro::coroutine_handle<promise_type> coro;
};

}// namespace nova