#pragma once

#include <nova/config.hpp>

namespace nova {

namespace detail {

template<typename Awaitable>
concept member_co_await = requires() { std::declval<Awaitable>().operator co_await(); };

template<typename Awaitable>
concept non_member_co_await = requires() { operator co_await(std::declval<Awaitable>()); };

template<typename Awaitable>
auto get_awaiter(Awaitable &&awaitable) -> decltype(auto) {
    if constexpr (member_co_await<Awaitable>) {
        return std::forward<Awaitable>(awaitable).operator co_await();
    } else if constexpr (non_member_co_await<Awaitable>) {
        return operator co_await(std::forward<Awaitable>(awaitable));
    } else {
        return std::forward<Awaitable>(awaitable);
    }
}

template<typename Awaitable>
struct awaitable_traits {
    using awaiter_t = decltype(detail::get_awaiter(std::declval<Awaitable>()));
    using awaiter_result_t = decltype(detail::get_awaiter(std::declval<Awaitable>()).await_resume());
};

}// namespace detail


template<typename Awaitable, typename = void>
struct awaitable_traits {
    inline static constexpr bool is_awaitable = false;
};

template<typename Awaitable>
struct awaitable_traits<Awaitable, std::void_t<typename detail::awaitable_traits<Awaitable>::awaiter_result_t>> {
    inline static constexpr bool is_awaitable = true;
    using awaiter_t = typename detail::awaitable_traits<Awaitable>::awaiter_t;
    using awaiter_result_t = typename detail::awaitable_traits<Awaitable>::awaiter_result_t;
};

struct always_true {
    template<typename Ts>
    constexpr bool operator()(Ts &&...) const {
        return true;
    }
};

}// namespace nova